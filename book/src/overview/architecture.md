# How the pieces fit together

The [Introduction](../introduction.md) described switch in layers: your tasks on top, the NIC at the bottom, and a drive loop that moves everything. This page turns that picture into something you can check a running program against. It names the crate behind each layer, shows which thread runs on which CPU for each backend, and follows one received packet and one sent frame through each. Every term here is explained again, in more depth, in the chapter the tables point to.

## Layers and crates

An application uses the top four rows directly. The rows below them are what switch-net drives on its behalf.

| Layer | Crate | What it does | Chapter |
|---|---|---|---|
| WebSocket | `switch-ws` | WebSocket client (`ws://`, `wss://`) and `ws://` server over a TCP stream | [TLS and WebSocket](../protocols/index.md) |
| TLS | `switch-tls` | TLS 1.3 client over a TCP stream | [TLS without WebSocket](../protocols/tls.md) |
| Sockets | `switch-net` | Async TCP and UDP sockets for tasks; `Net` for the drive loop, `NetHandle` for tasks | [Sockets with switch-net](../net/index.md) |
| Runtime | `switch` | Single-threaded async runtime that runs tasks when the drive loop polls it | [The switch runtime](../runtime/index.md) |
| Opening a NIC | `switch-open`, `switch-netconf` | Turn a network config file into a running Stack; write, check and apply that file | [Owning a NIC](../nic/index.md) |
| The Stack | `dpdk-smoltcp` | TCP/IP over one NIC port on one thread | [Programming the Stack directly](../stack/index.md) |
| Junction | `switch-junction`, `switch-junctiond` | Client session and the daemon that shares one NIC between processes | [Sharing a NIC](../junction/index.md) |
| Shared memory | `switch-frame`, `shared-memory`, `slice_ringbuffer`, `ceph-*` | Frames, shared-memory files and the rings the junction backend uses | [Under the hood](../blocks/index.md) |
| DPDK | `switch-buffer`, `switch-dpdk-sys` | Packet buffers and pools; bindings to DPDK 25.11.3 | [Packet-buffer pools](../nic/pools.md) |

The [crate map](../reference/crates.md) lists every package in the repository, including the tools, benchmarks and test support.

## The local backend: one process, one pinned CPU

With the local backend, the whole network path lives in your process. `switch_open::open` starts DPDK, opens the NIC port and builds the Stack; `Net::new` wraps the Stack, and your drive loop turns it.

```text
 one Linux machine
 +-----------------------------------------------------------------------+
 |  housekeeping CPUs                        pinned CPU                  |
 |  +-----------------------------+          +-------------------------+ |
 |  | OS, interrupts, SSH         |          | your drive loop thread: | |
 |  | DPDK helper threads         |          |   Net::turn             | |
 |  |   (started by open, kept    |          |     the Stack           | |
 |  |    off the pinned CPU)      |          |     DPDK NIC driver     | |
 |  +-----------------------------+          |   Runtime::poll         | |
 |                                           |     your tasks          | |
 |  management NIC (Linux)                   +-----------+-------------+ |
 +-----------------------------------------------------------+-----------+
                                                             |
                                                         data NIC
```

Your drive loop, the Stack, the NIC driver and every task run on one thread, on the pinned CPU. When DPDK starts it creates a few helper threads for interrupts and control work; `open` places them on other CPUs, the housekeeping CPUs, so they never interrupt the drive loop. [The pinned CPU and helper threads](../nic/cpus.md) explains how. The data NIC belongs to your process; the machine keeps a second NIC, the management NIC, for SSH and DNS.

## The junction backend: a daemon and its applications

With the junction backend, `switch-junctiond` owns the NIC and runs the Stack, and each application holds a session with it. A session uses two channels: a Unix socket for rare control messages (open a socket, a socket connected, close), and shared memory for packet data.

```text
 application process                                switch-junctiond
 +-----------------------------------+            +-----------------------------------+
 | pinned CPU (app):                 |            | pinned CPU (daemon):              |
 |   drive loop thread               |            |   network thread                  |
 |     Net::turn  -> Session         |  send ring |     reads the send rings,         |
 |       try_send copies bytes ------+-(one per --+---> applies sends to the Stack    |
 |                                   |  CPU, all  |     Stack, DPDK NIC driver ---+---+--> NIC
 |       recv gets frames <----------+- sessions) |     writes received data  <---+   |
 |                                   |            |       into receive rings          |
 |     Runtime::poll -> your tasks   | receive    |                                   |
 |                                   |<-ring------+                                   |
 |   junction-ctl thread             | (one per   | housekeeping CPUs:                |
 |     reads control messages <------+--session)  |   junction-control thread         |
 |                                   |            |     sessions, opens, events       |
 |                                   | control    |   DPDK helper threads             |
 |                                   |<-socket--->|                                   |
 +-----------------------------------+            +-----------------------------------+
```

The daemon's network thread runs on its own pinned CPU and does what your drive loop does in the local backend: it busy-polls the NIC and the Stack. Its control thread, `junction-control`, and DPDK's helper threads run on the housekeeping CPUs given by `--control-cpus`. [Running switch-junctiond](../junction/running.md) shows a typical layout.

The application runs no Stack and no DPDK. Its drive loop turns a `Net` built with `Net::remote(session, ..)`, and `Session::connect` starts one extra thread, `junction-ctl`, which reads control messages from the daemon and otherwise sleeps. That thread starts with the affinity of the thread that connected, so pin the process before connecting and it shares the application's pinned CPU. [Connecting an application through switch-net](../junction/apps.md) covers the setup.

In both backends your tasks use the same `NetHandle` and the same socket types. Only the `Net` underneath differs: with the local backend it holds the Stack, and with the junction backend it holds a session and exchanges records with the daemon over shared memory.

## Following the data

The two backends differ most in where bytes are copied and where outgoing data waits.

```text
 local backend                               junction backend

 received                                    received
   NIC --> RX pool packet buffer               NIC --> daemon's Stack
   the Stack reassembles TCP / UDP             daemon copies the payload into
   recv() returns a frame pointing             the session's receive ring
   at that packet buffer (no copy)             recv() returns a frame pointing
                                               into the ring (no copy)

 sent                                        sent
   tx_frame() takes a frame from               tx_frame() takes a frame from the
   the Stack's frame pool                      Net's own pool (RemoteConfig.tx_frames)
   try_send() queues it on the socket          try_send() copies the bytes into the
   the Stack transmits it in place;            send ring; the frame returns at once
   TCP holds it until the peer ACKs            daemon copies them into its Stack,
                                               which transmits them
```

With the local backend a received TCP chunk or UDP datagram reaches your task as a frame over the packet buffer the NIC wrote into, and a frame you send goes to the NIC without a copy. Two cases copy: a TCP frame longer than the connection's segment size ([TCP streams](../net/tcp.md)) and a UDP send of a sliced frame ([UDP sockets](../net/udp.md)). A TCP frame stays with its socket until the peer acknowledges it, so backpressure comes from the socket's send buffer and the shared frame pool. [Frames](../net/frames.md) and [Packet-buffer pools](../nic/pools.md) explain both.

With the junction backend the daemon copies each received chunk or datagram once, into the session's receive ring, and your task reads it there in place. Its space in the ring comes back once the frame is dropped and the next turn reclaims it. A send is copied into the send ring, which is shared by all sessions, and then once more into the daemon's Stack. `Ok` from a send means the bytes are in the send ring; `Full` means that ring has no room right now. [What applications share](../junction/sharing.md) explains what that means for pacing and for other applications.

## Where to go next

The [Getting started](../getting-started/environment.md) pages build a working program on the local backend, starting with a build environment. After that the book follows the layers from the top: [the runtime](../runtime/index.md), [switch-net](../net/index.md), [TLS and WebSocket](../protocols/index.md), then the two ways to deploy, [Owning a NIC](../nic/index.md) and [Sharing a NIC](../junction/index.md). The remaining parts go below switch-net: [the Stack without async](../stack/index.md), [frames, shared memory and rings](../blocks/index.md), [measuring latency](../measuring/index.md) and [testing](../testing/suite.md).

Next: [Setting up a build environment](../getting-started/environment.md).
