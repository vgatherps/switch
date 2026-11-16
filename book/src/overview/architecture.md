# How the pieces fit together

The [Introduction](../introduction.md) described switch in layers: your tasks on top, the NIC at the bottom, and a drive loop that moves everything. This page turns that picture into something you can check a running program against. It names the crate behind each layer, shows which thread runs on which CPU for each backend, and follows one received packet and one sent frame through each. Every term here is explained again, in more depth, in the chapter the tables point to.

## Layers and crates

Tasks use the top four rows. The program's setup code also opens the NIC with `switch-open` and `switch-netconf` (local backend) or connects a session (junction backend); the rows below are what switch-net and `open` drive for it.

| Layer | Crate | What it does | Chapter |
|---|---|---|---|
| WebSocket | `switch-ws` | WebSocket client (`ws://`, `wss://`) and `ws://` server over a TCP stream | [TLS and WebSocket](../protocols/index.md) |
| TLS | `switch-tls` | TLS 1.3 client over a TCP stream | [TLS without WebSocket](../protocols/tls.md) |
| Sockets | `switch-net` | Async TCP and UDP sockets for tasks: a `Net` that the drive loop turns, and `NetHandle`s that tasks use to open sockets | [Sockets with switch-net](../net/index.md) |
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
 |  | DPDK helper threads (here   |          |   Net::turn             | |
 |  |   when the pinned CPU is    |          |     the Stack           | |
 |  |   isolated or helper CPUs   |          |     DPDK NIC driver     | |
 |  |   are given)                |          |                         | |
 |  +-----------------------------+          |   Runtime::poll         | |
 |                                           |     your tasks          | |
 |  management NIC (Linux)                   +-----------+-------------+ |
 +-----------------------------------------------------------+-----------+
                                                             |
                                                         data NIC
```

Your drive loop, the Stack, the NIC driver and every task run on one thread, on the pinned CPU. When DPDK starts it creates a few helper threads for interrupts and control work. `open` puts them on the housekeeping CPUs when the pinned CPU is isolated from the kernel scheduler or when you name helper CPUs; otherwise, as in the one-CPU example container, they share the pinned CPU. [The pinned CPU and helper threads](../nic/cpus.md) explains the rules. The data NIC belongs to your process; the machine keeps a second NIC, the management NIC, for SSH and DNS.

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

The two backends differ most in where bytes are copied and where outgoing data waits. Both hand data to your tasks as frames: a frame is a handle to one buffer of packet bytes that can be passed along without copying them.

```text
 local backend                               junction backend

 received                                    received
   NIC --> packet buffer in your process       NIC --> the daemon's Stack
   the Stack sorts it to its socket            daemon copies the payload into
   your task gets a frame over that            your session's receive ring
   packet buffer (no copy)                     your task gets a frame over the
                                               ring (no copy)

 sent                                        sent
   your task fills a frame from the            your task fills a frame from its
   Stack's frame pool                          own small pool
   the socket keeps it; the Stack sends        the bytes are copied into the send
   it; TCP holds it until the peer ACKs        ring of your thread's CPU and the
                                               frame comes back at once; the
                                               daemon hands them to its Stack
```

With the local backend, a received TCP chunk or UDP datagram reaches your task as a frame over the packet buffer the NIC wrote into. A frame you send goes to the NIC without a copy when only you hold it, it starts at its original start and it fits in one TCP segment or one datagram; other sends are copied. A sent TCP frame stays with its socket until the peer acknowledges it, so backpressure comes from the socket's send buffer and the frame pool. [Frames](../net/frames.md) has the details.

With the junction backend, the daemon copies each received chunk or datagram into your session's receive ring, and your task reads it there in place; its ring space comes back once the frame is dropped and the next turn reclaims it. A send is copied into the send ring of the CPU your thread runs on. `Ok` means the bytes are in that ring, and `Full` means it has no room right now. TCP bytes the peer cannot take yet wait in the daemon. [Local and junction backends compared](../net/backends.md) and [What applications share](../junction/sharing.md) explain what this means for your program.

## Where to go next

The [Getting started](../getting-started/first-task.md) pages run the runtime on its own, set up a build environment, and then build a working program on the local backend. After that the book follows the layers from the top: [the runtime](../runtime/index.md), [switch-net](../net/index.md), [TLS and WebSocket](../protocols/index.md), then the two ways to deploy, [Owning a NIC](../nic/index.md) and [Sharing a NIC](../junction/index.md). The remaining parts go below switch-net: [the Stack without async](../stack/index.md), [frames, shared memory and rings](../blocks/index.md), [measuring latency](../measuring/index.md) and [testing](../testing/suite.md).

Next: [Your first task](../getting-started/first-task.md).
