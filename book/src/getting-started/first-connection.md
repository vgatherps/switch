# Your first network program

In [Your first task](first-task.md) you ran async code on the switch runtime with nothing but a loop and a clock. This page adds a network. We build a small TCP client that opens a NIC, connects to a server, sends one line, prints whatever the server sends back, and shuts everything down cleanly once the server hangs up.

You do not need a spare network card for this. The program talks to a **tap device**: a virtual DPDK device that shows up in Linux as an ordinary network interface. Your program sits on one side of it with its own TCP/IP stack, and the Linux kernel sits on the other side. A three-line Python server on the kernel side plays the remote server.

```text
   your process (pinned to one CPU)                 Linux kernel
 +-------------------------------------+       +----------------------+
 |  task: connect, send, print reply   |       |  python3 server      |
 |  switch-net  (Net, NetHandle)       |       |  192.0.2.1:7000      |
 |  the Stack   (TCP/IP in userspace)  |       |                      |
 |  DPDK tap driver  192.0.2.2         | <---> |  interface cdnic0    |
 +-------------------------------------+       +----------------------+
```

You need the build environment from [Setting up a build environment](environment.md): a Linux machine with DPDK, or a container from the Docker image started as shown in [Running a container for the examples](environment.md#running-a-container-for-the-examples), which allows the container on CPU 3 only. The whole program is `switch-net/examples/tcp_client_local.rs`; this page walks through it in the order it runs.

## The network config file

A program that owns a NIC describes it in a **network config file**, a small JSON file. For the tap device it looks like this (`switch-net/examples/tap.json`):

```json
{{#include ../../../switch-net/examples/tap.json}}
```

The parts that matter here: `device` asks DPDK to create a tap device whose Linux interface is called `cdnic0`, and `ipv4` gives your program the address 192.0.2.2. The `eal` section lets DPDK run without hugepages (large memory pages a machine reserves for DPDK's packet memory), which a tap device does not need. [The network config file](../nic/config.md) explains every key, and the same format describes a real NIC.

The program first loads the file and looks up the server's address:

```rust,ignore
{{#include ../../../switch-net/examples/tcp_client_local.rs:config}}
```

Resolve names before you open the NIC. Name lookup goes through the kernel's resolver and blocks, and once your program owns the NIC, its traffic no longer passes through the kernel.

## Opening the NIC

Opening takes a few lines, and each line sets up one piece:

```rust,ignore
{{#include ../../../switch-net/examples/tcp_client_local.rs:setup}}
```

`tcp::Config` is the socket's configuration: receive and send buffer sizes, and an upper bound on segment size taken from the config file's path MTU (the largest IP packet the config allows toward that destination). `Sizing` tells `switch_open::open` how many sockets to make room for and how many packet buffers to set aside for sending. A packet buffer is DPDK's fixed-size memory buffer for one packet; DPDK makes a fixed number of them when the NIC opens. The program has one TCP socket and no UDP sockets, and its packet-buffer share comes from `tcp::tx_mbuf_budget`, which estimates what one socket with this config can hold at once. [Packet-buffer pools](../nic/pools.md) explains the numbers.

A few values in this block deserve a word each:

- The 40 in `path_mtu - 40` is the IPv4 and TCP headers, so `mss_cap` is the largest TCP payload one packet can carry.
- `frame_share`, `rx_staging`, `tx_pending` and the two socket counts size the packet-buffer pools ([Packet-buffer pools](../nic/pools.md)). This program reserves one TCP socket and no UDP sockets.
- The `None` passed to `open` means no helper CPUs: unless the pinned CPU is isolated, DPDK's helper threads share it ([The pinned CPU and helper threads](../nic/cpus.md)).
- The closure passed to `open` is the clock it uses for the Stack's start time and while it waits for the gateway's ARP reply. It reads the same clock the drive loop uses.
- `ingress_budget` in `NetConfig` is the most received packets one turn processes ([The drive loop](../net/drive-loop.md)).

The clock comes next. Everything in this program takes its time from one `std::time::Instant` taken at startup:

```rust,ignore
{{#include ../../../switch-net/examples/tcp_client_local.rs:clock}}
```

The network stack counts time in microseconds; the switch runtime counts ticks, and we use nanoseconds. `sample` reads the clock once and returns both, so the network and the tasks always agree on the time. Use one clock for every call; never feed a second clock into the same program.

`switch_open::open` starts the EAL, DPDK's per-process setup, opens the NIC port (one receive queue and one send queue of the NIC, set up for DPDK), and builds **the Stack**, the TCP/IP stack that runs inside your process. It is `unsafe` because your code promises that this thread alone uses the NIC port and that DPDK's state (`opened.eal`) outlives it. It also checks that the calling thread may run on exactly one CPU and refuses to start otherwise (`taskset` on the process satisfies it). That one CPU is the **pinned CPU**: the program's network work and its tasks all run there. [Opening the NIC from code](../nic/open.md) covers `open` in full.

Then the program hands the Stack to switch-net and creates the runtime:

- `Net::new(stack, NetConfig { .. })` wraps the Stack. The `Net` is what the drive loop holds; it is the only thing that moves packets.
- `net.handle()` gives a `NetHandle`. Tasks hold handles to open sockets and get buffers to send. A handle is cheap to clone.
- `Runtime::new(clock.ns())` creates the switch runtime at the current tick, and the task is spawned on it.

Using a `Net` this way, with the Stack inside your process, is the **local backend**. The other one, the junction backend, appeared in the [Introduction](../introduction.md#two-ways-to-get-a-nic); [Sockets with switch-net](../net/index.md) uses both.

## The task

The task does the actual work, written as ordinary async Rust:

```rust,ignore
{{#include ../../../switch-net/examples/tcp_client_local.rs:task}}
```

`tcp_connect` opens a TCP connection to the server from local port 40000. There is no ephemeral port allocator, so a connection always names a nonzero local port. When the connection is established, `split` divides it into a receive half `rx` and a send half `tx`.

Sending works with **frames**. A frame is a handle to a packet buffer that the Stack can transmit without copying. The task asks the handle for an empty frame, copies the line into it, cuts it to the line's length with `truncate`, and `freeze`s it into an immutable frame that is ready to send. `room(len)` waits until the socket has space for that many bytes, and `try_send` queues the frame. If `try_send` fails it gives the frame back along with the error; here we just keep the error. [Frames](../net/frames.md) and [TCP streams](../net/tcp.md) explain both steps.

Receiving is a loop over `recv`. Each call returns the next piece of data the server sent, as a frame, in order. When the server closes its side, `recv` returns `Err(Error::Finished)`. The task then closes its own side with `shutdown` and waits for `delivered`, which resolves once the server has acknowledged everything we sent, including our end-of-stream, or once the connection is gone and nothing more can be delivered. When `talk` returns, both halves are dropped, which releases the socket.

The task stores its result in a shared cell so the drive loop can see that it is done.

## The drive loop

Nothing in the task touches the network directly. `try_send` only queues; packets leave and arrive when the drive loop says so. The **drive loop** is your own loop that runs on the pinned CPU:

```rust,ignore
{{#include ../../../switch-net/examples/tcp_client_local.rs:drive}}
```

Each time through the loop, it calls `switch_net::round`, which does two things with one clock sample. First a **turn**: the Net takes in received packets, runs TCP's timers, sends what tasks queued, and wakes the tasks whose sockets changed. Then one poll of the runtime, which runs those tasks until they wait again.

```text
        +---------------------------+
        | clock.sample()            |   one reading: µs for the Stack,
        |                           |   ns ticks for the runtime
        +-------------+-------------+
                      v
        +---------------------------+
        | Net::turn(now)            |   receive, timers, send,
        |                           |   wake tasks whose sockets changed
        +-------------+-------------+
                      v
        +---------------------------+
        | Runtime::poll(tick)       |   run woken tasks until they wait
        +-------------+-------------+
                      v
             moved anything?
           yes /          \ no
    loop again          sleep 1 ms, then loop again
```

`round` returns whether anything moved. Here the loop sleeps for a millisecond when nothing did, because in this setup the kernel-side server shares the CPU. A latency-sensitive program calls `round` again at once instead, which is called **busy polling**. [The drive loop](../net/drive-loop.md) covers both, and what a turn does in detail.

## Teardown

When the task is done, the program takes everything apart in order:

```rust,ignore
{{#include ../../../switch-net/examples/tcp_client_local.rs:teardown}}
```

`rt.shutdown()` drops any tasks that are still alive, together with the sockets and handles they hold. `net.into_stack()` gives the Stack back, but only when no `NetHandle`, no socket half and no future holding a socket is left anywhere; otherwise it returns the `Net` as an error. `into_port().try_close()` closes the NIC port and checks that every packet buffer came back. Received frames hold packet buffers, so drop any you kept before `try_close`. [Shutting down cleanly](../net/shutdown.md) explains each failure.

## Run it

Build the example, then start the server, the client, and give the tap interface its kernel-side address once the client has created it:

```text
cargo build -p switch-net --example tcp_client_local

python3 -c 'import socket; s = socket.create_server(("0.0.0.0", 7000)); c, _ = s.accept(); c.recv(100); c.sendall(b"tick\n"); c.close()' &
taskset -c 3 target/debug/examples/tcp_client_local switch-net/examples/tap.json 192.0.2.1:7000 &
until ip link show cdnic0 >/dev/null 2>&1; do sleep 0.1; done
ip addr add 192.0.2.1/24 dev cdnic0 && ip link set cdnic0 up
wait
```

`taskset -c 3` allows the process on exactly one CPU (use the CPU the container was started with, `--cpuset-cpus`). The client first prints the report from `open`, which describes the port it opened, then the server's reply `tick`, then `port closed`. If `open` fails, [When the NIC won't open](../nic/troubleshooting.md) lists each message.

## Where each piece is explained

| In this program | Explained in |
|---|---|
| `tap.json`, the config file | [The network config file](../nic/config.md) |
| `switch_open::open`, `Sizing` | [Opening the NIC from code](../nic/open.md), [Packet-buffer pools](../nic/pools.md) |
| `round`, turns, the clock, busy polling | [The drive loop](../net/drive-loop.md) |
| `tcp_connect`, the local port | [Opening sockets](../net/opening.md) |
| `tx_frame`, `truncate`, `freeze`, received frames | [Frames](../net/frames.md) |
| `room`, `try_send`, `recv`, `shutdown`, `delivered` | [TCP streams](../net/tcp.md) |
| shutdown order | [Shutting down cleanly](../net/shutdown.md) |

The same task also runs unchanged when the NIC belongs to the junction daemon, `switch-junctiond`, a separate process that owns the NIC and shares it among several applications; only setup and teardown differ. [Connecting an application through switch-net](../junction/apps.md) shows that version.

Next, [The switch runtime](../runtime/index.md) looks at the runtime in depth, and [Sockets with switch-net](../net/index.md) starts the network chapter.
