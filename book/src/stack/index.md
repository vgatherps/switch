# Programming the Stack directly

Everything in the switch-net chapter runs on top of one type: `Stack`, from the `dpdk-smoltcp` crate. The Stack is a TCP/IP stack for one NIC port. It is built on smoltcp (a small TCP/IP stack written in Rust, vendored in this repository with changes) and talks to the NIC through DPDK. switch-net wraps it in async sockets, and the junction daemon, whose part of the book ended on the [previous page](../junction/closing.md), runs one for many applications.

You can also call the Stack yourself, with no async runtime at all. This part of the book shows how. It is for programs that want a plain synchronous loop, for library and daemon authors building their own layer on top, and for anyone who wants to know what switch-net does underneath.

## What the Stack is like to program

Three properties shape every program that uses the Stack directly.

It runs on one thread. The thread that starts DPDK owns the `Eal`, the `Port`, the `Stack`, every socket handle, and every frame and datagram the Stack hands out. All of them must be used and dropped on that thread. `Eal`, `TcpHandle` and `UdpHandle` are not `Send`, so the compiler enforces most of this.

It reads no clock. Each call that does network work takes the current time as an argument, in microseconds, and the time must never go backwards. You choose the clock; the [drive loop page](drive-loop.md) shows the pattern.

It never waits. No call blocks or sleeps. A call does the work that is possible now and returns. Your loop calls it again, either at once (busy polling on a pinned CPU) or after a short sleep.

Sockets are named by handles. `add_tcp` returns a `TcpHandle`, `add_udp` a `UdpHandle`. You pass `&handle` to Stack methods to reach the socket. A handle belongs to the Stack that made it: using it on another Stack panics with "TCP handle belongs to another Stack" (or the UDP equivalent). Dropping a handle closes its socket, as [TCP sockets on the Stack](tcp.md) describes.

## Lifecycle

A program that uses the Stack goes through the same steps every time:

```text
 only_allowed_cpu()            this thread may run on exactly one CPU
        │
 Eal::init(args) ──▶ Eal       DPDK starts once per process; keep it alive
        │
 find or create the device     port_by_name, or a test device
        │
 Port::open(&eal, config) ──▶ Port     queues and the three pools
        │
 Stack::new(port, config, now) ──▶ Stack
        │
 add_tcp / add_udp ──▶ handles
        │
 ┌──▶ turn(now) / receive(now) … send()     the drive loop
 └──────┘
        │
 drop frames you hold, then
 stack.into_port() ──▶ Port
        │
 port.try_close()              Ok: every packet buffer is back
        │
 Eal is never torn down; it lives until the process exits
```

`switch_open::open` does the first five steps from a network config file and returns the `Stack` and the `Eal` together (see [opening the NIC from code](../nic/open.md)). The [by-hand page](by-hand.md) does them one by one.

The end of the lifecycle matters as much as the start. `into_port` drops the sockets and every packet still queued inside the Stack, and gives back the `Port`. `try_close` then closes the NIC port, but only if every packet buffer has come back to its pool. A received frame your code still holds keeps its buffer, even after the Stack is gone, so drop those first. If something is still held, `try_close` returns the `Port` together with `PortError::OutstandingBuffers { rx, tx, frame }`, counting what is missing from each pool.

## A complete client

The `switch-open` crate has a small program, `examples/tcp_client.rs`, that uses the Stack with no runtime. It connects to a TCP server, sends one line, prints what comes back, and closes when the server closes. The rest of this page walks through it.

First it decides how big its socket is and sizes the pools for one socket (pool sizing is explained in [packet-buffer pools](../nic/pools.md)):

```rust,ignore
{{#include ../../../conduit/crates/switch-open/examples/tcp_client.rs:sizing}}
```

Then it makes the one clock every drive call will use, and opens the NIC:

```rust,ignore
{{#include ../../../conduit/crates/switch-open/examples/tcp_client.rs:clock}}
```

```rust,ignore
{{#include ../../../conduit/crates/switch-open/examples/tcp_client.rs:open}}
```

The clock starts at zero when the program starts and counts microseconds from a `std::time::Instant`. `open` calls it for the Stack's start time (and again while it waits for the gateway's ARP reply), and the loop calls it on every iteration, so the Stack only ever sees time move forward. `_eal` is kept in a variable for the rest of `main` because the `Eal` must outlive every packet buffer.

Next it adds one TCP socket and starts connecting:

```rust,ignore
{{#include ../../../conduit/crates/switch-open/examples/tcp_client.rs:connect}}
```

`connect` takes the server's address and a local port. There is no automatic port choice, so the local port must be nonzero. `connect` only queues the SYN; nothing is sent until the loop runs.

The loop is the heart of the program:

```rust,ignore
{{#include ../../../conduit/crates/switch-open/examples/tcp_client.rs:loop}}
```

Each iteration calls `receive` with the current time. That takes arriving packets from the NIC and processes them. Then the program reads and writes its socket: it prints every received frame and drops it, sends its line once the connection may send, and closes its side once the server has closed (`State::CloseWait`). Finally `send` builds the outgoing segments, including what the program just wrote, and hands them to the NIC. The loop never sleeps; it is meant to run alone on its pinned CPU.

When the connection reaches `Closed`, the program checks how it ended and closes the port:

```rust,ignore
{{#include ../../../conduit/crates/switch-open/examples/tcp_client.rs:close}}
```

Every received frame was dropped inside the loop, so `try_close` finds all packet buffers back in their pools.

To run it, see the instructions at the top of the example file: it needs a network config file, a server, and `taskset -c N` so the process may run on exactly one CPU.

## The rest of this part

[Starting DPDK and opening a NIC port by hand](by-hand.md) replaces `open` with the individual calls. [The Stack's drive loop](drive-loop.md) explains `turn`, `receive`, `send` and `flush`, time, and sleeping. [TCP sockets on the Stack](tcp.md) and [UDP sockets on the Stack](udp.md) cover the socket calls. [Waiting on many sockets](readiness.md) shows how a loop learns which sockets changed. [Stack errors and panics](errors.md) is the reference table for everything that can go wrong.

Next: [Starting DPDK and opening a NIC port by hand](by-hand.md).
