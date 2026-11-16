# Introduction

switch is a set of Rust crates for writing network programs that send and receive with low, predictable latency: market-data feeds, order gateways, echo servers, load generators. Your program is written as async Rust tasks that open TCP and UDP sockets, and optionally TLS and WebSocket connections on top of them. Here is the core of the TCP client this book builds in [Your first network program](getting-started/first-connection.md). It connects, splits the connection into a receive half and a send half, puts one line into a frame (a buffer the network can send without copying), sends it, then prints what the server sends back until the server closes the connection:

```rust,ignore
{{#include ../../switch-net/examples/tcp_client_local.rs:core}}
```

What is different is everything underneath that code. This page explains it in plain terms, then shows how the rest of the book is laid out.

## The network stack runs in your program

A normal Linux program never touches the network card (the NIC). It calls `connect` and `send`, and the kernel's TCP/IP stack builds the packets and hands them to the kernel's driver. Every send and receive crosses into the kernel and back, and the kernel decides when your thread runs again.

A switch program takes the NIC away from the kernel. It uses DPDK, a userspace NIC driver framework, to put packets on the NIC and take them off directly from its own memory. The TCP/IP stack runs inside the program too. This book calls it the Stack: one TCP/IP stack over one NIC, built on smoltcp, a TCP/IP stack written in Rust. Bypassing the kernel this way removes the system calls and copies of the normal path, at a price: the program now has to do the work the kernel used to do, and it has to do it all the time.

## You own the loop, the CPU and the clock

Nothing in switch runs by itself. There is no background thread that receives packets and no executor that decides when tasks run. Your program has one loop, the drive loop, and each time through it does two things:

1. Turn the network: take in the packets that arrived, run TCP's timers, send what tasks queued, and wake the tasks whose sockets changed.
2. Poll the switch runtime: run every task that is ready until it waits again.

```text
   drive loop, on one pinned CPU
   +-----------------------------------------------+
   |  read the clock once                          |
   |  turn the network   (receive, timers, send)   |
   |  poll the runtime   (run woken tasks)         |
   |  start again at once, or sleep briefly        |
   +-----------------------------------------------+
```

One thread drives both the network and the tasks, and it does so on one CPU that it is pinned to: the thread that drives the network is allowed to run on that CPU only. Between two trips through the loop nothing happens at all. A task that called `try_send` has only queued its data; the packet leaves on the next turn. A task waiting on `recv` runs again only after a turn has found data for it and a poll has run it.

The program also supplies time. The runtime and the Stack have no clock of their own. The drive loop reads one clock once each time through and hands the same reading to both. A loop that wants the lowest latency starts again at once without sleeping, which keeps its CPU busy all the time.

If you have used tokio, this is the main thing to unlearn: here your code calls the runtime, and the runtime never calls back on its own.

## Two ways to get a NIC

DPDK gives a NIC to exactly one process. switch offers two ways to work with that, and calls each one a backend: the way a program's sockets reach the NIC.

With the local backend, your program owns the NIC. It opens the NIC from a small JSON file describing it, the Stack runs inside your process on your pinned CPU, and packets go between the NIC and your tasks with no other process involved.

With the junction backend, a daemon owns the NIC. `switch-junctiond` runs the Stack on its own pinned CPU and serves sockets to any number of application processes on the same machine. Each application connects to it with a session and exchanges data with it through shared memory, memory that both processes map.

```text
   local backend                         junction backend

   your process                          app A        app B
   +-----------------+                   +--------+   +--------+
   | your tasks      |                   | tasks  |   | tasks  |
   | switch-net      |                   | switch-|   | switch-|
   | the Stack       |                   |  net   |   |  net   |
   | DPDK            |                   +---+----+   +---+----+
   +--------+--------+                       |  shared   |
            |                                |  memory   |
           NIC                           +---+-----------+---+
                                         | switch-junctiond  |
                                         | the Stack, DPDK   |
                                         +---------+---------+
                                                   |
                                                  NIC
```

The socket code is the same on both. Tasks use the same `NetHandle`, the same streams and frames and the same error codes; only the few lines that set up and tear down the program change. A few behaviors differ, mostly in where outgoing data waits, and [Local and junction backends compared](net/backends.md) lists them.

## The pieces

The crates stack up like this, from your tasks down to the wire:

```text
   your tasks          async fns marked #[switch::task]
   switch-ws           WebSocket messages (ws://, wss://)
   switch-tls          TLS 1.3 client
   switch-net          async TCP and UDP sockets: Net, NetHandle, frames
   switch (runtime)    runs the tasks when the drive loop polls it
   ---------------------------------------------------------------
   the Stack           TCP/IP in userspace (dpdk-smoltcp), in your process
     or a session      to switch-junctiond, which runs the Stack for you
   DPDK                userspace NIC driver
   NIC                 a real card, or a tap device for trying things out
```

A tap device is a virtual NIC that DPDK creates and that Linux sees as an ordinary network interface, so programs on the Linux side can be the other end of the wire. It is how this book's examples run on a laptop with no spare network card. [How the pieces fit together](overview/architecture.md) draws the same layers as processes, threads and CPUs.

## Where it runs

The runtime and the TLS and WebSocket code (without their switch-net integration) build on macOS and Linux with a Rust toolchain. Everything that touches the NIC uses DPDK, and DPDK builds only on Linux. On macOS you use the repository's Docker image, which runs Linux with DPDK installed; [Setting up a build environment](getting-started/environment.md) lists which crates build where and how to start the container.

## How to read this book

The book is written to be read front to back by someone who has not used switch before. Each page ends by pointing to the next one.

If you are writing an application, read Getting started first: [Your first task](getting-started/first-task.md) (the runtime alone, on any machine), [Setting up a build environment](getting-started/environment.md) and [Your first network program](getting-started/first-connection.md) (a TCP client over a tap device). Then read [The switch runtime](runtime/index.md) and [Sockets with switch-net](net/index.md) in order, and [TLS and WebSocket](protocols/index.md) if your program speaks `wss://` or `ws://`. To deploy, choose a backend: [Owning a NIC](nic/index.md) for the local backend, or [Sharing a NIC with the junction daemon](junction/index.md) for the junction backend.

If you run machines, read this page and [How the pieces fit together](overview/architecture.md), then [Owning a NIC](nic/index.md) (preparing a Linux machine or an EC2 instance, the network config file, the `switch-netconf` and `switch-nic` tools), [Running switch-junctiond](junction/running.md), and [Measuring latency under load](measuring/index.md).

If you work on switch itself, start with [Running the test suite](testing/suite.md) and [Writing tests that use DPDK](testing/writing-tests.md). [Programming the Stack directly](stack/index.md) and [Under the hood](blocks/index.md) describe the layers beneath switch-net. The [crate map](reference/crates.md) lists every package in the repository, and the [glossary](reference/glossary.md) defines the words this book uses.

Next: [How the pieces fit together](overview/architecture.md).
