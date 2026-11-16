# Sharing a NIC with the junction daemon

In [Owning a NIC](../nic/index.md) one program opens the NIC and runs the Stack inside its own process. That is the simplest setup, and it has one consequence: DPDK hands a NIC port to exactly one process. While your program runs, nothing else on the machine can use that NIC. The daemon in this part opens its NIC the same way, so the machine preparation from that part and the error table in [When the NIC won't open](../nic/troubleshooting.md) apply to it too.

Often you want several programs on one NIC: a market-data feed, an order gateway and a monitoring tool, each started and stopped on its own schedule. The junction daemon, `switch-junctiond`, is how you do that. This part of the book explains how to run it and how applications use it.

## Why a daemon

The daemon is one long-running process that owns the NIC. It opens the NIC port from the same [network config file](../nic/config.md) an application would use, runs [the Stack](../nic/index.md) on its own [pinned CPU](../nic/cpus.md), and serves TCP and UDP sockets to any number of application processes on the same machine.

An application that talks to the daemon has no Stack of its own. It asks the daemon to open sockets, hands it bytes to send, and reads the packets the daemon received for it. All of the IP address, ARP, TCP state and timers live in the daemon. If the daemon stops, every application loses its sockets.

Each application's connection to the daemon is called a **session**. A session is created with `Session::connect` from the `switch-junction` crate and lasts until the application ends it or either process goes away.

## What an application does

Almost every application uses the daemon through [switch-net](../net/index.md), with the same tasks and socket calls as a program that owns its NIC. Only setup and teardown differ:

```text
 local backend                         junction backend
 -------------                         ----------------
 switch_open::open(config, ...)        Session::connect(".../control.sock", max_sockets)
 Net::new(stack, NetConfig { .. })     Net::remote(session, RemoteConfig { tx_frames })
           ... same tasks, same drive loop ...
 net.into_stack(), close the port      net.into_session(), finish the session
```

switch-net calls this the junction backend; [Local and junction backends compared](../net/backends.md) lists every behavior that differs between the two.

An application that uses the daemon through switch-net still builds against DPDK, because switch-net depends on the Stack's crate; `switch-junction` on its own does not. See [Setting up a build environment](../getting-started/environment.md).

## The picture

```text
 +---------------+  +---------------+  +---------------+
 | application A |  | application B |  | application C |
 +-------+-------+  +-------+-------+  +-------+-------+
         |                  |                  |
         +------------------+------------------+
                            |   one session each
                 +----------+-----------+
                 |   switch-junctiond   |
                 +----------+-----------+
                            |
                           NIC
```

Each session uses three channels. The **control socket** is a Unix socket, `control.sock` in the daemon's directory; rare messages such as "open a TCP connection to 10.0.0.5:443" or "socket 3 is now connected" travel over it. The **send ring** carries what applications send: shared memory, one ring per CPU, written by every session sending from that CPU and read by the daemon. The **receive ring** belongs to one session: the daemon writes each received TCP chunk or UDP datagram into it as one **record**, and the application reads records out as [frames](../net/frames.md) without copying. Shared memory is memory both processes map, so sending and receiving involve no system calls and no kernel copies.

Control messages and data take different paths, so they are not ordered against each other. Most applications never notice this, because switch-net handles it. Programs that use a `Session` directly need to know the rules, which [Using a Session directly](session.md) covers. The detailed picture of the processes, threads and rings is in [Under the hood](../blocks/index.md).

## This part of the book

The pages follow the order in which you need them:

1. [Running switch-junctiond](running.md): start, check and stop the daemon, its flags, CPUs and directory.
2. [Connecting an application through switch-net](apps.md): the first network program again, this time through the daemon.
3. [What applications share](sharing.md): what one application's behavior can do to another, and what a successful send means.
4. [Using a Session directly](session.md): the raw `Session` API for programs that do not use switch-net.
5. [Closing sockets and ending a session](closing.md): every way a socket and a session end, and what the peers see.

The shared memory and rings underneath are described in [Under the hood](../blocks/index.md). You do not need them to use the daemon. The wire format of the control socket is in the reference chapter, [The junction control protocol](../reference/junction-protocol.md), for anyone writing another client.

Next: [Running switch-junctiond](running.md).
