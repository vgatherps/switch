# Sockets with switch-net

switch-net gives switch tasks async TCP and UDP sockets. A task connects, accepts, binds, sends and receives with `.await`, much as it would with any async socket library. What is different is who moves the packets: your own drive loop moves them, on the same thread as the tasks. [Your first network program](../getting-started/first-connection.md) showed the whole shape once; this chapter goes through each part.

## Net and NetHandle

switch-net splits the work between two types.

The `Net` belongs to the drive loop. It owns the connection to the NIC and is the only thing that turns the network: `Net::turn` takes in packets, runs timers, sends what tasks queued and wakes tasks. Each drive loop owns its `Net`; it is not `Clone`.

A `NetHandle` belongs to tasks. You get one from `net.handle()` and clone it freely. With a handle a task opens sockets (`tcp_connect`, `tcp_accept`, `udp_bind`) and gets empty frames to send (`tx_frame`). A handle cannot turn the network.

```text
  drive loop (pinned CPU)                      tasks (same thread)
  +-------------------------+                +--------------------------+
  | Net                     |   handle()     | NetHandle (Clone)        |
  |   turn(now)             | -------------> |   tcp_connect / accept   |
  |   poll_at()             |                |   udp_bind, tx_frame     |
  |   into_stack/_session   |                | TcpStream, UdpSocket     |
  +-----------+-------------+                +-------------+------------+
              |                                            |
              +---------------- one shared state ----------+
                                  |
               +------------------+------------------+
               |                                     |
         local backend                         junction backend
   the Stack in your process            a Session to switch-junctiond
   DPDK  ->  NIC                        shared memory -> daemon -> Stack -> NIC
```

Everything in switch-net is single-threaded. No type is `Send`, and every future switch-net returns must be awaited inside a switch task, because a waiting socket remembers which task to wake. Tasks may freely hold sockets, frames and handles across `.await`.

## Two backends, one API

A **backend** is how a `Net` reaches the NIC. There are two, and they share the same socket, frame and error types, so task code does not change between them.

With the **local backend**, your process owns the NIC. The Stack, the TCP/IP stack built over one DPDK NIC port, lives inside the `Net`. You create it with `Net::new(stack, NetConfig { ingress_budget })`, where the Stack usually comes from `switch_open::open` ([Opening the NIC from code](../nic/open.md)).

With the **junction backend**, the NIC belongs to the junction daemon, `switch-junctiond`, and your process is one of possibly many applications using it. The `Net` holds a session with the daemon, and packets travel through shared memory. You create it with `Net::remote(session, RemoteConfig { tx_frames })`; [Connecting an application through switch-net](../junction/apps.md) shows how.

The switch-bench tool picks the backend from its command line and builds the `Net` either way; the rest of the program is the same:

```rust,ignore
// Local backend
let net = Net::new(opened.stack, NetConfig { ingress_budget: 32 });

// Junction backend
// 8 socket slots for this session; 64 client-side frames to send from
let session = Session::connect(Path::new("/run/switch-junctiond/control.sock"), 8)?;
let net = Net::remote(session, RemoteConfig { tx_frames: 64 });
```

The backends do differ in a handful of behaviors: when errors arrive, what a successful `room` promises, how frames are held. Each page mentions the difference where it matters, and [Local and junction backends compared](backends.md) collects them in one table.

## What this chapter covers

The pages follow the order a program meets things:

1. [The drive loop](drive-loop.md): turns, rounds, the clock and busy polling.
2. [Opening sockets](opening.md): connecting, accepting, binding, and when errors appear.
3. [Frames](frames.md): the buffers you send and receive.
4. [TCP streams](tcp.md) and [UDP sockets](udp.md).
5. [Timeouts and racing futures](timeouts.md): `within` and `first`.
6. [Errors](errors.md): every error code and what to do about it.
7. [Local and junction backends compared](backends.md).
8. [Shutting down cleanly](shutdown.md).

The chapter builds on [The switch runtime](../runtime/index.md), which ended with [Panics, errors and shutdown](../runtime/errors.md). Start with [The drive loop](drive-loop.md).
