# Opening sockets

Tasks open sockets through their `NetHandle`. There are three calls: `tcp_connect` for an outgoing TCP connection, `tcp_accept` for an incoming one, and `udp_bind` for a UDP socket. Each returns a future that resolves to the socket or to an `Error`. This page shows each call, explains when errors arrive, and covers the socket settings. It assumes you know [the drive loop](drive-loop.md), since nothing opens until turns run.

One rule holds for all three: the socket is opened when you call the function, not when you first await the future. The future only waits for the outcome. If you drop the future before it resolves, the socket is dropped with it, the same as closing it.

## Connecting

`tcp_connect` takes the socket configuration, the remote endpoint and the local endpoint:

```rust,ignore
let remote = IpEndpoint::new(KERNEL_ADDR.into(), port);
let local = IpListenEndpoint { addr: None, port: next_local_port() };
let stream = handle.tcp_connect(socket, remote, local).await.expect("TCP connect");
```

There is no ephemeral port allocator, so the local port must be nonzero; choose it yourself. `addr: None` means any of the Net's addresses. The remote port must also be nonzero and the remote address specified. Breaking either rule gives `Error::Unaddressable`.

The future waits while the handshake is in progress. It resolves to a `TcpStream` once the connection is established, to `Error::Refused` if the peer answered with a reset, or to `Error::Closed` if the attempt ended another way.

## Accepting

There is no accept queue. Each `tcp_accept` call creates one listening socket, and that socket becomes the one connection it accepts. To serve many connections on a port, keep a listener armed at all times: when an accept completes, call `tcp_accept` again for the next connection before you start serving the one you got.

```text
   tcp_accept(port 7000) --- SYN arrives ---> TcpStream #1
                                                  |
                    (first thing the task does)   v
   tcp_accept(port 7000) --- SYN arrives ---> TcpStream #2      serve #1 ...
                                                  |
                                                  v
   tcp_accept(port 7000) --- ...                               serve #2 ...
```

switch-bench's echo server does exactly this. The accept is opened by `arm_accept` and handed to a new task, which waits for it, arms the next one, and only then serves:

```rust,ignore
{{#include ../../../switch-bench/src/serve.rs:arm_accept}}
```

A few rules govern listeners:

- Port 0 is not a valid listening port and gives `Error::Unaddressable`.
- Several listeners may share a port. An arriving SYN goes to a listener bound to the SYN's destination address if there is one, otherwise to a listener on all addresses (`addr: None`). If several qualify, the one in the lowest socket slot takes it.
- A reset that arrives while the handshake is still in progress sends the listener back to listening, and the `tcp_accept` future keeps waiting.
- If the connection already closed before the task polled the future, the future still resolves to a `TcpStream`. Its `max_segment_payload()` is `None`, and any data the peer sent is still there to read.

## Binding UDP

`udp_bind` takes a `UdpConfig`:

```rust,ignore
UdpConfig { port, local_addr: None, rx_queue: UDP_QUEUE, tx_checksum: true, broadcast: false }
```

`port` is the local port. `local_addr` binds the socket to one of the Net's addresses; `None` binds all of them, and such a socket receives what no socket bound to the datagram's destination address takes. `rx_queue` is how many received datagrams the socket holds, counting those your code still holds; [UDP sockets](udp.md#receive-queue-overflow) explains it. `tx_checksum: true` computes the UDP checksum of outgoing datagrams in software; `false` writes 0. [UDP sockets on the Stack](../stack/udp.md) covers every field. A bind can fail with `PortInUse`, `SocketsFull`, `Unaddressable` or `InvalidConfig`.

## When errors arrive

On the local backend every opening error resolves on the first poll of the future, and no socket is left behind. `SocketsFull`, `PortInUse` and `Unaddressable` all come back at once.

On the junction backend the daemon decides whether to accept the socket. Errors the client can see on its own still come on the first poll: `SocketsFull` when the session has no free socket slot, `InvalidConfig`, and `HostLost` when the session is already gone. A rejection by the daemon arrives only after a round trip to it, so the first poll returns `Pending` and a later turn delivers the error.

On the local backend a connection you dropped keeps its slot in the Stack until it has finished closing. A program that closes and reconnects quickly can therefore see `SocketsFull` even though it holds fewer sockets than the limit.

## Socket configuration

TCP sockets take a `tcp::Config`:

```rust,ignore
let socket = tcp::Config {
    rx_capacity: BUFFER,
    tx_capacity: BUFFER,
    mss_cap: Some(config.path_mtu(*server.ip()) - 40),
};
```

`rx_capacity` is the receive window the socket advertises to the peer. It does not limit how much received data your code may hold ([Frames](frames.md) explains what holding costs). `tx_capacity` is how many bytes may be queued and not yet acknowledged; on the local backend it is also the largest `room` you can wait for. `mss_cap` caps the segment size, here at the path MTU from the [network config file](../nic/config.md) minus 40 bytes of IP and TCP headers. Once a connection is open, `TcpTx::max_segment_payload()` gives the segment size it settled on. It stays `Some` after the connection closes or resets, except in the accept case above.

Invalid settings are handled differently by the two backends. On the local backend, an `rx_capacity` above 1 GiB or `mss_cap: Some(0)` makes the Stack panic when the socket is added. On the junction backend `mss_cap: Some(0)`, or a capacity that does not fit in a `u32`, returns `Error::InvalidConfig` on the first poll.

Next: [Frames](frames.md), the buffers you send and receive. [Errors](errors.md) lists every error code.
