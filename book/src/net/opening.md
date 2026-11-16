# Opening sockets

Tasks open sockets through their `NetHandle`. There are three calls: `tcp_connect` for an outgoing TCP connection, `tcp_accept` for an incoming one, and `udp_bind` for a UDP socket. Each returns a future that resolves to the socket or to an `Error`. Handshakes and the daemon's answers travel only when [the drive loop](drive-loop.md) turns the network, so most outcomes arrive in a later turn.

One rule holds for all three: the socket is opened when you call the function, not when you first await the future. The future only waits for the outcome. If you drop the future before it resolves, the socket is dropped with it, the same as closing it.

## Connecting

`tcp_connect` takes the socket configuration, the remote endpoint and the local endpoint:

```rust,ignore
{{#include ../../../switch-net/examples/tcp_client_local.rs:connect}}
```

This is the connect from [the first network program](../getting-started/first-connection.md); the `tcp::Config` is explained under [Socket configuration](#socket-configuration) below.

Choose the local port yourself; it must be nonzero. `addr: None` means any of the Net's addresses. The remote port must also be nonzero and the remote address specified. Breaking either rule gives `Error::Unaddressable`.

The future waits while the handshake is in progress. It resolves to a `TcpStream` once the connection is established, to `Error::Refused` if the peer answered with a reset, or to `Error::Closed` if the attempt ended another way.

## Accepting

Each `tcp_accept` call creates one listening socket, which becomes one connection. To serve many connections on a port, keep a listener armed at all times: when an accept completes, call `tcp_accept` again for the next connection before you start serving the one you got.

```text
   tcp_accept(port 7000) --- SYN arrives ---> TcpStream #1
                                                  |
                    (first thing the task does)   v
   tcp_accept(port 7000) --- SYN arrives ---> TcpStream #2      serve #1 ...
                                                  |
                                                  v
   tcp_accept(port 7000) --- ...                               serve #2 ...
```

The echo server example `switch-net/examples/tcp_echo_server.rs` does exactly this. Each time an accept completes, it arms the next accept before serving this connection, which it hands to a new task:

```rust,ignore
{{#include ../../../switch-net/examples/tcp_echo_server.rs:accept_loop}}
```

A few rules govern listeners:

- Port 0 is not a valid listening port and gives `Error::Unaddressable`.
- Several listeners may share a port. An arriving SYN goes to a listener bound to the SYN's destination address if there is one, otherwise to a listener on all addresses (`addr: None`). If several qualify, the one in the lowest socket slot takes it. Each socket takes one slot in the `Net`'s socket table, which has room for `max_sockets` TCP and `max_udp_sockets` UDP sockets on the local backend (from `Sizing`), or for `max_sockets` sockets of either kind on the junction backend (from `Session::connect`).
- A reset that arrives while the handshake is still in progress sends the listener back to listening, and the `tcp_accept` future keeps waiting.
- If the connection already closed before the task polled the future, the future still resolves to a `TcpStream`. Its `max_segment_payload()` is `None`, and any data the peer sent is still there to read.

## Binding UDP

`udp_bind` takes a `UdpConfig`:

```rust,ignore
UdpConfig { port, local_addr: None, rx_queue: UDP_QUEUE, tx_checksum: true, broadcast: false }
```

`port` is the local port. `local_addr` binds the socket to one of the Net's addresses; `None` binds all of them, and such a socket receives what no socket bound to the datagram's destination address takes. `rx_queue` is how many received datagrams the socket holds; on the local backend that counts those your code still holds ([UDP sockets](udp.md#receive-queue-overflow) explains it). `tx_checksum: true` computes the UDP checksum of outgoing datagrams in software; `false` writes 0. With the NIC's UDP TX checksum offload on, the NIC computes the checksum either way. [UDP sockets on the Stack](../stack/udp.md) covers every field. A bind can fail with `PortInUse`, `SocketsFull`, `Unaddressable` or `InvalidConfig`.

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

Invalid settings are handled differently by the two backends. On the local backend, an `rx_capacity` above 1 GiB or `mss_cap: Some(0)` makes the Stack panic when the socket is added. On the junction backend, `mss_cap: Some(0)`, an `rx_capacity` above 1 GiB, a `tx_capacity` of 0, a capacity that does not fit in a `u32`, or for UDP a port of 0 or an `rx_queue` of 0 returns `Error::InvalidConfig` on the first poll.

Next: [Frames](frames.md), the buffers you send and receive. [Errors](errors.md) lists every error code.
