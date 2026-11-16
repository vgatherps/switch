# Using a Session directly

switch-net is the usual way to use the daemon. Underneath it is `Session`, from the `switch-junction` crate, and a program can use it directly: a program with its own loop and no async runtime, a test tool, or a client library for another framework. This page walks through the raw API, starting from the session you already know how to connect (see [Connecting an application through switch-net](apps.md#connecting)).

A `Session` never blocks and never runs anything in the background apart from the `junction-ctl` thread that reads the control socket. Your program calls it in a loop: open sockets, poll for events and packets, send. Like switch-net, it belongs to the thread that connected it.

## Socket ids

Each session has a table of `max_sockets` socket slots, the size it asked for when connecting. Opening a socket takes a free socket slot and returns a `SocketId`:

```rust,ignore
pub struct SocketId {
    pub slot: u32,
    pub generation: u32,
}
```

`slot` is the index in the table. `generation` counts how often that slot has been used, so a socket id stays unique even after its slot is reused. An id from an earlier use of the slot is stale. Passing a stale id to the session's methods panics, and packets that arrive for it are dropped without a callback.

A socket slot moves through these states:

```text
                    connect_tcp / listen_tcp / bind_udp
   free ------------------------------------------------> open
    ^                                                       |
    |  Rejected(code)                                       |  Connected / Bound,
    +-------------------------------------------------------+  then PeerFin, Delivered,
    |                                                       |  Closed as they happen
    |                                                       |
    |                      close / abort                    v
    |  Freed           +-------------------------------- (open)
    +----------- closing <---+
       (next open of this slot gets generation + 1)
```

A slot is free again only after the daemon answers with `Freed` (after a close) or `Rejected` (the open failed). A closing socket still occupies its slot, so `OpenError::NoFreeSlot` means every slot is open or closing.

## Opening sockets and events

There are three ways to open a socket:

```rust,ignore
fn connect_tcp(&mut self, config: TcpConfig, remote: SocketAddrV4, local: ListenEndpoint)
    -> Result<SocketId, OpenError>;
fn listen_tcp(&mut self, config: TcpConfig, local: ListenEndpoint) -> Result<SocketId, OpenError>;
fn bind_udp(&mut self, config: UdpConfig) -> Result<SocketId, OpenError>;
```

Each returns at once. The id is yours to use, but the daemon has not acted yet; it answers later with an **event**, a short control message about one socket. The configuration types are checked before a slot is taken: a TCP `rx_capacity` above 1 GiB, a TCP `tx_capacity` of 0, a UDP port of 0 or a UDP `rx_queue` of 0 gives `OpenError::Config`. Addresses are IPv4 only.

| Event | Meaning |
|---|---|
| `Connected { mss }` | A TCP socket is connected. For `connect_tcp` the handshake finished; for `listen_tcp` a peer connected to it. `mss` is the largest segment payload agreed with the peer. |
| `Bound` | A UDP socket is bound. |
| `Rejected(code)` | The open failed. The socket never existed in the daemon and its slot is free again; no close is needed. |
| `PeerFin` | The peer finished sending. Every byte it sent is already in the receive ring. You may still send. |
| `Delivered` | After your `shutdown`, the peer acknowledged your FIN and all data before it. |
| `Closed { reset_by_peer }` | The connection is fully closed, by FINs in both directions or by a reset, and no unread data remains in the daemon. You must still call `close` to free the slot. |
| `Freed` | The daemon removed the socket after your `close` or `abort`. The slot is free. |

The `RejectCode` says why an open failed: `TcpSocketsFull` or `UdpSocketsFull` (the daemon's global limits; see [What applications share](sharing.md#limits-shared-across-sessions)), `ConnectAddressInUse` or `UdpPortInUse`, `ConnectUnaddressable`, `ListenUnaddressable` or `UdpUnaddressable` (an address or port the Stack cannot use), and `ConnectInvalidState` or `ListenInvalidState`.

A TCP listener is one connection, not an accept queue. `listen_tcp` opens one socket that waits on the port; when a peer connects, that same socket id becomes the connection and gets `Connected`. To serve several connections, open several listeners on the same port and open a new one when one is taken.

Wait for `Connected` (TCP) or `Bound` (UDP) before sending on a socket. Opens travel over the control socket and sends travel through the send ring, and the two are not ordered. A send that reaches the daemon before its open ends the whole session with a protocol error. `Session` does not check this for you.

## Polling

Three methods take what the daemon sent:

```rust,ignore
fn poll(&mut self, on_event: impl FnMut(SocketId, Event),
        on_packet: impl FnMut(SocketId, PacketMeta, OwnedFrame)) -> Result<(), Lost>;
fn poll_events(&mut self, on_event: impl FnMut(SocketId, Event)) -> Result<(), Lost>;
fn poll_packets(&mut self, on_packet: impl FnMut(SocketId, PacketMeta, OwnedFrame));
```

`poll_events` sends any control messages still waiting to be written, then calls `on_event` for each event that has arrived. `poll_packets` calls `on_packet` for each received packet and then reclaims receive-ring space. `poll` does both, events first. None of them waits: if nothing has arrived they return at once.

Polling is also the only way the program learns that the session is lost. `poll` and `poll_events` return `Err(Lost)` when that happens; [Closing sockets and ending a session](closing.md#handling-loss) covers what to do. The send methods do not look for loss, so a program that only sends must still poll regularly.

## A UDP exchange

The smallest complete use of a session binds a UDP socket, receives one datagram from a kernel peer and answers it. This comes from the daemon's own tests, which run the real daemon on a tap device with the kernel at 10.77.0.1:30001 and the daemon at 10.77.0.2:

```rust,ignore
{{#include ../../../switch-junctiond/tests/process.rs:exchange}}
```

It follows the rules above in order. It opens the socket, then polls events until `Bound` arrives. It then polls until the datagram arrives; `&*frame` is the payload, and `meta` says where it came from. Only then does it send. While it waits for the kernel to receive the answer it keeps calling `poll_events`, which flushes control output and notices loss.

The test sleeps 1 ms between polls because it shares its CPU with the kernel peer. A latency-sensitive program polls in a loop on its pinned CPU instead.

## TCP

A TCP client opens with `connect_tcp` and waits for `Connected`. The daemon's tests do it like this, then wait for the `Connected` event for `id`:

```rust,ignore
let id = client
    .connect_tcp(tcp_config(), peer_endpoint(PEER_PORT), local(port))
    .unwrap();
```

The local port must be nonzero. If the peer refuses the connection, the socket gets `Closed { reset_by_peer: true }` without a `Connected` first; call `close(id, false)` and wait for `Freed`.

Once connected, send bytes with `send_tcp(id, payload)`. Each call commits one record to the send ring, so a payload may be at most `max_record() − 16` bytes; larger data goes in several calls. For UDP, `send_udp(id, to, payload)` takes at most `max_record() − 24` bytes. Both return:

| Result | Meaning |
|---|---|
| `Ok(())` | the record is in the send ring (see [What applications share](sharing.md#what-ok-and-full-mean)) |
| `Err(SendError::Full)` | the send ring has no room right now; retry later |
| `Err(SendError::TooLarge)` | header plus payload is larger than `max_record()` |
| `Err(SendError::NotOpen)` | the socket is closing, or it is a TCP socket after `shutdown` |

Sending on a socket of the wrong kind, or with a stale id, panics.

Received TCP data arrives as packets in byte order. The chunks do not match the sender's writes: the daemon writes whatever the Stack received, and splits a chunk larger than the ring can hold into several records.

## Received packets and ring space

Each packet comes with a `PacketMeta` and an `OwnedFrame`. The frame derefs to the payload bytes, and you may also modify it, slice it or turn it into a shared `ImmutableFrame` (see [switch-frame](../blocks/frames.md)). The useful `PacketMeta` fields are:

| Field | Meaning |
|---|---|
| `kind` | `switch_junction::rx::TCP` (1) or `switch_junction::rx::UDP` (2) |
| `src_addr`, `src_port` | the sender, for UDP |
| `dst_addr` | the address the datagram was sent to, for UDP |
| `rx_timestamp` | the NIC's [hardware timestamp](../nic/offloads.md) for the packet; 0 means none |

For TCP the address and port fields are zero; the socket id already says which connection the data belongs to.

The frame points into the session's receive ring. You can keep it as long as you like, but its space returns to the daemon only after you drop it and a later `poll` or `poll_packets` reclaims it. Reclaiming stops at the oldest frame still alive, so one long-held frame blocks the reuse of everything received after it, across all of the session's sockets. [Shared memory and the receive ring](../blocks/rings.md) shows how this works.

## Event and packet ordering

Events and packets reach the session on different paths, so their order needs care:

```text
 control socket:   ... Connected(3) ......... PeerFin(3) ...
 receive ring:     ... data(3) ... data(3) ... data(3) .........

 one poll():  [ every event that has arrived ] then [ every packet that has arrived ]
```

Within one `poll`, all event callbacks run before any packet callback. A socket's first packet can arrive before its `Connected` event, so a program that needs the event first should hold such packets per socket until it arrives. The other direction is guaranteed: by the time you handle `PeerFin`, every byte the peer sent before its FIN is in the ring, and the same `poll` (or an earlier one) delivers it. Packets for sockets that are already freed, or for an earlier generation of their slot, are dropped.

Next: [Closing sockets and ending a session](closing.md) shows how sockets and sessions end. The control socket's wire format is in [The junction control protocol](../reference/junction-protocol.md).
