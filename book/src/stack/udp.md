# UDP sockets on the Stack

UDP on the Stack works like [TCP](tcp.md) in one way: calls only queue work, and packets move when the [drive loop](drive-loop.md) runs. Otherwise it is simpler. A socket is bound to a port, received datagrams wait in a queue until you take them, and a send either goes into the Stack's pending queue for the NIC or returns an error saying why it could not.

The Stack speaks IPv4 only. It has no multicast.

## Binding

`stack.add_udp(config)` binds a socket and returns its `UdpHandle`. The `UdpConfig`, as the UDP tests build it:

```rust,ignore
{{#include ../../../conduit/crates/dpdk-smoltcp/tests/stack_udp.rs:udp_config}}
```

| Field | Meaning |
|---|---|
| `port` | The local port. Must be nonzero. |
| `local_addr` | One address the Stack owns to receive on and send from, or `None` for all of them. |
| `rx_queue` | How many received datagrams the socket may hold, counting both those queued and those your code still holds. Must be nonzero. |
| `tx_checksum` | Compute the UDP checksum in software (`true`) or write 0 (`false`). With the NIC's TX UDP checksum offload on, the NIC computes it either way. |
| `broadcast` | Allow sending to the subnet's broadcast address. |

A port can carry one socket per owned address plus one socket for all addresses. An arriving datagram goes to the socket bound to its destination address if there is one, otherwise to the port's all-address socket. Broadcasts go only to the all-address socket.

A refused bind changes nothing and returns `AddUdpError`, checked in this order: `InvalidConfig(reason)` for a zero port or zero `rx_queue`, `Unaddressable` for a `local_addr` the Stack does not own, `PortInUse` when that address (or all addresses) and port are already bound, and `Full` when every one of the `max_udp_sockets` slots is taken.

## Receiving

`stack.udp_mut(&handle).recv()` returns the next datagram, or `None` when the queue is empty. It never fails. The UDP test reads one like this:

```rust,ignore
let datagram = stack.udp_mut(&handle).recv().unwrap();
assert_eq!(datagram.payload(), b"mbuf payload");
assert_eq!(
    datagram.src(),
    IpEndpoint::new(Side::B.ip().into(), PEER_PORT)
);
assert_eq!(datagram.dst_addr(), Side::A.ip());
```

A `UdpDatagram` has `payload()`, `payload_mut()`, `src()` (the sender's address and port), `dst_addr()` (which of our addresses it was sent to) and `rx_timestamp()` (the [hardware timestamp](../nic/offloads.md), 0 when there is none). The payload is the received packet buffer itself; nothing was copied. `into_frame()` turns the datagram into an `OwnedFrame` you can keep; read the addresses and the timestamp first, because they do not travel with the frame.

The socket's `rx_queue` limit counts datagrams waiting in the queue and datagrams your code holds, including frames made from them, until the last one is dropped. When the limit is reached, newly arriving datagrams are dropped and counted: `stack.udp(&handle).stats().rx_dropped_full`. `rx_queued()` gives the number waiting in the queue.

## Sending by copy: `udp_send`

`stack.udp_send(&handle, to, payload)` copies `payload` into a frame from the frame pool and queues it as one datagram:

```rust,ignore
stack.udp_send(&handle, peer(PEER_PORT), &payload).unwrap();
```

`to` is the destination address and port. It may be `None` if you have connected the socket: `stack.udp_mut(&handle).connect(peer)` fixes a peer (unicast IPv4 and a nonzero port, or `UdpConnectError::Invalid`), and sends with `None` go there.

The datagram leaves on the next flush: `flush()` itself, or the flush inside `turn`, `receive` or `send`.

## Sending in place: `udp_send_frame`

To avoid the copy, fill a frame from the Stack and send the frame. The UDP frame tests use this helper:

```rust,ignore
{{#include ../../../conduit/crates/dpdk-smoltcp/tests/udp_frames.rs:frame}}
```

`stack.tx_frame()` returns an empty `OwnedFrame` of IP MTU − 28 bytes, the largest UDP payload, or `None` when the frame pool is empty. Write the payload, `truncate` to its length, `freeze`, and pass it to `stack.udp_send_frame(&handle, to, frame)`. On success the frame belongs to the Stack. On any error you get the frame back untouched, with a `UdpSendFrameError`: `Foreign` if the frame came from another Stack's pool, or `Udp(error)` with one of the send errors below.

Whether the send copies depends on the frame. A frame you hold alone, whose payload still starts where the Stack handed it out, is sent in place: the headers are written in front of the payload and nothing is copied. A frame that is shared (it has clones) or sliced (its payload no longer starts at its original position) is copied into a fresh frame first, so the send needs one free frame from the frame pool. `truncate` only shortens the end, so a truncated frame is still sent in place.

A zero-length frame sends an empty datagram.

## Send errors

Both send calls check in the same order and return the first error that applies:

| Error | Meaning | What to do |
|---|---|---|
| `NoDestination` | `to` is `None` and the socket has no peer. | Pass a destination. |
| `PeerMismatch` | `to` differs from the connected peer. | Send to the peer, or use another socket. |
| `Unaddressable` | The destination is unspecified, multicast, one of our own addresses, port 0, or the broadcast address without `broadcast: true`. | Fix the destination. |
| `TooLarge` | The payload is longer than the destination's path MTU − 28. There is no fragmentation. | Send less. |
| `WouldBlock` | The frame pool is empty (only when a copy is needed), or the pending queue is full. | Flush, then retry. |
| `NoRoute` | The destination is off the subnet and there is no gateway. | Configure a gateway, or send on-link. |
| `NeighborPending` | The next hop's MAC address is not known yet. The Stack queued an ARP request; this datagram was dropped. | Run the drive loop so the ARP reply is taken in, then send again. |

`NeighborPending` is the one that surprises people. The first send to a new neighbor always fails this way, because the Stack does not keep datagrams waiting for ARP. A sending loop must keep calling `receive` or `turn`, both to take in the ARP reply and to move time forward.

`WouldBlock` from a full pending queue means the NIC driver has not taken the packets already queued. Flush at least once every `tx_pending` sends (the `StackConfig` field) to keep it from filling.

The Stack drops IPv4 fragments it receives and never fragments what it sends.

## Removing a socket

`stack.remove_udp(handle)` frees the socket and the datagrams still queued on it. Datagrams your code already took keep their packet buffers until you drop them. Dropping the handle has the same effect, processed at the start of the Stack's next `turn`, `receive`, `send`, `add_tcp` or `add_udp`.

Next: [waiting on many sockets](readiness.md).
