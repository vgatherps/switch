# UDP sockets on the Stack

A UDP socket on the Stack is bound to a local port. Received datagrams wait in the socket's queue until you take them, each one still in the packet buffer it arrived in. A send either goes into the Stack's pending queue for the NIC or returns an error saying why it could not. As with [TCP](tcp.md), calls only queue work, and packets move when the [drive loop](drive-loop.md) runs.

## Binding

`stack.add_udp(config)` binds a socket and returns its `UdpHandle`. The `UdpConfig`, as the UDP tests build it (their helper takes the port and the queue size):

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

`stack.udp_mut(&handle).recv()` returns the next datagram, or `None` when the queue is empty. It never fails. The UDP test reads one like this; `Side::A` and `Side::B` are the test's two hosts, and `PEER_PORT` is the sender's port:

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

`stack.udp_send(&handle, to, payload)` copies `payload` into a frame from the frame pool and queues it as one datagram. `to` is the destination address and port. It may be `None` if you have connected the socket: `stack.udp_mut(&handle).connect(peer)` fixes a peer (unicast IPv4 and a nonzero port, or `UdpConnectError::Invalid`), and sends with `None` go there. The datagram leaves on the next flush: `flush()` itself, or the flush inside `turn`, `receive` or `send`.

A send can fail for a reason that goes away by itself, and a sending program must be ready for that. A send whose next hop's MAC address is not cached yet returns `NeighborPending` after queuing an ARP request; the datagram is not kept. Keep the payload, keep calling `receive` or `turn` (to take in the ARP reply and move time forward), and send again. A learned address stays usable after it ages: the Stack keeps sending to it while it refreshes it with an ARP probe. So sends through the gateway that `open` resolved before it returned do not hit this, unless the ARP cache fills with more neighbors than `neighbor_capacity` and evicts the gateway. `WouldBlock` is the other error to retry; the [table below](#send-errors) gives its two causes.

`switch-nic`'s UDP echo server shows the pattern. When a reply cannot be sent, it keeps the received datagram in `slot.pending`, returns to its drive loop, and tries that datagram first the next time it visits the socket:

```rust,ignore
{{#include ../../../conduit/crates/switch-nic/src/server.rs:udp_retry}}
```

`turns.flush` is the server's own wrapper around `Stack::flush`.

## Sending in place: `udp_send_frame`

To avoid the copy, fill a frame from the Stack and send the frame. The UDP frame tests use this helper:

```rust,ignore
{{#include ../../../conduit/crates/dpdk-smoltcp/tests/udp_frames.rs:frame}}
```

`stack.tx_frame()` returns an empty `OwnedFrame` of IP MTU − 28 bytes, the largest UDP payload, or `None` when the frame pool is empty. Write the payload, `truncate` to its length, `freeze`, and pass it to `stack.udp_send_frame(&handle, to, frame)`. On success the frame belongs to the Stack. On any error you get the frame back untouched, with a `UdpSendFrameError`: `Foreign` if the frame came from another Stack's pool, or `Udp(error)` with one of the send errors below.

Whether the send copies depends on the frame. A frame you hold alone, whose payload still starts where the Stack handed it out, is sent in place: the headers are written in front of the payload and nothing is copied. A frame that is shared (it has clones) or sliced (its payload no longer starts at its original position) is copied into a fresh frame first, so the send needs one free frame from the frame pool. `truncate` only shortens the end, so a truncated frame is still sent in place.

A zero-length frame sends an empty datagram.

## Send errors

Both send calls check in this order and return the first error that applies:

| Error | Meaning | What to do |
|---|---|---|
| `NoDestination` | `to` is `None` and the socket has no peer. | Pass a destination. |
| `PeerMismatch` | `to` differs from the connected peer. | Send to the peer, or use another socket. |
| `Unaddressable` | The destination is unspecified, multicast, one of our own addresses, port 0, or the broadcast address without `broadcast: true`. | Fix the destination. |
| `TooLarge` | The payload is longer than the destination's path MTU − 28. There is no fragmentation. | Send less. |
| `WouldBlock` (frame pool) | The frame pool is empty. Checked here only when the send needs a copy: always for `udp_send`, and for a shared or sliced frame. | Retry once frames return. |
| `NoRoute` | The destination is off the subnet and there is no gateway, or there is no source address for it. | Configure a gateway, or send on-link. |
| `NeighborPending` | The next hop's MAC address is not known yet. The Stack queued an ARP request; this datagram was dropped. | Run the drive loop so the ARP reply is taken in, then send again. |
| `WouldBlock` (pending queue) | The pending queue for the NIC is full. | Flush, then retry. |

`WouldBlock` from a full pending queue means the NIC driver has not taken the packets already queued. Flush at least once every `tx_pending` sends (the `StackConfig` field) to keep it from filling.

The Stack speaks IPv4 only and has no multicast. It drops IPv4 fragments it receives and never fragments what it sends.

## Removing a socket

`stack.remove_udp(handle)` frees the socket and the datagrams still queued on it. Datagrams your code already took keep their packet buffers until you drop them. Dropping the handle has the same effect, processed at the start of the Stack's next `turn`, `receive`, `send`, `add_tcp` or `add_udp`.

Next: [waiting on many sockets](readiness.md).
