# UDP sockets

A `UdpSocket` sends and receives datagrams on one local port. It is bound with `handle.udp_bind(config)` ([Opening sockets](opening.md#binding-udp)) and, like a [TCP stream](tcp.md), splits into a receive half `UdpRx` and a send half `UdpTx`. Receiving never fails; sending fails in a few ways, two of which you retry.

## Receive

`rx.recv().await` returns the next queued `Datagram`. It never fails: when nothing is queued it waits. A `Datagram` carries the payload, the sender's endpoint `src()`, the address it was sent to `dst_addr()`, and `rx_timestamp()` ([Frames](frames.md#received-frames)).

## Send and retry

`tx.send(Some(to), frame)` sends one `ImmutableFrame` as one datagram to `to`. Always pass a destination; `None` returns `Error::NoDestination`. A zero-length frame is a valid, empty datagram. Like TCP's `try_send`, `send` does not wait, and every error returns the frame so you can try again.

Two errors are temporary:

- `Full`: the Stack cannot take the datagram right now, because its queue of outgoing packets is full or no frame is free for a copy. It clears after a turn.
- `NeighborPending`: the Stack has not yet learned the destination's hardware address (by ARP). It clears once a later turn receives the ARP reply.

The others mean the send cannot succeed as it stands: `NoRoute`, `Unaddressable`, `TooLarge`, `InvalidState`, `Foreign`. switch-bench's UDP echo copies each datagram into a fresh frame, drops the datagram, and retries the two temporary errors, yielding to the drive loop between tries:

```rust,ignore
{{#include ../../../switch-bench/src/serve.rs:echo_datagrams}}
```

`switch::yield_now()` lets the task run again in the next poll, after the drive loop has turned the network. A client that starts the conversation does the same with a destination it knows: `tx.send(Some(IpEndpoint::new(addr.into(), port)), frame)`.

## When a send copies

On the local backend, whether a send copies depends on the frame:

A frame you hold alone, whose payload still starts where the Stack handed it out, is sent in place: the headers are written in front of the payload and nothing is copied. A frame that is shared (it has clones) or sliced (its payload no longer starts at its original position) is copied into a fresh frame first, so the send needs one free frame from the frame pool. `truncate` only shortens the end, so a truncated frame is still sent in place.

On the junction backend every send copies the bytes into the send ring to the daemon, and the frame returns to the pool at once.

## Receive queue overflow

On the local backend, `UdpConfig::rx_queue` is the most datagrams a socket holds, counting both those waiting for `recv` and those your code still holds. There, a datagram that arrives when the queue is full is dropped and counted. `tx.stats()` returns `UdpSocketStats`, whose `rx_dropped_full` field is that count. `stats()` borrows the whole `Net`, so do not call it in a hot path.

## On the junction backend

The daemon runs the Stack, and a few things look different from the application:

- The daemon applies `rx_queue` to datagrams waiting in its own socket. Datagrams your code holds count against the session's receive ring instead ([Frames](frames.md#what-holding-a-frame-costs)). `stats()` returns zeros.
- A datagram is handed to the daemon as soon as `send` copies it into the send ring. If the daemon then cannot route it, or the neighbor is not resolved, or the destination is not addressable, it drops the datagram; `send` never returns `NoRoute`, `NeighborPending` or `Unaddressable` on this backend. `Full` means the send ring is full.
- After the session with the daemon is lost, `recv` keeps waiting; it does not return an error. Watch the drive loop's turn error instead ([When the daemon goes away](errors.md#when-the-daemon-goes-away)).

Next: [Timeouts and racing futures](timeouts.md).
