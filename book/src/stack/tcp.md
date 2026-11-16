# TCP sockets on the Stack

This page covers TCP on the Stack directly: adding a socket, connecting and listening, reading received frames, sending, and the four ways a connection ends. Every call here only changes the socket's state; packets move when the [drive loop](drive-loop.md) runs.

## Adding a socket

`stack.add_tcp(config)` creates a TCP socket and returns its `TcpHandle`. The `tcp::Config` has three fields:

```rust,ignore
pub struct Config {
    pub rx_capacity: usize,
    pub tx_capacity: usize,
    pub mss_cap: Option<u16>,
}
```

`rx_capacity` is the receive window the socket advertises to its peer, at most 1 GiB. It is a target, not a limit on memory: received data lives in packet buffers from the shared RX pool (see [packet-buffer pools](../nic/pools.md)). `tx_capacity` is how many bytes the socket may hold for sending, counting both data waiting to be sent and data sent but not yet acknowledged. `mss_cap` caps the segment size this socket advertises and sends; `None` leaves it to the MTU and the peer.

`add_tcp` fails only when every slot is taken, with `AddTcpError::SocketsFull(config)`, which gives your config back. It never fails because a pool is small. It panics if `rx_capacity` is over 1 GiB or `mss_cap` is `Some(0)`.

The socket starts closed. Use it to connect or to listen.

## Connecting

```rust,ignore
{{#include ../../../conduit/crates/switch-open/examples/tcp_client.rs:connect}}
```

`stack.connect(&handle, remote, local)` starts the handshake. `remote` is the peer's address and port. `local` is our port, or an address and port. The Stack has no automatic port choice, so the local port must be nonzero. `connect` only queues the SYN; it leaves on the next egress.

A refused connect leaves the socket unchanged and returns `tcp::ConnectError`. `InvalidState` means the socket is not closed. `Unaddressable` means a zero port on either side, an unspecified remote address, a local address the Stack does not own, or no source address for that destination. `AddressInUse` means another socket that is neither closed nor listening already holds the same local and remote address and port; that includes a connection waiting out TIME-WAIT (10 seconds after it closed).

Watch the socket's state to see the handshake finish. `stack.tcp(&handle).state()` returns a `tcp::State`; the socket can send once `may_send()` is true.

## Listening

`stack.listen(&handle, local)` makes the socket wait for one incoming connection. `local` is a port (all our addresses) or an address and port. An address the Stack does not own, or port 0, returns `ListenError::Unaddressable`.

There is no accept queue. A listening socket becomes the connection when a peer connects to it: the same handle, now in `SynReceived` and then `Established`. To accept several connections at once, add several sockets and listen on the same port with each. When a connection is finished and its socket is `Closed` again, call `listen` on it once more. The tap echo harness does both:

```rust,ignore
{{#include ../../../conduit/crates/switch-tap-echo/src/lib.rs:listeners}}
```

```rust,ignore
{{#include ../../../conduit/crates/switch-tap-echo/src/lib.rs:relisten}}
```

## Receiving

`stack.tcp_mut(&handle).recv()` returns the next received frame:

- `Ok(Some(frame))`: one whole in-order segment's payload, as a `TcpFrame`.
- `Ok(None)`: nothing is queued right now.
- `Err(RecvError::Finished)`: the peer sent FIN and every byte before it has been read.
- `Err(RecvError::InvalidState)`: the receive side is not open: the connection was reset, or it is not established yet.

Data comes frame by frame, exactly as the peer's segments arrived. Frame boundaries are not message boundaries: a message your peer wrote with one call can be split over several frames, and several messages can share one. There is no byte-oriented read and no peek. `can_recv()` says whether in-order data is queued; `recv_queue()` gives the byte count.

A `TcpFrame` gives access to `payload()`, `payload_mut()` and `rx_timestamp()` (the [hardware timestamp](../nic/offloads.md), 0 when there is none). It owns its packet buffer from the RX pool until you drop it. It stays valid after the socket is removed, the handle dropped, or even the Stack turned back into a port. `into_frame()` turns it into an `OwnedFrame` you can keep, `freeze()` and clone; read `rx_timestamp()` first, because the timestamp does not travel with the frame.

Holding received frames does not shrink the window the socket advertises. It does use the shared RX pool, so a program that holds many frames for a long time can run that pool dry.

## Sending

There are two ways to send: copy bytes in, or hand over a frame you filled.

### Copying bytes: `send_slice`

`stack.tcp_mut(&handle).send_slice(data)` copies as much of `data` as fits and returns how many bytes it took. It fills the frame at the end of the send queue first, then takes new frames from the shared frame pool, each holding at most one segment. Two limits apply: the socket's free transmit bytes (`tx_capacity` minus what it holds), and free frames in the frame pool. When neither has room it returns `Ok(0)`; that is not an error. It returns `Err(SendError::InvalidState)` only when the socket's transmit side is not open.

The echo step in the tap echo harness shows the usual pattern: read a frame, copy as much of it as fits, keep the rest for the next time through the loop.

```rust,ignore
{{#include ../../../conduit/crates/switch-tap-echo/src/lib.rs:echo}}
```

When `send_slice` stops short because the frame pool is empty, nothing tells the socket when frames come back. Call it again on a later time through the loop; `stack.free_frames()` says how many frames the pool has now.

### Handing over a frame: `tcp_send_frame`

To write data in place, without a copy, take an empty frame from the Stack, fill it, shorten it to the length you wrote, freeze it and give it to the socket. In this excerpt from the readiness test, `pair.a` is a Stack and `h[0]` its TCP handle:

```rust,ignore
let mut out = pair.a.tx_frame().unwrap();
out.as_mut()[..1000].fill(7);
out.truncate(1000);
let out = out.freeze();
assert!(pair.a.tcp_send_frame(&h[0], out).is_ok());
```

`stack.tx_frame()` returns an `OwnedFrame` whose length is the largest UDP payload, IP MTU − 28 bytes, or `None` when the frame pool is empty. `stack.tcp_send_frame(&handle, frame)` queues the frozen frame. A small frame that fits whole into the unsent frame at the end of the send queue, without making that frame longer than one segment, is copied there and freed at once, so several small writes share one segment. Any other frame becomes its own entry in the send queue, and the socket holds it until the peer acknowledges its last byte. Every error gives the frame back, untouched:

| Error | Meaning |
|---|---|
| `SendFrameError::Foreign` | The frame came from another Stack's `tx_frame`. |
| `SendFrameError::InvalidState` | The socket may not send. |
| `SendFrameError::Empty` | The frame has zero length. |
| `SendFrameError::Full` | The frame is longer than the socket's free transmit bytes (`frame_send_room()`). |

A queued frame goes out in place when it is sent as one whole segment. When that is not possible, the Stack builds the segment in a packet from the TX pool and copies the bytes there: the frame is longer than the connection's segment size, the frame is shared or sliced, the segment spans several frames, or only part of a frame is retransmitted. `max_segment_payload()` returns the segment size once the connection is established.

## Closing

There are four ways to end a connection. They differ in what goes on the wire and when the socket slot becomes free.

| Call | On the wire | Unread data | Slot |
|---|---|---|---|
| `stack.close(&handle)` | FIN after the queued data. In `Listen` and `SynSent` the socket closes at once; in `SynReceived` it sends a reset. | stays readable | yours; keep driving until `Closed`, then reuse or remove |
| `stack.abort(&handle)` | one reset at the next egress, for an open connection | discarded | yours, at once `Closed` |
| `stack.remove_tcp(handle)` | nothing | discarded | free at once |
| drop the `TcpHandle` | reset if unread in-order data remains, else FIN (like Linux `close()`) | discarded | held until the connection has fully closed |

Dropping a handle does not touch the socket immediately. The Stack processes dropped handles at the start of its next `turn`, `receive`, `send`, `add_tcp` or `add_udp`. From then on the connection closes on its own:

```text
 handle dropped
       |  (next turn / receive / send / add_tcp / add_udp)
       v
 unread in-order data? --yes--> discard it, send reset ----------+
       | no                                                      |
       v                                                         |
 SynSent? --yes--> closed, nothing sent -------------------------+
       | no                                                      |
       v                                                         |
 close: FIN after queued data                                    |
       |                                                         |
       +- FinWait2 with no FIN from peer for 60 s --> closed ----+
       +- peer stops answering: closed after 8 backoffs ---------+
       v                                                         |
 normal close, TIME-WAIT 10 s -----------------------------------+
                                                                 v
                                                     slot free again
```

Until it is free, the slot counts against `max_sockets`, and `add_tcp` returns `SocketsFull`.

None of `abort`, `remove_tcp` or dropping the handle cancels packets already waiting in the Stack's pending queue for the NIC. Those still go out.

A `TcpHandle` may outlive its Stack; dropping it afterwards does nothing.

## Inspecting and tuning a socket

`stack.tcp(&handle)` gives read access to the socket without marking it changed; `stack.tcp_mut(&handle)` gives write access. The useful readings:

| Method | Returns |
|---|---|
| `state()` | the TCP state (`Established`, `CloseWait`, `TimeWait`, ...) |
| `may_send()`, `may_recv()`, `can_send()`, `can_recv()` | whether each half is open, and whether there is room or data now |
| `send_queue()`, `send_capacity()` | bytes held for sending, and `tx_capacity` |
| `recv_queue()`, `recv_capacity()` | bytes queued to read, and `rx_capacity` |
| `max_segment_payload()` | the connection's segment size, `None` before the handshake |
| `local_endpoint()`, `remote_endpoint()` | the connection's addresses |
| `reset_by_peer()` | whether the peer's reset closed the connection |
| `retransmits()`, `loss_events()` | retransmitted segments, and loss episodes |
| `congestion_window()`, `ssthresh()` | Cubic's window and slow-start threshold, in bytes |
| `rx_refused_segments()` | out-of-order segments refused because the socket already held 1024 frames past its first gap |

And the settings, on `tcp_mut`:

| Method | Effect |
|---|---|
| `set_ack_delay(Option<Duration>)` | delayed-ACK time, 10 ms by default; `None` turns it off; over 60 s panics |
| `ack_now()` | send an owed ACK at the next egress |
| `set_initial_window(usize)` | Cubic's initial window, 1 MiB by default; zero panics; set before connect or listen |
| `set_hop_limit(Option<u8>)` | IPv4 TTL, 64 by default; `Some(0)` panics |
| `limit_retries()` | give up after 8 retransmission backoffs, as an orphaned socket does |

Next: [UDP sockets on the Stack](udp.md).
