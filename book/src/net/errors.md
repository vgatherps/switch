# Errors

Socket operations in switch-net return `switch_net::Error`, one error type shared by both backends. The drive loop has its own errors, `TurnError` and `RoundError`. Below are all of them with what to do about each, followed by what a program sees when the junction daemon goes away. The operations were introduced in [TCP streams](tcp.md) and [UDP sockets](udp.md), and [Timeouts and racing futures](timeouts.md) showed how to stop waiting on them.

## Socket errors

`Error` is a `Copy` enum with `Display` and `std::error::Error`. It is `#[repr(u32)]` and each variant has a fixed number, so you can log or pass it as a code. The text column is what `Display` prints.

| Code | Variant | Text | Returned by | What to do |
|---|---|---|---|---|
| 1 | `SocketsFull` | no free socket slot | `tcp_connect`, `tcp_accept`, `udp_bind` | Drop sockets you no longer need, or open with room for more sockets. Locally, a closing connection keeps its slot until it finishes closing. |
| 2 | `PortInUse` | port in use | `tcp_connect`, `udp_bind` | Use another local port. On the junction backend, a port you just closed can still be in use briefly. |
| 3 | `InvalidConfig` | invalid socket configuration | `udp_bind`; on the junction backend also `tcp_connect`, `tcp_accept` | Fix the `tcp::Config` or `UdpConfig`. |
| 4 | `Unaddressable` | unaddressable endpoint | opening calls; UDP `send` (local) | Use a nonzero local port, a nonzero remote port and a specified remote address. |
| 5 | `Refused` | connection refused | `tcp_connect` | The peer reset the connection attempt. |
| 6 | `Closed` | connection closed | `tcp_connect` | The attempt ended without a reset from the peer. |
| 7 | `Finished` | receive finished | TCP `recv` | The peer sent FIN. Stop reading; you may still send. |
| 8 | `InvalidState` | invalid socket state | TCP `recv`, `room`, `try_send`; UDP `send`; opening calls | The connection was reset or closed, you shut down sending, or the daemon is gone. Drop the socket. |
| 9 | `TooLarge` | payload too large | `room`, UDP `send` | The data can never fit: split it, or raise the capacity. |
| 10 | `Full` | send queue full | `try_send`, UDP `send` | Keep the returned frame; wait for `room` (TCP) or the next round (UDP) and retry. |
| 11 | `Empty` | empty frame | `try_send` | TCP cannot send a zero-length frame. |
| 12 | `Foreign` | frame belongs to another Net | `try_send`, UDP `send` | Get frames from a handle of the same `Net`. |
| 13 | `NoDestination` | no destination | UDP `send` | Pass `Some(endpoint)`. |
| 14 | `PeerMismatch` | destination differs from connected peer | not returned by switch-net | — |
| 15 | `NoRoute` | no route | UDP `send` (local) | The Stack has no route to that address; check the address and the config file's routes. |
| 16 | `NeighborPending` | neighbor resolution pending | UDP `send` (local) | Keep the frame and retry in a later round, after a receive step has taken in the ARP reply. |
| 17 | `HostLost` | junction host session lost | opening calls (junction) | The daemon is gone; see below. |

On the junction backend, opening errors that the daemon decides (`SocketsFull`, `PortInUse`, `Unaddressable`, `InvalidState`) arrive one or more rounds after the call; see [When errors arrive](opening.md#when-errors-arrive).

## Loop errors

`Net::receive` and `Net::send` return `TurnError`. It has `Display` and `std::error::Error`, and two variants:

- `TurnError::Stack(StackError)` on the local backend, when the Stack fails during the receive or send step.
- `TurnError::HostLost(Lost)` on the junction backend, when the session with the daemon ends.

`switch_net::round` returns `RoundError`, which is either `Turn(TurnError)` from the receive or send step, or `Runtime(RuntimeError)` from the runtime poll ([Panics, errors and shutdown](../runtime/errors.md)). `RoundError` implements only `Debug`, so print it with `{:?}`:

```rust,ignore
let moved = switch_net::round(&mut net, &mut rt, now, tick)
    .map_err(|error| format!("round: {error:?}"))?;
```

A receive step that fails still wakes the tasks it found ready; see [Errors from a round](drive-loop.md#errors-from-a-round).

## When the daemon goes away

On the junction backend, the daemon can end your session: it shut down, it ended the session on purpose, or the connection to it broke. The next receive step returns `TurnError::HostLost(lost)`, and `lost` carries the reason ([Closing sockets and ending a session](../junction/closing.md) lists them). That same receive step wakes the tasks waiting to open, receive on TCP, wait for room or wait for delivery, so each sees the loss. Every later receive or send step returns `HostLost(Lost::Disconnected)`, so handle the first error and stop.

From then on, sockets behave like this:

- Opens that were waiting, and any new `tcp_connect`, `tcp_accept` or `udp_bind`, return `HostLost`.
- TCP `recv` first returns data that had already arrived, then `InvalidState`.
- TCP `room` and `try_send`, and UDP `send`, return `InvalidState`.
- UDP `recv` keeps waiting.
- `tx_frame()` still hands out frames, since that pool lives in your process.

A program usually stops its drive loop on the first `HostLost`, shuts down the runtime, and ends the program.

Next: [Local and junction backends compared](backends.md).
