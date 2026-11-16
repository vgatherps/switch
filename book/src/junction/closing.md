# Closing sockets and ending a session

This page covers the end of things: closing one socket, finishing a session cleanly, and every other way a session can end, with what your program and the peers on the network see in each case. It uses the raw `Session` API from [the previous page](session.md); switch-net does the same steps for you when you drop a stream and finish the session after `into_session` (see [Connecting an application through switch-net](apps.md#teardown)).

## Closing a socket

A session has three ways to close a socket and one to half-close it:

| Call | What the daemon does | Socket slot |
|---|---|---|
| `shutdown(id)` | TCP only. Sends a FIN after the data already sent. The socket stays open for receiving. `Delivered` follows once the peer acknowledges. | stays open |
| `close(id, false)` | Closes gracefully: waits until the send rings are idle and the socket's queued bytes are written, then sends a FIN and removes the socket. If the daemon still holds received bytes the program never read, it resets instead. | `Freed` follows |
| `close(id, true)` | Resets the connection at once and discards queued bytes. Pass `true` when your program left received data unread. | `Freed` follows |
| `abort(id)` | TCP only. Resets the connection at once and discards queued bytes. | `Freed` follows |

The `unread` argument of `close` tells the daemon whether your program dropped received data without reading it. switch-net passes `true` when a stream is dropped while received frames are still queued for it. This mirrors what Linux does when a socket with unread data is closed.

After `close` or `abort` the socket slot is closing. It becomes free when `Freed` arrives, and the next open of that slot gets a new generation. If the open was rejected, the slot is already free and a `close` you call for it is ignored.

A graceful close waits for the send rings to go idle, so it can wait while another application keeps sending; [What applications share](sharing.md#closing-sockets) explains why. Resets do not wait.

For a UDP socket, `close` unbinds it and later returns `Freed`; no FIN or reset applies. `close(id, false)` waits behind earlier send records, as for TCP; `close(id, true)` does not.

Some calls are mistakes and panic: closing a socket that is already closing, `shutdown` on a UDP socket or on a socket that is not open, `abort` on a UDP socket, and any call with a stale or out-of-range socket id.

## Finishing a session

When the program is done with the daemon, it finishes the session. This is the end of the raw TCP example from the previous page:

```rust,ignore
{{#include ../../../switch-junction/examples/tcp_session.rs:finish}}
```

A switch-net program does the same after `into_session`; [Teardown](apps.md#teardown) shows that version.

`finish()` asks the daemon to end the session after everything the program has already sent. It returns one of two errors when it cannot queue that request yet:

- `FinishError::ControlPending`: control messages are still waiting to be written to the daemon.
- `FinishError::StreamFull`: the send ring has no room for the end record.

In both cases the end request was not queued. Poll and call `finish()` again. Once it succeeds, further calls return `Ok` and do nothing. Success does not mean the session is over yet: keep polling until `closed()` returns true, which is the daemon confirming.

When the daemon finishes a session, it applies every close and shutdown the program asked for and closes the remaining TCP connections gracefully, with a FIN. A connection that still has bytes queued keeps running on its own until those bytes are sent and then closes with a FIN. If the peer stops acknowledging, the daemon gives up after 8 retransmission back-offs and resets the connection (see [TCP sockets on the Stack](../stack/tcp.md)). A connection with received data the program never read is reset.

After a successful `finish()`, opening sockets, sending and `shutdown` panic. `close` and `abort` do nothing, so code in destructors that closes sockets stays safe.

Dropping a `Session` without finishing it is treated as a crash: the daemon resets every socket the session still had. Finishing and then dropping the session before `closed()` is true still gives the graceful result.

## How a session can end

| How it ends | What the program sees | What the daemon does with open TCP connections |
|---|---|---|
| The program finishes, then polls until `closed()` | `Ok`, and `closed()` is true | closes them gracefully (FIN); unread data means reset |
| The program finishes, then drops the session early | nothing (the program has moved on) | same as above |
| The program drops the session without finishing, or exits or is killed | nothing | resets all of them |
| The daemon is stopped with SIGINT or SIGTERM | `Lost::Ended(EndReason::HostShutdown)`, or `Lost::Disconnected` | drops them with the Stack and closes the NIC; peers see the connections go silent, with no FIN or reset |
| The daemon is killed or crashes | `Lost::Disconnected` | nothing; the daemon is gone |
| The daemon kicks the session | `Lost::Ended(EndReason::Kicked)` | resets all of them |
| The program sent something invalid, such as data before `Connected` | `Lost::Ended(EndReason::ProtocolError)` | resets all of them |
| Writing to the control socket fails | `Lost::Io(..)` or `Lost::Disconnected` | resets all of them, as for a dropped session |
| The daemon sends bytes the program cannot decode | `Lost::Protocol(..)` | nothing until the program drops the session; then it resets the session's sockets, as for a dropped session |

A kick means the daemon ended the session because its send stream had a gap it could not fill. [cephalopod](../blocks/cephalopod.md) describes how the daemon puts each session's sends in order.

The shutdown notice when the daemon stops is best effort. Depending on timing, the program sees `Ended(HostShutdown)` or `Disconnected`, so treat the two the same.

## Handling loss

`Lost` is the error `poll` and `poll_events` return when the session ended without a clean finish. The first error carries the reason. After `Lost::Ended(..)`, later polls return `Ok` or `Lost::Disconnected`. After `Lost::Io`, `Lost::Protocol` or `Lost::Disconnected`, later polls report the loss again. Handle the first `Lost`, log it, and stop using the session for sockets.

Data that reached the receive ring before the loss is not lost with it. `poll` delivers every packet already in the ring before it returns the error, and `poll_packets` keeps working after the loss, so a program can still read what arrived.

`send_tcp` and `send_udp` do not check for loss and may still return `Ok` or `Full`. Only polling tells you the session is gone.

Through switch-net, the receive step of a round polls the session, so that is where loss first shows up: `net.receive` returns `TurnError::HostLost` carrying the `Lost` value, and `switch_net::round` returns it as `RoundError::Turn`. Later receive and send steps return `TurnError::HostLost(Lost::Disconnected)`. Pending opens then fail with `Error::HostLost` and established sockets fail as described in [Errors](../net/errors.md#when-the-daemon-goes-away). There is no way to resume a lost session: connect a new one and open new sockets.

This ends the junction part. The wire format of the control socket, for people writing another client, is in [The junction control protocol](../reference/junction-protocol.md). The book continues with [Programming the Stack directly](../stack/index.md).
