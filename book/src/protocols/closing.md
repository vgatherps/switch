# Closing connections and handling failures

A WebSocket connection ends with a close handshake: one side sends a close frame with a status code and a reason, the other answers with its own close frame, and then the transport closes. Over `wss://` that means TLS close_notify (TLS's "I am done sending" record) and then a TCP FIN. This page shows how the API exposes that handshake, how a connection ends without one, and every way a connection can fail. It follows [Sending and receiving messages](messages.md).

The short version: call `close`, then call `recv` until it returns `None`, and put a time limit around both.

## Closing from your side

```rust,ignore
pub async fn close(&mut self, code: u16, reason: &str) -> Result<(), Error>
```

`close` sends a close frame and nothing else. The connection stays open for receiving: the server may still be sending messages that crossed your close on the wire, and its close frame has to arrive. Keep calling `recv`. Messages that arrive before the server's close are still returned. When the server's close frame arrives, `recv` sends close_notify (on `wss://`) and FIN, waits until the server has acknowledged them, and returns `Ok(None)`. From that moment you may drop the `Wss`, and the `Net` too: nothing is left in flight.

The feed closes like this, under a 10-second limit:

```rust,ignore
{{#include ../../../conduit/crates/switch-binance-feed/src/lib.rs:close}}
```

Here is the whole sequence for a client-initiated close:

```text
 client task                                           server
  close(1000, "")        ---- close frame (1000) ---->
  recv() ...             <--- message (in flight) ----   returned by recv
  recv() ...             <--- close frame (1000) -----
                         ---- close_notify ---------->   (wss:// only)
                         ---- FIN ------------------->
                         <--- ACK of FIN -------------
  recv() returns Ok(None)
```

The code and reason must be ones a WebSocket endpoint may send: a valid close code and a reason of at most 123 bytes. Otherwise `close` returns `InvalidClose` and sends nothing. Calling `close` a second time while the first close frame is queued or sent does nothing.

## The peer closes

When the server starts the close, you see it in `recv`. `recv` answers with a close frame of its own, sends close_notify and FIN, waits until they are acknowledged, and returns `Ok(None)`.

```text
 client task                                           server
  recv() ...             <--- close frame (1001, "bye")
                         ---- close frame (echo) ---->
                         ---- close_notify ---------->   (wss:// only)
                         ---- FIN ------------------->
                         <--- ACK of FIN -------------
  recv() returns Ok(None)
  peer_close() == Some((1001, "bye"))
```

After `recv` has returned `None`, `peer_close()` gives the server's status code and reason. A close frame with no status reads as code 1005. A status code that the peer was not allowed to send reads as 1002, which is also what was echoed back. In the repository's test of a server-initiated close, the server sends close code 1001 (going away) with reason `"bye"`. The client's part of the test is one `recv` that returns `None`, then `peer_close`:

```rust,ignore
{{#include ../../../switch-ws/tests/wss_switch_net.rs:server_close_client}}
```

If both sides send a close at the same time, only one close frame and one FIN go out from your side. Bytes the server sends after its close frame are read and dropped. Once the server's close has arrived, a TCP reset that follows counts as a clean end, not as a failure.

## Ending without a close frame

```rust,ignore
pub async fn linger(&mut self) -> Result<(), Error>
```

`linger` ends the connection without a WebSocket close frame (a close frame you already queued still goes out). It sends close_notify (on `wss://`, unless TLS itself failed) and FIN, then reads and drops whatever the peer still sends until the peer's FIN. The point of the reading is that a socket dropped with unread data is aborted with a TCP reset; after `linger` returns, dropping the connection sends no reset. `linger` returns `Err(Failed(Failure::Dead))` if the socket was reset before the peer's close.

The main use is after the peer broke the protocol, as in the [echo server](ws-server.md).

## Failures

Once a connection is up, a failure is reported as `Error::Failed(failure)`. The first failure is kept: every later `send`, `close` and `recv` returns the same one. Before `recv` returns it, the connection delivers whatever it still owes the peer (a TLS alert, close_notify, FIN), so you may drop the connection as soon as you have the error. If the peer may still be sending, call `linger` first so the drop sends no reset ([Ending without a close frame](#ending-without-a-close-frame)).

| Failure | What happened | What the connection sent before reporting it |
|---|---|---|
| `Failure::Protocol(e)` | The peer broke the WebSocket protocol; `e` is a `ProtocolError` naming what (bad reserved bits, an unmasked client frame, invalid UTF-8 in a text message, a frame or message over your `Limits`, ...). | No close frame. close_notify (on `wss://`) and FIN. |
| `Failure::PeerEof` | The peer ended the stream (FIN, or close_notify on `wss://`) before sending a WebSocket close. | close_notify (on `wss://`) and FIN. |
| `Failure::Tls(switch_tls::Error::Truncated)` | On `wss://`: TCP ended without close_notify. | FIN; no close_notify. |
| `Failure::Tls(switch_tls::Error::Tls(e))` | On `wss://`: a TLS record from the peer failed. | The TLS alert and FIN; no close_notify. |
| `Failure::Dead` | The socket was reset or closed under the connection. | Nothing; nothing more can be sent. |

`PeerEof` and `Tls(Truncated)` differ in how the peer ended. A peer that sent close_notify and then FIN, without a WebSocket close, is `PeerEof`. A peer whose TCP stream simply stopped, with no close_notify, is `Truncated`.

Calls can also return errors that do not fail the connection. `InvalidText`, `InvalidClose` and `TooBig` refuse one call and send nothing; `Closed` means you already called `close` or `linger` and tried to send again. The errors from connecting are on [Connecting to a wss:// server](wss-client.md#when-it-fails).

The paths out of a connection, in one picture:

```text
 event while receiving             owed output, sent first       recv returns
 --------------------------------  ----------------------------  ---------------------------
 peer's close frame                close echo, close_notify, FIN Ok(None)
 peer breaks the protocol          close_notify, FIN             Err(Failed(Protocol(e)))
 bad TLS record                    alert, FIN                    Err(Failed(Tls(Tls(e))))
 FIN without close_notify          FIN                           Err(Failed(Tls(Truncated)))
 close_notify/FIN, no close frame  close_notify, FIN             Err(Failed(PeerEof))
 reset before peer's close         nothing                       Err(Failed(Dead))
 reset after peer's close          nothing                       Ok(None)
```

## Bound every wait

Each of `recv`, `close` and `linger` can wait on the peer: for its close frame, for the acknowledgement of your FIN, for its FIN. A peer that goes silent keeps the call waiting for as long as it stays silent. Wrap the close sequence in [`within`](../net/timeouts.md), as the feed does with its 10-second `CLOSE_LIMIT`, and drop the connection when the limit passes.

The next page turns the API around and serves connections: [Serving ws:// connections](ws-server.md).
