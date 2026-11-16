# TLS without WebSocket

switch-tls is a TLS 1.3 client that gives you an encrypted byte stream over a switch-net TCP connection, for services that speak their own protocol over TLS with no WebSocket in between. It is the same TLS client that `wss://` connections use (see [Connecting to a wss:// server](wss-client.md)), so the configuration rules are the same: a `rustls::ClientConfig` that allows TLS 1.3 only. This page follows [Serving ws:// connections](ws-server.md).

The API is lower level than switch-ws. You get the received plaintext as runs of bytes, not as messages, and you decide where one unit of your protocol ends.

## Connecting

```rust,ignore
pub async fn tls_connect(
    handle: &NetHandle,
    stream: TcpStream,
    config: Arc<ClientConfig>,
    host: &str,
    min_plain: usize,
) -> Result<Tls, Error>
```

`tls_connect` takes a connected TCP stream and runs the TLS 1.3 handshake for `host`, which is the server name sent in the handshake and checked against the certificate. `min_plain` is the size of the largest piece of plaintext you will send as one TLS record; 0 counts as 1. Frames are sized to the connection's MSS, so that each is one TCP segment, but never below one record of `min_plain` bytes; TCP cuts a larger frame into segments. That record must fit in a frame and in the socket's send buffer, and the config's `max_fragment_size` must allow it. Otherwise `tls_connect` returns `Setup(SetupError::RoomTooSmall { room, need })` before it sends the ClientHello. Passing 1 is fine if you send through `send_bytes` (below), which splits data to fit.

As with `wss://`, a failing handshake sends the TLS alert and a FIN, best effort, and returns only after the server acknowledged them or the connection ended. Bound the connect with [`within`](../net/timeouts.md). A server that negotiates a version older than TLS 1.3, which the config can allow, is refused with `Handshake`.

The result is a `Tls` with three public fields:

```rust,ignore
pub struct Tls {
    pub rx: TlsRx<TcpFrame, SwitchTx>, // received plaintext
    pub tx: TlsTx<SwitchTx>,           // the sending half
    pub tcp: TcpRx,                    // the TCP receive half that feeds rx
}
```

## A whole connection

The repository's driver for [TLS-Anvil](../testing/conformance.md#tls-anvil), a test suite that checks a TLS client's handshake and error handling, shows a complete client connection: handshake, a request, reading until the server closes, then closing:

```rust,ignore
{{#include ../../../switch-tls/examples/tls_anvil.rs:connection}}
```

The sections below take this apart.

## Reading

`Tls::ready().await` waits until there is plaintext to read. It returns `Ok(true)` when `tls.rx.current()` holds bytes, and `Ok(false)` when the server has sent close_notify, TLS's "I am done sending" record, and there is nothing more to read.

`tls.rx.current()` is the unread plaintext of one TLS record, as a `&[u8]`. Where the record lay inside one frame, this is a slice of the frame itself, decrypted in place. Tell the receiver how much you used with `tls.rx.consume(n)`; the rest stays in `current()` for the next call. A loop that reads everything looks like this:

```rust,ignore
loop {
    match tls.ready().await {
        Ok(true) => {
            let n = tls.rx.current().len();
            tls.rx.consume(n);
        }
        Ok(false) => break,
        Err(e) => return format!("receive: {e}"),
    }
}
```

## Writing

```rust,ignore
pub async fn send_bytes<L: FrameTx>(lower: &mut L, bytes: &[u8]) -> Result<(), L::Error>
```

`send_bytes(&mut tls.tx, bytes)` encrypts and sends `bytes`, one frame (and one TLS record) at a time, waiting for room in the TCP send buffer as needed:

```rust,ignore
if let Err(e) = send_bytes(&mut tls.tx, REQUEST).await {
    return format!("send request: {e}");
}
```

Its error type is `switch_tls::TxError<switch_net::Error>`:

| `TxError` | Meaning |
|---|---|
| `Received(e)` | Receiving already failed with `e`; that ends sending too. |
| `Closed` | You already sent close_notify. |
| `Lower(e)` | TCP failed underneath; `e` is the switch-net error. |
| `Broken` | An encrypted record was lost below; no later record can be decrypted by the server. The connection is unusable. |

## Closing

`Tls::close().await` sends close_notify, then FIN, and returns once the server has acknowledged them or the connection has ended. After that the `Tls` can be dropped. close_notify is sealed as soon as a frame is free; if TCP has no room for it, `close` waits for room for those few sealed bytes, not for a whole frame. `close` does not read; data the server sends meanwhile stays queued in TCP. A server that never acknowledges keeps `close` waiting, so bound it.

## Errors

`tls_connect`, `ready` and `close` return `switch_tls::net::Error`:

| Error | Meaning |
|---|---|
| `Tls(switch_tls::Error::Tls(e))` | The handshake or a received record failed (`e` is the rustls error). switch-tls sent the alert and FIN, best effort, and waited until the server acknowledged them or the connection ended. |
| `Tls(switch_tls::Error::Truncated)` | The TCP stream ended without the server's close_notify. |
| `Recv(e)` | TCP receive failed (for example, a reset). |
| `Send(e)` | Sending failed; `e` is a `TxError` as above. |
| `Handshake(msg)` | The handshake could not start (an invalid server name), negotiated a version other than TLS 1.3, or ended in an unexpected state. |
| `Setup(e)` | The connection could not be set up and nothing was sent: `RoomTooSmall`, `ForeignHandle` (the stream belongs to another `Net`), or `Dead` (the socket had already closed or reset). |

Like a failing handshake, a TLS error in `ready` sends the alert and FIN and waits for them to be delivered before returning, so `ready` too can wait on a silent server. A record whose header announces more than rustls accepts fails as soon as the header is complete. A record over the TLS 1.3 limit of 2^14 + 256 bytes but within that bound fails once all of it has arrived, and the alert sent is `record_overflow`.

## Embedding on other frames

The TLS layer itself does not depend on switch-net. `TlsRx` and `TlsTx` work over any frame source that implements three traits from the crate root: `RxFrame` (a received buffer with its timestamp), `TxFrame` (a writable buffer) and `FrameTx` (something that allocates, seals and sends frames). switch-net's `TcpFrame` and `OwnedFrame` implement them in the `switch_net` module, and switch-ws uses the same traits for its own layers. If you build on these traits directly, one rule matters: a frame that has been sealed into a TLS record has used up a record sequence number, so it must reach `try_send` (offered again if handed back). Dropping it makes every later record fail with `TxError::Broken`. The types make sure an unsealed frame cannot be sent.

The last page of this part puts all of it together in a real program: [Walkthrough: a market-data feed](binance-feed.md).
