WebSocket over a TCP stream: a client for `wss://` and `ws://` servers, and a `ws://` server.

Each function in [`net`] takes a connected or accepted [`TcpStream`](crate::net::TcpStream), runs the upgrade, and returns a connection with `recv`, `send` and `close`.

- [`net::connect`]: a `wss://` client connection, a [`net::Wss`]. TLS 1.3 comes from [`switch::tls`](crate::tls).
- [`net::connect_ws`]: a `ws://` client connection, a [`net::Ws`].
- [`net::accept_ws`]: the server side of `ws://`, also a [`net::Ws`].
- [`Message`]: one received message, text or binary.
- [`Kind`]: text or binary, for `send`.
- [`Limits`]: the largest message accepted and sent.
- [`net::Error`]: what can fail, during the upgrade and afterwards.

```rust,no_run
use switch::net::{NetHandle, TcpStream};
use switch::ws::net::{accept_ws, Error};
use switch::ws::Limits;

/// Serves one accepted connection: sends every message back.
async fn echo(handle: &NetHandle, stream: TcpStream) -> Result<(), Error> {
    let mut ws = accept_ws(handle, stream, Limits::default()).await?;
    while let Some((message, mut sender)) = ws.recv_with_sender().await? {
        sender.send(message.kind(), message.bytes()).await?;
    }
    Ok(())
}
```

[WebSocket](crate::guide::websocket) covers connecting, messages, closing and failures. [A complete program](crate::guide::feed) is a `wss://` client from its first line to its last.
