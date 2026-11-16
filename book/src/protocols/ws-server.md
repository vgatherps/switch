# Serving ws:// connections

A switch-ws server takes an accepted TCP connection, answers the client's `ws://` upgrade request, and gives you the same `Ws` type the client uses. This page shows a ws:// echo server piece by piece; `switch-bench serve --proto ws` runs a complete one (source `switch-bench/src/serve.rs`). It follows [Closing connections and handling failures](closing.md).

## Accepting

Connections are accepted as on [Accepting](../net/opening.md#accepting): each `tcp_accept` is one listening socket that becomes one connection, so the next accept is armed before this connection is served. The accept loop from the TCP echo example:

```rust,ignore
{{#include ../../../switch-net/examples/tcp_echo_server.rs:accept_loop}}
```

## Upgrading

```rust,ignore
pub async fn accept_ws(handle: &NetHandle, stream: TcpStream, limits: Limits) -> Result<Ws, Error>
```

`accept_ws` reads the client's HTTP request and, if it is a valid WebSocket upgrade, answers `101 Switching Protocols` and returns a `Ws`. It accepts any valid upgrade, whatever the path, and selects no extension and no subprotocol. A request that is not a valid WebSocket upgrade (not a GET, not HTTP/1.1, no `Sec-WebSocket-Key`, a version other than 13, a head over 16 KiB, ...) returns `Error::Upgrade` with the reason, and nothing is sent back. The setup errors from [the client page](wss-client.md#when-it-fails) (`Config(RoomTooSmall)`, `Config(ForeignHandle)`, `Failed(Dead)`) apply here too.

For a WebSocket server, the task that serves an accepted stream first passes it to `accept_ws`, where the TCP example's `serve` task starts its byte echo. In switch-bench's server that line is:

```rust,ignore
match accept_ws(&handle, stream, Limits::default()).await {
```

The upgrade itself is not bounded by `accept_ws`. A client that connects and never sends a request keeps `accept_ws` waiting, so a server that must not leak connections wraps it in [`within`](../net/timeouts.md).

## Echoing without a copy

A `Message` borrows the connection, so normally you cannot call `send` while you hold one. For echo-style servers, `Ws` has a second receive method that hands you a sender along with the message:

```rust,ignore
pub async fn recv_with_sender(&mut self) -> Result<Option<(Message<'_>, WsSender<'_>)>, Error>
```

`WsSender::send(kind, payload)` behaves exactly like `Ws::send`, and `payload` may borrow the message. The bytes go from the received packet buffer straight into the outgoing frame, with no buffer of your own in between:

```rust,ignore
{{#include ../../../switch-bench/src/serve.rs:ws_echo}}
```

`recv_with_sender` exists on `Ws` only.

## Ending connections

A client that closes normally makes `recv_with_sender` (or `recv`) return `Ok(None)` after the close echo and FIN have been acknowledged; the task can return and drop the `Ws`.

A client that breaks the protocol needs more care. switch-ws sends no close frame in that case, and the client may keep sending. If you drop the connection with its data unread, the socket is aborted with a TCP reset. Call `linger` first: it sends FIN, reads and drops what the client still sends until the client's FIN, and then the connection can be dropped quietly:

```rust,ignore
Err(Error::Failed(WsFailure::Protocol(_))) => {
    return match ws.linger().await {
        Ok(()) => "protocol error",
        Err(_) => "protocol error, then receive failed",
    };
}
```

That arm comes from the repository's driver for the Autobahn conformance suite, described in [Protocol conformance](../testing/conformance.md).

The next page drops the WebSocket layer and uses TLS on its own: [TLS without WebSocket](tls.md).
