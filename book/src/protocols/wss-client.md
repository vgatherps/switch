# Connecting to a wss:// server

This page connects a task to a `wss://` server, from opening the TCP socket to holding a `Wss` that can receive the first message, using the `connect` function from the [table on the previous page](index.md#which-function-to-call). The code comes from the market-data feed in `switch-binance-feed`, which the [walkthrough](binance-feed.md) reads end to end. You should already know how a task [opens a TCP connection](../net/opening.md) and how [`within`](../net/timeouts.md) bounds a wait.

A connect goes through four steps, each a round trip with the server:

```text
 task                                         server
  |  tcp_connect(...).await                      |
  |  ---- SYN --------------------------------->  |
  |  <--- SYN-ACK ------------------------------  |   TcpStream
  |  switch_ws::switch_net::connect(...).await    |
  |  ---- ClientHello -------------------------> |
  |  <--- ServerHello ... Finished -------------- |   TLS 1.3 handshake
  |  ---- Finished, GET /path Upgrade ---------> |
  |  <--- HTTP/1.1 101 Switching Protocols ------ |   WebSocket upgrade
  |                                               |   Wss
  |  wss.recv().await                             |
  |  <--- first message ------------------------- |
  |                                               |
  |<------------ within(&spawner, limit, ...) -->|   one bound for all of it
```

The first step is switch-net's; `connect` runs the TLS handshake and the upgrade.

## 1. The TLS configuration

switch-tls uses [rustls](https://docs.rs/rustls) for the cryptography and takes a ready `Arc<rustls::ClientConfig>`. You choose the crypto provider and the root certificates. The config must allow TLS 1.3 only. The feed builds its config with the `ring` provider and the Mozilla root certificates from `webpki-roots`:

```rust,ignore
{{#include ../../../conduit/crates/switch-binance-feed/src/lib.rs:client_config}}
```

Certificate checking is whatever this config does; switch-tls adds nothing to it. Build the config once and clone the `Arc` for each connection.

## 2. Open the TCP connection

The socket needs buffers big enough for the traffic you expect. The feed uses 256 KiB each way:

```rust,ignore
{{#include ../../../conduit/crates/switch-binance-feed/src/lib.rs:socket}}
```

The send buffer (`tx_capacity`) has a lower bound that matters here. Each frame must be able to carry the largest WebSocket control frame inside one TLS record, so a very small send buffer makes `connect` fail with `Config(RoomTooSmall)` before anything is sent. Any ordinary buffer size is far above that bound.

Then open the connection as on [Opening sockets](../net/opening.md). There is no ephemeral port, so pick a nonzero local port yourself.

## 3. Run `connect`

```rust,ignore
pub async fn connect(
    handle: &NetHandle,
    stream: TcpStream,
    config: Arc<ClientConfig>,
    host: &str,
    port: u16,
    path: &str,
    limits: Limits,
) -> Result<Wss, Error>
```

`host` is used twice: it is the TLS server name (sent in the handshake and checked against the certificate) and the HTTP `Host:` header of the upgrade request. `port` is the server's port and only appears in the `Host:` header, and only when it is not 443. `path` is the request path, such as `/ws/btcusdt@bookTicker`. `limits` caps message sizes; `Limits::default()` is right for most uses and is explained on the [messages page](messages.md#limits).

`connect` consumes the stream. It splits it, runs the TLS handshake, sends the upgrade request, reads the server's response, and returns a `Wss` once the server answered `101 Switching Protocols`. The handle must be the one the stream came from; a stream from another `Net` is refused as `Config(ForeignHandle)` before anything is sent.

## 4. Bound the whole connect

A connect can wait a long time: for the SYN-ACK, for the server's handshake, for the upgrade response. A failing handshake also waits. When the server's handshake is rejected, switch-tls sends the TLS alert and a FIN and returns the error only after the server has acknowledged them, or the connection has ended. A server that goes silent therefore keeps `connect` pending for as long as it stays silent. Always wrap the connect in `within`.

The feed puts TCP, TLS and WebSocket under one limit:

```rust,ignore
{{#include ../../../conduit/crates/switch-binance-feed/src/lib.rs:connect}}
```

The `within` used here is a small helper in the same file that turns a timeout into the feed's own error type:

```rust,ignore
{{#include ../../../conduit/crates/switch-binance-feed/src/lib.rs:within}}
```

When the limit passes, `within` drops the connect future, and with it the socket. Nothing else needs cleaning up.

## 5. When it fails

The errors that can come out of `connect` are all variants of `switch_ws::switch_net::Error`:

| Error | Meaning | What to do |
|---|---|---|
| `Tls(e)` | The TLS handshake failed, for example an unknown certificate issuer. The alert has been delivered. | Check the config's roots and the host name. |
| `Handshake(msg)` | The handshake could not start (the host is not a valid server name) or ended in an unexpected state. | Fix the host name. |
| `Upgrade(e)` | The server refused or garbled the WebSocket upgrade. `UpgradeError::Status(Some(code))` carries the HTTP status when it is not 101. | Check the path and the server; see below. |
| `Recv(e)`, `Send(e)` | TCP failed under the handshake or the upgrade. | Treat as a lost connection. |
| `Config(SetupError::RoomTooSmall { room, need })` | One frame cannot carry the largest control frame or TLS record. | Increase `tcp::Config::tx_capacity`. |
| `Config(SetupError::ForeignHandle)` | The stream belongs to another `Net`. | Pass the stream's own handle. |
| `Failed(Failure::Dead)` | The socket was already closed or reset. | Treat as a lost connection. |

A few upgrade failures are worth knowing by name. The upgrade response head may be at most 16 KiB; a longer one is `UpgradeError::HeadTooBig`. A server that selects an extension or a subprotocol fails with `UpgradeError::Unrequested`, since switch-ws asks for none. A response with the wrong `Sec-WebSocket-Accept` is `UpgradeError::WrongAccept`. Binance's servers answer HTTP 451 from restricted locations, which arrives as `Upgrade(UpgradeError::Status(Some(451)))`.

`Error` implements `Display`, so `"{e}"` prints a readable line.

## A plain ws:// client

For a server without TLS, call `connect_ws` instead. It takes no TLS config and returns a `Ws`, which has the same methods as `Wss`:

```rust,ignore
let mut ws = connect_ws(&handle, stream, HOST, SERVER_PORT, PATH, Limits::default()).await.expect("ws connect");
```

Here the `Host:` header carries the port when it is not 80.

With a `Wss` in hand, the next page reads and writes messages: [Sending and receiving messages](messages.md).
