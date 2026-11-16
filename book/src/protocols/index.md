# TLS and WebSocket over switch-net

switch-net gives a task TCP streams: it opens one with `NetHandle::tcp_connect`, reads [frames](../net/frames.md) from it and sends frames into it. Many real services do not speak raw TCP, though. A market-data feed is usually a `wss://` URL: WebSocket messages inside TLS inside TCP. Two crates add those layers on top of a switch-net TCP stream:

- `switch-tls` is a TLS 1.3 client. It turns a connected TCP stream into an encrypted byte stream.
- `switch-ws` is a WebSocket (RFC 6455) client, for `ws://` and `wss://`, and a `ws://` server. It turns a TCP stream, or a TLS stream from `switch-tls`, into a stream of whole messages.

They run entirely inside your tasks, on the same drive loop as everything else. This page shows how the layers fit and which function to call. The following pages walk through a `wss://` client in detail, then a `ws://` server, then TLS on its own, and end with a complete application.

## The layers

Each layer takes the one below it and hands whole units of its own kind to the one above:

```text
 your task
    |   send(Kind::Text, b"...")         recv() -> Message<'_>
    v                                        ^
 switch-ws      WebSocket messages: Wss (wss://), Ws (ws:// client or server)
    |   plaintext bytes                      ^
    v                                        |
 switch-tls     TLS records: Tls { rx, tx, tcp }        (wss:// only)
    |   encrypted bytes                      ^
    v                                        |
 switch-net     TcpStream -> TcpRx / TcpTx: frames of TCP payload
    |                                        ^
    v                                        |
 the Stack in your process, or the junction daemon
```

On a `ws://` connection the TLS layer is missing and switch-ws reads TCP frames directly.

Three different units appear here, and they do not line up with each other. A *frame* is what switch-net gives you: the payload of one received TCP segment, held in a packet buffer. A *TLS record* is TLS's unit of encryption; one record can sit inside one frame or run across several. A *WebSocket frame* is WebSocket's unit on the wire, and a *message* is one whole WebSocket message, which the sender may split into several WebSocket frames. You only ever see messages; switch-ws joins the pieces for you.

```text
 TCP frames      |---- frame 1 ----|---- frame 2 ----|---- frame 3 ----|
 TLS records     |-- record A --|---- record B ----|--- record C -----|
 WebSocket       |hdr| message 1 |hdr|  message 2 (two WebSocket frames) |
```

In this picture record B straddles frames 1 and 2, and message 2 straddles records B and C.

## Which function to call

All entry points take a `TcpStream` that is already connected (or accepted), plus the `NetHandle` it came from. They split the stream themselves.

| You want | Call | You get |
|---|---|---|
| a `wss://` client | `switch_ws::net::connect(handle, stream, tls_config, host, port, path, limits)` | `Wss` |
| a `ws://` client | `switch_ws::net::connect_ws(handle, stream, host, port, path, limits)` | `Ws` |
| a `ws://` server | `switch_ws::net::accept_ws(handle, stream, limits)` | `Ws` |
| TLS with your own protocol on top | `switch_tls::net::tls_connect(handle, stream, tls_config, host, min_plain)` | `Tls` |

`Wss` and `Ws` have the same methods: `recv`, `send`, `close`, `linger` and `peer_close`. `Ws` also has `recv_with_sender` for echo-style servers.

The `switch_net` modules of both crates sit behind the crate feature `switch-net`. Without it, only the protocol cores build; they need no DPDK and are tested on any machine. A `wss://` client depends on switch-ws with the feature, and on rustls with a crypto provider and a set of root certificates. These are the lines the market-data feed uses:

```toml
switch-ws = { path = "<clone>/switch-ws", features = ["switch-net"] }
rustls = { version = "=0.23.43", default-features = false, features = ["ring", "std"] }
webpki-roots = "=1.0.9"
```

`<clone>` is the directory where you cloned this repository. Turning on switch-ws's `switch-net` feature turns on switch-tls's as well. For TLS without WebSocket, depend on `switch-tls` with the same feature.

The TLS client speaks TLS 1.3 only: give it a `rustls::ClientConfig` that allows TLS 1.3, and a connection that negotiates an older version fails. The WebSocket server is `ws://`. switch-ws asks for no extensions or subprotocols and fails the upgrade (`UpgradeError::Unrequested`) if a server picks one.

## Copies and timestamps

Received messages are, where possible, slices of the packet buffer they arrived in; sent messages are written straight into outgoing frames. [Sending and receiving messages](messages.md#where-copies-happen) shows what that means for you.

Each received frame carries a [hardware timestamp](../nic/offloads.md), the NIC's receive time (0 when the NIC gave none). That stamp survives the layers: `Message::rx_timestamp()` returns the stamp of the frame that held the message's first byte (over `wss://`, the frame holding the TLS record with that byte).

## Same task model as switch-net

Everything on these pages is an `async fn` awaited inside a switch task, and none of it makes progress on its own. A `Wss` sends and receives only while your task is inside one of its methods, and only when the drive loop keeps calling [`round`](../net/drive-loop.md). The [messages page](messages.md#no-background-task) explains what that means for pings and for long sends.

The protocols use only the `NetHandle` and `TcpStream` API, so they run on a `NetHandle` of either [backend](../net/backends.md): a Stack in your own process or the junction daemon.

The next page builds a `wss://` client step by step: [Connecting to a wss:// server](wss-client.md).
