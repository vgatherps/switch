# Serving ws:// connections

So far the pages have been about clients. switch-ws also has a server side for plain `ws://`: it takes an accepted TCP connection, answers the client's upgrade request, and gives you the same `Ws` type the client uses. This page builds an echo server that sends every message back without copying it into a buffer of its own. It assumes you have read [Closing connections and handling failures](closing.md) and the accept pattern on [Opening sockets](../net/opening.md).

There is no `wss://` server: switch-tls is a TLS client only.

## Accepting

switch-net has no listen queue. `NetHandle::tcp_accept` opens one listening socket that turns into exactly one connection. To serve many clients, open the next listener as soon as one accepts, before you start serving the connection, so that the next client's SYN finds a listener waiting.

The echo server in `switch-bench` does this with one task per connection. Each task waits for its listener, arms the next one, and then serves:

```rust,ignore
{{#include ../../../switch-bench/src/serve.rs:arm_accept}}
```

```text
 task 1: accept ---> connection 1 ---> arm accept 2 ---> upgrade, echo ... done
                                          |
 task 2:                                  accept ---> connection 2 ---> arm accept 3 ---> ...
```

## Upgrading

```rust,ignore
pub async fn accept_ws(handle: &NetHandle, stream: TcpStream, limits: Limits) -> Result<Ws, Error>
```

`accept_ws` reads the client's HTTP request and, if it is a valid WebSocket upgrade, answers `101 Switching Protocols` and returns a `Ws`. It accepts any valid upgrade, whatever the path, and selects no extension and no subprotocol. A request that is not a valid WebSocket upgrade (not a GET, not HTTP/1.1, no `Sec-WebSocket-Key`, a version other than 13, a head over 16 KiB, ...) returns `Error::Upgrade` with the reason, and nothing is sent back. The setup errors from [the client page](wss-client.md#5-when-it-fails) (`Config(RoomTooSmall)`, `Config(ForeignHandle)`, `Failed(Dead)`) apply here too.

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

A client that breaks the protocol needs more care. switch-ws sends no close frame in that case, and the client may keep sending. If you drop the connection with its data unread, the socket is aborted with a TCP reset. Call `linger` first: it sends FIN, reads and drops what the client still sends until the client's FIN, and then the connection can be dropped quietly. The repository's driver for Autobahn, the standard WebSocket conformance test suite, handles each ending this way in its echo loop:

```rust,ignore
{{#include ../../../switch-ws/src/bin/switch-ws-autobahn/main.rs:echo}}
```

That driver serves one connection at a time and bounds each one with `within`:

```rust,ignore
{{#include ../../../switch-ws/src/bin/switch-ws-autobahn/main.rs:server}}
```

How that driver is used to check switch-ws against the Autobahn conformance suite is described in [Protocol conformance](../testing/conformance.md).

The next page drops the WebSocket layer and uses TLS on its own: [TLS without WebSocket](tls.md).
