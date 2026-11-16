# Sending and receiving messages

The [previous page](wss-client.md) ended with a connected `Wss`. This page uses it: how messages arrive, how long you may keep them, how sending works, and the one rule that surprises people coming from other WebSocket libraries: the connection only does work while your task is inside one of its methods. Everything here applies to `Ws` (`ws://`) the same way.

## Receiving

```rust,ignore
pub async fn recv(&mut self) -> Result<Option<Message<'_>>, Error>
```

`recv` waits for the next whole message. `Ok(Some(message))` is a message; `Ok(None)` means the close handshake has finished and the connection is over (see [Closing](closing.md)); `Err` means the connection failed.

A `Message` has four methods:

| Method | Returns |
|---|---|
| `kind()` | `Kind::Text` or `Kind::Binary` |
| `bytes()` | the payload, `&[u8]` |
| `text()` | `Some(&str)` for a Text message, `None` for Binary. The UTF-8 was checked when the bytes arrived, so this costs nothing. |
| `rx_timestamp()` | the [hardware timestamp](../nic/offloads.md) of the frame holding the message's first byte; 0 means none |

You only see Text and Binary messages. Ping, pong and close are WebSocket control frames; `recv` handles them itself and never returns one. Masking of client frames is handled for you as well.

The feed's read loop is a good model. It races `recv` against a stop signal with [`first`](../net/timeouts.md#racing-a-receive-against-a-stop-signal), records the latency from the hardware timestamp, and parses the text as JSON without copying it:

```rust,ignore
{{#include ../../../conduit/crates/switch-binance-feed/src/lib.rs:read}}
```

`first` returns `None` when the stop signal finishes first; the `recv` future is then dropped. That is safe: dropping a `recv` future at any point loses nothing. A message that was partly received stays buffered, and the next `recv` continues where the dropped one stopped.

## Keeping data past the next `recv`

A `Message<'_>` borrows the connection. Its bytes are, where possible, a slice of the packet buffer they arrived in, so the next `recv` cannot run while you hold the message. The compiler enforces this. The following does not compile:

```rust,ignore
{{#include ../../../switch-ws/tests/ui/second_recv_while_message_held.rs}}
```

If you need the data after the next `recv`, copy what you need (`message.bytes().to_vec()`, or parse it into owned values) and drop the message. The feed's `BookTicker` type borrows its strings from the message and is finished with them before the loop goes round.

## Sending

```rust,ignore
pub async fn send(&mut self, kind: Kind, payload: &[u8]) -> Result<(), Error>
```

`send` sends `payload` as one message:

```rust,ignore
wss.send(Kind::Text, sent.as_bytes()).await.expect("send");
```

`send` waits for [room](../net/tcp.md) in the TCP send buffer when the buffer is full; that is the backpressure, and `send` never returns a "full" error. If the peer stops reading, your `send` waits until it reads again.

A message goes out as one WebSocket message, in as many frames as it needs, and is never interleaved with another message. A message that does not fit one frame finishes during later calls (see the next section). `send` refuses some messages outright, with nothing sent:

| Error | Why |
|---|---|
| `InvalidText` | `Kind::Text` with bytes that are not UTF-8 |
| `TooBig` | the payload is over `Limits::max_send` |
| `Closed` | you already called `close` or `linger` |
| `Failed(f)` | the connection failed earlier; `f` is that first failure |

## No background task

There is no task behind the connection. It sends and receives only while your task is inside `send`, `close`, `recv` or `linger`. Whatever one call leaves unsent goes out during the next of those calls: the rest of a long message, the answer (pong) to a server's ping, the echo of the server's close.

```text
            your task calls ...          what goes out during the call
            ---------------------------  -------------------------------------
            send(m2)                     rest of m1 (if any), then m2
            recv()                       queued output (rest of a message,
                                         pong, close echo), then waits for
                                         the next message
            close(code, reason)          queued output, then the close frame
            linger()                     queued output, then the end of TLS
                                         and TCP
            (no call)                    nothing
```

Two consequences follow. First, a program that only sends must still call `recv` from time to time, or the server's pings go unanswered and output queued by earlier calls stays queued. Second, `send` and `recv` both take `&mut self`, so one connection cannot do both at once. If both ends send more than the other's receive window before either reads, both wait for room forever. Keep your sends within the window, or alternate them with reads.

A connection is used from one task.

## Cancellation

You may drop a `send` or `recv` future, for example when it loses a race in `first` or a `within` runs out.

A dropped `recv` loses nothing, as described above.

A dropped `send` depends on how far it got. A message is *accepted* once it is committed to the connection: sealed whole into one frame, or copied into the send buffer if it needs several frames. If the `send` future is dropped before that, nothing of the message is sent. Once it has been accepted, it goes out whole, and the next `send`, `close` or `recv` finishes it. A later `send` is always a separate message.

## Limits

`Limits` sets three sizes for a connection:

```rust,ignore
pub struct Limits {
    pub max_frame: usize,   // largest received WebSocket frame payload
    pub max_message: usize, // largest received message
    pub max_send: usize,    // largest message you may send
}
```

`Limits::default()` is 16 MiB per WebSocket frame, 64 MiB per received message and 64 MiB per sent message. A received WebSocket frame or message over its limit fails the connection with `Failed(Failure::Protocol(ProtocolError::FrameTooBig))` or `MessageTooBig`. A send over `max_send` is refused with `TooBig` and nothing is sent.

The receive buffer for messages that straddle frames grows only as bytes arrive, so a large `max_message` costs memory only when a large message actually comes. Messages that fit in one frame are not copied at all.

The next page ends connections and handles everything that can go wrong: [Closing connections and handling failures](closing.md).
