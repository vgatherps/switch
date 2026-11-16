# TCP streams

A `TcpStream` is an open TCP connection, from `tcp_connect` or `tcp_accept` ([Opening sockets](opening.md)). You read from it and write to it with [frames](frames.md), wait for room when the connection cannot take more data, and close it by shutting down and dropping its halves.

```text
   tcp_connect / tcp_accept
            |
            v
      established ---------------------------------------------+
       |        |                                              |
  rx.recv()   tx.room(len) + tx.try_send(frame)           tx.shutdown()
       |                                                       |  (FIN after queued data,
       |                                                       |   receiving continues)
       v                                                       v
  data frames, in order                                 tx.delivered().await
       |                                                 (everything incl. FIN ACKed,
       +-- peer sent FIN  ------> Err(Finished)            or the connection is gone)
       +-- peer sent reset -----> Err(InvalidState)
```

## Two halves

`split()` divides a stream into a receive half, `TcpRx`, and a send half, `TcpTx`. They are separate values, so one task can receive while another sends, or one task can hold both. Each half has at most one waiting operation at a time: its methods take `&mut self`. `TcpStream` also has `recv` directly, for code that only reads before splitting.

## Receiving

`rx.recv().await` returns the next piece of in-order data as a `TcpFrame`. Data that has already arrived is always delivered first. After that, `recv` tells you how the connection ended:

- `Err(Error::Finished)` when the peer sent FIN, the end of its stream, and did not reset the connection. Your side can still send.
- `Err(Error::InvalidState)` when the peer reset the connection, even if a FIN came before the reset, or when the socket is in a state with nothing to receive.

The first network program reads until `Finished`:

```rust,ignore
{{#include ../../../switch-net/examples/tcp_client_local.rs:recv}}
```

Dropping a `recv` future before it completes loses nothing; data stays queued for the next call.

## Sending

Sending takes two steps. `tx.room(len).await` waits until the socket has room for `len` bytes. `tx.try_send(frame)` then queues the frame at once, without waiting. If `try_send` fails, it returns the same frame together with the error, so you can keep the frame and try again. This helper from the test suite sends a buffer in chunks:

```rust,ignore
{{#include ../../../switch-net/tests/tx.rs:send_all}}
```

`room` resolves with:

- `Ok(())` when a frame of `len` bytes fits.
- `Err(Error::TooLarge)`, on the first poll, when `len` is larger than `tx.send_capacity()`, because it would never fit.
- `Err(Error::InvalidState)` once the socket can no longer send: it was reset or closed, or you called `shutdown`.

`try_send` can fail with `Full` (no room now), `InvalidState` (the socket cannot send), `Empty` (the frame has zero length) or `Foreign` (the frame came from another `Net`'s pool). `tx.is_on(&handle)` tells you whether a stream belongs to the same `Net` as a handle, and so whether that handle's frames are its own.

On the local backend, a frame longer than the connection's segment size goes out in segment-sized pieces. TCP borrows a piece within one queued frame while assembling the segment, but copies it into the outgoing packet. A segment spanning two queued frames also needs a copy to join those bytes first. A frame from `tx_frame()` is IP MTU − 28 bytes long, longer than a TCP segment, so a full-size frame always takes more than one segment. Fill at most `tx.max_segment_payload()` bytes to keep each frame to one segment and allow [in-place transmission](../overview/architecture.md#following-the-data). A frame small enough to fit into the end of the previous frame that has not been sent yet, within one segment, is copied there and freed at once, so many small writes share one segment.

## Backpressure

**Backpressure** is what happens when a send cannot be queued right now. It looks different on the two backends.

On the local backend, a queued frame (one not copied into the previous frame) stays in the socket until the peer acknowledges its last byte. Unacknowledged data is limited by the socket's `tx_capacity` and by the frame pool, which all sockets share. `room` waits until acknowledgements free enough bytes. The shared frame pool limits you earlier, at `tx_frame().await`. When `room(len)` resolves, a `try_send` of a frame that size succeeds until the next receive step.

On the junction backend, `try_send` copies the bytes into the shared send ring to the daemon and gives the frame back to the pool at once; the peer's window does not slow your sends. `room` is only a hint here: `try_send` can still return `Full` because the send ring is full. After any socket of the `Net` gets `Full`, `room` on every socket of that `Net` waits until the next receive step. The daemon keeps bytes the peer cannot take yet, with no size limit. A program that must bound memory on a long-lived connection paces its sends with acknowledgements in its own protocol. For a fixed amount of data, send it, call `shutdown`, then await `delivered()` ([What applications share](../junction/sharing.md)).

Code that works on both backends keeps the frame it got back and waits for room again, as switch-bench's echo server does:

```rust,ignore
{{#include ../../../switch-bench/src/serve.rs:tcp_echo}}
```

## Half-close and waiting for delivery

`tx.shutdown()` closes your sending side: the socket sends a FIN after everything already queued. You can still receive. After `shutdown`, `room` returns `InvalidState`.

`tx.delivered().await` resolves once the peer has acknowledged every byte and the FIN, or once the connection is gone and nothing more can be delivered. Until then the queued bytes exist only in the socket. If the program tears down the Stack before `delivered` resolves, those bytes are never sent.

```rust,ignore
tx.shutdown();
tx.delivered().await;
```

## Closing

Dropping both halves closes the socket the way Linux `close()` does. If received data is still unread, the connection is reset. Otherwise a FIN is sent. Dropping only the send half leaves the receive half working, and dropping only the receive half leaves a waiting sender in place.

On the junction backend the daemon applies the close asynchronously. Binding the same port again right after dropping a socket can race with the close and return `PortInUse`.

Next: [UDP sockets](udp.md).
