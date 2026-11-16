# TCP streams

A [`TcpStream`](crate::net::TcpStream) is an open connection. `split` divides it into a receiving half, [`TcpRx`](crate::net::TcpRx), and a sending half, [`TcpTx`](crate::net::TcpTx), so one task can hold both or two tasks can hold one each.

```text
        tcp_connect / tcp_accept
                  |
                  v
  rx.recv() <-- open --> tx.room(len), tx.try_send(frame)
      |                         |
      | data frames, in order   | tx.shutdown()        your side is finished
      |                         v
      | Err(Finished)         tx.delivered().await    the peer has all of it
      v  the peer is finished
  drop both halves: the connection is closed
```

## Receiving

`recv` resolves to the next data from the peer, in order, as a [frame](crate::guide::frames). A frame holds whatever one packet carried; TCP does not keep your message boundaries, so a protocol on top has to find them itself.

```rust
use switch::net::{Error, TcpRx};

async fn print_all(rx: &mut TcpRx) -> Result<(), Error> {
    loop {
        match rx.recv().await {
            Ok(frame) => print!("{}", String::from_utf8_lossy(frame.payload())),
            Err(Error::Finished) => return Ok(()),
            Err(error) => return Err(error),
        }
    }
}
```

Data that has arrived is always delivered first. After it, `recv` says how the stream ended: `Finished` when the peer closed its side normally, `InvalidState` when the connection was reset. After `Finished` you can still send.

## Sending

Sending is two calls. `room(len)` waits until the connection can take `len` more bytes. `try_send(frame)` then queues the frame without waiting, and the frame leaves at the end of the [round](crate::guide::drive_loop). This function sends a buffer of any length, a frame at a time (from the example `tcp_echo_server`):

```rust
use switch::net::{Error, NetHandle, TcpTx};

async fn send_all(handle: &NetHandle, tx: &mut TcpTx, mut rest: &[u8]) -> Result<(), Error> {
    let max_payload = tx.max_segment_payload().ok_or(Error::Closed)?;
    while !rest.is_empty() {
        let mut frame = handle.tx_frame().await;
        let len = rest.len().min(max_payload).min(frame.as_mut().len());
        frame.as_mut()[..len].copy_from_slice(&rest[..len]);
        frame.truncate(len);
        tx.room(len).await?;
        tx.try_send(frame.freeze()).map_err(|(_frame, error)| error)?;
        rest = &rest[len..];
    }
    Ok(())
}
```

`max_segment_payload` is the most bytes one TCP packet of this connection carries. Filling each frame up to that keeps one frame to one packet.

`room` fails with `InvalidState` once the connection can no longer send, and with `TooLarge` if `len` is more than the socket's `tx_capacity`. `try_send` fails with `Full` if you did not wait for room. Its error comes with the frame, as the `map_err` above shows.

## When the peer is slow

A sent frame stays with its socket until the peer acknowledges it. The socket holds at most `tx_capacity` bytes that are queued or unacknowledged, and `room` waits while it is full. That wait is how a slow reader slows your sender down: when the peer stops reading, its window closes, acknowledgements stop freeing space, and your task stays in `room` until the peer reads again.

A task that must not wait behind a slow peer puts a [time limit](crate::guide::timeouts) on `room`.

## Finishing

```rust
# use switch::net::TcpTx;
# async fn finish(mut tx: TcpTx) {
tx.shutdown();
tx.delivered().await;
# }
```

`shutdown` closes your sending side: the peer sees the end of your stream after everything already queued. Receiving continues. `delivered` resolves once the peer has acknowledged every byte and the end of the stream, or once the connection is gone and nothing more can arrive there.

Wait for `delivered` before the program exits. Queued data lives only in the socket until then, and it is sent by the loop, so data still queued when the loop stops is never sent.

## Dropping a stream

Dropping both halves closes the connection, like `close()` on a Linux socket. If received data is still unread, the peer gets a reset. Otherwise queued data and the end of the stream are still sent in later rounds, with a limited number of retries. Dropping one half leaves the other working.

A dropped connection keeps its place among the program's sockets, and its local port, until TCP has finished closing it, which can take ten seconds or more. A program that reconnects often gives each attempt a new local port and reserves more than one place in `max_sockets`.

## Timers

| Behavior | Value |
|---|---|
| Retransmission | 300 ms after the first send; once the connection has measured its round-trip time, that time plus a margin of at least 50 ms. Each further retransmission doubles the wait, up to 60 s. |
| Giving up | A connection your task holds never gives up; it retransmits until the peer answers. A dropped connection gives up after 8 doublings. |
| Keep-alive | None. An idle connection sends nothing, so a peer that vanished is noticed only when you send. |
| Delayed acknowledgement | 10 ms |
| Small writes | Sent at once; there is no Nagle delay. |
| After you close first | The connection lingers 10 s before its port can be used again for the same peer. |

Because a held connection never gives up, `connect`, `recv`, `room` and `delivered` can each wait forever on a silent peer. [Timeouts and cancelling](crate::guide::timeouts) shows how to bound them.

Next: [UDP sockets](crate::guide::udp).
