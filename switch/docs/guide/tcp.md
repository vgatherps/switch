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

`recv` resolves to the next data from the peer, in order, as a [frame](crate::guide::frames). Each frame holds payload from a single received segment.

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

Data that has arrived is delivered before `recv` reports how the stream ended, which is `Finished` after the peer's FIN and `InvalidState` after an RST. Sending is still possible after `Finished`.

## Sending

To send, wait in `room(len)` until the connection can take `len` more bytes, then queue the frame with `try_send(frame)`, which does not wait. This function sends a buffer of any length, a frame at a time (from the example `tcp_echo_server`):

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

`max_segment_payload` is the connection's effective MSS, and a frame no longer than that can go out as one segment.

`room` fails with `InvalidState` once the connection can no longer send, and with `TooLarge` if `len` is more than the socket's `tx_capacity`. `try_send` fails with `Full` if the caller did not wait for room, and returns the frame with the error.

## When the peer is slow

The socket holds at most `tx_capacity` bytes that are queued or unacknowledged, and `room` waits while it is full, so backpressure from a slow peer reaches the task as a wait in `room`.

## Finishing

```rust
# use switch::net::TcpTx;
# async fn finish(mut tx: TcpTx) {
tx.shutdown();
tx.delivered().await;
# }
```

`shutdown` half-closes the connection, queuing a FIN behind the data already queued while receiving continues. `delivered` resolves once the peer has acknowledged every byte and the FIN, or once the connection is gone and nothing more can arrive there.

Wait for `delivered` before the program exits, because data still queued when the loop stops is never sent.

## Dropping a stream

Dropping both halves closes the connection. If received data is still unread, the peer gets an RST. Otherwise queued data and the FIN are still sent in later rounds. Dropping one half leaves the other working.

When the program owns the card, a dropped connection keeps its place in `max_sockets` and its 4-tuple until TCP has finished closing it, which can take ten seconds or more.

## When the peer is silent

A connection that a task holds has no retransmission limit and no keep-alive. `connect`, `recv`, `room` and `delivered` can therefore each wait forever. [Timeouts and cancelling](crate::guide::timeouts) shows how to bound them.

Small writes are sent at once; there is no Nagle delay.

Next: [UDP sockets](crate::guide::udp).
