TCP and UDP sockets for tasks, and the loop that moves their packets.

The program's loop owns a [`Net`] and calls [`round`] on it: receive packets, run the tasks, send what they queued. Tasks hold a [`NetHandle`], which opens sockets and hands out the frames they send.

- [`Net`]: the network as the loop sees it. Made with [`Net::new`] from an opened card, or [`Net::remote`] from a session with the sharing daemon.
- [`round`]: one trip through the loop.
- [`NetHandle`]: `tcp_connect`, `tcp_accept`, `udp_bind` and `tx_frame`, for tasks.
- [`TcpStream`], with its halves [`TcpRx`] and [`TcpTx`]: an open TCP connection.
- [`UdpSocket`], with [`UdpRx`] and [`UdpTx`]: a bound UDP port.
- [`OwnedFrame`] and [`ImmutableFrame`]: the packet buffers data is sent and received in.
- [`within`] and [`first`]: a time limit on a wait, and stopping a wait when something else finishes.
- [`Error`]: every way a socket call can fail.

```rust
use switch::net::{Error, NetHandle, TcpTx};

/// Sends `line`, which must fit in one frame, on an open connection.
async fn send_line(handle: &NetHandle, tx: &mut TcpTx, line: &[u8]) -> Result<(), Error> {
    let mut frame = handle.tx_frame().await;
    frame.as_mut()[..line.len()].copy_from_slice(line);
    frame.truncate(line.len());
    tx.room(line.len()).await?;
    tx.try_send(frame.freeze()).map_err(|(_frame, error)| error)
}
```

The guide teaches this module from [Your first network program](crate::guide::first_connection) through [Shutting down](crate::guide::shutdown); start with [The drive loop](crate::guide::drive_loop) and [Opening sockets](crate::guide::sockets).
