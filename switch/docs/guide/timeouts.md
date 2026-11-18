# Timeouts and cancelling

No socket call takes a time limit. A limit is applied from outside with [`within`](crate::net::within) or [`first`](crate::net::first), which work on any future.

## A time limit

`within(spawner, limit, work)` runs `work` and returns `Some(output)`, or `None` if `limit` passes first.

```rust
use std::error::Error;
use std::time::Duration;

use switch::Spawner;
use switch::net::{tcp, within, IpEndpoint, IpListenEndpoint, NetHandle, TcpStream};

async fn connect(
    spawner: &Spawner,
    handle: &NetHandle,
    socket: tcp::Config,
    server: IpEndpoint,
    local: IpListenEndpoint,
) -> Result<TcpStream, Box<dyn Error>> {
    let connecting = handle.tcp_connect(socket, server, local);
    let stream = within(spawner, Duration::from_secs(5), connecting)
        .await
        .ok_or("connect timed out")??;
    Ok(stream)
}
```

`work` can be an `async` block, so one limit can cover several steps:

```rust
use std::error::Error;
use std::time::Duration;

use switch::Spawner;
use switch::net::{within, TcpRx, TcpTx};

async fn finish(spawner: &Spawner, rx: &mut TcpRx, tx: &mut TcpTx) -> Result<(), Box<dyn Error>> {
    let closing = async {
        tx.shutdown();
        tx.delivered().await;
        while rx.recv().await.is_ok() {}
    };
    within(spawner, Duration::from_secs(2), closing).await.ok_or("close timed out")?;
    Ok(())
}
```

The limit is a [sleep](crate::guide::tasks) on the task's runtime, whose ticks must be nanoseconds ([The drive loop](crate::guide::drive_loop)). If `work` is ready in the same poll in which the limit expires, `work` wins.

## Whichever comes first

`first(a, b)` is a two-way select biased towards `a`. It returns `Some(output of a)`, or `None` if `b` finishes first. Here a task reads a stream until it ends or until the loop in `main` sends on a [oneshot channel](crate::guide::tasks):

```rust
use switch::net::{first, Error, TcpRx};
use switch::oneshot::Receiver;

async fn read_until_stopped(rx: &mut TcpRx, mut stop: Receiver<()>) -> Result<(), Error> {
    loop {
        match first(rx.recv(), &mut stop).await {
            Some(Ok(frame)) => print!("{}", String::from_utf8_lossy(frame.payload())),
            Some(Err(Error::Finished)) | None => return Ok(()),
            Some(Err(error)) => return Err(error),
        }
    }
}
```

## What cancelling does

When the limit passes or `b` wins, the other future is dropped. The effect depends on the future:

| Dropped while waiting | Effect |
|---|---|
| `tcp_connect`, `udp_bind` | The socket being opened is closed. |
| `accept` | Nothing is lost. Waiting connections stay in the listener. |
| `recv` on TCP or UDP | Nothing is lost. Data stays queued for the next `recv`. |
| `room`, `delivered`, `tx_frame` | Nothing is lost. Call it again. |
| An `async` block of yours | Cancellation drops values the block owns. Borrowed sockets stay with the caller. Operations that already took effect stay in effect. |

Next: [Shutting down](crate::guide::shutdown).
