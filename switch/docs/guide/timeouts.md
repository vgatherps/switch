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

The first `?` handles the timeout and the second the connect's own error.

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

The limit is a [sleep](crate::guide::tasks) on the task's runtime, so it is as punctual as any sleep, and the runtime's ticks must be nanoseconds ([The drive loop](crate::guide::drive_loop)). If `work` is ready in the same poll in which the limit expires, `work` wins.

## Whichever comes first

`first(a, b)` waits for both futures and returns `Some(output of a)`, or `None` if `b` finishes first. It usually serves to stop a wait when something else happens. Here a task reads a stream until it ends or until the loop in `main` sends on a [oneshot channel](crate::guide::tasks):

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

`a` is checked before `b`, so data that has already arrived is printed before a stop is obeyed. `stop` is passed as `&mut stop` so that when `recv` wins, only the borrow is dropped and the receiver is still there for the next iteration.

## What cancelling does

When the limit passes or `b` wins, the other future is dropped, which is how a wait is cancelled in switch. The effect depends on the future:

| Dropped while waiting | Effect |
|---|---|
| `tcp_connect`, `tcp_accept`, `udp_bind` | The socket being opened is closed. |
| `recv` on TCP or UDP | Nothing is lost. Data stays queued for the next `recv`. |
| `room`, `delivered`, `tx_frame` | Nothing is lost. Call it again. |
| An `async` block of yours | Everything the block owns is dropped, so its sockets close as in [Dropping a stream](crate::guide::tcp) and its frames return to the pool. What it only borrowed stays with its owner, and steps it had finished are not undone. |

Sends cannot be cancelled halfway, because `try_send` and UDP's `send` do not wait and a frame is either queued or handed back.

Next: [Shutting down](crate::guide::shutdown).
