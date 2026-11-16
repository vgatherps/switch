# Shutting down

A program ends in the reverse of the order it started: the tasks go first, then the [`Net`](crate::net::Net), then the card. Each step requires that the one before it gave everything back.

```text
 1. rt.shutdown()                    drops every task, and with each task the
                                     sockets, handles and frames it holds
 2. drop what main itself holds      NetHandles, sockets, frames
 3. net.into_stack()                 needs: no NetHandle or socket left anywhere
 4. stack.into_port().try_close()    needs: no frame left anywhere
```

The end of the first network program is exactly this:

```rust
use switch::Runtime;
use switch::net::Net;

fn close(mut rt: Runtime, net: Net) -> Result<(), Box<dyn std::error::Error>> {
    rt.shutdown();
    let stack = net.into_stack().map_err(|_net| "a socket or handle outlived the tasks")?;
    stack.into_port().try_close().map_err(|(_port, error)| format!("port close: {error}"))?;
    Ok(())
}
```

## Before you start

Shutting the runtime down drops tasks where they stand. A connection held by a dropped task is closed as in [Dropping a stream](crate::guide::tcp), but the loop has stopped, so nothing queued on it is sent any more. If data must reach the peer, let the tasks finish first: have each call `shutdown` and await `delivered` on its connections, and keep the loop going round until they are done. A [oneshot channel](crate::guide::tasks) per task, or a shared flag the tasks check, tells them it is time. Put a time limit on that wait in the loop, as the first network program does with `LIMIT`.

## The steps

[`rt.shutdown()`](crate::Runtime::shutdown) drops all remaining tasks, in no particular order. After it, `poll` and `spawn` return `Closed`, and a [`JoinHandle`](crate::JoinHandle) for a task that had not finished gives `Err(Canceled)`.

[`net.into_stack()`](crate::net::Net::into_stack) succeeds only when the `Net` is the last thing referring to the network: no `NetHandle`, no socket half and no unfinished open is alive. Otherwise it returns the `Net` in `Err`, unchanged. That almost always means `main` still has a handle or a socket in a variable, or a task stored one in a shared cell. Drop it and call `into_stack` again.

`stack.into_port()` drops the remaining sockets and everything still queued to send. `try_close()` then closes the card and checks that every packet buffer is back. If a frame is still held somewhere, it fails with `OutstandingBuffers { rx, tx, frame }`, which counts the missing buffers of each kind, and returns the port with the error. Frames outlive the `Net`, so step 3 does not catch this. Drop the frame and call `try_close` on the returned port.

Keep the `eal` value that [`open`](crate::nic::open) returned alive until `try_close` has succeeded.

The same order applies when the loop ended because a [round returned an error](crate::guide::drive_loop).

Next: [WebSocket](crate::guide::websocket).
