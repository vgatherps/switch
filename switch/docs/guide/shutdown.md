# Shutting down

A program ends in the reverse of the order it started: the tasks go first, then the [`Net`](crate::net::Net), then the card.

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

## Let the tasks finish first

Shutting the runtime down drops tasks where they stand, and data still queued when the loop stops is never sent ([TCP streams](crate::guide::tcp)). If data must reach the peer, have each task call `shutdown` and await `delivered` on its connections, and keep the loop going round until the tasks are done.

## The steps

[`rt.shutdown()`](crate::Runtime::shutdown) drops all remaining tasks, in no particular order.

[`net.into_stack()`](crate::net::Net::into_stack) succeeds only when no `NetHandle`, socket half or unfinished open is alive. Otherwise it returns the `Net` in `Err`. Drop what is left, usually a handle or socket that `main` still holds or a task stored in a shared cell, and call `into_stack` again.

`stack.into_port()` drops the remaining sockets and everything still queued to send. `try_close()` then closes the card, or fails with `OutstandingBuffers { rx, tx, frame }` and returns the port if a frame is still held. Drop the frame and call `try_close` on the returned port.

Keep the `eal` value that [`open`](crate::nic::open) returned alive until `try_close` has succeeded.

The same order applies when the loop ended because a [round returned an error](crate::guide::drive_loop).

Next: [WebSocket](crate::guide::websocket).
