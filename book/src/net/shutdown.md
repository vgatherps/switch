# Shutting down cleanly

A switch-net program ends by taking things apart in a fixed order. Done right, every packet buffer goes back to its pool and, on the junction backend, the daemon gets a clean goodbye. The order differs a little between the backends ([Local and junction backends compared](backends.md)), and each step can fail in one specific way. Closing a single connection is covered in [TCP streams](tcp.md#closing); here it is the whole program.

## The order

```text
 1. rt.shutdown()           drops every remaining task, and the sockets, handles
                            and frames they hold
 2. drop what you kept      sockets, NetHandles and frames held outside tasks
 3. take the backend back   local:    net.into_stack()   -> Stack
                            junction: net.into_session() -> Session
 4. close it                local:    stack.into_port().try_close()
                            junction: session.finish() and poll until closed()
```

Shut the runtime down first ([Panics, errors and shutdown](../runtime/errors.md#shut-the-runtime-down-before-closing-the-network)). Tasks own most sockets and handles, and dropping the tasks drops them.

`into_stack` and `into_session` succeed only when nothing else refers to the `Net`. If any `NetHandle`, any socket half, or any future still holding a socket is alive, they return `Err(net)`: you get the `Net` back unchanged. That error almost always means a socket or handle is still held outside the tasks, for example in a variable in `main` or in a shared cell.

Frames are different: they do not refer to the `Net`, and they stay valid after it is gone ([Frames](frames.md#received-frames)). You may keep them past step 3, but on the local backend they must be dropped before step 4.

## Local backend

The first network program ends like this:

```rust,ignore
{{#include ../../../examples/tcp_client_local.rs:teardown}}
```

`into_port()` turns the Stack back into the NIC port. It drops the Stack's sockets and any packets still waiting to be sent, so nothing more goes out; let `delivered` resolve first for any data that must reach the peer. `try_close()` then checks that every packet buffer has come back to the port's pools and closes the port. If a buffer is still out, it fails with `PortError::OutstandingBuffers`, which says how many RX, TX and frame buffers are missing, and returns the port with the error. Find the frame you still hold, drop it, and call `try_close` again. Keep the `Eal` value from `switch_open::open` alive until the port is closed. DPDK itself is never torn down; it is released when the process exits. [Opening the NIC from code](../nic/open.md) has more on the port and `Eal`.

## Junction backend

On the junction backend `into_session()` gives back the `Session`, and your code finishes it: call `finish()` and keep polling until `closed()` returns `true`. The junction version of the first network program, `switch-net/examples/tcp_client_junction.rs`, ends this way, with a time limit:

```rust,ignore
{{#include ../../../examples/tcp_client_junction.rs:teardown}}
```

`finish()` sends the end-of-session message once all earlier control messages are out. Until then it returns an error, which is why the example calls it again each time through the loop. `closed()` becomes `true` when the daemon acknowledges that the session ended cleanly. Dropping a `Session` without finishing it looks to the daemon like a crash: it resets the session's remaining connections. [Closing sockets and ending a session](../junction/closing.md) explains finishing in detail.

## What a failure means

| Failure | Meaning | What to do |
|---|---|---|
| `into_stack` / `into_session` returns `Err(net)` | a `NetHandle`, socket half or opening future is still alive | drop it, then try again |
| `try_close` returns `OutstandingBuffers` | your code still holds a frame | drop the frame, then call `try_close` on the returned port |
| `session.poll` returns an error while finishing | the session with the daemon was lost before it could end cleanly | nothing more to send; report it and exit |
| `closed()` does not become true within your limit | the daemon has not acknowledged the end | report it; bound the wait as the example does |

That completes the switch-net chapter. Next, [TLS and WebSocket over switch-net](../protocols/index.md) builds TLS and WebSocket connections on top of these TCP streams.
