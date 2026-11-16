# Local and junction backends compared

switch-net runs the same task code over two backends. With the local backend your process owns the NIC and runs the Stack itself. With the junction backend the junction daemon owns the NIC and runs the Stack, and your process talks to it through shared memory. The earlier pages of this chapter, through [Errors](errors.md), mention each difference where it comes up. This page collects them in one place, for choosing a backend and for moving code from one to the other.

## What is the same

Both backends use the same types and the same rules for everything a task touches: `NetHandle`, `TcpStream`, `UdpSocket`, frames, and the `Error` codes. Received data arrives in order and ends the same way (`Finished` after FIN, `InvalidState` after a reset). `shutdown` and `delivered` mean the same thing. Received frames outlive the `Net` on both. Each socket half allows one waiting operation, and dropping a future cancels it the same way. The drive loop is the same `switch_net::round`.

## Where data waits

The biggest difference is where outgoing data waits, and so where backpressure comes from:

```text
 local backend                                   junction backend

  task --try_send--> socket send queue            task --try_send--> copy into send ring
                     (frame held until ACK)                          (frame back to pool)
                           |                                               |
          bounded by tx_capacity and                    bounded by ring space shared with
          the shared frame pool                         other sockets and applications
                           |                                               v
                           v                                  daemon: Stack, its own queue
                     Stack -> NIC                             per socket -> NIC
```

Locally, a frame stays with the socket until the peer acknowledges it, so the socket's send buffer and the frame pool slow the sender down. Through the daemon, `try_send` copies the bytes into the send ring and returns the frame at once; `Full` means the ring is full, and the peer's window does not slow you down.

## The differences

| Behavior | Local backend | Junction backend |
|---|---|---|
| Who owns the NIC | your process (the Stack is inside the `Net`) | `switch-junctiond` |
| Create the `Net` | `Net::new(stack, NetConfig)` | `Net::remote(session, RemoteConfig)` |
| Tear down | `into_stack`, then close the port | `into_session`, then finish the session |
| `Net::poll_at()` | the Stack's next timer | always `None` (timers run in the daemon) |
| `rx_staged`, `tx_pending`, `rx_rejected` in a turn | real counts | always 0 |
| Opening errors | all on the first poll | client-side ones on the first poll; daemon rejections after a round trip |
| Invalid `tcp::Config` | the Stack panics when adding the socket | `Error::InvalidConfig` |
| Frames from `tx_frame` | the Stack's frame pool, shared by all sockets; length IP MTU − 28 | `RemoteConfig::tx_frames` buffers of the daemon's frame capacity |
| A sent TCP frame | held by the socket until acknowledged | copied into the send ring, returned at once |
| `send_capacity()` | the socket's send buffer | the largest record (one message in the send ring) the session allows, minus 16 bytes |
| What `room` promises | a fitting `try_send` succeeds until the next turn | a hint; `try_send` can still return `Full` |
| After one `Full` | only that socket waits | `room` on every socket of the `Net` waits until the next turn |
| Many sends in one poll | the poll ends early so the loop turns sooner | no equivalent |
| UDP `NoRoute`, `NeighborPending`, `Unaddressable` | returned by `send` | the daemon drops the datagram |
| `UdpTx::stats()` | real drop counts | always zeros |
| Received frames held | hold RX pool buffers | hold receive ring space shared by the session |
| Closing a socket | the Stack closes it; the slot frees when it finishes closing | the daemon closes it asynchronously; an immediate rebind can get `PortInUse` |
| Losing the daemon | does not apply | `HostLost`, then `InvalidState` on sockets |

The repository's tools choose the backend on the command line with `--backend conduit|junction`. `conduit` there means the local backend.

## Choosing

Own the NIC with the local backend when one program uses the network card and you want the shortest path: packets go between the NIC and your tasks on one CPU, with no other process involved. [Owning a NIC](../nic/index.md) shows how to set it up.

Share the NIC through the junction daemon when several programs need the same card. The daemon runs the Stack on its own pinned CPU, and each application connects a session to it. [Sharing a NIC with the junction daemon](../junction/index.md) explains how to run it, and [What applications share](../junction/sharing.md) what applications sharing one daemon see of each other.

Next: [Shutting down cleanly](shutdown.md).
