# What applications share

Applications connected to the same daemon share one NIC, one Stack and one set of rings. Most of the time they do not notice each other. This page explains the places where they can: what a successful send means, how to pace TCP, why a close can wait, what holding received frames costs, and which limits are global. It assumes the [previous page](apps.md) and [TCP streams](../net/tcp.md).

## A send, step by step

```text
 your task                 send ring                 daemon network thread            wire
 ---------                 ---------                 ---------------------            ----
 try_send(frame) --copy--> record ----------------->  takes records in order,
   Ok: the record is here   (Full shows up here,       hands bytes to the Stack ------> peer
   Full: no room right now   and only here)            keeps what the Stack cannot
                                                       take yet, sends it later

 wire                      daemon network thread       receive ring          your task
 ----                      ---------------------       ------------          ---------
 peer ----------------->   Stack receives, writes ---> record -------------> recv() gives a frame
                           one record per chunk        space returns when
                           or datagram                 the frame is dropped
                                                       and the next turn runs
```

## What `Ok` and `Full` mean

When `try_send` (TCP) or `send` (UDP) returns `Ok` on the junction backend, the bytes have been copied into the send ring, and nothing more. The daemon has not looked at them yet, and the peer has certainly not received them. The frame you passed in has already gone back to your frame pool.

`Full` means the send ring has no room for this record right now. The send ring is shared by every session on the daemon, so another application sending fast can fill it as easily as your own program. Keep the frame that came back with the error and try again after the next turn. switch-net helps with this: after any socket of a `Net` sees `Full`, `room` on every socket of that `Net` waits until the next turn.

`Full` says nothing about the peer. A peer that reads slowly, or a TCP window that is closed, does not make `try_send` fail or wait. This is the main difference from the local backend, where `room` waits until the peer acknowledges data.

## Pacing TCP

The daemon gives each TCP socket's bytes to the Stack as fast as the Stack accepts them. Bytes the Stack cannot take yet, because its send buffer for that socket is full, wait in a queue inside the daemon. That queue has no size limit. An application that keeps sending to a peer that does not read keeps getting `Ok`, and the daemon's memory grows.

An application that must bound this memory paces its TCP sends itself, with one of two signals:

- its own application-level acknowledgements from the peer (a request/response protocol does this naturally: send the next request after the reply arrives), or
- `tx.shutdown()` followed by `tx.delivered().await`, which resolves once the peer has acknowledged every byte and the FIN. This fits a sender that has a fixed amount of data to send.

## Closing sockets

Dropping both halves of a TCP stream closes it. On the junction backend the daemon does not apply that close immediately. It waits until the shared send ring is idle, so that every byte any session committed before the close is applied first, and until the socket's own queued bytes have gone to the Stack. Then it sends the FIN and frees the socket.

A consequence is that one application's close or `shutdown` can wait while another application keeps sending. This is how the daemon keeps every close behind the data that preceded it, and it is the intended behavior. A reset (dropping a stream with unread received data) does not wait.

Because closes are applied later, opening a socket on the same port right after closing one can fail with `Error::PortInUse`: the old socket still exists in the daemon. A program that reuses ports should retry after a short delay. With a raw `Session`, wait for the `Freed` event, described in [Using a Session directly](session.md).

## Receive ring space

Each session has one receive ring, `--rx-size` bytes, which the daemon fills with the data it receives for all of that session's sockets. A received frame is a view into that ring. Its space becomes free again only when the frame is dropped and a later turn reclaims it, and reclaiming goes in order: space after the oldest frame still held cannot be reused until that frame is dropped too.

So holding received frames shrinks the receive ring for the whole session, not only for the socket the frames came from. When the ring is full, the daemon keeps the next pending chunk or datagram for each socket and retries on every pass. TCP data meanwhile waits in the daemon, and TCP's own flow control slows the peer. Notices that a peer closed the connection also wait, because they are sent only after the data before them is in the ring. Copy out what you need to keep for a long time and drop the frame.

## Limits shared across sessions

Some limits belong to the daemon as a whole:

| Limit | Set by | What you see when it is reached |
|---|---|---|
| TCP sockets in the Stack | `--tcp-sockets` | `Error::SocketsFull` when opening, even if your own session's socket table has free entries |
| UDP sockets in the Stack | `--udp-sockets` | `Error::SocketsFull` when binding |
| sessions | `--max-sessions` | `ConnectError::Rejected(SessionsFull)` when connecting |
| send ring space | 1 MiB per CPU | `Error::Full` when sending |

Each session also has its own table of `max_sockets` entries; running out of those also gives `Error::SocketsFull`.

## Control events and data

Opens, closes and the events that answer them (such as "connected" or "peer closed") travel over the control socket, which the daemon's control thread serves about once a millisecond. Packet data does not go that way. So events can trail the data they relate to by up to about a millisecond, while data itself moves as fast as the drive loops of the daemon and the application turn. switch-net hides the ordering consequences; [Using a Session directly](session.md#event-and-packet-ordering) spells them out for raw sessions.

## Delivery guarantees

Delivery guarantees are TCP's and UDP's; the daemon passes data between the send ring, the Stack and the receive ring and keeps no acknowledgements of its own. A UDP datagram the Stack cannot send, for example because the destination's hardware address is not resolved yet, is dropped after `send` returned `Ok`, and switch-net's `UdpTx::stats()` reports zeros on the junction backend.

Next: [Using a Session directly](session.md).
