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
                                                       and the next receive step runs
```

## What `Ok` and `Full` mean

When `try_send` (TCP) or `send` (UDP) returns `Ok` on the junction backend, the bytes have been copied into the send ring as one [record](../reference/glossary.md), and nothing more. The daemon has not looked at them yet, and the peer has not received them. The frame you passed returns to your frame pool, unless you kept a clone or slice of it.

`Full` means the send ring for the CPU your thread is running on has no room right now. There is one 2 MiB ring per CPU, and every session sending from that CPU writes into it, so another application on the same CPU can fill it as easily as your own program. Applications pinned to different CPUs use different rings. Keep the frame that came back with the error and try again in the next [round](../net/drive-loop.md). switch-net helps with this: after any socket of a `Net` sees `Full`, [`room`](../net/tcp.md) on every socket of that `Net` waits until the receive step of the next round, which wakes it to try again.

`Full` says nothing about the peer. A peer that reads slowly, or a TCP window that is closed, does not make `try_send` fail or wait. This is the main difference from the local backend, where `room` waits until the peer acknowledges data.

## Pacing TCP

The daemon gives each TCP socket's bytes to the Stack as fast as the Stack accepts them. Bytes the Stack cannot take yet, because its send buffer for that socket is full, wait in a queue inside the daemon. That queue has no size limit. An application that keeps sending to a peer that does not read keeps getting `Ok`, and the daemon's memory grows.

An application that must bound this memory paces its TCP sends itself, with one of two signals:

- its own application-level acknowledgements from the peer (a request/response protocol does this naturally: send the next request after the reply arrives), or
- `tx.shutdown()` followed by `tx.delivered().await`, which resolves once the peer has acknowledged every byte and the FIN, or when the connection or session is lost and no more can be delivered. This fits a sender that has a fixed amount of data to send. When you need to know the peer received or processed the data, use a reply in your own protocol.

## Closing sockets

Dropping both halves of a TCP stream closes it. On the junction backend the daemon does not apply that close immediately. It waits until the send rings are idle, so that every byte any session committed before the close is applied first, and until the socket's own queued bytes have gone to the Stack. Then it sends the FIN and frees the socket.

A consequence is that one application's close or `shutdown` can wait while another application keeps sending. The close travels over the control socket and the data through the rings, so the daemon waits until the rings are empty to be sure none of this session's earlier sends is still in one. A reset (dropping a stream with unread received data) does not wait.

Because closes are applied later, opening a socket on the same port right after closing one can fail with `Error::PortInUse`: the old socket still exists in the daemon. A program that reuses ports should retry after a short delay. With a raw `Session`, wait for the `Freed` event, described in [Using a Session directly](session.md).

## Receive ring space

Each session has one receive ring, `--rx-size` bytes, which the daemon fills with the data it receives for all of that session's sockets. A received frame is a view into that ring. Its space becomes free again only when the frame is dropped and the receive step of a later [round](../net/drive-loop.md) polls the session and reclaims it, and reclaiming goes in order: space after the oldest frame still held cannot be reused until that frame is dropped too.

So holding received frames shrinks the receive ring for the whole session, not only for the socket the frames came from. When the ring is full, the daemon keeps the next pending chunk or datagram for each socket and retries every time through its network loop. TCP data meanwhile waits in the daemon, and TCP's own flow control slows the peer. Notices that a peer closed the connection also wait, because they are sent only after the data before them is in the ring. Copy out what you need to keep for a long time and drop the frame.

UDP datagrams are not split the way TCP data is: a datagram larger than `--rx-size` − 40 bytes is dropped. Size the ring for your largest datagram as well as the frames you hold.

## Limits shared across sessions

Some limits belong to the daemon as a whole:

| Limit | Set by | What you see when it is reached |
|---|---|---|
| TCP sockets in the Stack | `--tcp-sockets` | `Error::SocketsFull` when opening, even if your own session's socket table has free entries |
| UDP sockets in the Stack | `--udp-sockets` | `Error::SocketsFull` when binding |
| sessions | `--max-sessions` | `ConnectError::Rejected(SessionsFull)` when connecting |
| send ring space | 2 MiB per CPU, shared by senders on that CPU | `Error::Full` when sending |

Each session also has its own table of `max_sockets` entries; running out of those also gives `Error::SocketsFull`.

## Addresses and ports

Every application connected to a daemon uses the daemon's IP address (10.77.0.2 in the tap example) and one shared set of ports. A UDP port bound by one application is in use for all of them. Two applications cannot hold connections with the same local port to the same remote address and port; give each application its own local ports. Listeners on the same port in different applications all wait for connections on it, and each new connection goes to one of them ([Opening sockets](../net/opening.md#accepting)).

A UDP bind or a TCP connect that collides with a port another application holds fails with `Error::PortInUse`, the same as if your own program held it.

## Control events and data

Opens, closes and the events that answer them (such as "connected" or "peer closed") travel over the control socket. Packet data does not go that way. Requests from your program reach the daemon at once. Events from the daemon's network thread are forwarded at least once a millisecond, so they can trail the data they relate to by about that much, while data itself moves as fast as the drive loops of the daemon and the application run. switch-net hides the ordering consequences; [Using a Session directly](session.md#event-and-packet-ordering) spells them out for raw sessions.

## Delivery guarantees

Delivery guarantees are TCP's and UDP's; the daemon passes data between the send ring, the Stack and the receive ring and keeps no acknowledgements of its own. A UDP datagram the Stack cannot send, for example because the destination's hardware address is not resolved yet, is dropped after `send` returned `Ok`, and switch-net's `UdpTx::stats()` reports zeros on the junction backend.

Next: [Using a Session directly](session.md).
