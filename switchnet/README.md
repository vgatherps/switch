# switchnet

Async sockets over a local zhida `Stack` or a junction `Session`, for the `switch` runtime. Tasks use the same socket/frame types on either backend; one loop owner turns the Net and then polls the runtime. Not published.

Depends on [`dpdk-smoltcp`](../conduit/crates/dpdk-smoltcp/README.md), the DPDK-free `junction` client, and [`switch`](../switch/README.md).

## API

- `Net::new(stack, NetConfig { ingress_budget })`: the loop owner. Once the frames handed to the Stack since the last successful turn (successful `UdpTx::send` and `TcpTx::try_send`) reach one `Stack::tx_burst`, each further one calls `switch::request_turn()`, so the running `rt.poll` ends after that task and the loop turns sooner. `turn(now)` runs `Stack::turn` and wakes every task parked on a socket that became ready; it checks only the sockets whose readiness tokens (`Stack::arm_tcp`/`arm_udp` with the key taken when the socket opens, armed when a task parks) fired, so an idle turn costs the same with any number of parked tasks. `poll_at()`; `into_stack()`; `handle()`.
- `Net::remote(session, RemoteConfig { tx_frames })`: a junction-backed Net. `turn` flushes control, routes events and received packets, and wakes parked tasks; `poll_at` is `None`. Opens wait for host confirmation, caching the host's effective MSS. `into_session` returns the session once every handle is gone; the caller finishes it. `into_stack` only succeeds for a local Net; `into_session` only for a remote Net.
- `NetHandle`: held by tasks. Opens return futures directly: `tcp_connect` → `Connecting`, `tcp_accept` → `Accepting`, `udp_bind` → `Binding`. Each yields `Result<socket, Error>`; synchronous add/connect/listen failures resolve on the first poll, with no socket left behind. Dropping an unfinished open drops its socket. `tx_frame().await` hands out TX frames, waiting while the pool is empty; a turn that finds a free frame wakes waiters. The handle cannot turn the stack.
- `TcpStream`: `recv()` yields a `TcpFrame` (`payload`, `payload_mut`, `rx_timestamp`); `split()` gives `TcpRx` (`recv`) and `TcpTx`.
- `TcpTx`: `room(len).await` resolves `Ok` once a frame of `len` bytes fits (free bytes and a free frame slot), `Err(InvalidState)` once the socket cannot send, `Err(TooLarge)` on the first poll if `len` exceeds the send buffer (it would never fit). `try_send(frame)` queues it synchronously; every error returns the same frame (`Full`, `InvalidState`, `Empty`, `Foreign`). No future ever owns a frame, so a filled (e.g. sealed) frame is never lost to a dropped future. `max_segment_payload() -> Option<usize>` reads the effective MSS cached at open completion, without borrowing the Stack: `None` only if an accepted connection was already closed at completion; a cached `Some` stays unchanged after close/reset. `shutdown()` sends FIN after queued data; receiving continues. `delivered().await`, after `shutdown`, resolves once the peer acked every byte and the FIN (FinWait2, TimeWait) or the connection is gone (Closed): until then bytes exist only in the socket, and dropping the Stack loses them.
- `UdpSocket`: `recv()` yields a `Datagram` (`payload`, `src`, `dst_addr`, `rx_timestamp`); `split()` gives `UdpRx` (`recv`) and `UdpTx` (`send(to, frame)`, synchronous; errors return the frame; `Full` clears after a turn).
- `Error`: the single socket-operation error, a Copy `repr(u32)` enum implementing `Debug`, `Display` and `std::error::Error`. Explicit stable codes, in order from 1 through 16: `SocketsFull`, `PortInUse`, `InvalidConfig`, `Unaddressable`, `Refused`, `Closed`, `Finished`, `InvalidState`, `TooLarge`, `Full`, `Empty`, `Foreign`, `NoDestination`, `PeerMismatch`, `NoRoute`, `NeighborPending`. A reset during connect reports `Refused`; TCP receive drains queued data before reporting `Finished` after FIN or `InvalidState` after reset. `Accepting` waits through Listen/SynReceived (including RST back to Listen), and returns an already-closed stream so queued data remains readable.
- Running tasks: `round(net, rt, now, tick)` is the loop owner's step (turn, poll; returns whether anything moved, and never sleeps: a loop that may sleep when idle does so itself); `first(a, b)` races two futures; `within(spawner, limit, f)` bounds one by a Runtime sleep. For `within`, Runtime initialization and polling ticks and all relative sleeps on that Runtime must use monotonic nanoseconds; the Stack's `Instant` remains in microseconds. Used by binance-feed, nic-runner and relay-autobahn.
- Re-exports: `Stack`, `Turn`, `StackError`, `UdpConfig`, `UdpSocketStats`, netframe's `OwnedFrame` and `ImmutableFrame`, `Instant`, the `tcp` module and address types. Socket operations use `Error`, not conduit's error types; `RoundError`, `StackError` and `Turn` remain loop-owner types. `tx_frame().await` yields an `OwnedFrame` covering the full payload room; fill it through its mutable slice, `truncate(n)`, then `freeze()` for `TcpTx::try_send` or `UdpTx::send`.
- Remote sends copy once into the session stream. `send_capacity` is `max_record - 16`; `room(len)` rejects larger lengths, otherwise resolves unless the stream last returned `Full`, in which case the next turn wakes it for a retry. `Full`/`TooLarge`/`InvalidState` return the original frame; UDP requires a destination (`NoDestination`). The client pool has `tx_frames` buffers of the host's advertised frame capacity. Remote UDP stats exclude host-side drops (currently no client-visible drops).
- `Error::HostLost = 17` reports session loss to pending and later opens. Established TCP receive drains queued data then returns `InvalidState`; TCP room and TCP/UDP send return `InvalidState`. `tx_frame` remains usable because its storage is local; UDP receive remains parked without new datagrams. `Net::turn` returns `TurnError::{Stack(StackError), HostLost(junction::session::Lost)}`; `round` wraps that in `RoundError::Turn`.
- Remote TCP/UDP sends reject frames from another pool with `Foreign` before socket-state checks. Empty TCP frames return `Empty`; empty UDP datagrams remain valid.
- Remote receive drains already-published ring records before reporting session loss or deciding whether the last socket half closes with unread data. Receive-queue storage is retained across socket reuse.

## Constraints

- Everything is `Rc` based, so no type is `Send`.
- One task may wait per socket half (`recv` and `room` take `&mut`).
- Futures must be polled by `switch` tasks: parking stores a `switch::StoredWaker`.
- Received frames stay valid after the socket, `Net` and `Stack` are gone. `TcpFrame::into_frame()` and `Datagram::into_frame()` transfer their payload to `netframe::OwnedFrame`; save metadata first if needed. Freeze to clone without copying. The RX mbuf and held count (UDP: one `rx_queue` credit) remain held until the last clone drops; TCP's receive window is unchanged.
- Remote UDP receive limits are enforced per socket on the host and bounded by the session ring on the client; the client has no per-socket receive credit.
- Remote host-side UDP send errors after commit (`NoRoute`, `NeighborPending`, `Unaddressable`) are dropped by the host, not returned to the sender.

## Build and test

```sh
make test-switchnet    # from conduit/: cargo test -p switchnet, then --benches --no-run
cargo bench -p switchnet --bench turn
```

Needs DPDK 25.11.3 and a one-CPU process (Docker image on macOS; `make docker-test` includes it). Tests are in `tests/rx.rs` (receive, connect, accept), `tests/tx.rs` (send), `tests/open.rs` (first-poll opening failures, cleanup, cached negotiated MSS), `tests/ready.rs` (which parked tasks a turn wakes) and `tests/multi_ip.rs` (secondary addresses, per-address binds, bound-over-all-IP precedence).
