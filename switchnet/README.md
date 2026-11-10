# switchnet

Async sockets over one zhida `Stack` for the `switch` runtime. Tasks await owned received frames and send TX frames; one loop owner turns the stack and then polls the runtime. Not published.

Depends on [`dpdk-smoltcp`](../conduit/crates/dpdk-smoltcp/README.md) and [`switch`](../switch/README.md). Dev-dependencies are `zhida-test-kit`, `zhida-test-support` and criterion. Used by relay, sheath, binance-feed and nic-runner.

## API

- `Net::new(stack, NetConfig { ingress_budget })`: the loop owner. Once the frames handed to the Stack since the last successful turn (successful `UdpTx::send` and `TcpTx::try_send`) reach one `Stack::tx_burst`, each further one calls `switch::request_turn()`, so the running `rt.poll` ends after that task and the loop turns sooner. `turn(now)` runs `Stack::turn` and wakes every task parked on a socket that became ready; it checks only the sockets whose readiness tokens (`Stack::arm_tcp`/`arm_udp` with the key taken when the socket opens, armed when a task parks) fired, so an idle turn costs the same with any number of parked tasks. `poll_at()`; `into_stack()`; `handle()`.
- `NetHandle`: held by tasks. Opens return futures directly: `tcp_connect` → `Connecting`, `tcp_accept` → `Accepting`, `udp_bind` → `Binding`. Each yields `Result<socket, Error>`; synchronous add/connect/listen failures resolve on the first poll, with no socket left behind. Dropping an unfinished open drops its socket. `tx_frame().await` hands out TX frames, waiting while the pool is empty; a turn that finds a free frame wakes waiters. The handle cannot turn the stack.
- `TcpStream`: `recv()` yields a `TcpFrame` (`payload`, `payload_mut`, `rx_timestamp`); `split()` gives `TcpRx` (`recv`) and `TcpTx`.
- `TcpTx`: `room(len).await` resolves `Ok` once a frame of `len` bytes fits (free bytes and a free frame slot), `Err(InvalidState)` once the socket cannot send, `Err(TooLarge)` on the first poll if `len` exceeds the send buffer (it would never fit). `try_send(frame)` queues it synchronously; every error returns the same frame (`Full`, `InvalidState`, `Empty`, `Foreign`). No future ever owns a frame, so a filled (e.g. sealed) frame is never lost to a dropped future. `max_segment_payload() -> Option<usize>` reads the effective MSS cached at open completion, without borrowing the Stack: `None` only if an accepted connection was already closed at completion; a cached `Some` stays unchanged after close/reset. `shutdown()` sends FIN after queued data; receiving continues. `delivered().await`, after `shutdown`, resolves once the peer acked every byte and the FIN (FinWait2, TimeWait) or the connection is gone (Closed): until then bytes exist only in the socket, and dropping the Stack loses them.
- `UdpSocket`: `recv()` yields a `Datagram` (`payload`, `src`, `dst_addr`, `rx_timestamp`); `split()` gives `UdpRx` (`recv`) and `UdpTx` (`send(to, frame)`, synchronous; errors return the frame; `Full` clears after a turn).
- `Error`: the single socket-operation error, a Copy `repr(u32)` enum implementing `Debug`, `Display` and `std::error::Error`. Explicit stable codes, in order from 1 through 16: `SocketsFull`, `PortInUse`, `InvalidConfig`, `Unaddressable`, `Refused`, `Closed`, `Finished`, `InvalidState`, `TooLarge`, `Full`, `Empty`, `Foreign`, `NoDestination`, `PeerMismatch`, `NoRoute`, `NeighborPending`. A reset during connect reports `Refused`; TCP receive drains queued data before reporting `Finished` after FIN or `InvalidState` after reset. `Accepting` waits through Listen/SynReceived (including RST back to Listen), and returns an already-closed stream so queued data remains readable.
- Running tasks: `round(net, rt, now, tick)` is the loop owner's step (turn, poll; returns whether anything moved, and never sleeps: a loop that may sleep when idle does so itself); `first(a, b)` races two futures; `within(spawner, limit, f)` bounds one by a Runtime sleep. For `within`, Runtime initialization and polling ticks and all relative sleeps on that Runtime must use monotonic nanoseconds; the Stack's `Instant` remains in microseconds. Used by binance-feed, nic-runner and relay-autobahn.
- Re-exports: `Stack`, `Turn`, `StackError`, `UdpConfig`, `UdpSocketStats`, netframe's `OwnedFrame` and `ImmutableFrame`, `Instant`, the `tcp` module and address types. Socket operations use `Error`, not conduit's error types; `RoundError`, `StackError` and `Turn` remain loop-owner types. `tx_frame().await` yields an `OwnedFrame` covering the full payload room; fill it through its mutable slice, `truncate(n)`, then `freeze()` for `TcpTx::try_send` or `UdpTx::send`.

## Constraints

- Everything is `Rc` based, so no type is `Send`.
- One task may wait per socket half (`recv` and `room` take `&mut`).
- Futures must be polled by `switch` tasks: parking stores a `switch::StoredWaker`.
- Received frames stay valid after the socket, `Net` and `Stack` are gone. `TcpFrame::into_frame()` and `Datagram::into_frame()` transfer their payload to `netframe::OwnedFrame`; save metadata first if needed. Freeze to clone without copying. The RX mbuf and held count (UDP: one `rx_queue` credit) remain held until the last clone drops; TCP's receive window is unchanged.

## Build and test

```sh
make test-switchnet    # from conduit/: cargo test -p switchnet, then --benches --no-run
cargo bench -p switchnet --bench turn
```

Needs DPDK 25.11.3 and a one-CPU process (Docker image on macOS; `make docker-test` includes it). Tests are in `tests/rx.rs` (receive, connect, accept), `tests/tx.rs` (send), `tests/open.rs` (first-poll opening failures, cleanup, cached negotiated MSS), `tests/ready.rs` (which parked tasks a turn wakes) and `tests/multi_ip.rs` (secondary addresses, per-address binds, bound-over-all-IP precedence).
