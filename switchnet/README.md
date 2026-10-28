# switchnet

Async sockets over one zhida `Stack` for the `switch` runtime. Tasks await owned received frames and send TX frames; one loop owner turns the stack and then polls the runtime. Not published.

Depends on [`dpdk-smoltcp`](../conduit/crates/dpdk-smoltcp/README.md) and [`switch`](../switch/README.md). Dev-dependencies are `zhida-test-kit`, `zhida-test-support` and criterion. Nothing depends on it.

## API

- `Net::new(stack, NetConfig { ingress_budget })`: the loop owner. `turn(now)` runs `Stack::turn` and wakes every task parked on a socket that became ready; `poll_at()`; `into_stack()`; `handle()`.
- `NetHandle`: held by tasks. It opens sockets (`tcp_connect`, `tcp_accept`, `udp_bind`) and hands out TX frames (`tx_frame().await`: waits while the frame pool is empty; a turn that finds a free frame wakes the waiters). It cannot turn the stack.
- `TcpStream`: `recv()` yields a `TcpFrame` (`payload`, `payload_mut`, `rx_timestamp`); `split()` gives `TcpRx` (`recv`) and `TcpTx`.
- `TcpTx`: `room(len).await` resolves `Ok` once a frame of `len` bytes fits (free bytes and a free frame slot), `Err(InvalidState)` once the socket cannot send. `try_send(frame)` queues it synchronously; every error returns the same frame (`Full`, `InvalidState`, `Empty`, `Foreign`). No future ever owns a frame, so a filled (e.g. sealed) frame is never lost to a dropped future. `shutdown()` sends FIN after the queued data; receiving continues.
- `UdpSocket`: `recv()` yields a `Datagram` (`payload`, `src`, `dst_addr`, `rx_timestamp`); `split()` gives `UdpRx` (`recv`) and `UdpTx` (`send(to, frame)`, synchronous; errors return the frame; `WouldBlock` clears after a turn).
- `Connecting`, `Accepting` and `Closed` are the connection-setup futures and error.
- Re-exports from `dpdk-smoltcp`: `Stack`, `Turn`, `StackError`, `UdpConfig`, `TxFrame`, `UdpSendError`, `UdpSendFrameError`, `Instant`, the `tcp` module (with `SendError`, `SendFrameError`) and address types.

## Constraints

- Everything is `Rc` based, so no type is `Send`.
- One task may wait per socket half (`recv` and `room` take `&mut`).
- Futures must be polled by `switch` tasks: parking stores a `switch::StoredWaker`.
- A received frame holds its socket's receive budget until dropped, and stays valid after the socket, `Net` and `Stack` are gone.

## Build and test

```sh
make test-switchnet    # from conduit/: cargo test -p switchnet, then --benches --no-run
cargo bench -p switchnet --bench turn
```

Needs DPDK 25.11.3 and a one-CPU process (Docker image on macOS; `make docker-test` includes it). Tests are in `tests/rx.rs` (receive, connect, accept) and `tests/tx.rs` (send).
