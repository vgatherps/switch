# switchnet

Async sockets over one zhida `Stack`, receive side, for the `switch` runtime. Tasks await owned received frames; one loop owner turns the stack and then polls the runtime. Not published.

Depends on [`dpdk-smoltcp`](../conduit/crates/dpdk-smoltcp/README.md) and [`switch`](../switch/README.md). Dev-dependencies are `zhida-test-kit`, `zhida-test-support` and criterion. Nothing depends on it.

## API

- `Net::new(stack, NetConfig { ingress_budget })`: the loop owner. `turn(now)` runs `Stack::turn` and wakes every task parked on a socket that became ready; `poll_at()`; `into_stack()`; `handle()`.
- `NetHandle`: held by tasks. It opens sockets (`tcp_connect`, `tcp_accept`, `udp_bind`) but cannot turn the stack.
- `TcpStream::recv()` yields a `TcpFrame` (`payload`, `payload_mut`, `rx_timestamp`). `UdpSocket::recv()` yields a `Datagram` (`payload`, `src`, `dst_addr`, `rx_timestamp`). `Connecting`, `Accepting` and `Closed` are the other futures.
- Re-exports from `dpdk-smoltcp`: `Stack`, `Turn`, `StackError`, `UdpConfig`, `Instant`, the `tcp` module and address types.

## Constraints

- Everything is `Rc` based, so no type is `Send`.
- One task may wait per socket (`recv` takes `&mut`).
- Futures must be polled by `switch` tasks: parking stores a `switch::StoredWaker`.
- A received frame holds its socket's receive budget until dropped, and stays valid after the socket, `Net` and `Stack` are gone.

## Build and test

```sh
make test-switchnet    # from conduit/: cargo test -p switchnet, then --benches --no-run
cargo bench -p switchnet --bench turn
```

Needs DPDK 25.11.3 and a one-CPU process (Docker image on macOS; `make docker-test` includes it). Tests are in `tests/rx.rs`.
