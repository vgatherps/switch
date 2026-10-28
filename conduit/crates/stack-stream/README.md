# stack-stream

`StackStream`: nonblocking `std::io::Read` and `Write` over one TCP socket of a borrowed `dpdk_smoltcp::Stack`. It lets code written against `Read + Write`, such as tungstenite and rustls, run on a zhida socket.

Used by `ws-autobahn` and `binance-feed`. Depends on [`dpdk-smoltcp`](../dpdk-smoltcp/README.md).

## API

- `StackStream::new(&mut Stack, TcpHandle)`: valid only for a socket that is Established or CloseWait.
- `stack()`, `handle()`, `parts()`: access the borrowed Stack, so the caller can turn it while the stream is held.
- `into_parts()`, `into_stack()`: give the Stack borrow (and handle) back.

Behavior of the `io` impls:

- Nothing moves until the caller turns the Stack. `flush` returns `Ok(())`; written bytes sit in the send buffer and leave on later turns.
- `read` copies queued frames into the buffer and returns `WouldBlock` when none is queued, `Ok(0)` after FIN once all bytes are read, `ConnectionReset` after a reset.
- `write` returns `WouldBlock` when the send buffer is full (never `Ok(0)`), `ConnectionReset` after a peer reset, `BrokenPipe` after our own close or abort.

The full list is in the doc comment on `StackStream`.

## Build and test

```sh
make test-stream           # cargo test -p stack-stream -- --test-threads=1
make test-stack-release    # also runs it with --release
```

Needs DPDK 25.11.3 and a one-CPU process (Docker image on macOS). Tests in `tests/stream.rs` use `zhida-test-kit` and tungstenite.
