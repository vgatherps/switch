# dpdk-smoltcp

The zhida stack: the vendored smoltcp (`vendor/smoltcp`) running on one DPDK port, IPv4 TCP and UDP over Ethernet, driven from one thread. Received data stays in DPDK mbufs.

Most applications open it through `zhida-open` rather than calling `Eal::init` and `Port::open` directly. The API guide is [docs/USAGE.md](../../docs/USAGE.md).

Depended on by `zhida-open`, `tap-echo`, `binance-feed`, `memif-bench`, `nic-runner`, `switchnet` and `zhida-test-kit`. It builds on [`zhida-buffer`](../zhida-buffer/README.md) and [`zhida-dpdk-sys`](../zhida-dpdk-sys/README.md).

## Main types

| type | role |
|---|---|
| `Eal` | `Eal::init(args)`; owns the process's DPDK environment. Not `Send`, and cannot be initialized twice. |
| `Port`, `PortConfig` | One ethdev, RX queue 0 and TX queue 0. `Port::alloc_tx`, `rx_burst`, `tx_burst`, `try_close`. |
| `device_info`, `port_by_name`, `Offloads` | Look up an ethdev and its advertised offloads. |
| `Stack`, `StackConfig` | `Stack::new(port, config, now)`; `turn`, `receive`, `send`, `flush`, `poll_at`, `into_port`. |
| `TcpHandle`, `UdpHandle` | Socket handles from `add_tcp` / `add_udp`. Using one with another Stack panics. |
| `rx_reserve`, `tx_reserve`, `frame_reserve` | Pool floors `Stack::new` checks. Size each pool as its floor plus what all sockets together may hold; sockets share the pools, nothing is reserved per socket. |
| `smoltcp`, `TcpFrame`, `HeldFrames`, `UdpSocket` | Re-exports. |

The caller supplies the time on every drive call. The stack never reads a clock or blocks.

## Build and test

Needs DPDK 25.11.3 on Linux (`make doctor`); on macOS use the Docker image.

```sh
make test-stack            # dpdk-smoltcp and zhida-test-kit, one test thread
make test-stack-release    # dpdk-smoltcp, --release
make bench-stack           # Criterion benches (ring, stack, scaling, neighbor, emit, rx, udp)
make soak                  # ignored two-stack soak (SOAK_SECONDS, SOAK_SEED)
```

`cargo test -p dpdk-smoltcp` must be a per-package run: the crate's `compile_fail` doctests check that `zhida-buffer`'s test-only helpers are not exported, and testing several packages at once unifies the `testing` feature and breaks them.

The `bench-offload` feature exposes `Port::force_tx_udp_checksum`; the `udp` bench requires it.

## Constraints

- One thread and one `Stack` per process, on its own CPU. For several cores, run one process per NIC.
- The process must be restricted to one CPU for tests (`taskset -c N`, `docker --cpuset-cpus=N`).
- The vendored smoltcp is cut down to IPv4 TCP and UDP over Ethernet.
