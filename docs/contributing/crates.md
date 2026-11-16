# Crate map

Applications depend on one package, `switch`, which re-exports what an application uses from the others. Everything else in the workspace is a building block under `switch`, a tool, a benchmark or test infrastructure.

"Needs DPDK" means the package links DPDK's C libraries, so it builds only on Linux with DPDK 25.11.3, or in the Docker image. The packages marked "no" build and test on macOS too; [Running the tests](testing.md) says which of them `make test` covers.

## What applications use

| Package | Directory | Needs DPDK | What it is |
|---|---|---|---|
| `switch` | `switch/` | only with feature `net` | The crate applications depend on. By default it re-exports the runtime: `Runtime`, `Spawner`, `#[switch::task]` and the rest. Feature `net` adds `switch::net`, `switch::nic` and `switch::junction`; `tls` adds `switch::tls`; `ws` adds `switch::ws` |
| `switch-examples` | `examples/` | only with feature `net`, `tls` or `ws` | The runnable programs the guide uses, depending only on `switch`: `cargo run -p switch-examples --example first_task`; network ones need `--features net`, and the TLS and WebSocket ones `--features tls` or `--features ws` |

## The runtime

| Package | Directory | Needs DPDK | What it is |
|---|---|---|---|
| `switch-runtime` | `switch-runtime/` | no | The single-threaded async runtime: tasks, spawning, sleeps, oneshot channels. It runs tasks only when the caller polls it |
| `switch-macros` | `switch-macros/` | no | The task attribute: `#[switch_runtime::task]` inside the workspace, and the `#[switch::task]` that `switch` re-exports |
| `switch-depot` | `switch-depot/` | no | The slot allocator `switch-runtime` stores tasks in |

## Networking

| Package | Directory | Needs DPDK | What it is |
|---|---|---|---|
| `switch-net` | `switch-net/` | yes | Async TCP and UDP sockets for tasks, over a card the process owns or one shared through `switch-junctiond`; `switch::net` |
| `switch-tls` | `switch-tls/` | only with feature `switch-net` | A TLS 1.3 client over a TCP stream; `switch::tls` |
| `switch-ws` | `switch-ws/` | only with feature `switch-net` | WebSocket: `wss://` and `ws://` clients and a `ws://` server; `switch::ws`. Also the binary `switch-ws-autobahn` (feature `e2e`), the [Autobahn](conformance.md#autobahn) driver |
| `switch-junction` | `switch-junction/` | no | The client side of sharing a card through the daemon: a session with `switch-junctiond` over a Unix socket and shared memory; `switch::junction` |
| `switch-junctiond` | `switch-junctiond/` | yes | The daemon, which owns one card and serves sockets to applications on the same machine. Its library holds the daemon's network loop for in-process test fixtures |

## Owning a network card

| Package | Directory | Needs DPDK | What it is |
|---|---|---|---|
| `switch-netconf` | `conduit/crates/switch-netconf/` | no | The network config file: its types, loading and checking, and the `switch-netconf` tool |
| `switch-open` | `conduit/crates/switch-open/` | yes | Opens a card from a network config file and returns a running stack; part of `switch::nic` |
| `dpdk-smoltcp` | `conduit/crates/dpdk-smoltcp/` | yes | The TCP/UDP stack for one DPDK port, driven from one thread, built on smoltcp |
| `switch-buffer` | `conduit/crates/switch-buffer/` | yes | DPDK packet buffers and their pools |
| `switch-dpdk-sys` | `conduit/crates/switch-dpdk-sys/` | yes | Bindings to DPDK 25.11.3. Its build script needs DPDK's pkg-config files and clang |
| `switch-nic` | `conduit/crates/switch-nic/` | yes | The `switch-nic` tool: checks a card, answers sockperf clients, runs the market-data feed on real hardware |
| `switch-sockperf-wire` | `conduit/crates/switch-sockperf-wire/` | no | sockperf's message format, for `switch-nic` |

## Frames, shared memory and rings

These sit under `switch-net` and `switch-junctiond`.

| Package | Directory | Needs DPDK | What it is |
|---|---|---|---|
| `switch-frame` | `switch-frame/` | no | Packet frames, the buffers every layer hands to application code |
| `shared-memory` | `shared-memory/` | no | Shared-memory backings and single or mirrored mappings |
| `slice_ringbuffer` | `vendor/slice_ringbuffer/` | no | A one-writer, one-reader shared-memory record ring: how received packets reach an application that shares a card through the daemon |
| `ceph-ring` | `cephalopod/crates/ring/` | no | Per-CPU shared-memory rings with many writers and one reader: how such applications send to the daemon |
| `ceph-rseq` | `cephalopod/crates/rseq/` | no | The record layout and the write into a per-CPU ring that `ceph-ring` uses |
| `ceph-fifo` | `cephalopod/crates/fifo/` | no | Ordered delivery per stream over `ceph-ring` |
| `ceph-proto` | `cephalopod/crates/proto/` | no | Types shared by `ceph-fifo` and `ceph-control` |
| `ceph-control` | `cephalopod/crates/control/` | no | A Unix-socket control channel that hands out stream ids, for using cephalopod on its own |

## Programs, benchmarks and tests

| Package | Directory | Needs DPDK | What it is |
|---|---|---|---|
| `switch-binance-feed` | `conduit/crates/switch-binance-feed/` | yes | A `wss://` client of Binance's bookTicker stream that measures latency |
| `switch-bench` | `switch-bench/` | yes | The [latency benchmark](../benchmarks/README.md) |
| `switch-memif-bench` | `conduit/crates/switch-memif-bench/` | yes | [Stack-to-stack throughput](../benchmarks/memif.md) |
| `switch-tap-echo` | `conduit/crates/switch-tap-echo/` | yes | A TCP echo server on a tap device, the target of the packetdrill scripts and the TCP fuzzer, and the tap driver other test programs share |
| `switch-test-support` | `conduit/switch-test-support/` | no | `#[pinned_test]`, for tests that start DPDK ([Writing tests](writing-tests.md)) |
| `switch-test-support-macros` | `conduit/switch-test-support/switch-test-support-macros/` | no | The `#[pinned_test]` attribute itself; use it through `switch-test-support` |
| `switch-test-kit` | `conduit/switch-test-support/switch-test-kit/` | yes | DPDK test fixtures: fake ports, simulated wires, two-stack pairs |

The vendored smoltcp, which `dpdk-smoltcp` is built on, is outside the workspace in `conduit/vendor/smoltcp`; its tests run in `make test`. So is rustls, which `switch-tls` uses, in `vendor/rustls`: the crates.io release with the patches in `vendor/patches/rustls/` applied. Never edit it by hand; `make vendor-check` fails if it differs from release plus patches.
