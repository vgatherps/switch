# Crate map

This page lists every package in the repository's Cargo workspace: what it is for, what kind of package it is, whether building it needs DPDK, and the chapter that explains it. "Needs DPDK" means the package links DPDK's C libraries, so it builds only on Linux with DPDK 25.11.3 installed, or in the Docker image ([Setting up a build environment](../getting-started/environment.md)).

Each library's API reference is its rustdoc. Build and open it with:

```sh
cargo doc --no-deps -p switch-net --open
```

Use the package name from the first column. Packages that need DPDK document only where DPDK is installed, as in the Docker container.

## Writing applications

| Package | Kind | Needs DPDK | What it is | Chapter |
|---|---|---|---|---|
| `switch` | library | no | The switch runtime: tasks, spawner, polls, sleeps, oneshot channels | [The switch runtime](../runtime/index.md) |
| `switch-macros` | proc-macro | no | The `#[switch::task]` attribute; use it through `switch` | [Writing and spawning tasks](../runtime/tasks.md) |
| `switch-depot` | library | no | Slot allocator that `switch` uses for task storage | [Panics, errors and shutdown](../runtime/errors.md) |
| `switch-net` | library | yes, for either backend | Async TCP and UDP sockets for switch tasks: `Net`, `NetHandle`, streams, frames | [Sockets with switch-net](../net/index.md) |
| `switch-tls` | library | only with feature `switch-net` | TLS 1.3 client over a TCP stream | [TLS without WebSocket](../protocols/tls.md) |
| `switch-ws` | library | only with feature `switch-net` | WebSocket client (`ws://`, `wss://`) and `ws://` server | [TLS and WebSocket over switch-net](../protocols/index.md) |

## Owning a NIC

| Package | Kind | Needs DPDK | What it is | Chapter |
|---|---|---|---|---|
| `switch-netconf` | library and binary `switch-netconf` | no | The network config file: types, loading, checking, discovery, binding a NIC | [The network config file](../nic/config.md), [switch-netconf](../nic/netconf-tool.md) |
| `switch-open` | library | yes | `switch_open::open`: from a network config file to a running Stack | [Opening the NIC from code](../nic/open.md) |
| `dpdk-smoltcp` | library | yes | The Stack: TCP/IP over one NIC port on one thread, with `Eal` and `Port` | [Programming the Stack directly](../stack/index.md) |
| `switch-buffer` | library | yes | Packet buffers and pools under the NIC port; users see only `PoolConfig` | [Packet-buffer pools](../nic/pools.md) |
| `switch-dpdk-sys` | library | yes | Bindings to DPDK 25.11.3, and `only_allowed_cpu` | [The pinned CPU and helper threads](../nic/cpus.md) |
| `switch-nic` | binary `switch-nic` | yes | Checks a NIC (`info`), answers sockperf clients, runs the market-data feed on real hardware | [switch-nic](../nic/switch-nic.md) |
| `switch-sockperf-wire` | library | no | sockperf message framing used by `switch-nic` | [switch-nic](../nic/switch-nic.md) |
| `switch-tap-echo` | library and binary `switch-tap-echo` | yes | TCP echo server on a tap device, and the tap driver the test programs share | [Starting DPDK and opening a NIC port by hand](../stack/by-hand.md) |

## Sharing a NIC

| Package | Kind | Needs DPDK | What it is | Chapter |
|---|---|---|---|---|
| `switch-junction` | library | no | The client session to the daemon; most applications use it through switch-net | [Sharing a NIC with the junction daemon](../junction/index.md) |
| `switch-junctiond` | binary `switch-junctiond`, and library | yes | The junction daemon; its library holds the network loop for in-process test fixtures | [Running switch-junctiond](../junction/running.md) |

## Frames, shared memory and rings

| Package | Kind | Needs DPDK | What it is | Chapter |
|---|---|---|---|---|
| `switch-frame` | library | no | `OwnedFrame` and `ImmutableFrame`, the frame types every layer hands to application code | [switch-frame](../blocks/frames.md) |
| `shared-memory` | library | no | Shared-memory backings and single or mirrored mappings | [Shared memory and the receive ring](../blocks/rings.md) |
| `slice_ringbuffer` | library | no | The receive ring: one writer, one reader, records returned as frames | [Shared memory and the receive ring](../blocks/rings.md) |
| `ceph-ring` | library | no | Per-CPU shared-memory rings with many writers and one reader | [cephalopod](../blocks/cephalopod.md) |
| `ceph-fifo` | library | no | Ordered delivery per stream on top of `ceph-ring` | [cephalopod](../blocks/cephalopod.md) |
| `ceph-control` | library | no | Unix-socket control channel that hands out stream ids, for standalone use | [cephalopod](../blocks/cephalopod.md) |
| `ceph-proto` | library | no | Types shared by `ceph-fifo` and `ceph-control` | [cephalopod](../blocks/cephalopod.md) |
| `ceph-rseq` | library | no | The record layout and the push that survives preemption, used by `ceph-ring` | [cephalopod](../blocks/cephalopod.md) |

## Programs, benchmarks and tests

| Package | Kind | Needs DPDK | What it is | Chapter |
|---|---|---|---|---|
| `switch-binance-feed` | library and binary `switch-binance-feed` | yes | A `wss://` client of Binance's bookTicker stream that measures latency | [Walkthrough: a market-data feed](../protocols/binance-feed.md) |
| `switch-ws-autobahn` | binary in `switch-ws` (feature `e2e`) | yes | WebSocket echo server for the Autobahn conformance suite | [Protocol conformance](../testing/conformance.md) |
| `switch-bench` | binary `switch-bench`, and library | yes | Open-loop latency benchmark: `serve`, `gen`, `sweep`, `kernel-serve` | [Measuring latency under load](../measuring/index.md) |
| `switch-memif-bench` | binary `switch-memif-bench` | yes | Throughput of two Stacks wired through DPDK shared memory | [switch-memif-bench](../measuring/memif.md) |
| `switch-test-support` | library | no | The `#[pinned_test]` attribute's runner | [Writing tests that use DPDK](../testing/writing-tests.md) |
| `switch-test-support-macros` | proc-macro | no | The `#[pinned_test]` attribute; use it through `switch-test-support` | [Writing tests that use DPDK](../testing/writing-tests.md) |
| `switch-test-kit` | library | yes | DPDK test fixtures: fake NICs, simulated wires, two-Stack pairs | [Writing tests that use DPDK](../testing/writing-tests.md) |

`switch-ws-autobahn` is a binary target of the `switch-ws` package, not a package of its own; it is listed here because it has its own chapter.

Two vendored crates sit outside the workspace members: smoltcp, the TCP/IP stack the Stack is built on, under `conduit/vendor/smoltcp`, and rustls, which `switch-tls` uses, under `vendor/rustls`.

Next: [The junction control protocol](junction-protocol.md).
