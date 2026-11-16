# switch

switch is a userspace TCP/UDP stack for low-latency market feeds. It runs a modified smoltcp on one
DPDK port: packets go from the NIC to your process with no kernel in the path, and received data
stays in DPDK buffers instead of being copied into a socket buffer (a small in-order segment can be
appended to the buffer before it). Everything runs on one thread, one `Stack` per process on its own
CPU core. Your program drives the stack in a loop and passes it the current time. IPv4 over
Ethernet.

## Repository

| directory | package | contents |
|---|---|---|
| `crates/dpdk-smoltcp` | `dpdk-smoltcp` | The stack: `Eal`, `Port`, `Stack`, TCP and UDP sockets |
| `crates/switch-open` | `switch-open` | `open()`: config file to running `Stack` in one call |
| `crates/netconf` | `switch-netconf` | Config file format, and the `switch-netconf` tool that writes it and binds the NIC |
| `crates/switch-buffer` | `switch-buffer` | DPDK mbufs and pools with ownership-checked chaining |
| `crates/switch-dpdk-sys` | `switch-dpdk-sys` | Bindings to DPDK 25.11.3 |
| `crates/switch-nic` | `switch-nic` | `switch-nic`: runs a Stack on a real NIC (link info, sockperf server, feed client) |
| `crates/switch-tap-echo` | `switch-tap-echo` | TCP echo server over a tap device, the target for packetdrill and the fuzzers |
| `crates/switch-binance-feed` | `switch-binance-feed` | Binance bookTicker client: switch-ws's wss client (TLS 1.3 through switch-tls) on switch-net |
| `crates/switch-memif-bench` | `switch-memif-bench` | Benchmarks over DPDK memif |
| `crates/switch-sockperf-wire` | `switch-sockperf-wire` | sockperf's message format, used by `switch-nic` |
| `vendor/smoltcp` | `smoltcp` | smoltcp 0.14.0, cut down to IPv4 TCP and UDP over Ethernet, with mbuf-native receive |
| `test-support` | `switch-test-support`, `switch-test-kit`, `switch-test-support-macros` | Test helpers |

`fuzz/` has the packetdrill scripts and the Scapy fuzzers, `autobahn/` the WebSocket conformance
setup, `docker/` the test image.

## Tests

Docker Desktop with at least 8 CPUs for its VM. No NIC or hugepages needed.

```sh
make docker-image    # Ubuntu 24.04, Rust 1.93.0, DPDK 25.11.3 built from source
make docker-build    # whole workspace, all targets except the `e2e` ones (make test builds those)
make docker-test     # every test suite, then the Autobahn WebSocket suite
make docker-fuzz     # Scapy TCP fuzzer
```

`make docker-test` passes when make exits 0 and its last line is

```text
docker-test: PASS: make test exit 0, docker-autobahn exit 0
```

`make help` lists the per-suite targets (`test-stack`, `test-buffer`, `test-netconf`, ...), which
run natively on Linux or inside the image. [docker/README.md](docker/README.md) covers each suite,
the CPU layout, benchmarks and native Linux setup.

## Running on a NIC

[crates/switch-nic/README.md](crates/switch-nic/README.md): supported drivers, writing the config
with `switch-netconf discover`, hugepages, binding the NIC to vfio-pci, and `switch-nic`.

## Writing a program

Applications run on switch-net (async sockets for `switch` tasks), with switch-ws (WebSocket) and
switch-tls (TLS 1.3) on top; [docs/USAGE.md](docs/USAGE.md) starts with which layer to use, then is the
guide to the Stack underneath: setup, pool sizing, the drive loop, time, TCP, UDP, errors and
shutdown. [crates/switch-open/examples/tcp_client.rs](crates/switch-open/examples/tcp_client.rs)
loads a config, opens the stack, connects to a TCP server, prints what it sends and closes the port.

```sh
cargo build -p switch-open --example tcp_client
```

## License

Copyright (C) 2026 vgatherps. GPL-3.0-only, see [LICENSE](LICENSE).

Imported files keep their own licenses:

- `vendor/smoltcp`: 0BSD (`vendor/smoltcp/LICENSE-0BSD.txt`).
- packetdrill scripts from freebsd-net/tcp-testsuite: BSD-2-Clause, notice in each file.
- packetdrill scripts from google/packetdrill, and `docker/packetdrill-relearn-isn.patch`:
  GPL-2.0-or-later.
- `docker/dpdk-ena-read-clock.patch` (to DPDK's ENA driver): BSD-3-Clause.
- `autobahn/expected-results-switch-ws.json`: derived from tungstenite's expected results, MIT OR
  Apache-2.0 (`autobahn/LICENSE-MIT-tungstenite`).

Sources and commits: [fuzz/packetdrill/README.md](fuzz/packetdrill/README.md#upstream-suites),
[autobahn/README.md](autobahn/README.md).
