# zhida-open

`open(config, sizing, clock)`: one call from a `zhida-netconf` `Config` to a running `Stack`. The package is `zhida-open`; the directory is `crates/netconf-open`.

Used by `nic-runner`. Depends on [`zhida-netconf`](../netconf/README.md), [`dpdk-smoltcp`](../dpdk-smoltcp/README.md), `zhida-buffer` and `zhida-dpdk-sys`.

## API

- `unsafe fn open(&Config, &Sizing, &mut impl FnMut() -> Instant) -> Result<Opened, OpenError>`: builds the EAL arguments from the config (main lcore on the one allowed CPU, `--file-prefix`, `-a <pci>,<devargs>` or `--vdev`), initializes EAL, sizes the pools, picks offloads, opens the `Port` and `Stack` (with the config's `owned_ipv4`), waits for link, and resolves the gateway if one is configured. The safety contract is in the doc comment.
- `Opened { stack, eal, report }` and `OpenReport` (its `Display` prints the lines `zhida-nic` shows: link status, `gateway: <ip> at <mac> after <n> ms`, ...). `OpenError` names the failing step.
- `Sizing`: required fields `tcp_rx_frames`, `udp_rx_datagrams`, `max_sockets` and `max_udp_sockets` (zero disables UDP).
- `offloads`: resolves `Offloads::Auto` and explicit settings against what the PMD advertises.

`open` fails with `OpenError::NotPinned` unless the process may run on exactly one CPU, so run under `taskset -c N` or `docker --cpuset-cpus=N`. The setup code in [docs/USAGE.md](../../docs/USAGE.md) is `examples/setup.rs`, and a test fails if they differ.

## Example

```sh
cargo build -p zhida-open --example tcp_client
taskset -c 3 target/debug/examples/tcp_client dut.json 192.0.2.1:7000
```

`examples/tcp_client.rs` connects, sends a line, prints the reply and closes the Port. The header of the file has a no-NIC variant using a tap device (root on Linux, or the Docker image).

## Test

```sh
make test-netconf    # cargo test -p zhida-netconf -p zhida-open -- --test-threads=1
```

Needs DPDK 25.11.3 (Docker image on macOS). Tests in `tests/open_*.rs` use net_tap, so the container needs `CAP_NET_ADMIN` and `/dev/net/tun`.
