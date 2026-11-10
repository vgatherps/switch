# zhida-open

`open(config, sizing, helper_cpus, clock)`: one call from a `zhida-netconf` `Config` to a running `Stack`. The package is `zhida-open`; the directory is `crates/netconf-open`.

Used by `nic-runner`. Depends on [`zhida-netconf`](../netconf/README.md), [`dpdk-smoltcp`](../dpdk-smoltcp/README.md), `zhida-buffer` and `zhida-dpdk-sys`.

## API

- `unsafe fn open(&Config, &Sizing, Option<&[usize]>, &mut impl FnMut() -> Instant) -> Result<Opened, OpenError>`: builds the EAL arguments from the config (main lcore on the one allowed CPU, `--file-prefix`, `-a <pci>,<devargs>` or `--vdev`), initializes EAL, sizes the pools, picks offloads, opens the `Port` and `Stack` (with the config's `owned_ipv4`), waits for link, and resolves the gateway if one is configured. The safety contract is in the doc comment.
- `Opened { stack, eal, report }` and `OpenReport` (its `Display` prints the lines `zhida-nic` shows: link status, `gateway: <ip> at <mac> after <n> ms`, ...). `OpenError` names the failing step.
- `Sizing`: required fields `frame_share` (frames all sockets together may hold beyond the frame reserve; nothing is reserved per socket), `max_sockets` and `max_udp_sockets` (zero disables UDP). The RX pool is a fixed 32767 mbufs (`docs/USAGE.md`, "Pool sizing").
- `offloads`: resolves `Offloads::Auto` and explicit settings against what the PMD advertises.

`open` fails with `OpenError::NotPinned` unless the process may run on exactly one CPU, so run under `taskset -c N` or `docker --cpuset-cpus=N`. The setup code in [docs/USAGE.md](../../docs/USAGE.md) is `examples/setup.rs`, and a test fails if they differ.

On Linux, `Some(helper_cpus)` explicitly selects EAL helper CPUs, including
when the polling CPU is not kernel-isolated (for example, in Docker). The set
must be nonempty with CPU indices below `CPU_SETSIZE`, and its CPUs must be
available in the container/host cpuset. Setup temporarily includes those CPUs
in the initial EAL affinity; DPDK excludes the main lcore from helper affinity
when other CPUs are available. The opening thread returns to its original
single CPU. `Some` is unsupported outside Linux.
If the explicit set contains only the polling CPU, automatic housekeeping
selection remains enabled as with `None`. Otherwise setup reads back the
effective widened mask before EAL starts and rejects any cpuset truncation,
restoring the original single CPU first.

With `None`, behavior is unchanged: if the polling CPU appears in
`/sys/devices/system/cpu/isolated`, setup temporarily includes online non-isolated CPUs in the calling thread's
affinity before EAL starts. The kernel applies the effective cgroup cpuset;
DPDK can place interrupt/control helpers on the available housekeeping CPUs
while its main lcore stays on the original polling CPU. No other isolated CPU
is included. With no usable housekeeping CPU (including a one-CPU Docker
cpuset), helpers retain the existing single-CPU behavior. Non-isolated polling
CPUs keep the existing startup behavior. Affinity is restored after failed EAL
initialization too; an EAL error and any restoration error are both reported.
This affects setup only and adds no polling-loop work or netconf JSON fields.

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

The Linux `eal_affinity::tests::eal_helpers_avoid_polling_cpu` regression starts
real EAL in a child process, checks main/helper thread masks through Linux
affinity syscalls, and checks restoration after invalid EAL arguments. It
models the isolated polling CPU in the internal setup function, so it does not
require changing host boot isolation. It skips when fewer than two CPUs are
allowed or no allowed non-isolated housekeeping CPU remains. DPDK startup
failures are test failures, not skips.
