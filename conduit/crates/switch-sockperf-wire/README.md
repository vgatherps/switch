# switch-sockperf-wire

The message format of sockperf 3.10, which `switch-nic sockperf-server` uses to answer kernel `sockperf` clients. It has no dependencies and does not use DPDK.

Its API reference is the crate's rustdoc (`make docs` at the repository root); the server is described in [docs/operations/switch-nic.md](../../../docs/operations/switch-nic.md#sockperf-server).

```sh
cargo test --locked -p switch-sockperf-wire   # no DPDK; also run by make -C conduit test-switch-nic
```
