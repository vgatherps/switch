# switch-open

Opens a NIC from a network config file and returns a running Stack, in one call. Applications reach it as `switch::nic::open`.

Its reference is the `switch` crate's API documentation (`make docs`), under `switch::nic`. The config file is described in [docs/operations/config.md](../../../docs/operations/config.md), CPU pinning in [docs/operations/cpus.md](../../../docs/operations/cpus.md).

## Build and test

Needs DPDK 25.11.3 (the Docker image on macOS).

```sh
make test-netconf    # cargo test -p switch-netconf -p switch-open -- --test-threads=1
```

The `tests/open_*.rs` tests use net_tap, so the container needs `CAP_NET_ADMIN` and `/dev/net/tun`, and the process must be restricted to one CPU.
