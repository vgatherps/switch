# dpdk-smoltcp

The switch Stack: a synchronous TCP/UDP stack (the vendored smoltcp in `conduit/vendor/smoltcp`) over one DPDK NIC port, IPv4 over Ethernet, driven from one thread. Applications get a `Stack` from `switch::nic::open` and use it through `switch::net`; programs that drive it directly use this crate's API.

Its reference is the API documentation (`make docs`): the `Stack` type and its methods.

## Build and test

Needs DPDK 25.11.3 on Linux (`make doctor`); on macOS use the Docker image. Tests need the process restricted to one CPU (`taskset -c N`, `docker --cpuset-cpus=N`).

```sh
make test-stack            # dpdk-smoltcp and switch-test-kit, one test thread
make test-stack-release    # dpdk-smoltcp, --release
make bench-stack           # Criterion benches (ring, stack, scaling, neighbor, emit, rx, udp, tx_phases)
make soak                  # ignored two-stack soak (SOAK_SECONDS, SOAK_SEED)
```

`cargo test -p dpdk-smoltcp` must be a per-package run: its `compile_fail` doctests check that `switch-buffer`'s test-only helpers are not exported, and testing several packages at once unifies the `testing` feature and breaks them.
