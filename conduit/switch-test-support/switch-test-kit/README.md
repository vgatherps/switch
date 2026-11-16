# switch-test-kit

DPDK fixtures for the workspace's tests and benches: fake NIC ports, simulated wires and two-stack pairs. It needs DPDK and is not published.

How to use it is in [docs/contributing/writing-tests.md](../../../docs/contributing/writing-tests.md); its API reference is the crate's rustdoc (`make docs` at the repository root).

```sh
make -C conduit test-stack   # dpdk-smoltcp and this crate; DPDK, one CPU
```
