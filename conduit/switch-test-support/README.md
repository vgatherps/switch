# switch-test-support

`#[pinned_test]`, which runs every DPDK test body of a test binary on one long-lived thread, the thread that owns DPDK's EAL. It needs no DPDK itself and is not published.

How to use it is in [docs/contributing/writing-tests.md](../../docs/contributing/writing-tests.md); its API reference is the crate's rustdoc (`make docs` at the repository root).

```sh
make -C conduit test-support   # this crate and switch-test-support-macros; no DPDK
```
