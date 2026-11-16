# switch-buffer

DPDK packet buffers (mbufs) and their pools, wrapped for the Stack in `dpdk-smoltcp`. Applications do not use it directly.

Its reference is the API documentation (`make docs`). How tests are organized is in [docs/contributing/testing.md](../../../docs/contributing/testing.md).

## Build and test

Needs DPDK 25.11.3 (the Docker image on macOS). Every test claims EAL on the one CPU the process may run on: run under `taskset -c N` or in the Docker image.

```sh
make test-buffer    # cargo test -p switch-buffer -- --test-threads=1
make test-props     # proptests comparing against plain DPDK mbufs
```

`fuzz/` is a cargo-fuzz harness with its own workspace and lockfile; on a Linux host with DPDK and nightly cargo-fuzz:

```sh
taskset -c 3 cargo +nightly fuzz run fuzz_ops -- -max_total_time=60
```

The test helpers (`ensure_eal_init`, `acquire_test_pool`, `TestPoolGuard`, `PoolManager::new`, the `testing` module) exist only with the `testing` feature, which this crate's own tests enable.
