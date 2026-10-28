# zhida-dpdk-sys

Raw bindings to DPDK 25.11.3 plus a small C shim for the mbuf, mempool and ethdev paths that DPDK only provides as inline functions.

Used by `zhida-buffer`, `dpdk-smoltcp`, `zhida-open`, `tap-echo`, `memif-bench`, `nic-runner` and `zhida-test-kit`. `binance-feed` uses it in tests.

## Contents

- `build.rs` finds `libdpdk` through pkg-config, runs bindgen over `wrapper.h` (an allowlist of `rte_mbuf`, `rte_mempool`, `rte_ring` and related types), and compiles the `shim_*.c` files with clang at `-O3`. It also links `rte_net_ring` and `rte_bus_vdev`, which the pkg-config file leaves out. The CPU baseline is `corei7` on x86_64 and `armv8-a+crc` on aarch64, matching the DPDK build in `docker/Dockerfile`.
- `src/lib.rs` re-exports the generated bindings and declares the shim functions: `dpdk_raw_*` (direct inline ethdev, ring and mbuf calls) and `zhida_pktmbuf_*` (alloc with a precomputed cache pointer, free, chain, `chain_safe`, clone, attach, indirect-head helpers). It also adds accessor methods on `rte_mbuf` (`data_len`, `refcnt`, `prepend`, `adjust`, `trim`, `set_next`, ...).
- `src/affinity.rs` has `only_allowed_cpu()`, which returns the one CPU the calling thread may run on, or `AffinityError`. EAL setup in this workspace pins its main lcore to that CPU.

In debug builds the shims are compiled with `ZHIDA_ASSUME_CHECKED`. The higher-level API over these functions is `zhida-buffer`.

## Build and test

Needs a Linux host with clang, pkg-config and `libdpdk` 25.11.3 (`make doctor` checks). On macOS use the Docker image: `make docker-image`, then `make docker-test`.

```sh
make test-sys    # cargo test -p zhida-dpdk-sys -- --test-threads=1
```

Tests (`tests/dpdk_integration.rs`, `tests/cache_owner.rs`) initialize EAL, so they run on one test thread and need the process restricted to one CPU (`taskset -c N` or `docker --cpuset-cpus=N`).

## Constraints

- The shim functions are `unsafe` and assume valid pools, started ports and mbuf ownership as stated in the doc comment on each.
- Every pool the shim allocates from must have been prepared with `zhida_pool_init_cache_ptrs`; `zhida-buffer`'s `PoolManager` does this.
