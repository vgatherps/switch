# switch-dpdk-sys

Raw bindings to DPDK 25.11.3 (bindgen over `wrapper.h`) plus a C shim for the mbuf, mempool and ethdev functions DPDK provides only inline. It is the base of `switch-buffer` and `dpdk-smoltcp`; applications do not use it directly.

Its reference is the API documentation (`make docs`).

## Build and test

Needs Linux with clang, pkg-config and `libdpdk` 25.11.3 (`make doctor` checks); on macOS use the Docker image (`make docker-image`, then `make docker-test`). Tests start EAL, so the process must be restricted to one CPU.

```sh
make test-sys    # cargo test -p switch-dpdk-sys -- --test-threads=1
```
