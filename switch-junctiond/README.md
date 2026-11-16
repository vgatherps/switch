# switch-junctiond

The junction daemon: one Linux process that owns a DPDK network card and serves TCP and UDP sockets to programs on the same machine, which connect with `switch::junction::session::Session`. Its library holds the daemon's network loop for in-process test fixtures.

Running it: [docs/operations/junctiond.md](../docs/operations/junctiond.md). Using it from a program: the `switch` crate's API reference and guide (`make docs`).

```sh
cargo build --locked --release -p switch-junctiond --bins
```

Tests run in the DPDK container ([docs/contributing/testing.md](../docs/contributing/testing.md)); `test-junction` runs `switch-junction`'s tests, then this crate's pinned to the container's CPU:

```sh
make -C conduit test-junction DOCKER_CPUSET=3
```
