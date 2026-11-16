# switch-net

Async TCP and UDP sockets for tasks on the switch runtime, over a Stack running in the process or through the junction daemon. Applications use it as `switch::net`.

Documentation: the `switch` crate's API reference (`make docs` from the repository root), page `switch::net`.

## Build and test

```sh
make test-switch-net    # from conduit/
cargo bench -p switch-net --bench turn
```

`make test-switch-net` runs `cargo test --locked -p switch-net -- --test-threads=1`, builds the benches, and builds the `switch-examples` examples with `--features net`. It needs DPDK and a process allowed on one CPU; on macOS run it in the Docker image (`make docker-test`).
