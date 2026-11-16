# switch-runtime

The caller-driven, single-threaded async runtime of switch: tasks run only when the program calls `Runtime::poll` with the current tick. Applications use it through the `switch` crate, whose root re-exports its items.

Documentation is the `switch` crate's API reference: `make docs` at the repository root builds it.

## Build and test

```sh
make test                          # every crate that builds without DPDK, this one included
cargo test -p switch-runtime       # this crate only
make miri                          # its unsafe code under Miri (nightly)
```
