# switch-depot

A growable allocator of `u32` slot numbers that always hands out the lowest free slot. `switch-runtime` uses it to number task slots; applications do not use it.

Its API reference is built by `make docs` at the repository root, or alone with `cargo doc -p switch-depot --no-deps`.

## Build and test

```sh
make test                        # the host tests that need no DPDK, this crate included
cargo test -p switch-depot       # this crate only
```
