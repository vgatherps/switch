# depot

`DynDepot`: slot numbers from 0 with no capacity. `alloc()` takes the lowest free slot; `free(slot)` returns one. A bitmap of free slots (one bit per slot) and a summary above it (one bit per bitmap word: the word has a free slot) both grow by a word when every slot is taken, so never-used slots go out in increasing order. A free slot is found by scanning summary words; one covers 4096 slots. No handles or generations: the caller frees each slot it got exactly once.

`switch` uses it to hand out task slots (blocks fill in order, so a pool adds a block only when every slot of its blocks is in use). No dependencies, no DPDK or Docker: `cargo test -p depot`.

Tests in `src/lib.rs`: allocation order across bitmap and summary word boundaries, lowest-first reuse, and a seeded random walk of allocs and frees against a model.
