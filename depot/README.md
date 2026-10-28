# depot

Fixed-capacity slot allocator with generation-checked 32-bit handles, plus a pool layer of
thread-owned static pools, RAII entities and claim-carrying ids.

The crate has no dependencies. It is vendored into this workspace from `~/src/meminfra/depot`, and `switch`
uses `DynDepot` to hand out task slots. It builds with the workspace's edition 2024 toolchain and needs no
DPDK or Docker: `cargo test -p depot`.

## Allocator

### Sizing

```rust
type Depot = depot::fixed_depot_type!(4096);
let mut depot = Depot::new(); // const fn, all zero bytes: fine in a static
let handle = depot.alloc().unwrap();
depot.free(handle).unwrap();
```

`fixed_depot_type!(N)` rounds `N` up to a multiple of 64. Slot 0 is a reserved sentinel row, so
usable capacity is the rounded count minus one. `FixedDepot::validate_config()` rejects 0 slots
and more than 2^25 slots at compile time; `new()` runs it.

### Handles

```
bits [31:25] = generation (7 bits; 1..=127 live, 0 never live)
bits [24:0]  = slot       (25 bits)
```

`DepotHandle` wraps a `NonZeroU32`, so `Option<DepotHandle>` is 4 bytes. Each slot stores one
generation byte:

| Stored byte | Meaning |
|-------------|---------|
| `0` | never allocated, or poisoned |
| `g` (1..=127) | live with generation `g` |
| `0x80 \| g` | free; the next tenant gets generation `g` |

`free` advances the generation (127 wraps to 1) and sets the dead bit `0x80`. No handle has that
bit, so no handle matches a free slot: a stale copy of a handle cannot free or validate the slot
again. After 127 reuses a slot comes back to an old generation, and a handle that old matches
again.

### Allocation order

1. The 16-entry ring of recently freed handles, newest first. A free that overflows the ring
   moves its oldest entry into the bitmaps.
2. The lowest freed slot in the bitmaps. Each 64-slot slab has a `u64` of freed slots; three
   summary levels (`l1`, `l2`, `l3`) mark which slabs and groups have any.
3. The next never-used slot from a bump cursor. Never-used slots are not in the bitmaps.

`tests/props.rs` pins this order against a reference model.

### Checks

| Method | Behavior |
|--------|----------|
| `free(h) -> Result<(), FreeError>` | `OutOfRange` for slot 0 or past the end; `StaleGeneration` unless `h` is live |
| `validate(h) -> Result<u32, AccessError>` | slot of a live handle; otherwise `OutOfRange` or `StaleGeneration` |
| `validate_live(h) -> Option<u32>` | slot of a live handle, else `None` |
| `get_or_sentinel(Option<h>) -> Option<u32>` | `None` and slot-0 handles give `Some(0)`; a live handle gives its slot; anything else `None` |
| `free_unchecked` / `poison_unchecked` | unsafe; poison sets the byte to 0, so the slot is never handed out again |
| `check_invariants()` | full consistency check of bitmaps, caches, ring and generation bytes |

`DepotAlloc` is the trait the pool layer uses; only `FixedDepot` implements it. `FixedDepot` has compile-time capacity; `DynDepot::new(slots)`
has runtime capacity (rounded up to a multiple of 64, at most 2^25, slot 0 reserved) and the same allocation
algorithm, which `src/algorithm.rs` shares between the two.

## Pools

```rust
depot::define_static_ecs!(OrderPool, 4096);

let claim = OrderPool::claim().unwrap();
let entity = claim.alloc().unwrap();
let id = entity.id();
drop(entity);
```

`define_static_ecs!(Name, N)` declares a static pool; `define_static_ecs!(Name, N, core: T = init)`
adds pool-wide core storage. The first `Name::claim()` binds the pool to the calling thread for
the rest of the process. That thread can claim again; every other thread gets
`ClaimError::OwnedByOtherThread`. Static pools are told apart by type.

| Type | Role |
|------|------|
| `Claim<B>` / `StaticClaim<H>` | proof of access to one pool; `Copy` for static pools; never `Send` or `Sync` |
| `Entity<B>` | owns a slot; drop frees it |
| `EntityId<B>` | non-owning id holding its claim and handle; `Copy` for static pools; never `Send` or `Sync`; equality compares pool and handle |
| `EntityRef<B>` | reads slot, binding and handle of an entity or id with no liveness check |
| `PoolBinding` | unsafe trait: pool identity plus depot, core and borrow-set pointers |
| `PoolHandle` / `StaticPoolHandle` | copyable bindings / bindings for one static pool (what `define_static_ecs!` implements) |
| `PoolClaim` | sealed trait with the safe claim operations |
| `pool::PoolClaimUnsafeExt` | unsafe pool-wide `core`, `core_mut`, `component`, `component_mut`, `get_mut_or_none` |
| `Component<B>` | unsafe trait for hand-written component storage reached through the binding; `get` / `get_mut` are unsafe and take the raw storage pointer |

Safe component access goes through the claim and is tracked per slot:

- `claim.get::<C>(&entity)` reads through an owning entity.
- `claim.get_mut::<C>(&mut entity)` takes a unique slot borrow; `try_get_mut` returns
  `ClaimBorrowError` instead of panicking.
- `claim.get_or_none::<C>(Option<EntityId>)` takes a shared slot borrow. A `None` id reads row 0; a
  dead id returns `None`. `get_or_none_ref` takes `Option<&EntityId>`.
- `claim.entity_id_from_handle(raw)` rebuilds an id only if `raw` is live in this pool.

A `Component` keeps its storage behind `UnsafeCell` (a `SyncCell` static or the pool core) with
one element per depot slot. The pool never forms a reference to the whole storage: it passes the
raw pointer and a slot, and `get` / `get_mut` must project to that slot's element before forming
a reference, and never reference any byte outside it. So a live `&mut` from one entity's
`get_mut` guard stays valid while other slots are read or written.

Entity drop always frees the slot. A shared guard from `get_or_none` can outlive the entity: the
old id becomes dead, but the guard keeps reading the value stored in that slot. The per-slot borrow
counter is not reset by free. If the slot is reused while the old guard is live, `get_mut` and
`try_get_mut` for the new tenant fail with `ClaimBorrowError::AlreadyBorrowed` until the guard
drops. A shared guard leaked with `mem::forget` leaves the slot shared-borrowed for every later
tenant.

A `get_mut` guard borrows the entity mutably, so it cannot be live at drop
(`LocalPool` is test-only; using an entity of one `LocalPool` with another `LocalPool`'s claim is
not supported and voids this). If
one is leaked with `mem::forget`, the entity drops and frees normally, but the slot stays
uniquely borrowed forever: every later `get_mut` / `try_get_mut` and id-based read on that slot,
for any future tenant, fails with `ClaimBorrowError::AlreadyBorrowed` (the non-`try` forms
panic).

`LocalPool` / `OwnedClaim` / `LocalPoolBinding`: a pool owned through `Rc`, compiled only under
`cfg(test)` or the `test-utils` feature.

## Files

| File | Contains |
|------|----------|
| `src/lib.rs` | `DepotHandle`, `DepotAlloc`, error types, `define_static_ecs!` |
| `src/fixed.rs` | `FixedDepot`, `fixed_depot_type!` |
| `src/dyn_depot.rs` | `DynDepot`, the runtime-capacity depot |
| `src/algorithm.rs` | `depot_algorithm!`: the bitmap hierarchy and allocation order both depots use |
| `src/index_ring.rs` | 16-entry ring of freed handles |
| `src/pool.rs` | claims, entities, ids, components, `GlobalPool`, `LocalPool` |
| `src/test_heap.rs` | `SlabHeap<T>`: a pointer-linked max-heap on a depot, used as an allocator oracle |
| `src/stress_tests.rs` | seeded stress runs of `SlabHeap` against `BinaryHeap` |

## Testing

- Unit tests in `src/`.
- quickcheck: `tests/props.rs` (allocation order vs a reference model, no double alloc, handle
  encoding, capacity, invariants, LIFO reuse, burst locality, bump growth) and
  `tests/heap_props.rs` (`SlabHeap` vs `BinaryHeap`).
- trybuild: `tests/compile_tests.rs` over `tests/ui` (unsafe core access, sealed `PoolClaim`, ids
  not `Send`/`Sync`, guards tied to the claim, `get_mut` needs `&mut Entity`) and `tests/pass`.
- Fuzz: `(cd fuzz && cargo fuzz run heap_ops)`.
- Benchmarks: `benches/alloc.rs` (raw alloc/free patterns), `benches/pool.rs` (claim/entity/component
  paths, static vs local pool), `benches/pool_2p24.rs` (a full 2^24-slot static pool in random
  order: raw free vs entity drop, `get`, `get_mut`, `get_or_none`; plain timing loop, prints
  ns/op).

```sh
cargo test --offline -p depot
cargo miri test --offline -p depot --lib -- --skip stress_tests --skip test_heap
cargo bench --offline -p depot
```
