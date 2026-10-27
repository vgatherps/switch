//! Pool paths over a full 2^24-slot static pool, visited in random order.
//!
//! Each trial allocates every usable slot, shuffles the entities, then times one pass of each
//! operation. Raw depot free on a separate depot of the same size is the baseline for entity drop.
//! Prints min / median / max ns per operation over the trials. Trial count is the first numeric
//! argument (default 5): `cargo bench --offline -p depot --bench pool_2p24 -- 9`.

use depot::{Claim, Component, DepotHandle, Entity, EntityId, PoolBinding, SyncCell};
use rand::{SeedableRng, rngs::SmallRng, seq::SliceRandom};
use std::hint::black_box;
use std::time::Instant;

const N: usize = 1 << 24;

depot::define_static_ecs!(BigPool, N);

type BigDepot = <BigPool as PoolBinding>::Depot;

// `store_write_raw` and `get_mut` write this static, so LLVM cannot treat it as never written and
// fold the timed reads to zero.
static STORE: SyncCell<[u64; N]> = SyncCell::new([0; N]);
static RAW_DEPOT: SyncCell<BigDepot> = SyncCell::new(BigDepot::new());

struct Value;

// SAFETY: `STORE` is a `SyncCell` static with one element per slot of `BigPool`, and both
// projections reference only the requested element.
unsafe impl Component<BigPool> for Value {
    type Storage = [u64; N];
    type Ref<'a> = &'a u64;
    type RefMut<'a> = &'a mut u64;

    #[inline(always)]
    fn storage_ptr(_: &BigPool) -> *mut Self::Storage {
        STORE.get()
    }

    #[inline(always)]
    unsafe fn get<'a>(storage: *const Self::Storage, slot: u32) -> &'a u64 {
        // SAFETY: The caller passes `STORE` and a slot below `N`, so the element pointer is in
        // bounds; only that element is referenced.
        unsafe { &*storage.cast::<u64>().add(slot as usize) }
    }

    #[inline(always)]
    unsafe fn get_mut<'a>(storage: *mut Self::Storage, slot: u32) -> &'a mut u64 {
        // SAFETY: As in `get`, and the caller guarantees no other reference to this slot.
        unsafe { &mut *storage.cast::<u64>().add(slot as usize) }
    }
}

fn ns_per_op(start: Instant, ops: usize) -> f64 {
    start.elapsed().as_nanos() as f64 / ops as f64
}

#[inline(never)]
fn store_read_raw(slots: &[u32]) -> u64 {
    let store = STORE.get().cast::<u64>();
    let mut sum = 0u64;
    for &slot in slots {
        // SAFETY: Every slot is below `N`, and no reference into `STORE` is live.
        sum = sum.wrapping_add(unsafe { *store.add(slot as usize) });
    }
    sum
}

#[inline(never)]
fn store_write_raw(slots: &[u32]) {
    let store = STORE.get().cast::<u64>();
    for &slot in slots {
        // SAFETY: Every slot is below `N`, and no reference into `STORE` is live.
        unsafe { *store.add(slot as usize) = slot as u64 };
    }
}

#[inline(never)]
fn get_all(claim: &Claim<BigPool>, entities: &[Entity<BigPool>]) -> u64 {
    let mut sum = 0u64;
    for entity in entities {
        sum = sum.wrapping_add(*claim.get::<Value>(entity));
    }
    sum
}

#[inline(never)]
fn get_mut_all(claim: &Claim<BigPool>, entities: &mut [Entity<BigPool>]) {
    for entity in entities {
        let value = entity.slot() as u64 + 1;
        *claim.get_mut::<Value>(entity).view_mut() = value;
    }
}

#[inline(never)]
fn get_or_none_all(claim: &Claim<BigPool>, ids: &[EntityId<BigPool>]) -> u64 {
    let mut sum = 0u64;
    for id in ids {
        sum = sum.wrapping_add(*claim.get_or_none_ref::<Value>(Some(id)).unwrap().view());
    }
    sum
}

#[inline(never)]
fn entity_drop_all(entities: Vec<Entity<BigPool>>) {
    for entity in entities {
        drop(entity);
    }
}

#[inline(never)]
fn raw_free_all(depot: &mut BigDepot, handles: &[DepotHandle]) {
    for &handle in handles {
        // SAFETY: Every handle was allocated from `depot` and is freed once.
        unsafe { depot.free_unchecked(handle) };
    }
}

fn main() {
    let trials: usize = std::env::args()
        .skip(1)
        .find_map(|arg| arg.parse().ok())
        .unwrap_or(5);
    let claim = BigPool::claim().unwrap();
    // SAFETY: Only this thread touches `RAW_DEPOT`, and this is the only reference to it.
    let raw_depot = unsafe { &mut *RAW_DEPOT.get() };
    let mut rng = SmallRng::seed_from_u64(7);

    let mut rows: [(&str, Vec<f64>); 7] = [
        ("store_read_raw", Vec::new()),
        ("store_write_raw", Vec::new()),
        ("get", Vec::new()),
        ("get_mut", Vec::new()),
        ("get_or_none", Vec::new()),
        ("entity_drop", Vec::new()),
        ("raw_free_unchecked", Vec::new()),
    ];

    for _ in 0..trials {
        let mut entities: Vec<Entity<BigPool>> =
            core::iter::from_fn(|| claim.alloc()).collect();
        assert_eq!(entities.len(), N - 1);
        entities.shuffle(&mut rng);
        let ops = entities.len();
        let slots: Vec<u32> = entities.iter().map(|entity| entity.slot()).collect();
        let ids: Vec<EntityId<BigPool>> = entities.iter().map(|entity| entity.id()).collect();

        let start = Instant::now();
        black_box(store_read_raw(&slots));
        rows[0].1.push(ns_per_op(start, ops));

        let start = Instant::now();
        store_write_raw(&slots);
        rows[1].1.push(ns_per_op(start, ops));

        let start = Instant::now();
        black_box(get_all(&claim, &entities));
        rows[2].1.push(ns_per_op(start, ops));

        let start = Instant::now();
        get_mut_all(&claim, &mut entities);
        rows[3].1.push(ns_per_op(start, ops));

        let start = Instant::now();
        black_box(get_or_none_all(&claim, &ids));
        rows[4].1.push(ns_per_op(start, ops));

        let start = Instant::now();
        entity_drop_all(entities);
        rows[5].1.push(ns_per_op(start, ops));

        let mut handles: Vec<DepotHandle> =
            core::iter::from_fn(|| raw_depot.alloc()).collect();
        assert_eq!(handles.len(), N - 1);
        handles.shuffle(&mut rng);
        let start = Instant::now();
        raw_free_all(raw_depot, &handles);
        rows[6].1.push(ns_per_op(start, ops));
    }

    println!("2^24-slot pool, random order, {trials} trials");
    for (name, mut samples) in rows {
        samples.sort_by(f64::total_cmp);
        println!(
            "{name:20} min {:6.2}  med {:6.2}  max {:6.2}  ns/op",
            samples[0],
            samples[samples.len() / 2],
            samples[samples.len() - 1]
        );
    }
}
