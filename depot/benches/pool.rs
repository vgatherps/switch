//! High-level pool interface benchmarks.
//!
//! These measure the normal claim/entity/component path, not just raw depot alloc/free.

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use depot::{
    Component, Entity, LocalPool, LocalPoolBinding, PoolBinding, PoolClaim, fixed_depot_type,
};

const SLOTS: usize = 4096;
const LIVE: usize = 512;

type BenchDepot = fixed_depot_type!(SLOTS);
type Core = [u64; SLOTS];
type LocalBinding = LocalPoolBinding<BenchDepot, Core>;

depot::define_static_ecs!(BenchStaticPool, SLOTS, core: Core = [0; SLOTS]);

struct Counter;

unsafe impl Component<BenchStaticPool> for Counter {
    type Storage = Core;
    type Ref<'a> = &'a u64;
    type RefMut<'a> = &'a mut u64;

    #[inline(always)]
    fn storage_ptr(binding: &BenchStaticPool) -> *mut Self::Storage {
        PoolBinding::core_ptr(binding)
    }

    #[inline(always)]
    unsafe fn get<'a>(storage: *const Self::Storage, slot: u32) -> &'a u64 {
        // SAFETY: The caller passes this pool's storage and a slot below `SLOTS`, so the element
        // pointer is in bounds; only that element is referenced.
        unsafe { &*storage.cast::<u64>().add(slot as usize) }
    }

    #[inline(always)]
    unsafe fn get_mut<'a>(storage: *mut Self::Storage, slot: u32) -> &'a mut u64 {
        // SAFETY: As in `get`, and the caller guarantees no other reference to this slot.
        unsafe { &mut *storage.cast::<u64>().add(slot as usize) }
    }
}

unsafe impl Component<LocalBinding> for Counter {
    type Storage = Core;
    type Ref<'a> = &'a u64;
    type RefMut<'a> = &'a mut u64;

    #[inline(always)]
    fn storage_ptr(binding: &LocalBinding) -> *mut Self::Storage {
        PoolBinding::core_ptr(binding)
    }

    #[inline(always)]
    unsafe fn get<'a>(storage: *const Self::Storage, slot: u32) -> &'a u64 {
        // SAFETY: The caller passes this pool's storage and a slot below `SLOTS`, so the element
        // pointer is in bounds; only that element is referenced.
        unsafe { &*storage.cast::<u64>().add(slot as usize) }
    }

    #[inline(always)]
    unsafe fn get_mut<'a>(storage: *mut Self::Storage, slot: u32) -> &'a mut u64 {
        // SAFETY: As in `get`, and the caller guarantees no other reference to this slot.
        unsafe { &mut *storage.cast::<u64>().add(slot as usize) }
    }
}

fn bench_alloc_drop(c: &mut Criterion) {
    let mut group = c.benchmark_group("pool_alloc_drop");

    let static_claim = BenchStaticPool::claim().unwrap();
    group.bench_function("static", |b| {
        b.iter(|| {
            let entity = static_claim.alloc().unwrap();
            drop(entity);
        })
    });

    let local_pool = LocalPool::new(BenchDepot::new(), [0; SLOTS]);
    let local_claim = local_pool.claim();
    group.bench_function("plain_rc", |b| {
        b.iter(|| {
            let entity = local_claim.alloc().unwrap();
            drop(entity);
        })
    });

    group.finish();
}

fn bench_get_live_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("pool_get_live_set");
    group.throughput(Throughput::Elements(LIVE as u64));

    let static_claim = BenchStaticPool::claim().unwrap();
    let mut static_entities = alloc_live(&static_claim);
    for entity in &mut static_entities {
        let slot = entity.slot() as u64;
        *static_claim.get_mut::<Counter>(entity).view_mut() = slot;
    }
    group.bench_function("static", |b| {
        b.iter(|| {
            let mut sum = 0u64;
            for entity in &static_entities {
                sum = sum.wrapping_add(*static_claim.get::<Counter>(entity));
            }
            black_box(sum);
        })
    });

    let local_pool = LocalPool::new(BenchDepot::new(), [0; SLOTS]);
    let local_claim = local_pool.claim();
    let mut local_entities = alloc_live(&local_claim);
    for entity in &mut local_entities {
        let slot = entity.slot() as u64;
        *local_claim.get_mut::<Counter>(entity).view_mut() = slot;
    }
    group.bench_function("plain_rc", |b| {
        b.iter(|| {
            let mut sum = 0u64;
            for entity in &local_entities {
                sum = sum.wrapping_add(*local_claim.get::<Counter>(entity));
            }
            black_box(sum);
        })
    });

    group.finish();
}

fn bench_get_mut_live_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("pool_get_mut_live_set");
    group.throughput(Throughput::Elements(LIVE as u64));

    let static_claim = BenchStaticPool::claim().unwrap();
    let mut static_entities = alloc_live(&static_claim);
    group.bench_function("static", |b| {
        b.iter(|| {
            for entity in &mut static_entities {
                let slot = entity.slot() as u64;
                *static_claim.get_mut::<Counter>(entity).view_mut() = black_box(slot);
            }
        })
    });

    let local_pool = LocalPool::new(BenchDepot::new(), [0; SLOTS]);
    let local_claim = local_pool.claim();
    let mut local_entities = alloc_live(&local_claim);
    group.bench_function("plain_rc", |b| {
        b.iter(|| {
            for entity in &mut local_entities {
                let slot = entity.slot() as u64;
                *local_claim.get_mut::<Counter>(entity).view_mut() = black_box(slot);
            }
        })
    });

    group.finish();
}

fn bench_get_or_none_live_ids(c: &mut Criterion) {
    let mut group = c.benchmark_group("pool_get_or_none_live_ids");
    group.throughput(Throughput::Elements(LIVE as u64));

    let static_claim = BenchStaticPool::claim().unwrap();
    let static_entities = alloc_live(&static_claim);
    let static_ids: Vec<_> = static_entities.iter().map(Entity::id).collect();
    group.bench_function("static", |b| {
        b.iter(|| {
            let mut sum = 0u64;
            for id in &static_ids {
                sum = sum.wrapping_add(
                    *static_claim
                        .get_or_none_ref::<Counter>(Some(id))
                        .unwrap()
                        .view(),
                );
            }
            black_box(sum);
        })
    });

    let local_pool = LocalPool::new(BenchDepot::new(), [0; SLOTS]);
    let local_claim = local_pool.claim();
    let local_entities = alloc_live(&local_claim);
    let local_ids: Vec<_> = local_entities.iter().map(Entity::id).collect();
    group.bench_function("plain_rc", |b| {
        b.iter(|| {
            let mut sum = 0u64;
            for id in &local_ids {
                sum = sum.wrapping_add(
                    *local_claim
                        .get_or_none_ref::<Counter>(Some(id))
                        .unwrap()
                        .view(),
                );
            }
            black_box(sum);
        })
    });

    group.finish();
}

fn alloc_live<P>(claim: &P) -> Vec<Entity<P::Binding>>
where
    P: PoolClaim,
{
    (0..LIVE).map(|_| claim.alloc().unwrap()).collect()
}

criterion_group!(
    benches,
    bench_alloc_drop,
    bench_get_live_set,
    bench_get_mut_live_set,
    bench_get_or_none_live_ids,
);
criterion_main!(benches);
