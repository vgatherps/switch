//! Benchmarks for the slab allocator.

use criterion::{BatchSize, BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use depot::{DepotHandle, DynDepot, fixed_depot_type};

type BenchDepot = fixed_depot_type!(4096);

trait AllocBench: Sized {
    fn new() -> Self;
    fn alloc(&mut self) -> Option<DepotHandle>;
    fn free(&mut self, handle: DepotHandle);
}

impl AllocBench for BenchDepot {
    #[inline(always)]
    fn new() -> Self {
        BenchDepot::new()
    }

    #[inline(always)]
    fn alloc(&mut self) -> Option<DepotHandle> {
        BenchDepot::alloc(self)
    }

    #[inline(always)]
    fn free(&mut self, handle: DepotHandle) {
        BenchDepot::free(self, handle).unwrap();
    }
}

impl AllocBench for DynDepot {
    #[inline(always)]
    fn new() -> Self {
        DynDepot::new(4096)
    }

    #[inline(always)]
    fn alloc(&mut self) -> Option<DepotHandle> {
        DynDepot::alloc(self)
    }

    #[inline(always)]
    fn free(&mut self, handle: DepotHandle) {
        DynDepot::free(self, handle).unwrap();
    }
}

fn fresh_alloc<D: AllocBench>(c: &mut Criterion, name: &str) {
    c.bench_function(name, |b| {
        b.iter_batched_ref(
            || Box::new(D::new()),
            |alloc| {
                for _ in 0..1000 {
                    black_box(alloc.alloc().unwrap());
                }
            },
            BatchSize::SmallInput,
        )
    });
}

/// Fresh allocation from a new depot.
fn bench_fresh_alloc(c: &mut Criterion) {
    fresh_alloc::<BenchDepot>(c, "fresh_alloc");
    fresh_alloc::<DynDepot>(c, "dyn_fresh_alloc");
}

fn alloc_free_cycle<D: AllocBench>(c: &mut Criterion, name: &str) {
    let mut alloc = D::new();
    let idx = alloc.alloc().unwrap();
    alloc.free(idx);

    c.bench_function(name, |b| {
        b.iter(|| {
            let idx = alloc.alloc().unwrap();
            alloc.free(idx);
        })
    });
}

/// Single alloc/free cycle - measures L1 hot path
fn bench_alloc_free_cycle(c: &mut Criterion) {
    alloc_free_cycle::<BenchDepot>(c, "alloc_free_single");
    alloc_free_cycle::<DynDepot>(c, "dyn_alloc_free_single");
}

fn burst_alloc<D: AllocBench>(c: &mut Criterion, name: &str) {
    let mut group = c.benchmark_group(name);

    for size in [16, 32, 64, 128] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let mut alloc = D::new();
            let mut indices = Vec::with_capacity(size);

            b.iter(|| {
                indices.clear();
                for _ in 0..size {
                    indices.push(alloc.alloc().unwrap());
                }
                for idx in indices.drain(..) {
                    alloc.free(idx);
                }
            });
        });
    }
    group.finish();
}

/// Burst allocation - measures same-slab allocation speed
fn bench_burst_alloc(c: &mut Criterion) {
    burst_alloc::<BenchDepot>(c, "burst_alloc");
    burst_alloc::<DynDepot>(c, "dyn_burst_alloc");
}

fn interleaved<D: AllocBench>(c: &mut Criterion, name: &str) {
    c.bench_function(name, |b| {
        let mut alloc = D::new();
        let mut live = Vec::with_capacity(100);

        b.iter(|| {
            for _ in 0..10 {
                live.push(alloc.alloc().unwrap());
            }
            for _ in 0..10 {
                alloc.free(live.pop().unwrap());
            }
        });
    });
}

/// Interleaved alloc/free - simulates packet processing pattern
fn bench_interleaved(c: &mut Criterion) {
    interleaved::<BenchDepot>(c, "interleaved_alloc_free");
    interleaved::<DynDepot>(c, "dyn_interleaved_alloc_free");
}

fn bucket_alloc<D: AllocBench>(c: &mut Criterion, name: &str) {
    c.bench_function(name, |b| {
        let mut alloc = D::new();
        let mut indices: Vec<_> = (0..1000).filter_map(|_| alloc.alloc()).collect();

        b.iter(|| {
            for _ in 0..20 {
                if let Some(idx) = indices.pop() {
                    alloc.free(idx);
                }
            }
            for _ in 0..20 {
                indices.push(alloc.alloc().unwrap());
            }
        });
    });
}

/// Allocation from cold state (bucket lookup required)
fn bench_bucket_alloc(c: &mut Criterion) {
    bucket_alloc::<BenchDepot>(c, "bucket_alloc");
    bucket_alloc::<DynDepot>(c, "dyn_bucket_alloc");
}

criterion_group!(
    benches,
    bench_fresh_alloc,
    bench_alloc_free_cycle,
    bench_burst_alloc,
    bench_interleaved,
    bench_bucket_alloc,
);
criterion_main!(benches);
