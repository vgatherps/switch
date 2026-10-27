//! Benchmarks for the slab allocator.

use criterion::{BatchSize, BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use depot::fixed_depot_type;

type BenchDepot = fixed_depot_type!(4096);

/// Fresh allocation from a new depot.
fn bench_fresh_alloc(c: &mut Criterion) {
    c.bench_function("fresh_alloc", |b| {
        b.iter_batched_ref(
            || Box::new(BenchDepot::new()),
            |alloc| {
                for _ in 0..1000 {
                    black_box(alloc.alloc().unwrap());
                }
            },
            BatchSize::SmallInput,
        )
    });
}

/// Single alloc/free cycle - measures L1 hot path
fn bench_alloc_free_cycle(c: &mut Criterion) {
    let mut alloc = BenchDepot::new();
    // Pre-warm: alloc and free to get index into ring
    let idx = alloc.alloc().unwrap();
    alloc.free(idx).unwrap();

    c.bench_function("alloc_free_single", |b| {
        b.iter(|| {
            let idx = alloc.alloc().unwrap();
            alloc.free(idx).unwrap();
        })
    });
}

/// Burst allocation - measures same-slab allocation speed
fn bench_burst_alloc(c: &mut Criterion) {
    let mut group = c.benchmark_group("burst_alloc");

    for size in [16, 32, 64, 128] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let mut alloc = BenchDepot::new();
            let mut indices = Vec::with_capacity(size);

            b.iter(|| {
                indices.clear();
                for _ in 0..size {
                    indices.push(alloc.alloc().unwrap());
                }
                for idx in indices.drain(..) {
                    alloc.free(idx).unwrap();
                }
            });
        });
    }
    group.finish();
}

/// Interleaved alloc/free - simulates packet processing pattern
fn bench_interleaved(c: &mut Criterion) {
    c.bench_function("interleaved_alloc_free", |b| {
        let mut alloc = BenchDepot::new();
        let mut live = Vec::with_capacity(100);

        b.iter(|| {
            // Allocate 10
            for _ in 0..10 {
                live.push(alloc.alloc().unwrap());
            }
            // Free all 10
            for _ in 0..10 {
                alloc.free(live.pop().unwrap()).unwrap();
            }
        });
    });
}

/// Allocation from cold state (bucket lookup required)
fn bench_bucket_alloc(c: &mut Criterion) {
    c.bench_function("bucket_alloc", |b| {
        let mut alloc = BenchDepot::new();

        // Pre-allocate many indices across many slabs
        let mut indices: Vec<_> = (0..1000).filter_map(|_| alloc.alloc()).collect();

        b.iter(|| {
            // Free 20 indices to various slabs (overflows stack)
            for _ in 0..20 {
                if let Some(idx) = indices.pop() {
                    alloc.free(idx).unwrap();
                }
            }
            // Allocate 20 - first 16 from stack, remaining 4 from hierarchy
            for _ in 0..20 {
                indices.push(alloc.alloc().unwrap());
            }
        });
    });
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
