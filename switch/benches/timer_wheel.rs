// This bench uses part of the module.
#[allow(dead_code)]
#[path = "../src/intrusive/mod.rs"]
mod intrusive;
// This bench uses part of the module.
#[allow(dead_code)]
#[path = "../src/timer_wheel.rs"]
mod timer_wheel;
use timer_wheel::benchmark_workloads as workloads;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, black_box};
use std::time::Duration;
use timer_wheel::TimerWheel;

fn main() {
    let mut criterion = Criterion::default()
        .sample_size(30)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(2))
        .configure_from_args();

    let mut group = criterion.benchmark_group("wheel_insert_remove_near");
    for count in [1, 32, 1024] {
        workloads::check_insert_remove(count);
        group.throughput(Throughput::Elements(2 * count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(count),
            &count,
            |bench, &count| {
                bench.iter_batched_ref(
                    || (TimerWheel::new(0), workloads::near_entries(count)),
                    |(wheel, entries)| {
                        workloads::insert_all(wheel, black_box(entries));
                        workloads::remove_all(wheel, entries);
                        black_box(wheel.has_pending());
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();

    let mut group = criterion.benchmark_group("wheel_advance");
    for (name, count, deadline, end) in [
        ("dense_l0", 1024, 7, 7),
        ("cascade", 1024, 40, 40),
        ("sparse_gap", 1, 100001, 1048576),
        ("sparse_gap", 32, 100001, 1048576),
    ] {
        workloads::check_expiry(count, deadline, end);
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::new(name, count), &count, |bench, &count| {
            bench.iter_batched_ref(
                || workloads::prepared(count, deadline),
                |state| {
                    workloads::advance(black_box(state), black_box(end));
                    black_box(&state.2);
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
    criterion.final_summary();
}
