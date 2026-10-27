#[path = "../tests/support/runtime_workload.rs"]
mod workload;
use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};
use switch::{Capacity, Runtime};

fn runtime_benches(c: &mut Criterion) {
    // Controls use the exact production fixture outside timing.
    let mut control = workload::Mixed::new();
    let reports = control.run();
    control.validate(reports);
    let mut pending_control = workload::PendingSleep::new();
    assert_eq!(workload::counts(pending_control.step()), (1, 0, 0, 1, 1));
    assert_eq!(pending_control.finish().tasks_completed, 1);
    assert_eq!(pending_control.drops.get(), 1);

    let mut repeated = Runtime::new(0);
    c.bench_function("runtime_empty_repeated", |b| {
        b.iter(|| black_box(repeated.poll(0, || {}).unwrap()))
    });
    let mut advancing = Runtime::new(0);
    let mut tick = 0u64;
    c.bench_function("runtime_empty_advancing", |b| {
        b.iter(|| {
            tick = tick.checked_add(1).unwrap();
            black_box(advancing.poll(black_box(tick), || {}).unwrap())
        })
    });
    let mut pending = workload::PendingSleep::new();
    c.bench_function("runtime_registered_pending_sleep", |b| {
        b.iter(|| black_box(pending.step()))
    });
    let polls = pending.polls.get();
    assert!(polls > 1);
    assert_eq!(pending.finish().tasks_completed, 1);
    assert_eq!(pending.drops.get(), 1);
    black_box(polls);

    c.bench_function("runtime_expiry_32", |b| {
        b.iter_batched_ref(
            || {
                let mut fixture = workload::Mixed::new();
                fixture.step(0);
                fixture.step(1);
                fixture
            },
            |fixture| {
                let report = fixture.step(3);
                black_box((
                    report,
                    fixture.audit.checksum.get(),
                    fixture.audit.drops.get(),
                ))
            },
            BatchSize::LargeInput,
        )
    });
    c.bench_function("runtime_mixed_six_turns", |b| {
        b.iter_batched_ref(
            workload::Mixed::new,
            |fixture| {
                black_box((
                    fixture.run(),
                    fixture.audit.checksum.get(),
                    fixture.audit.drops.get(),
                ))
            },
            BatchSize::LargeInput,
        )
    });
    c.bench_function("runtime_cold_create_drop", |b| {
        b.iter(|| {
            let runtime = Runtime::with_capacity(
                100,
                Capacity {
                    tasks: 40,
                    timers: 32,
                },
            );
            black_box(&runtime);
            drop(runtime);
        })
    });
}
criterion_group!(benches, runtime_benches);
criterion_main!(benches);
