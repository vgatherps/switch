// This bench uses part of the module.
#[allow(dead_code)]
#[path = "../tests/support/runtime_workload.rs"]
mod workload;
use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::rc::Rc;
use switch::{Capacity, Runtime, Sleep};

#[switch::task]
fn sleeps_then(sleeps: Vec<Sleep>, audit: Rc<workload::Audit>) -> impl Future<Output = ()> {
    workload::Task {
        sleeps,
        yields: 0,
        value: 528,
        audit,
    }
}

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
        b.iter(|| black_box(repeated.poll(0).unwrap()))
    });
    let mut advancing = Runtime::new(0);
    let mut tick = 0u64;
    c.bench_function("runtime_empty_advancing", |b| {
        b.iter(|| {
            tick = tick.checked_add(1).unwrap();
            black_box(advancing.poll(black_box(tick)).unwrap())
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

    // 32 sleeps expire in one turn and complete their task. One long-lived
    // runtime; each iteration's untimed setup spawns the task and polls it once.
    let expiry = RefCell::new(Runtime::with_capacity(
        0,
        Capacity {
            tasks: 2,
            timers: 32,
        },
    ));
    let audit = Rc::new(workload::Audit::default());
    let tick = Cell::new(0u64);
    c.bench_function("runtime_expiry_32", |b| {
        b.iter_batched(
            || {
                // `complete` records each value; keep the record from growing.
                audit.output.borrow_mut().clear();
                let mut runtime = expiry.borrow_mut();
                let spawner = runtime.spawner();
                let deadline = tick.get() + 2;
                let sleeps = (0..32)
                    .map(|_| spawner.sleep_until(deadline).unwrap())
                    .collect();
                spawner.spawn(sleeps_then(sleeps, audit.clone())).unwrap();
                runtime.poll(deadline - 1).unwrap();
                tick.set(deadline);
            },
            |()| {
                let report = expiry.borrow_mut().poll(tick.get()).unwrap();
                black_box((report, audit.checksum.get(), audit.drops.get()))
            },
            BatchSize::PerIteration,
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
