// This bench uses part of the module.
#[allow(dead_code)]
#[path = "../tests/support/runtime_workload.rs"]
mod workload;
use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::rc::Rc;
use switch::{Capacity, PollReport, Runtime, Sleep, Spawner};

#[switch::task]
fn mixed(
    sleeps: Vec<Sleep>,
    yields: usize,
    value: u64,
    audit: Rc<workload::Audit>,
) -> impl Future<Output = ()> {
    workload::Task {
        sleeps,
        yields,
        value,
        audit,
    }
}

/// `workload::Mixed`'s six turns on a long-lived runtime: the same tasks,
/// events and per-turn counts, with ticks relative to the runtime's clock
/// (+0, +0, +1, +2, +3, +4) instead of the fixture's jumps to 2^60 and u64::MAX.
struct Rolling {
    runtime: Runtime,
    spawner: Spawner,
    audit: Rc<workload::Audit>,
    events: VecDeque<u64>,
    base: u64,
}

impl Rolling {
    const STEPS: [u64; 6] = [0, 0, 1, 2, 3, 4];

    fn new() -> Self {
        let runtime = Runtime::with_capacity(
            0,
            Capacity {
                tasks: 40,
                timers: 32,
            },
        );
        Self {
            spawner: runtime.spawner(),
            runtime,
            audit: Rc::new(workload::Audit::default()),
            events: VecDeque::with_capacity(2),
            base: 0,
        }
    }

    /// The fixture's two initial tasks; untimed.
    fn prepare(&mut self) {
        self.audit.output.borrow_mut().clear();
        let deadline = self.base + 2;
        let sleeps = (0..32)
            .map(|_| self.spawner.sleep_until(deadline).unwrap())
            .collect();
        let spawner = &self.spawner;
        spawner
            .spawn(mixed(sleeps, 0, 528, self.audit.clone()))
            .unwrap();
        spawner
            .spawn(mixed(Vec::new(), 1, 40, self.audit.clone()))
            .unwrap();
    }

    fn step(&mut self, index: usize) -> PollReport {
        match index {
            0 => self.events.push_back(1),
            2 => {
                self.events.push_back(2);
                self.events.push_back(3);
            }
            4 => self.events.push_back(4),
            5 => self.events.push_back(5),
            _ => {}
        }
        while let Some(value) = self.events.pop_front() {
            let sleeps = if value == 4 {
                vec![self.spawner.sleep_until(self.base + 4).unwrap()]
            } else {
                Vec::new()
            };
            self.spawner
                .spawn(mixed(sleeps, 0, value, self.audit.clone()))
                .unwrap();
        }
        self.runtime.poll(self.base + Self::STEPS[index]).unwrap()
    }

    fn run(&mut self) -> [PollReport; 6] {
        let reports = std::array::from_fn(|index| self.step(index));
        self.base += 4;
        reports
    }
}

/// Six-turn runs over `runtimes` long-lived runtimes, taken in turn.
fn bench_rolling(c: &mut Criterion, name: &str, runtimes: usize) {
    let fixtures = RefCell::new((0..runtimes).map(|_| Rolling::new()).collect::<Vec<_>>());
    let next = Cell::new(0);
    c.bench_function(name, |b| {
        b.iter_batched(
            || {
                let index = next.get();
                next.set((index + 1) % runtimes);
                fixtures.borrow_mut()[index].prepare();
                index
            },
            |index| {
                let fixture = &mut fixtures.borrow_mut()[index];
                black_box((
                    fixture.run(),
                    fixture.audit.checksum.get(),
                    fixture.audit.drops.get(),
                ))
            },
            BatchSize::PerIteration,
        )
    });
}

fn runtime_benches(c: &mut Criterion) {
    // Controls use the exact production fixture outside timing.
    let mut control = workload::Mixed::new();
    let reports = control.run();
    control.validate(reports);
    let mut rolling = Rolling::new();
    for _ in 0..2 {
        rolling.prepare();
        assert_eq!(rolling.run().map(workload::counts), workload::EXPECTED);
        assert_eq!(*rolling.audit.output.borrow(), [1, 40, 2, 3, 528, 5, 4]);
    }
    assert_eq!(rolling.audit.drops.get(), 14);
    let mut pending_control = workload::PendingSleep::new();
    assert_eq!(workload::counts(pending_control.step()), (1, 1));
    assert_eq!(workload::counts(pending_control.finish()), (1, 0));
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
    assert_eq!(workload::counts(pending.finish()), (1, 0));
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
                spawner.spawn(mixed(sleeps, 0, 528, audit.clone())).unwrap();
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
    // One runtime, and 64 runtimes on this thread, each running whole six-turn
    // runs in turn. `runtime_mixed_six_turns` is the one-runtime case.
    bench_rolling(c, "runtime_mixed_six_turns", 1);
    bench_rolling(c, "runtime_mixed_six_turns_1rt", 1);
    bench_rolling(c, "runtime_mixed_six_turns_64rt", 64);
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
