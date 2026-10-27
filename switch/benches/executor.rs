//! Executor benchmarks.

use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};
use std::cell::RefCell;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use switch::Runtime;

#[macro_use]
#[path = "../tests/support/task_macro.rs"]
mod task_macro;

struct Ready;
impl Future for Ready {
    type Output = ();
    #[inline]
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        Poll::Ready(())
    }
}

struct YieldN(u32);
impl Future for YieldN {
    type Output = ();
    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 == 0 {
            Poll::Ready(())
        } else {
            self.0 -= 1;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

task! {
    fn ready() -> impl Future<Output = ()> {
        Ready
    }
}

task! {
    fn yield_n(n: u32) -> impl Future<Output = ()> {
        YieldN(n)
    }
}

fn bench_spawn_ready(c: &mut Criterion) {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    c.bench_function("spawn_ready_1", |b| {
        b.iter(|| {
            spawner.spawn(ready()).unwrap();
            runtime.poll(0).unwrap();
        })
    });
}

fn bench_spawn_yield(c: &mut Criterion) {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    c.bench_function("spawn_yield_10", |b| {
        b.iter(|| {
            spawner.spawn(yield_n(10)).unwrap();
            while runtime.poll(0).unwrap().live_tasks != 0 {}
        })
    });
}

fn bench_many_tasks(c: &mut Criterion) {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    c.bench_function("spawn_64_ready", |b| {
        b.iter(|| {
            for _ in 0..64 {
                spawner.spawn(ready()).unwrap();
            }
            while runtime.poll(0).unwrap().live_tasks != 0 {}
        })
    });
}

fn bench_waker_overhead(c: &mut Criterion) {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    c.bench_function("waker_wake_overhead", |b| {
        b.iter(|| {
            spawner.spawn(yield_n(1)).unwrap();
            runtime.poll(0).unwrap();
            runtime.poll(0).unwrap();
        })
    });
}

fn bench_poll_empty(c: &mut Criterion) {
    let mut runtime = Runtime::new(0);
    c.bench_function("poll_empty", |b| {
        b.iter(|| {
            runtime.poll(0).unwrap();
        })
    });
}

fn bench_yield_chain(c: &mut Criterion) {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    c.bench_function("yield_chain_100", |b| {
        b.iter(|| {
            spawner.spawn(yield_n(100)).unwrap();
            while runtime.poll(0).unwrap().live_tasks != 0 {}
        })
    });
}

/// Pure yield benchmark - no spawn/destroy in the hot path.
/// Spawns once, then each iteration is just: poll -> wake -> pending.
fn bench_pure_yield(c: &mut Criterion) {
    use std::cell::Cell;
    use std::rc::Rc;

    // Future that yields forever until we tell it to stop
    struct YieldForever {
        stop: Rc<Cell<bool>>,
    }

    impl Future for YieldForever {
        type Output = ();
        #[inline]
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.stop.get() {
                Poll::Ready(())
            } else {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    task! {
        fn yield_forever(stop: Rc<Cell<bool>>) -> impl Future<Output = ()> {
            YieldForever { stop }
        }
    }

    let stop = Rc::new(Cell::new(false));
    let stop_clone = stop.clone();
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    spawner.spawn(yield_forever(stop_clone)).unwrap();

    c.bench_function("pure_yield", |b| {
        b.iter(|| {
            runtime.poll(0).unwrap();
        })
    });

    // Clean up
    stop.set(true);
    runtime.poll(0).unwrap();
}

/// Parks forever; publishes a clone of its first waker.
struct Publish(Rc<RefCell<Option<Waker>>>);
impl Future for Publish {
    type Output = ();
    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut slot = self.0.borrow_mut();
        if slot.is_none() {
            *slot = Some(cx.waker().clone());
        }
        Poll::Pending
    }
}

task! {
    fn publish(slot: Rc<RefCell<Option<Waker>>>) -> impl Future<Output = ()> {
        Publish(slot)
    }
}

/// Spawn `n` parked tasks, poll them once, and return their wakers in spawn order.
fn parked_wakers(runtime: &mut Runtime, n: usize) -> Vec<Waker> {
    let spawner = runtime.spawner();
    let slots: Vec<_> = (0..n).map(|_| Rc::new(RefCell::new(None))).collect();
    for slot in &slots {
        spawner.spawn(publish(slot.clone())).unwrap();
    }
    runtime.poll(0).unwrap();
    slots
        .iter()
        .map(|slot| slot.borrow().clone().unwrap())
        .collect()
}

fn bench_waker_clone(c: &mut Criterion) {
    let mut runtime = Runtime::new(0);
    let waker = parked_wakers(&mut runtime, 1).remove(0);
    c.bench_function("waker_clone_drop", |b| {
        b.iter(|| drop(black_box(&waker).clone()))
    });
    let mut target = waker.clone();
    c.bench_function("waker_clone_from_same", |b| {
        b.iter(|| black_box(&mut target).clone_from(black_box(&waker)))
    });
}

/// The 65th spawn into a runtime whose first slab is full. Slabs are never
/// freed, so after warm-up the new slab comes from the function's spare list.
fn bench_spawn_new_slab(c: &mut Criterion) {
    c.bench_function("spawn_new_slab", |b| {
        b.iter_batched_ref(
            || {
                let mut runtime = Runtime::new(0);
                drop(parked_wakers(&mut runtime, 64));
                let spawner = runtime.spawner();
                let slot = Rc::new(RefCell::new(None));
                (runtime, spawner, slot)
            },
            |(_, spawner, slot)| spawner.spawn(publish(slot.clone())).unwrap(),
            BatchSize::SmallInput,
        )
    });
}

/// Wake and poll 64 tasks that sit in 64 different slabs.
fn bench_wake_spread(c: &mut Criterion) {
    let mut runtime = Runtime::new(0);
    let wakers: Vec<_> = parked_wakers(&mut runtime, 64 * 64)
        .into_iter()
        .step_by(64)
        .collect();
    c.bench_function("wake_spread_64_slabs", |b| {
        b.iter(|| {
            for waker in &wakers {
                waker.wake_by_ref();
            }
            black_box(runtime.poll(0).unwrap())
        })
    });
}

criterion_group!(
    benches,
    bench_spawn_ready,
    bench_spawn_yield,
    bench_many_tasks,
    bench_waker_overhead,
    bench_poll_empty,
    bench_yield_chain,
    bench_pure_yield,
    bench_waker_clone,
    bench_spawn_new_slab,
    bench_wake_spread,
);

criterion_main!(benches);
