//! Executor benchmarks.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use std::cell::{Cell, RefCell};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use switch::{Capacity, Runtime, StoredWaker};

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

#[switch::task]
fn ready() -> impl Future<Output = ()> {
    Ready
}

#[switch::task]
fn yield_n(n: u32) -> impl Future<Output = ()> {
    YieldN(n)
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

/// Spawn, a first poll that self-wakes, a second poll that completes.
fn bench_spawn_wake_complete(c: &mut Criterion) {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    c.bench_function("spawn_wake_complete", |b| {
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

    #[switch::task]
    fn yield_forever(stop: Rc<Cell<bool>>) -> impl Future<Output = ()> {
        YieldForever { stop }
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

#[switch::task]
fn publish(slot: Rc<RefCell<Option<Waker>>>) -> impl Future<Output = ()> {
    Publish(slot)
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

/// Parks forever; publishes a `StoredWaker` taken on its first poll.
struct PublishStored(Rc<Cell<Option<StoredWaker>>>);
impl Future for PublishStored {
    type Output = ();
    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0.get().is_none() {
            self.0.set(Some(StoredWaker::new(cx)));
        }
        Poll::Pending
    }
}

#[switch::task]
fn publish_stored(slot: Rc<Cell<Option<StoredWaker>>>) -> impl Future<Output = ()> {
    PublishStored(slot)
}

/// Wake an already-parked task, then poll it: through its std `Waker`, and
/// through a `StoredWaker` (direct, no vtable call).
fn bench_wake_parked(c: &mut Criterion) {
    let mut runtime = Runtime::new(0);
    let waker = parked_wakers(&mut runtime, 1).remove(0);
    c.bench_function("wake_parked_waker", |b| {
        b.iter(|| {
            black_box(&waker).wake_by_ref();
            black_box(runtime.poll(0).unwrap())
        })
    });
    let mut runtime = Runtime::new(0);
    let slot = Rc::new(Cell::new(None));
    runtime
        .spawner()
        .spawn(publish_stored(slot.clone()))
        .unwrap();
    runtime.poll(0).unwrap();
    let stored = slot.get().unwrap();
    c.bench_function("wake_parked_stored", |b| {
        b.iter(|| {
            black_box(stored).wake();
            black_box(runtime.poll(0).unwrap())
        })
    });
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

/// Parks forever like `Publish`; two fit a 64 KiB task block.
struct Wide(Publish, #[allow(dead_code)] [u8; 30_000]);
impl Future for Wide {
    type Output = ();
    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        Pin::new(&mut self.0).poll(cx)
    }
}

#[switch::task(capacity = 128)]
fn wide(slot: Rc<RefCell<Option<Waker>>>) -> impl Future<Output = ()> {
    Wide(Publish(slot), [0; 30_000])
}

/// Spawn `n` parked `wide` tasks, poll them once, and return their wakers.
fn wide_wakers(runtime: &mut Runtime, n: usize) -> Vec<Waker> {
    let spawner = runtime.spawner();
    let slots: Vec<_> = (0..n).map(|_| Rc::new(RefCell::new(None))).collect();
    for slot in &slots {
        spawner.spawn(wide(slot.clone())).unwrap();
    }
    runtime.poll(0).unwrap();
    slots
        .iter()
        .map(|slot| slot.borrow().clone().unwrap())
        .collect()
}

/// Most blocks `spawn_new_block` allocates in a run (256 MiB), and per sample.
const NEW_BLOCKS: u32 = 4096;
const SAMPLE_BLOCKS: u64 = 16;

/// Never polled; two fit a 64 KiB task block.
#[switch::task(capacity = 8192)]
fn wide_parked() -> impl Future<Output = ()> {
    Wide(Publish(Rc::new(RefCell::new(None))), [0; 30_000])
}

/// A spawn that takes the first slot of a new block: a cold allocation of a
/// fresh, zeroed 64 KiB block (blocks are never freed or reused). Every block
/// stays allocated, so one long-lived runtime takes a bounded number of blocks:
/// each sample measures at most `SAMPLE_BLOCKS` spawns and reports their mean
/// for all its iterations (criterion's warm-up then needs only a few calls);
/// after `NEW_BLOCKS` in total, a sample reports the mean of all earlier ones.
fn bench_spawn_new_block(c: &mut Criterion) {
    let runtime = Runtime::with_capacity(
        0,
        Capacity {
            tasks: 2 * NEW_BLOCKS as usize,
            timers: 0,
        },
    );
    let spawner = runtime.spawner();
    let (mut measured, mut total) = (0u32, Duration::ZERO);
    c.bench_function("spawn_new_block", |b| {
        b.iter_custom(|iters| {
            let n = iters
                .min(SAMPLE_BLOCKS)
                .min(u64::from(NEW_BLOCKS - measured)) as u32;
            let mut sample = Duration::ZERO;
            for _ in 0..n {
                let start = Instant::now();
                spawner.spawn(wide_parked()).unwrap();
                sample += start.elapsed();
                // Fill the block's second slot, so the next spawn needs a new one.
                spawner.spawn(wide_parked()).unwrap();
            }
            measured += n;
            total += sample;
            let mean = if n > 0 { sample / n } else { total / measured };
            mean.mul_f64(iters as f64)
        })
    });
    drop(runtime);
}

/// Wake and poll 64 tasks that sit in 64 different blocks.
fn bench_wake_spread(c: &mut Criterion) {
    let mut runtime = Runtime::new(0);
    let wakers: Vec<_> = wide_wakers(&mut runtime, 128)
        .into_iter()
        .step_by(2)
        .collect();
    c.bench_function("wake_spread_64_blocks", |b| {
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
    bench_spawn_wake_complete,
    bench_poll_empty,
    bench_yield_chain,
    bench_pure_yield,
    bench_wake_parked,
    bench_waker_clone,
    bench_spawn_new_block,
    bench_wake_spread,
);

criterion_main!(benches);
