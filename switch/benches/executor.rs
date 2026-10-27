//! Executor benchmarks.

use criterion::{Criterion, criterion_group, criterion_main};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use switch::Runtime;

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

fn bench_spawn_ready(c: &mut Criterion) {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    c.bench_function("spawn_ready_1", |b| {
        b.iter(|| {
            spawner.spawn(Ready).unwrap();
            runtime.poll(0).unwrap();
        })
    });
}

fn bench_spawn_yield(c: &mut Criterion) {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    c.bench_function("spawn_yield_10", |b| {
        b.iter(|| {
            spawner.spawn(YieldN(10)).unwrap();
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
                spawner.spawn(Ready).unwrap();
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
            spawner.spawn(YieldN(1)).unwrap();
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
            spawner.spawn(YieldN(100)).unwrap();
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

    let stop = Rc::new(Cell::new(false));
    let stop_clone = stop.clone();
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    spawner.spawn(YieldForever { stop: stop_clone }).unwrap();

    c.bench_function("pure_yield", |b| {
        b.iter(|| {
            runtime.poll(0).unwrap();
        })
    });

    // Clean up
    stop.set(true);
    runtime.poll(0).unwrap();
}

/// Includes checked ThinWaker extraction followed by direct local waking.
fn bench_pure_direct_yield(c: &mut Criterion) {
    use std::cell::Cell;
    use std::rc::Rc;

    struct DirectYieldForever(Rc<Cell<bool>>);
    impl Future for DirectYieldForever {
        type Output = ();
        #[inline]
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.0.get() {
                Poll::Ready(())
            } else {
                switch::current_task(cx).wake();
                Poll::Pending
            }
        }
    }
    let stop = Rc::new(Cell::new(false));
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    spawner.spawn(DirectYieldForever(stop.clone())).unwrap();
    c.bench_function("pure_direct_yield", |b| {
        b.iter(|| {
            runtime.poll(0).unwrap();
        })
    });
    stop.set(true);
    runtime.poll(0).unwrap();
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
    bench_pure_direct_yield,
);

criterion_main!(benches);
