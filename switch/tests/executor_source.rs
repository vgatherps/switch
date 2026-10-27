//! Integration tests.

use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use switch::{Runtime, StoredWaker, current_task};

struct YieldN(u32);

impl Future for YieldN {
    type Output = ();
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

/// Demonstrates how a runtime primitive would store a waker.
/// Uses the unsafe StoredWaker API instead of Waker::clone().
struct ManualWake {
    waker_slot: Rc<Cell<Option<StoredWaker>>>,
    done: bool,
}

impl Future for ManualWake {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.done {
            Poll::Ready(())
        } else {
            // SAFETY: Task is alive for duration of this test.
            self.waker_slot.set(Some(unsafe { StoredWaker::new(cx) }));
            self.done = true;
            Poll::Pending
        }
    }
}

#[test]
fn test_spawn_and_poll() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let ran = Rc::new(Cell::new(false));
    let ran_clone = ran.clone();

    spawner
        .spawn(async move {
            ran_clone.set(true);
        })
        .unwrap();

    let report = runtime.poll(0, || {}).unwrap();
    assert_eq!(
        (
            report.tasks_polled,
            report.tasks_completed,
            report.live_tasks
        ),
        (1, 1, 0)
    );
    assert!(ran.get());
}

#[test]
fn test_yield_and_resume() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let count = Rc::new(Cell::new(0));
    let count_clone = count.clone();

    spawner
        .spawn(async move {
            YieldN(3).await;
            count_clone.set(1);
        })
        .unwrap();

    let mut polls = 0;
    let mut report = runtime.poll(0, || {}).unwrap();
    polls += report.tasks_polled;
    while report.live_tasks != 0 {
        report = runtime.poll(0, || {}).unwrap();
        polls += report.tasks_polled;
    }

    assert_eq!(polls, 4);
    assert_eq!(count.get(), 1);
}

#[test]
fn test_multiple_tasks() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let count = Rc::new(Cell::new(0));

    for _ in 0..10 {
        let c = count.clone();
        spawner
            .spawn(async move {
                c.set(c.get() + 1);
            })
            .unwrap();
    }

    let report = runtime.poll(0, || {}).unwrap();
    assert_eq!(
        (
            report.tasks_polled,
            report.tasks_completed,
            report.live_tasks
        ),
        (10, 10, 0)
    );
    assert_eq!(count.get(), 10);
}

#[test]
fn test_nested_spawn() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let inner_spawner = spawner.clone();
    let inner_ran = Rc::new(Cell::new(false));
    let inner_clone = inner_ran.clone();

    spawner
        .spawn(async move {
            inner_spawner
                .spawn(async move {
                    inner_clone.set(true);
                })
                .unwrap();
        })
        .unwrap();

    let first = runtime.poll(0, || {}).unwrap();
    assert_eq!(
        (first.tasks_polled, first.tasks_completed, first.live_tasks),
        (1, 1, 1)
    );
    let second = runtime.poll(0, || {}).unwrap();
    assert_eq!(
        (
            second.tasks_polled,
            second.tasks_completed,
            second.live_tasks
        ),
        (1, 1, 0)
    );
    assert!(inner_ran.get());
}

#[test]
fn test_stored_waker_across_polls() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let waker_slot: Rc<Cell<Option<StoredWaker>>> = Rc::new(Cell::new(None));
    let slot_clone = waker_slot.clone();
    let done = Rc::new(Cell::new(false));
    let done_clone = done.clone();

    spawner
        .spawn(async move {
            ManualWake {
                waker_slot: slot_clone,
                done: false,
            }
            .await;
            done_clone.set(true);
        })
        .unwrap();

    let first = runtime.poll(0, || {}).unwrap(); // First poll stores waker
    assert_eq!((first.tasks_polled, first.live_tasks), (1, 1));
    assert!(!done.get());

    // Wake using the stored waker (task is still alive, same thread)
    let stored = waker_slot.take().unwrap();
    stored.wake();

    let second = runtime.poll(0, || {}).unwrap(); // Second poll completes
    assert_eq!(
        (
            second.tasks_polled,
            second.tasks_completed,
            second.live_tasks
        ),
        (1, 1, 0)
    );
    assert!(done.get());
}

#[test]
fn test_interleaved_yields() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let log = Rc::new(Cell::new(Vec::<&'static str>::new()));

    let log_a = log.clone();
    spawner
        .spawn(async move {
            let mut v = log_a.take();
            v.push("A start");
            log_a.set(v);
            YieldN(1).await;
            let mut v = log_a.take();
            v.push("A end");
            log_a.set(v);
        })
        .unwrap();

    let log_b = log.clone();
    spawner
        .spawn(async move {
            let mut v = log_b.take();
            v.push("B start");
            log_b.set(v);
            YieldN(1).await;
            let mut v = log_b.take();
            v.push("B end");
            log_b.set(v);
        })
        .unwrap();

    let mut report = runtime.poll(0, || {}).unwrap();
    while report.live_tasks != 0 {
        report = runtime.poll(0, || {}).unwrap();
    }

    let entries = log.take();
    assert!(entries.contains(&"A start"));
    assert!(entries.contains(&"A end"));
    assert!(entries.contains(&"B start"));
    assert!(entries.contains(&"B end"));
}

#[test]
fn test_empty_poll() {
    let mut runtime = Runtime::new(0);
    assert_eq!(
        runtime.poll(0, || {}).unwrap(),
        switch::PollReport::default()
    );
}

/// Future that uses ThinWaker instead of std Waker.
struct ThinYieldN(u32);

impl Future for ThinYieldN {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 == 0 {
            Poll::Ready(())
        } else {
            self.0 -= 1;
            // Wake directly via ThinWaker, no vtable indirection
            current_task(cx).wake();
            Poll::Pending
        }
    }
}

#[test]
fn test_thin_waker() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let count = Rc::new(Cell::new(0));
    let count_clone = count.clone();

    spawner
        .spawn(async move {
            ThinYieldN(3).await;
            count_clone.set(1);
        })
        .unwrap();

    let mut polls = 0;
    let mut report = runtime.poll(0, || {}).unwrap();
    polls += report.tasks_polled;
    while report.live_tasks != 0 {
        report = runtime.poll(0, || {}).unwrap();
        polls += report.tasks_polled;
    }

    assert_eq!(polls, 4);
    assert_eq!(count.get(), 1);
}
