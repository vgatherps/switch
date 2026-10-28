//! Integration tests.

use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use switch::{Runtime, Spawner};

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
struct ManualWake {
    waker_slot: Rc<Cell<Option<Waker>>>,
    done: bool,
}

impl Future for ManualWake {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.done {
            Poll::Ready(())
        } else {
            self.waker_slot.set(Some(cx.waker().clone()));
            self.done = true;
            Poll::Pending
        }
    }
}

#[switch::task]
async fn set_true(flag: Rc<Cell<bool>>) {
    flag.set(true);
}

#[switch::task]
async fn yield_then_set_one(count: Rc<Cell<u32>>) {
    YieldN(3).await;
    count.set(1);
}

#[switch::task]
async fn increment(count: Rc<Cell<u32>>) {
    count.set(count.get() + 1);
}

#[switch::task]
async fn spawn_set_true(spawner: Spawner, flag: Rc<Cell<bool>>) {
    spawner.spawn(set_true(flag)).unwrap();
}

#[switch::task]
async fn manual_wake_then_set(waker_slot: Rc<Cell<Option<Waker>>>, done: Rc<Cell<bool>>) {
    ManualWake {
        waker_slot,
        done: false,
    }
    .await;
    done.set(true);
}

#[switch::task]
async fn log_around_yield(
    log: Rc<Cell<Vec<&'static str>>>,
    start: &'static str,
    end: &'static str,
) {
    let mut v = log.take();
    v.push(start);
    log.set(v);
    YieldN(1).await;
    let mut v = log.take();
    v.push(end);
    log.set(v);
}

#[test]
fn test_spawn_and_poll() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let ran = Rc::new(Cell::new(false));

    spawner.spawn(set_true(ran.clone())).unwrap();

    let report = runtime.poll(0).unwrap();
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

    spawner.spawn(yield_then_set_one(count.clone())).unwrap();

    let mut polls = 0;
    let mut report = runtime.poll(0).unwrap();
    polls += report.tasks_polled;
    while report.live_tasks != 0 {
        report = runtime.poll(0).unwrap();
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
        spawner.spawn(increment(count.clone())).unwrap();
    }

    let report = runtime.poll(0).unwrap();
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

    spawner
        .spawn(spawn_set_true(inner_spawner, inner_ran.clone()))
        .unwrap();

    let first = runtime.poll(0).unwrap();
    assert_eq!(
        (first.tasks_polled, first.tasks_completed, first.live_tasks),
        (1, 1, 1)
    );
    let second = runtime.poll(0).unwrap();
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
    let waker_slot: Rc<Cell<Option<Waker>>> = Rc::new(Cell::new(None));
    let done = Rc::new(Cell::new(false));

    spawner
        .spawn(manual_wake_then_set(waker_slot.clone(), done.clone()))
        .unwrap();

    let first = runtime.poll(0).unwrap(); // First poll stores waker
    assert_eq!((first.tasks_polled, first.live_tasks), (1, 1));
    assert!(!done.get());

    // Wake using the stored waker (task is still alive, same thread)
    let stored = waker_slot.take().unwrap();
    stored.wake();

    let second = runtime.poll(0).unwrap(); // Second poll completes
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

    spawner
        .spawn(log_around_yield(log.clone(), "A start", "A end"))
        .unwrap();
    spawner
        .spawn(log_around_yield(log.clone(), "B start", "B end"))
        .unwrap();

    let mut report = runtime.poll(0).unwrap();
    while report.live_tasks != 0 {
        report = runtime.poll(0).unwrap();
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
    assert_eq!(runtime.poll(0).unwrap(), switch::PollReport::default());
}
