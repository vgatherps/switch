//! Integration tests.

use std::cell::Cell;
use std::rc::Rc;

use switch_runtime::{Runtime, Spawner};

#[switch_runtime::task]
async fn set_true(flag: Rc<Cell<bool>>) {
    flag.set(true);
}

#[switch_runtime::task]
async fn yield_then_set_one(count: Rc<Cell<u32>>) {
    for _ in 0..3 {
        switch_runtime::yield_now().await;
    }
    count.set(1);
}

#[switch_runtime::task]
async fn increment(count: Rc<Cell<u32>>) {
    count.set(count.get() + 1);
}

#[switch_runtime::task]
async fn spawn_set_true(spawner: Spawner, flag: Rc<Cell<bool>>) {
    spawner.spawn(set_true(flag)).unwrap();
}

#[switch_runtime::task]
async fn log_around_yield(
    log: Rc<Cell<Vec<&'static str>>>,
    start: &'static str,
    end: &'static str,
) {
    let mut v = log.take();
    v.push(start);
    log.set(v);
    switch_runtime::yield_now().await;
    let mut v = log.take();
    v.push(end);
    log.set(v);
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
