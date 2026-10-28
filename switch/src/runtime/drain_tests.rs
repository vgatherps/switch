//! `Runtime::poll` drains `incoming` in rounds up to the poll budget;
//! `StoredWaker::wake_deferred` and `yield_now` move a task to the next poll.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::rc::Rc;
use std::task::{Poll, Waker};

use crate::executor::POLL_BUDGET;
use crate::runtime::Runtime;
use crate::{StoredWaker, yield_now};

/// A baton passed between two tasks: each hop is one poll of one of them.
#[derive(Default)]
struct Baton {
    hops: Cell<u32>,
    turn: Cell<usize>,
    wakers: [RefCell<Option<Waker>>; 2],
}

impl Baton {
    fn wake(&self, side: usize) {
        if let Some(waker) = &*self.wakers[side].borrow() {
            waker.wake_by_ref();
        }
    }
}

#[switch::task]
fn player(baton: Rc<Baton>, side: usize, last: u32) -> impl Future<Output = ()> {
    std::future::poll_fn(move |cx| {
        *baton.wakers[side].borrow_mut() = Some(cx.waker().clone());
        if baton.hops.get() >= last {
            baton.wake(1 - side);
            return Poll::Ready(());
        }
        if baton.turn.get() == side {
            baton.hops.set(baton.hops.get() + 1);
            baton.turn.set(1 - side);
            baton.wake(1 - side);
        }
        Poll::Pending
    })
}

#[test]
fn a_ping_pong_within_the_budget_finishes_in_one_poll() {
    const HOPS: u32 = 300;
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let baton = Rc::new(Baton::default());
    for side in 0..2 {
        spawner.spawn(player(baton.clone(), side, HOPS)).unwrap();
    }
    let report = runtime.poll(0).unwrap();
    assert_eq!(baton.hops.get(), HOPS);
    assert_eq!((report.tasks_completed, report.live_tasks), (2, 0));
    assert!(report.tasks_polled < POLL_BUDGET);
}

#[switch::task]
fn spins(polls: Rc<Cell<usize>>) -> impl Future<Output = ()> {
    std::future::poll_fn(move |cx| {
        polls.set(polls.get() + 1);
        cx.waker().wake_by_ref();
        Poll::<()>::Pending
    })
}

#[test]
fn a_plain_self_wake_runs_again_in_this_poll_up_to_the_budget() {
    let mut runtime = Runtime::new(0);
    let polls = Rc::new(Cell::new(0));
    runtime.spawner().spawn(spins(polls.clone())).unwrap();
    for turn in 1..=2 {
        let report = runtime.poll(0).unwrap();
        assert_eq!((report.tasks_polled, report.ready_tasks), (POLL_BUDGET, 1));
        assert_eq!(polls.get(), turn * POLL_BUDGET);
    }
}

#[switch::task]
fn defers_once(polls: Rc<Cell<usize>>) -> impl Future<Output = ()> {
    std::future::poll_fn(move |cx| {
        polls.set(polls.get() + 1);
        if polls.get() == 2 {
            return Poll::Ready(());
        }
        StoredWaker::new(cx).wake_deferred();
        Poll::Pending
    })
}

#[test]
fn a_deferred_wake_runs_on_the_next_poll_not_this_one() {
    let mut runtime = Runtime::new(0);
    let polls = Rc::new(Cell::new(0));
    runtime.spawner().spawn(defers_once(polls.clone())).unwrap();
    let first = runtime.poll(0).unwrap();
    assert_eq!(
        (first.tasks_polled, first.ready_tasks, polls.get()),
        (1, 1, 1)
    );
    let second = runtime.poll(0).unwrap();
    assert_eq!((second.tasks_polled, second.tasks_completed), (1, 1));
}

#[switch::task]
async fn yields(times: usize, polls: Rc<Cell<usize>>) {
    for _ in 0..times {
        polls.set(polls.get() + 1);
        yield_now().await;
    }
}

#[test]
fn a_yielding_task_is_polled_once_per_poll() {
    let mut runtime = Runtime::new(0);
    let polls = Rc::new(Cell::new(0));
    runtime.spawner().spawn(yields(5, polls.clone())).unwrap();
    for turn in 1..=5 {
        assert_eq!(runtime.poll(0).unwrap().tasks_polled, 1);
        assert_eq!(polls.get(), turn);
    }
    let last = runtime.poll(0).unwrap();
    assert_eq!((last.tasks_polled, last.live_tasks), (1, 0));
}

#[test]
fn deferred_tasks_run_before_work_queued_after_them() {
    let mut runtime = Runtime::new(0);
    let order = Rc::new(RefCell::new(Vec::new()));
    #[switch::task]
    async fn named(name: &'static str, yield_first: bool, order: Rc<RefCell<Vec<&'static str>>>) {
        if yield_first {
            yield_now().await;
        }
        order.borrow_mut().push(name);
    }
    let spawner = runtime.spawner();
    spawner
        .spawn(named("deferred", true, order.clone()))
        .unwrap();
    runtime.poll(0).unwrap();
    spawner
        .spawn(named("spawned", false, order.clone()))
        .unwrap();
    runtime.poll(0).unwrap();
    assert_eq!(*order.borrow(), ["deferred", "spawned"]);
}
