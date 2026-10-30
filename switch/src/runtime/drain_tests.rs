//! `Runtime::poll` drains `incoming` in rounds up to the poll budget;
//! `StoredWaker::wake_deferred` and `yield_now` move a task to the next poll.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::rc::Rc;
use std::task::{Poll, Waker};

use crate::RemoteWaker;
use crate::executor::{Executor, POLL_BUDGET};
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
    assert_eq!(report.live_tasks, 0);
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
        assert_eq!(report.tasks_polled, POLL_BUDGET);
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
    assert_eq!((first.tasks_polled, polls.get()), (1, 1));
    let second = runtime.poll(0).unwrap();
    assert_eq!((second.tasks_polled, second.live_tasks), (1, 0));
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

/// Tasks woken or spawned between polls run before tasks deferred in the last poll.
#[test]
fn deferred_tasks_run_after_work_queued_between_polls() {
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
    assert_eq!(*order.borrow(), ["spawned", "deferred"]);
}

#[test]
fn the_budget_is_checked_between_rounds() {
    let mut runtime = Runtime::new(0);
    let polls = Rc::new(Cell::new(0));
    for _ in 0..3 {
        runtime.spawner().spawn(spins(polls.clone())).unwrap();
    }
    // 342 rounds of three reach 1026: the round that crosses 1024 completes.
    let report = runtime.poll(0).unwrap();
    assert_eq!(report.tasks_polled, 1026);
    assert_eq!(runtime.poll(0).unwrap().tasks_polled, 1026);
}

// ---- Upgrading a deferred task ----

type Log = Rc<RefCell<Vec<&'static str>>>;
type Wakers = Rc<RefCell<Vec<StoredWaker>>>;
/// `incoming` and `deferred` lengths, recorded inside a poll.
type Queues = Rc<Cell<(usize, usize)>>;

/// Publishes its `StoredWaker`, yields once (deferred), then logs `name`.
#[switch::task]
async fn defers(name: &'static str, wakers: Wakers, log: Log) {
    std::future::poll_fn(|cx| {
        wakers.borrow_mut().push(StoredWaker::new(cx));
        Poll::Ready(())
    })
    .await;
    yield_now().await;
    log.borrow_mut().push(name);
}

/// Wakes `wakers[index]` ordinarily, `times` times, after `rounds` self-wakes;
/// then checks that the queues hold one entry per queued task.
#[switch::task]
async fn wakes(
    wakers: Wakers,
    index: usize,
    times: usize,
    rounds: usize,
    root: *mut Executor,
    queues: Queues,
) {
    for _ in 0..rounds {
        let mut woke = false;
        std::future::poll_fn(|cx| {
            if woke {
                return Poll::Ready(());
            }
            woke = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        })
        .await;
    }
    let target = wakers.borrow()[index];
    for _ in 0..times {
        target.wake();
    }
    queues.set(unsafe { Executor::queue_lengths(root) });
}

fn setup() -> (Runtime, Wakers, Log, Queues) {
    (
        Runtime::new(0),
        Rc::new(RefCell::new(Vec::new())),
        Rc::new(RefCell::new(Vec::new())),
        Rc::new(Cell::new((0, 0))),
    )
}

#[test]
fn an_ordinary_wake_in_the_same_round_upgrades_a_deferred_task() {
    let (mut runtime, wakers, log, queues) = setup();
    let root = super::core(&runtime);
    let spawner = runtime.spawner();
    spawner
        .spawn(defers("a", wakers.clone(), log.clone()))
        .unwrap();
    spawner
        .spawn(wakes(wakers.clone(), 0, 2, 0, root, queues.clone()))
        .unwrap();
    // Round one: A yields (deferred), B wakes A twice. Round two: A finishes.
    let report = runtime.poll(0).unwrap();
    assert_eq!((report.tasks_polled, report.live_tasks), (3, 0));
    assert_eq!(*log.borrow(), ["a"]);
    // When B woke A: one entry, moved from `deferred` to `incoming`.
    assert_eq!(queues.get(), (1, 0));
}

#[test]
fn an_ordinary_wake_in_a_later_round_upgrades_a_deferred_task() {
    let (mut runtime, wakers, log, queues) = setup();
    let root = super::core(&runtime);
    let spawner = runtime.spawner();
    spawner
        .spawn(defers("a", wakers.clone(), log.clone()))
        .unwrap();
    spawner
        .spawn(wakes(wakers.clone(), 0, 1, 2, root, queues.clone()))
        .unwrap();
    // B wakes A in round three; A runs in round four.
    let report = runtime.poll(0).unwrap();
    assert_eq!((report.tasks_polled, report.live_tasks), (5, 0));
    assert_eq!(*log.borrow(), ["a"]);
    assert_eq!(queues.get(), (1, 0));
}

#[test]
fn an_upgrade_keeps_the_order_of_the_other_deferred_tasks() {
    let (mut runtime, wakers, log, queues) = setup();
    let root = super::core(&runtime);
    let spawner = runtime.spawner();
    for name in ["a", "b", "c"] {
        spawner
            .spawn(defers(name, wakers.clone(), log.clone()))
            .unwrap();
    }
    spawner
        .spawn(wakes(wakers.clone(), 1, 1, 0, root, queues.clone()))
        .unwrap();
    let first = runtime.poll(0).unwrap();
    assert_eq!(*log.borrow(), ["b"]);
    assert_eq!((first.tasks_polled, first.live_tasks), (5, 2));
    // B moved to `incoming`; A and C stayed in `deferred`.
    assert_eq!(queues.get(), (1, 2));
    assert_eq!(unsafe { Executor::queue_lengths(root) }, (0, 2));
    runtime.poll(0).unwrap();
    assert_eq!(*log.borrow(), ["b", "a", "c"]);
}

/// Publishes its `RemoteWaker`, yields once (deferred), then logs "a".
#[switch::task]
async fn publishes_and_defers(remote: Rc<Cell<Option<RemoteWaker>>>, log: Log) {
    std::future::poll_fn(|cx| {
        remote.set(Some(RemoteWaker::new(cx)));
        Poll::Ready(())
    })
    .await;
    yield_now().await;
    log.borrow_mut().push("a");
}

#[test]
fn a_remote_waker_on_the_owner_thread_upgrades() {
    #[switch::task]
    async fn wakes_remotely(target: Rc<Cell<Option<RemoteWaker>>>) {
        target.get().unwrap().wake();
    }
    let (mut runtime, _, log, _) = setup();
    let spawner = runtime.spawner();
    let remote = Rc::new(Cell::new(None));
    spawner
        .spawn(publishes_and_defers(remote.clone(), log.clone()))
        .unwrap();
    spawner.spawn(wakes_remotely(remote.clone())).unwrap();
    let report = runtime.poll(0).unwrap();
    assert_eq!((report.tasks_polled, report.live_tasks), (3, 0));
    assert_eq!(*log.borrow(), ["a"]);
}

#[test]
fn a_remote_wake_from_another_thread_of_a_deferred_task_runs_it_once() {
    let (mut runtime, _, log, _) = setup();
    let root = super::core(&runtime);
    let remote = Rc::new(Cell::new(None));
    runtime
        .spawner()
        .spawn(publishes_and_defers(remote.clone(), log.clone()))
        .unwrap();
    runtime.poll(0).unwrap();
    assert_eq!(unsafe { Executor::queue_lengths(root) }, (0, 1));
    let waker = remote.get().unwrap();
    std::thread::spawn(move || waker.wake()).join().unwrap();
    // Delivered at the start of the poll: upgraded, one entry, one poll.
    let report = runtime.poll(0).unwrap();
    assert_eq!((report.tasks_polled, report.live_tasks), (1, 0));
    assert_eq!(*log.borrow(), ["a"]);
    assert_eq!(unsafe { Executor::queue_lengths(root) }, (0, 0));
}

#[test]
fn deferring_and_waking_itself_every_poll_stops_at_the_budget() {
    #[switch::task]
    fn defers_and_wakes(polls: Rc<Cell<usize>>) -> impl Future<Output = ()> {
        std::future::poll_fn(move |cx| {
            polls.set(polls.get() + 1);
            let waker = StoredWaker::new(cx);
            waker.wake_deferred();
            waker.wake();
            Poll::<()>::Pending
        })
    }
    let mut runtime = Runtime::new(0);
    let polls = Rc::new(Cell::new(0));
    runtime
        .spawner()
        .spawn(defers_and_wakes(polls.clone()))
        .unwrap();
    let report = runtime.poll(0).unwrap();
    assert_eq!(
        (report.tasks_polled, polls.get()),
        (POLL_BUDGET, POLL_BUDGET)
    );
}

#[test]
fn a_task_upgraded_then_deferred_again_has_one_entry() {
    /// Defers twice, recording the queue lengths after each deferral.
    #[switch::task]
    async fn defers_twice(wakers: Wakers, root: *mut Executor, queues: Queues, log: Log) {
        std::future::poll_fn(|cx| {
            wakers.borrow_mut().push(StoredWaker::new(cx));
            Poll::Ready(())
        })
        .await;
        yield_now().await;
        log.borrow_mut().push("upgraded");
        yield_now().await;
        queues.set(unsafe { Executor::queue_lengths(root) });
        log.borrow_mut().push("done");
    }
    let (mut runtime, wakers, log, queues) = setup();
    let root = super::core(&runtime);
    let spawner = runtime.spawner();
    let woke = Rc::new(Cell::new((0, 0)));
    spawner
        .spawn(defers_twice(
            wakers.clone(),
            root,
            queues.clone(),
            log.clone(),
        ))
        .unwrap();
    spawner
        .spawn(wakes(wakers.clone(), 0, 1, 0, root, woke.clone()))
        .unwrap();
    // Round one: A defers, B upgrades it. Round two: A runs and defers again.
    let first = runtime.poll(0).unwrap();
    assert_eq!((first.tasks_polled, first.live_tasks), (3, 1));
    assert_eq!(*log.borrow(), ["upgraded"]);
    assert_eq!(woke.get(), (1, 0));
    assert_eq!(unsafe { Executor::queue_lengths(root) }, (0, 1));
    let second = runtime.poll(0).unwrap();
    assert_eq!((second.tasks_polled, second.live_tasks), (1, 0));
    assert_eq!(*log.borrow(), ["upgraded", "done"]);
    assert_eq!(queues.get(), (0, 0));
}

#[test]
fn a_wake_after_the_deferred_entry_moved_polls_the_task_once_this_poll() {
    /// Yields once (deferred); on resume wakes `wakers[0]` and records the queues.
    #[switch::task]
    async fn defers_then_wakes(wakers: Wakers, root: *mut Executor, queues: Queues, log: Log) {
        yield_now().await;
        wakers.borrow()[0].wake();
        queues.set(unsafe { Executor::queue_lengths(root) });
        log.borrow_mut().push("b");
    }
    let (mut runtime, wakers, log, queues) = setup();
    let root = super::core(&runtime);
    let spawner = runtime.spawner();
    spawner
        .spawn(defers_then_wakes(
            wakers.clone(),
            root,
            queues.clone(),
            log.clone(),
        ))
        .unwrap();
    spawner
        .spawn(defers("a", wakers.clone(), log.clone()))
        .unwrap();
    // Both defer: `deferred` is [b, a].
    runtime.poll(0).unwrap();
    assert_eq!(unsafe { Executor::queue_lengths(root) }, (0, 2));
    // Both entries move to this poll's first round; b, earlier in it, wakes a,
    // which is polled once, in its turn.
    let report = runtime.poll(0).unwrap();
    assert_eq!((report.tasks_polled, report.live_tasks), (2, 0));
    assert_eq!(*log.borrow(), ["b", "a"]);
    assert_eq!(queues.get(), (0, 0));
}
