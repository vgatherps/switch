//! A `TurnRequest` stops `Runtime::poll` after the task being polled; the next poll first
//! finishes that round, then tasks woken between polls, then deferred ones.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
use std::rc::Rc;
use std::task::{Poll, Waker};

use crate::executor::Executor;
use crate::runtime::{Runtime, TurnRequest};
use crate::StoredWaker;

type Log = Rc<RefCell<Vec<&'static str>>>;

/// What a step does on its first poll.
#[derive(Clone, Copy)]
enum Act {
    /// Park; later polls finish.
    Park,
    /// Set the turn request and wake itself; the next poll finishes.
    RequestAndWake,
    /// Set the turn request and finish.
    RequestAndFinish,
    /// Set the turn request and panic.
    RequestAndPanic,
    /// Defer itself; the next poll finishes.
    Defer,
}

/// Logs its name on every poll; drops count in `drops`.
struct Step {
    name: &'static str,
    act: Act,
    polls: u32,
    request: TurnRequest,
    log: Log,
    waker: Rc<RefCell<Option<Waker>>>,
    drops: Rc<Cell<u32>>,
}

impl Future for Step {
    type Output = ();
    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<()> {
        self.log.borrow_mut().push(self.name);
        self.polls += 1;
        *self.waker.borrow_mut() = Some(cx.waker().clone());
        if self.polls > 1 {
            return Poll::Ready(());
        }
        match self.act {
            Act::Park => Poll::Pending,
            Act::RequestAndWake => {
                self.request.set();
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Act::RequestAndFinish => {
                self.request.set();
                Poll::Ready(())
            }
            Act::RequestAndPanic => {
                self.request.set();
                panic_any(7u32)
            }
            Act::Defer => {
                StoredWaker::new(cx).wake_deferred();
                Poll::Pending
            }
        }
    }
}

impl Drop for Step {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

#[switch::task]
fn step(step: Step) -> impl Future<Output = ()> {
    step
}

struct Fixture {
    runtime: Runtime,
    request: TurnRequest,
    log: Log,
    wakers: Vec<Rc<RefCell<Option<Waker>>>>,
    drops: Rc<Cell<u32>>,
}

impl Fixture {
    /// One task per `(name, act)`, spawned in order.
    fn new(steps: &[(&'static str, Act)]) -> Self {
        let runtime = Runtime::new(0);
        let request = runtime.turn_request();
        let (log, drops) = (Log::default(), Rc::new(Cell::new(0)));
        let mut f = Self { runtime, request, log, wakers: Vec::new(), drops };
        for &(name, act) in steps {
            f.spawn(name, act);
        }
        f
    }

    fn spawn(&mut self, name: &'static str, act: Act) {
        let waker = Rc::new(RefCell::new(None));
        let task = Step {
            name,
            act,
            polls: 0,
            request: self.request.clone(),
            log: self.log.clone(),
            waker: waker.clone(),
            drops: self.drops.clone(),
        };
        self.runtime.spawner().spawn(step(task)).unwrap();
        self.wakers.push(waker);
    }

    fn wake(&self, index: usize) {
        self.wakers[index].borrow().as_ref().unwrap().wake_by_ref();
    }

    fn root(&self) -> *mut Executor {
        super::core(&self.runtime)
    }

    fn taken(&self) -> Vec<&'static str> {
        std::mem::take(&mut *self.log.borrow_mut())
    }
}

#[test]
fn a_set_request_polls_no_task_but_still_queues_work() {
    let mut f = Fixture::new(&[("a", Act::Park)]);
    f.request.set();
    let report = f.runtime.poll(0).unwrap();
    assert_eq!((report.tasks_polled, report.live_tasks), (0, 1));
    assert!(f.taken().is_empty());
    f.request.clear();
    assert_eq!(f.runtime.poll(0).unwrap().tasks_polled, 1);
    assert_eq!(f.taken(), ["a"]);
}

#[test]
fn a_request_after_a_self_wake_stops_the_poll_and_the_round_resumes_first() {
    let mut f = Fixture::new(&[("a", Act::RequestAndWake), ("b", Act::Park), ("c", Act::Park)]);
    let report = f.runtime.poll(0).unwrap();
    assert_eq!((report.tasks_polled, report.live_tasks), (1, 3));
    assert_eq!(f.taken(), ["a"]);
    // b and c are left in the round; a's self-wake is one entry in `incoming`.
    assert_eq!(unsafe { Executor::tail_len(f.root()) }, 2);
    assert_eq!(unsafe { Executor::queue_lengths(f.root()) }, (1, 0));
    // Still set: the next poll polls nothing.
    assert_eq!(f.runtime.poll(0).unwrap().tasks_polled, 0);
    f.request.clear();
    let report = f.runtime.poll(0).unwrap();
    assert_eq!((report.tasks_polled, report.live_tasks), (3, 2));
    assert_eq!(f.taken(), ["b", "c", "a"]);
}

#[test]
fn a_request_from_a_finishing_task_stops_after_it_is_reclaimed() {
    let mut f = Fixture::new(&[("a", Act::RequestAndFinish), ("b", Act::Park)]);
    let report = f.runtime.poll(0).unwrap();
    assert_eq!((report.tasks_polled, report.live_tasks), (1, 1));
    assert_eq!(f.taken(), ["a"]);
    assert_eq!(f.drops.get(), 1);
    assert_eq!(unsafe { Executor::tail_len(f.root()) }, 1);
    f.request.clear();
    // A task spawned between polls runs after the leftover b.
    f.spawn("n", Act::Park);
    let report = f.runtime.poll(0).unwrap();
    assert_eq!((report.tasks_polled, report.live_tasks), (2, 2));
    assert_eq!(f.taken(), ["b", "n"]);
    assert_eq!(unsafe { Executor::tail_len(f.root()) }, 0);
}

#[test]
fn shutdown_with_a_round_left_drops_each_task_once() {
    let mut f = Fixture::new(&[("a", Act::RequestAndWake), ("b", Act::Park), ("c", Act::Park)]);
    f.runtime.poll(0).unwrap();
    assert_eq!(unsafe { Executor::tail_len(f.root()) }, 2);
    f.runtime.shutdown();
    assert_eq!(f.taken(), ["a"]);
    assert_eq!(f.drops.get(), 3);
}

#[test]
fn a_panic_in_the_last_task_polled_leaves_the_round_to_shutdown() {
    let mut f = Fixture::new(&[("a", Act::RequestAndPanic), ("b", Act::Park), ("c", Act::Park)]);
    let payload = catch_unwind(AssertUnwindSafe(|| f.runtime.poll(0))).unwrap_err();
    assert_eq!(payload.downcast_ref::<u32>(), Some(&7));
    assert_eq!(f.taken(), ["a"]);
    f.request.clear();
    assert_eq!(f.runtime.poll(0).unwrap_err(), crate::RuntimeError::Panicked);
    f.runtime.shutdown();
    assert!(f.taken().is_empty());
    assert_eq!(f.drops.get(), 3);
}

/// With a round left: a task woken between polls and one deferred in the stopped poll run
/// after it, woken first; a wake of a task still in the round adds no entry.
#[test]
fn wakes_while_a_round_is_left_run_after_it_in_order() {
    let mut f = Fixture::new(&[("b", Act::Park), ("c", Act::Park), ("d", Act::Park)]);
    f.runtime.poll(0).unwrap();
    assert_eq!(f.taken(), ["b", "c", "d"]);
    // The next round: e defers itself, a stops the poll, b and c are left.
    f.spawn("e", Act::Defer);
    f.spawn("a", Act::RequestAndFinish);
    f.wake(0);
    f.wake(1);
    let report = f.runtime.poll(0).unwrap();
    assert_eq!(report.tasks_polled, 2);
    assert_eq!(f.taken(), ["e", "a"]);
    assert_eq!(unsafe { Executor::tail_len(f.root()) }, 2);
    assert_eq!(unsafe { Executor::queue_lengths(f.root()) }, (0, 1));
    f.wake(2);
    f.wake(0);
    assert_eq!(unsafe { Executor::tail_len(f.root()) }, 2);
    assert_eq!(unsafe { Executor::queue_lengths(f.root()) }, (1, 1));
    f.request.clear();
    let report = f.runtime.poll(0).unwrap();
    assert_eq!((report.tasks_polled, report.live_tasks), (4, 0));
    assert_eq!(f.taken(), ["b", "c", "d", "e"]);
}
