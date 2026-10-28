//! `RemoteWaker`: foreign-thread wakes through the mailbox, owner-thread wakes,
//! stale handles, and wakes after the runtime is gone.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::mem::ManuallyDrop;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::rc::Rc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, Waker};
use std::thread;

use super::{Mailbox, RemoteWaker};
use crate::runtime::Runtime;
use crate::task::TaskHeader;
use crate::task::block::Meta;

type Shared = Arc<OnceLock<RemoteWaker>>;

/// Parks, publishing a `RemoteWaker`, until `value` is nonzero; then stores it
/// in `seen`. Counts its polls.
#[switch::task(capacity = 1)]
fn waits(
    value: Arc<AtomicU32>,
    shared: Shared,
    polls: Rc<Cell<u32>>,
    seen: Rc<Cell<u32>>,
) -> impl Future<Output = ()> {
    std::future::poll_fn(move |cx| {
        polls.set(polls.get() + 1);
        let _ = shared.set(RemoteWaker::new(cx));
        match value.load(Ordering::Relaxed) {
            0 => Poll::Pending,
            value => {
                seen.set(value);
                Poll::Ready(())
            }
        }
    })
}

struct Task {
    value: Arc<AtomicU32>,
    shared: Shared,
    polls: Rc<Cell<u32>>,
    seen: Rc<Cell<u32>>,
}

/// Spawns one `waits` task and polls it once, so it is parked.
fn parked(runtime: &mut Runtime) -> Task {
    let task = Task {
        value: Arc::new(AtomicU32::new(0)),
        shared: Arc::new(OnceLock::new()),
        polls: Rc::new(Cell::new(0)),
        seen: Rc::new(Cell::new(0)),
    };
    runtime
        .spawner()
        .spawn(waits(
            task.value.clone(),
            task.shared.clone(),
            task.polls.clone(),
            task.seen.clone(),
        ))
        .unwrap();
    runtime.poll(0).unwrap();
    task
}

fn remote(task: &Task) -> RemoteWaker {
    *task.shared.get().unwrap()
}

/// Sets the value and wakes, from another thread.
fn wake_from_thread(task: &Task, value: u32) {
    let (shared, written) = (task.shared.clone(), task.value.clone());
    std::thread::spawn(move || {
        written.store(value, Ordering::Relaxed);
        shared.get().unwrap().wake();
    })
    .join()
    .unwrap();
}

fn pending(waker: RemoteWaker) -> bool {
    let header = waker.header.as_ptr().cast_const();
    unsafe { Mailbox::pending(Meta::mailbox(TaskHeader::meta(header))) }
}

fn mailbox_pending(task: &Task) -> bool {
    pending(remote(task))
}

#[test]
fn foreign_wake_polls_the_task_at_the_next_turn() {
    let mut runtime = Runtime::new(0);
    let task = parked(&mut runtime);
    assert_eq!(runtime.poll(0).unwrap().tasks_polled, 0);
    wake_from_thread(&task, 42);
    assert!(mailbox_pending(&task));
    let report = runtime.poll(0).unwrap();
    assert_eq!((report.tasks_polled, report.tasks_completed), (1, 1));
    assert_eq!((task.polls.get(), task.seen.get()), (2, 42));
    assert!(!mailbox_pending(&task));
}

#[test]
fn owner_thread_wake_is_local() {
    let mut runtime = Runtime::new(0);
    let task = parked(&mut runtime);
    task.value.store(7, Ordering::Relaxed);
    remote(&task).wake();
    assert!(!mailbox_pending(&task));
    assert_eq!(runtime.poll(0).unwrap().tasks_completed, 1);
    assert_eq!(task.seen.get(), 7);
}

#[test]
fn stale_remote_waker_polls_a_newer_task_at_most_once() {
    let mut runtime = Runtime::new(0);
    let old = parked(&mut runtime);
    let stale = remote(&old);
    old.value.store(1, Ordering::Relaxed);
    stale.wake();
    runtime.poll(0).unwrap();
    assert_eq!((old.polls.get(), old.seen.get()), (2, 1));
    // Capacity 1: the newer task takes the old one's slot.
    let new = parked(&mut runtime);
    assert_eq!(remote(&new), stale);
    std::thread::spawn(move || stale.wake()).join().unwrap();
    assert_eq!(runtime.poll(0).unwrap().tasks_polled, 1);
    assert_eq!(runtime.poll(0).unwrap().tasks_polled, 0);
    assert_eq!((new.polls.get(), new.seen.get()), (2, 0));
    assert_eq!(old.polls.get(), 2);
}

#[test]
fn wake_after_the_runtime_is_dropped_is_discarded() {
    for remote_first in [false, true] {
        let mut runtime = Runtime::new(0);
        let task = parked(&mut runtime);
        let waker = remote(&task);
        if remote_first {
            // Queued while open, discarded at shutdown.
            wake_from_thread(&task, 3);
        }
        drop(runtime);
        assert!(!mailbox_pending(&task));
        wake_from_thread(&task, 5);
        waker.wake();
        assert!(!mailbox_pending(&task));
        assert_eq!(task.polls.get(), 1);
    }
}

#[test]
fn new_rejects_a_foreign_waker() {
    let payload = std::panic::catch_unwind(|| {
        RemoteWaker::new(&Context::from_waker(Waker::noop()));
    })
    .unwrap_err();
    assert_eq!(
        payload.downcast_ref::<&str>(),
        Some(&"RemoteWaker needs a switch task waker")
    );
}

#[test]
fn remote_waker_is_send_and_sync_and_copy() {
    fn check<T: Send + Sync + Copy>() {}
    check::<RemoteWaker>();
}

#[test]
fn new_on_a_foreign_thread_panics() {
    #[switch::task]
    fn hands_out(slot: Rc<Cell<Option<Waker>>>) -> impl Future<Output = ()> {
        std::future::poll_fn(move |cx| {
            slot.set(Some(cx.waker().clone()));
            Poll::<()>::Pending
        })
    }
    let mut runtime = Runtime::new(0);
    let slot = Rc::new(Cell::new(None));
    runtime.spawner().spawn(hands_out(slot.clone())).unwrap();
    runtime.poll(0).unwrap();
    let waker = slot.take().unwrap();
    // Never dropped there: dropping a task waker off its thread panics too.
    let foreign = ManuallyDrop::new(waker.clone());
    let payload = thread::spawn(move || {
        catch_unwind(AssertUnwindSafe(|| {
            RemoteWaker::new(&Context::from_waker(&foreign));
        }))
        .unwrap_err()
    })
    .join()
    .unwrap();
    assert_eq!(
        payload.downcast_ref::<&str>(),
        Some(&"waker used on foreign thread")
    );
}

/// Parks, publishing a `RemoteWaker`, until `value` reaches `last`; records the
/// value it saw.
#[switch::task(capacity = 8)]
fn until(
    value: Arc<AtomicU32>,
    last: u32,
    shared: Shared,
    seen: Rc<Cell<u32>>,
) -> impl Future<Output = ()> {
    std::future::poll_fn(move |cx| {
        let _ = shared.set(RemoteWaker::new(cx));
        seen.set(value.load(Ordering::Relaxed));
        if seen.get() == last {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
}

#[test]
fn concurrent_remote_wakes_while_the_owner_polls_are_all_delivered() {
    const THREADS: usize = 3;
    const WAKES: u32 = 4;
    let mut runtime = Runtime::new(0);
    let tasks: Vec<_> = (0..THREADS)
        .map(|_| {
            let value = Arc::new(AtomicU32::new(0));
            let shared: Shared = Arc::new(OnceLock::new());
            let seen = Rc::new(Cell::new(0));
            runtime
                .spawner()
                .spawn(until(value.clone(), WAKES, shared.clone(), seen.clone()))
                .unwrap();
            (value, shared, seen)
        })
        .collect();
    runtime.poll(0).unwrap();
    let finished = AtomicUsize::new(0);
    thread::scope(|scope| {
        for (value, shared, _) in &tasks {
            let (value, waker, finished) = (value.clone(), *shared.get().unwrap(), &finished);
            scope.spawn(move || {
                for wake in 1..=WAKES {
                    value.store(wake, Ordering::Relaxed);
                    waker.wake();
                    thread::yield_now();
                }
                finished.fetch_add(1, Ordering::Release);
            });
        }
        // The owner polls while the threads wake.
        while finished.load(Ordering::Acquire) < THREADS {
            runtime.poll(0).unwrap();
            thread::yield_now();
        }
    });
    // Every last wake was queued before its thread finished.
    let report = runtime.poll(0).unwrap();
    assert_eq!(report.live_tasks, 0);
    assert!(tasks.iter().all(|(_, _, seen)| seen.get() == WAKES));
}

#[test]
fn remote_wakes_racing_a_runtime_drop_are_discarded() {
    let mut runtime = Runtime::new(0);
    let task = parked(&mut runtime);
    let waker = remote(&task);
    thread::scope(|scope| {
        for _ in 0..2 {
            scope.spawn(|| {
                for _ in 0..20 {
                    waker.wake();
                    thread::yield_now();
                }
            });
        }
        drop(runtime);
    });
    assert!(!pending(waker));
    assert_eq!(task.polls.get(), 1);
}

#[test]
fn a_runtime_dropped_inside_a_task_factory_closes_its_mailbox_at_once() {
    #[switch::task]
    fn drops_runtime(
        owner: Rc<RefCell<Option<Runtime>>>,
        waker: RemoteWaker,
        checked: Rc<Cell<bool>>,
    ) -> impl Future<Output = ()> {
        // The spawn holds a reservation, so the shutdown is deferred until this
        // factory returns; the mailbox must close now.
        drop(owner.borrow_mut().take());
        assert!(!pending(waker), "queued requests are dropped");
        thread::spawn(move || waker.wake()).join().unwrap();
        assert!(!pending(waker), "a later remote wake is discarded");
        checked.set(true);
        std::future::ready(())
    }
    let owner = Rc::new(RefCell::new(Some(Runtime::new(0))));
    let task = parked(owner.borrow_mut().as_mut().unwrap());
    let waker = remote(&task);
    wake_from_thread(&task, 1);
    assert!(pending(waker));
    let spawner = owner.borrow().as_ref().unwrap().spawner();
    let checked = Rc::new(Cell::new(false));
    spawner
        .spawn(drops_runtime(owner.clone(), waker, checked.clone()))
        .unwrap();
    assert!(checked.get());
    assert!(owner.borrow().is_none());
    assert_eq!(task.polls.get(), 1);
}
