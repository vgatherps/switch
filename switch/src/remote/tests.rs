//! `RemoteWaker`: foreign-thread wakes through the mailbox, owner-thread wakes,
//! stale handles, and wakes after the runtime is gone.

use std::cell::Cell;
use std::future::Future;
use std::rc::Rc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, Waker};

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

fn mailbox_pending(task: &Task) -> bool {
    let header = remote(task).header.as_ptr().cast_const();
    unsafe { Mailbox::pending(Meta::mailbox(TaskHeader::meta(header))) }
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
