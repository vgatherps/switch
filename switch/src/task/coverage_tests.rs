//! Waker ops after the owner thread exits, pool registration,
//! TLS-destructor first use, factory reentry and panicked-runtime wakes.

use std::cell::{Cell, RefCell};
use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
use std::pin::Pin;
use std::ptr;
use std::rc::Rc;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, Waker};
use std::thread;

use super::TaskHeader;
use super::header::{LIVE, QUEUED};
use super::tests::{Parked, State, header, runtime_pool, spawn_parked, state, waker};
use crate::runtime::{PollReport, Runtime, RuntimeError};

struct Probe {
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    ready: bool,
}

impl Future for Probe {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        if self.ready {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Drop for Probe {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[switch::task]
fn probe(
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    ready: bool,
) -> impl Future<Output = ()> {
    Probe {
        polls,
        drops,
        ready,
    }
}

/// Uses switch for the first time on its thread, from a TLS destructor.
struct FirstUseOnTlsDrop {
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    report: Arc<OnceLock<Result<PollReport, RuntimeError>>>,
}

impl Drop for FirstUseOnTlsDrop {
    fn drop(&mut self) {
        let mut runtime = Runtime::new(0);
        let spawner = runtime.spawner();
        for ready in [true, false] {
            spawner
                .spawn(probe(self.polls.clone(), self.drops.clone(), ready))
                .unwrap();
        }
        self.report.set(runtime.poll(0)).unwrap();
        drop(runtime);
    }
}

thread_local! {
    static FIRST_USE_ON_DROP: RefCell<Option<FirstUseOnTlsDrop>> = const { RefCell::new(None) };
}

#[test]
fn first_switch_use_inside_a_tls_destructor() {
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let report = Arc::new(OnceLock::new());
    let value = FirstUseOnTlsDrop {
        polls: polls.clone(),
        drops: drops.clone(),
        report: report.clone(),
    };
    thread::spawn(move || FIRST_USE_ON_DROP.with(|slot| *slot.borrow_mut() = Some(value)))
        .join()
        .unwrap();
    let report = report
        .get()
        .expect("destructor ran")
        .expect("poll succeeded");
    assert_eq!((report.tasks_polled, report.live_tasks), (2, 1));
    assert_eq!(polls.load(Ordering::SeqCst), 2);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
}

/// Keeps the deliberately forgotten runtime reachable for Miri's leak check.
static FORGOTTEN: AtomicPtr<Runtime> = AtomicPtr::new(ptr::null_mut());

/// After the owner thread exited, leaving its runtime alive, every waker operation from
/// another thread leaves the task's slot as it was: clone and drop do nothing, a wake
/// waits in the mailbox nobody empties.
#[test]
fn foreign_ops_after_owner_exit_with_forgotten_runtime_leave_the_slot_alone() {
    let wakers = thread::spawn(|| {
        let mut runtime = Runtime::new(0);
        let states = spawn_parked(&mut runtime, 1);
        let waker = waker(&states[0]);
        let wakers: Vec<Waker> = (0..4).map(|_| waker.clone()).collect();
        FORGOTTEN.store(Box::into_raw(Box::new(runtime)), Ordering::SeqCst);
        wakers
    })
    .join()
    .unwrap();
    // The owner thread is joined; its writes to the header happen-before this read.
    let flags = unsafe { TaskHeader::flags(header(&wakers[0])).get() };
    assert_eq!(flags & (LIVE | QUEUED), LIVE);
    for (operation, waker) in wakers.into_iter().enumerate() {
        let target = header(&waker);
        match operation {
            0 => waker.wake_by_ref(),
            1 => drop(Waker::clone(&waker)),
            2 => waker.wake(),
            _ => drop(waker),
        }
        assert_eq!(unsafe { TaskHeader::flags(target).get() }, flags);
    }
}

#[switch::task]
fn polls_in_factory(
    runtime: Rc<RefCell<Runtime>>,
    inner: Option<Rc<Cell<Option<PollReport>>>>,
    state: Rc<State>,
) -> impl Future<Output = ()> {
    // This call's slot is reserved, not yet committed.
    if let Some(inner) = inner {
        inner.set(Some(runtime.borrow_mut().poll(0).unwrap()));
    }
    Parked(state)
}

#[test]
fn factory_polls_the_runtime_between_reserve_and_commit() {
    let runtime = Rc::new(RefCell::new(Runtime::new(0)));
    let spawner = runtime.borrow().spawner();
    let earlier = state();
    spawner
        .spawn(polls_in_factory(runtime.clone(), None, earlier.clone()))
        .unwrap();
    runtime.borrow_mut().poll(0).unwrap();
    waker(&earlier).wake_by_ref();
    let inner = Rc::new(Cell::new(None));
    let new = state();
    spawner
        .spawn(polls_in_factory(
            runtime.clone(),
            Some(inner.clone()),
            new.clone(),
        ))
        .unwrap();
    let inner = inner.get().expect("factory polled");
    assert_eq!((inner.tasks_polled, inner.live_tasks), (1, 1));
    assert_eq!((earlier.polls.get(), new.polls.get()), (2, 0));
    let report = runtime.borrow_mut().poll(0).unwrap();
    assert_eq!((report.tasks_polled, report.live_tasks), (1, 2));
    assert_eq!((earlier.polls.get(), new.polls.get()), (2, 1));
    // Same function, same pool: the reserved slot is not the one polled inside.
    assert_eq!(runtime_pool(&waker(&new)), runtime_pool(&waker(&earlier)));
    assert_ne!(header(&waker(&earlier)), header(&waker(&new)));
    drop(runtime);
    assert_eq!((earlier.drops.get(), new.drops.get()), (1, 1));
}

#[switch::task]
async fn panics_when_polled() {
    panic_any(88usize)
}

#[test]
fn wake_of_live_slot_in_panicked_runtime_queues_but_never_polls() {
    let mut runtime = Runtime::new(0);
    let parked = spawn_parked(&mut runtime, 1).remove(0);
    let stored = waker(&parked);
    runtime.spawner().spawn(panics_when_polled()).unwrap();
    let payload = catch_unwind(AssertUnwindSafe(|| runtime.poll(0))).unwrap_err();
    assert_eq!(payload.downcast_ref::<usize>(), Some(&88));
    let flags = unsafe { TaskHeader::flags(header(&stored)) };
    assert_eq!(flags.get() & (LIVE | QUEUED), LIVE);
    stored.wake_by_ref();
    assert_eq!(flags.get() & (LIVE | QUEUED), LIVE | QUEUED);
    assert_eq!(runtime.poll(0), Err(RuntimeError::Panicked));
    assert_eq!((parked.polls.get(), parked.drops.get()), (1, 0));
    runtime.shutdown();
    assert_eq!((parked.polls.get(), parked.drops.get()), (1, 1));
}
