//! Cross-thread waker ops, pool registration,
//! TLS-destructor first use, factory reentry and panicked-runtime wakes.

use std::cell::{Cell, RefCell};
use std::mem::ManuallyDrop;
use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
use std::pin::Pin;
use std::ptr;
use std::rc::Rc;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, OnceLock};
use std::task::{Context, Poll, Waker};
use std::thread;

use super::TaskHeader;
use super::header::{LIVE, QUEUED};
use super::tests::{
    Parked, State, finish, function_pool, header, meta, parked, spawn_parked, state, waker,
};
use crate::runtime::{PollReport, Runtime, RuntimeError};

fn expect_foreign_panic(op: impl FnOnce()) {
    let payload = catch_unwind(AssertUnwindSafe(op)).unwrap_err();
    assert_eq!(
        payload.downcast_ref::<&str>().copied(),
        Some("waker used on foreign thread")
    );
}

/// All four waker operations from a non-owner thread; each must panic.
fn foreign_round(borrowed: &Waker, woken: Waker, dropped: Waker) {
    expect_foreign_panic(|| borrowed.wake_by_ref());
    expect_foreign_panic(|| drop(borrowed.clone()));
    expect_foreign_panic(move || woken.wake());
    expect_foreign_panic(move || drop(dropped));
}

#[test]
fn foreign_ops_overlap_completion_reuse_shutdown_and_spare_reuse() {
    const FOREIGN: usize = 2;
    const PHASES: usize = 4;
    const ROUNDS: usize = 5;
    let mut runtime = Runtime::new(0);
    let first = spawn_parked(&mut runtime, 1).remove(0);
    let target = waker(&first);
    let block = meta(&target);
    // Cloned here on the owner thread; a foreign thread drops only the clones
    // whose drop is the operation under test.
    let kits: Vec<(ManuallyDrop<Waker>, Vec<ManuallyDrop<Waker>>)> = (0..FOREIGN)
        .map(|_| {
            let owned = (0..2 * PHASES * ROUNDS)
                .map(|_| ManuallyDrop::new(target.clone()))
                .collect();
            (ManuallyDrop::new(target.clone()), owned)
        })
        .collect();
    let barrier = Barrier::new(FOREIGN + 1);
    thread::scope(|scope| {
        for (borrowed, mut owned) in kits {
            let barrier = &barrier;
            scope.spawn(move || {
                for _ in 0..PHASES {
                    barrier.wait();
                    for _ in 0..ROUNDS {
                        let woken = ManuallyDrop::into_inner(owned.pop().unwrap());
                        let dropped = ManuallyDrop::into_inner(owned.pop().unwrap());
                        foreign_round(&borrowed, woken, dropped);
                    }
                    barrier.wait();
                }
                assert!(owned.is_empty());
            });
        }
        // Each owner step runs between two barriers, alongside the foreign rounds.
        barrier.wait();
        finish(&mut runtime, &first);
        assert_eq!((first.polls.get(), first.drops.get()), (2, 1));
        barrier.wait();

        barrier.wait();
        let reused = spawn_parked(&mut runtime, 1).remove(0);
        assert_eq!(header(&waker(&reused)), header(&target));
        assert_eq!(runtime.poll(0).unwrap().tasks_polled, 0);
        assert_eq!(reused.polls.get(), 1);
        barrier.wait();

        barrier.wait();
        runtime.shutdown();
        assert_eq!((reused.polls.get(), reused.drops.get()), (1, 1));
        barrier.wait();

        barrier.wait();
        let mut later = Runtime::new(0);
        let lent = spawn_parked(&mut later, 1).remove(0);
        assert_eq!(meta(&waker(&lent)), block);
        assert_eq!(later.poll(0).unwrap().tasks_polled, 0);
        drop(later);
        assert_eq!((lent.polls.get(), lent.drops.get()), (1, 1));
        barrier.wait();
    });
    assert_eq!((first.polls.get(), first.drops.get()), (2, 1));
}

#[test]
fn concurrent_first_pool_registrations_are_distinct() {
    const THREADS: usize = 8;
    let barrier = Barrier::new(THREADS);
    let mut pools: Vec<usize> = thread::scope(|scope| {
        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                scope.spawn(|| {
                    let mut runtime = Runtime::new(0);
                    let spawner = runtime.spawner();
                    let state = state();
                    state.done.set(true);
                    barrier.wait();
                    spawner.spawn(parked(state.clone())).unwrap();
                    assert_eq!(runtime.poll(0).unwrap().tasks_completed, 1);
                    let pool = function_pool(&waker(&state)).addr();
                    drop(runtime);
                    assert_eq!((state.polls.get(), state.drops.get()), (1, 1));
                    pool
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    pools.sort_unstable();
    pools.dedup();
    assert_eq!(pools.len(), THREADS);
}

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
    assert_eq!(
        (
            report.tasks_polled,
            report.tasks_completed,
            report.live_tasks
        ),
        (2, 1, 1)
    );
    assert_eq!(polls.load(Ordering::SeqCst), 2);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
}

/// Keeps the deliberately forgotten runtime reachable for Miri's leak check.
static FORGOTTEN: AtomicPtr<Runtime> = AtomicPtr::new(ptr::null_mut());

#[test]
fn foreign_ops_panic_after_owner_exit_with_forgotten_runtime_and_live_slot() {
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
        // Never dropped here unless dropping is the operation under test.
        let waker = ManuallyDrop::new(waker);
        let target = header(&waker);
        expect_foreign_panic(move || match operation {
            0 => waker.wake_by_ref(),
            1 => drop(Waker::clone(&waker)),
            2 => ManuallyDrop::into_inner(waker).wake(),
            _ => drop(ManuallyDrop::into_inner(waker)),
        });
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
    assert_eq!(function_pool(&waker(&new)), function_pool(&waker(&earlier)));
    assert_ne!(header(&waker(&earlier)), header(&waker(&new)));
    drop(runtime);
    assert_eq!((earlier.drops.get(), new.drops.get()), (1, 1));
}

#[switch::task]
async fn panics_when_polled() {
    panic_any(88usize)
}

#[test]
fn wake_of_live_slot_in_panicked_runtime_does_nothing() {
    let mut runtime = Runtime::new(0);
    let parked = spawn_parked(&mut runtime, 1).remove(0);
    let stored = waker(&parked);
    runtime.spawner().spawn(panics_when_polled()).unwrap();
    let payload = catch_unwind(AssertUnwindSafe(|| runtime.poll(0))).unwrap_err();
    assert_eq!(payload.downcast_ref::<usize>(), Some(&88));
    let flags = unsafe { TaskHeader::flags(header(&stored)) };
    assert_eq!(flags.get() & (LIVE | QUEUED), LIVE);
    stored.wake_by_ref();
    assert_eq!(flags.get() & (LIVE | QUEUED), LIVE);
    assert_eq!(runtime.poll(0), Err(RuntimeError::Panicked));
    assert_eq!((parked.polls.get(), parked.drops.get()), (1, 0));
    runtime.shutdown();
    assert_eq!((parked.polls.get(), parked.drops.get()), (1, 1));
}
