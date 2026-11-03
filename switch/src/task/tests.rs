//! Block layout and growth, stale wakers, `will_wake`, `StoredWaker`, foreign threads,
//! tokens, spawn unwinding and TLS teardown.

use std::cell::{Cell, RefCell};
use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use super::TaskHeader;
use super::block::{BLOCK, MASK, Meta, Shape};
use super::pool::RuntimePool;
use crate::StoredWaker;
use crate::runtime::{Runtime, RuntimeError, Spawner};

#[derive(Default)]
pub(super) struct State {
    pub(super) polls: Cell<usize>,
    pub(super) drops: Cell<usize>,
    pub(super) done: Cell<bool>,
    pub(super) waker: RefCell<Option<Waker>>,
    /// Future address at the latest poll.
    pub(super) addr: Cell<usize>,
}

/// Records each poll and keeps a clone of its latest waker; ready once `done`.
pub(super) struct Parked(pub(super) Rc<State>);

impl Future for Parked {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let state = &self.0;
        state.polls.set(state.polls.get() + 1);
        state.addr.set(std::ptr::from_ref(&*self).addr());
        *state.waker.borrow_mut() = Some(cx.waker().clone());
        if state.done.get() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Drop for Parked {
    fn drop(&mut self) {
        self.0.drops.set(self.0.drops.get() + 1);
    }
}

#[switch::task]
pub(super) fn parked(state: Rc<State>) -> impl Future<Output = ()> {
    Parked(state)
}

#[switch::task]
fn parked_other(state: Rc<State>) -> impl Future<Output = ()> {
    Parked(state)
}

pub(super) fn state() -> Rc<State> {
    Rc::new(State::default())
}

pub(super) fn waker(state: &State) -> Waker {
    state.waker.borrow().clone().expect("task was polled")
}

pub(super) fn header(waker: &Waker) -> *const TaskHeader {
    waker.data().cast()
}

fn meta(waker: &Waker) -> *mut Meta {
    TaskHeader::meta(header(waker))
}

/// Holds 3 tasks per block, so a few tasks span several blocks.
struct Big {
    parked: Parked,
    _bulk: [u8; 20_000],
}

impl Future for Big {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        Pin::new(&mut self.parked).poll(cx)
    }
}

#[switch::task]
fn big(state: Rc<State>) -> impl Future<Output = ()> {
    Big {
        parked: Parked(state),
        _bulk: [0; 20_000],
    }
}

/// Spawn `n` tasks of `big` and poll them once.
fn spawn_big(runtime: &mut Runtime, n: usize) -> Vec<Rc<State>> {
    let spawner = runtime.spawner();
    let states: Vec<_> = (0..n).map(|_| state()).collect();
    for state in &states {
        spawner.spawn(big(state.clone())).unwrap();
    }
    runtime.poll(0).unwrap();
    states
}

pub(super) fn runtime_pool(waker: &Waker) -> *mut RuntimePool {
    unsafe { Meta::pool(meta(waker)) }
}

/// Spawn `n` parked tasks and poll them once.
pub(super) fn spawn_parked(runtime: &mut Runtime, n: usize) -> Vec<Rc<State>> {
    let spawner = runtime.spawner();
    let states: Vec<_> = (0..n).map(|_| state()).collect();
    for state in &states {
        spawner.spawn(parked(state.clone())).unwrap();
    }
    runtime.poll(0).unwrap();
    states
}

fn finish(runtime: &mut Runtime, state: &State) {
    state.done.set(true);
    waker(state).wake_by_ref();
    assert_eq!(runtime.poll(0).unwrap().tasks_polled, 1);
}

// ---- Blocks ----

#[test]
fn every_live_header_masks_to_its_block_across_blocks() {
    assert_eq!(Shape::<Big>::SLOTS, 3);
    let mut runtime = Runtime::new(0);
    let states = spawn_big(&mut runtime, 7);
    let metas: Vec<_> = states.iter().map(|s| meta(&waker(s))).collect();
    for (state, &meta) in states.iter().zip(&metas) {
        let header = header(&waker(state));
        assert_eq!(meta.addr() % BLOCK, 0);
        assert_eq!(header.addr() & MASK, meta.addr());
        let position = unsafe { Meta::position(meta, header) };
        assert_eq!(unsafe { Meta::header(meta, position) }, header);
    }
    // Slot s is position s % 3 of block s / 3: slots 0..7 fill two blocks and the
    // first position of a third.
    for (index, &meta) in metas.iter().enumerate() {
        assert_eq!(unsafe { Meta::block(meta) }, (index / 3) as u32);
    }
}

#[test]
fn a_completed_task_frees_its_slot_for_the_next_spawn() {
    let mut runtime = Runtime::new(0);
    let states = spawn_big(&mut runtime, 7);
    finish(&mut runtime, &states[4]);
    let again = spawn_big(&mut runtime, 1).remove(0);
    assert_eq!(header(&waker(&again)), header(&waker(&states[4])));
}

/// `crate::boxed` runs the future it was given: parked until woken, then to completion.
#[test]
fn boxed_runs_its_future() {
    let mut runtime = Runtime::new(0);
    let state = state();
    runtime.spawner().spawn(crate::boxed(Box::pin(Parked(state.clone())))).unwrap();
    let report = runtime.poll(0).unwrap();
    assert_eq!((report.tasks_polled, report.live_tasks), (1, 1));
    state.done.set(true);
    waker(&state).wake_by_ref();
    let report = runtime.poll(0).unwrap();
    assert_eq!((report.tasks_polled, report.live_tasks), (1, 0));
}

/// One task function holds 4096 live tasks (more than one 64 KiB block's slots), then all
/// complete.
#[test]
#[cfg_attr(miri, ignore)]
fn one_function_holds_4096_live_tasks() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let states: Vec<_> = (0..4096).map(|_| state()).collect();
    for state in &states {
        spawner.spawn(parked(state.clone())).unwrap();
    }
    let report = runtime.poll(0).unwrap();
    assert_eq!((report.tasks_polled, report.live_tasks), (4096, 4096));
    for state in &states {
        state.done.set(true);
        waker(state).wake_by_ref();
    }
    let report = runtime.poll(0).unwrap();
    assert_eq!((report.tasks_polled, report.live_tasks), (4096, 0));
}

#[repr(align(128))]
struct Aligned128 {
    state: Rc<State>,
    _bulk: [u8; 40],
}

impl Future for Aligned128 {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        assert_eq!(std::ptr::from_ref(&*self).addr() % 128, 0);
        let state = &self.state;
        state.polls.set(state.polls.get() + 1);
        *state.waker.borrow_mut() = Some(cx.waker().clone());
        if state.done.get() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Drop for Aligned128 {
    fn drop(&mut self) {
        assert_eq!(std::ptr::from_ref(self).addr() % 128, 0);
        self.state.drops.set(self.state.drops.get() + 1);
    }
}

#[switch::task]
fn aligned_128(state: Rc<State>) -> impl Future<Output = ()> {
    Aligned128 {
        state,
        _bulk: [0; 40],
    }
}

#[test]
fn over_aligned_future_polls_and_drops_aligned_through_slot_reuse() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let states: Vec<_> = (0..20).map(|_| state()).collect();
    for state in &states {
        spawner.spawn(aligned_128(state.clone())).unwrap();
    }
    runtime.poll(0).unwrap();
    for state in &states {
        state.done.set(true);
        waker(state).wake_by_ref();
    }
    assert_eq!(runtime.poll(0).unwrap().tasks_polled, 20);
    let again: Vec<_> = (0..20).map(|_| state()).collect();
    for state in &again {
        spawner.spawn(aligned_128(state.clone())).unwrap();
    }
    runtime.poll(0).unwrap();
    for state in &again {
        state.done.set(true);
        waker(state).wake_by_ref();
    }
    assert_eq!(runtime.poll(0).unwrap().tasks_polled, 20);
    assert!(
        states
            .iter()
            .chain(&again)
            .all(|s| s.drops.get() == 1 && s.polls.get() == 2)
    );
}

#[test]
fn a_completed_task_keeps_its_slot_until_its_queued_wake_drains() {
    #[switch::task]
    fn wake_then_finish(state: Rc<State>) -> impl Future<Output = ()> {
        std::future::poll_fn(move |cx| {
            state.polls.set(state.polls.get() + 1);
            *state.waker.borrow_mut() = Some(cx.waker().clone());
            // Deferred, so the wake is still queued when `poll` returns.
            StoredWaker::new(cx).wake_deferred();
            Poll::Ready(())
        })
    }
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let first = state();
    spawner.spawn(wake_then_finish(first.clone())).unwrap();
    let report = runtime.poll(0).unwrap();
    assert_eq!((report.tasks_polled, report.live_tasks), (1, 0));
    // The completed task's own wake is still queued, so its slot is occupied.
    let second = state();
    spawner.spawn(wake_then_finish(second.clone())).unwrap();
    // The next poll drains that wake, freeing the slot; `second` completes with its
    // own wake queued.
    assert_eq!(runtime.poll(0).unwrap().tasks_polled, 1);
    assert_ne!(header(&waker(&second)), header(&waker(&first)));
    let third = state();
    spawner.spawn(wake_then_finish(third.clone())).unwrap();
    runtime.poll(0).unwrap();
    assert_eq!(header(&waker(&third)), header(&waker(&first)));
}

// ---- Stale wakers ----

#[test]
fn stale_waker_of_free_slot_polls_nothing() {
    let mut runtime = Runtime::new(0);
    let states = spawn_parked(&mut runtime, 1);
    let stale = waker(&states[0]);
    finish(&mut runtime, &states[0]);
    stale.wake_by_ref();
    let by_value = stale.clone();
    by_value.wake();
    assert_eq!(runtime.poll(0).unwrap().tasks_polled, 0);
    assert_eq!(states[0].polls.get(), 2);
}

#[test]
fn stale_waker_of_reused_slot_polls_newer_task_once_per_drained_wake() {
    let mut runtime = Runtime::new(0);
    let old = spawn_parked(&mut runtime, 1).remove(0);
    let stale = waker(&old);
    finish(&mut runtime, &old);
    let new = spawn_parked(&mut runtime, 1).remove(0);
    assert_eq!(header(&waker(&new)), header(&stale));
    assert_eq!(new.polls.get(), 1);
    // Wakes coalesce while queued; each drain re-arms the stale waker.
    for drain in 1..=3 {
        stale.wake_by_ref();
        stale.wake_by_ref();
        assert_eq!(runtime.poll(0).unwrap().tasks_polled, 1);
        assert_eq!(new.polls.get(), 1 + drain);
    }
    assert_eq!(runtime.poll(0).unwrap().tasks_polled, 0);
    finish(&mut runtime, &new);
    assert_eq!((new.polls.get(), new.drops.get()), (5, 1));
}

#[test]
fn wake_after_shutdown_or_drop_has_no_effect() {
    for drop_runtime in [false, true] {
        let mut runtime = Runtime::new(0);
        let states = spawn_parked(&mut runtime, 3);
        let stale: Vec<_> = states.iter().map(|s| waker(s)).collect();
        if drop_runtime {
            drop(runtime);
        } else {
            runtime.shutdown();
        }
        // The executor is freed (or closed); the flags check stops the wake
        // before `Meta::executor` is read.
        for waker in &stale {
            assert_eq!(unsafe { TaskHeader::flags(header(waker)).get() }, 0);
            waker.wake_by_ref();
            let by_value = waker.clone();
            by_value.wake();
        }
        assert!(
            states
                .iter()
                .all(|s| s.drops.get() == 1 && s.polls.get() == 1)
        );
    }
}

/// The freed executor's memory may be reused by a later runtime's: a stale wake
/// must not reach it.
#[test]
fn wakers_of_a_dropped_runtime_never_reach_a_later_one() {
    let mut first = Runtime::new(0);
    let old = state();
    let stored = Rc::new(Cell::new(None));
    first
        .spawner()
        .spawn(stores(old.clone(), stored.clone()))
        .unwrap();
    first.poll(0).unwrap();
    let (stale, stale_stored) = (waker(&old), stored.get().unwrap());
    drop(first);
    let mut second = Runtime::new(0);
    let new = state();
    second
        .spawner()
        .spawn(stores(new.clone(), Rc::new(Cell::new(None))))
        .unwrap();
    second.poll(0).unwrap();
    assert_ne!(meta(&waker(&new)), meta(&stale));
    stale.wake_by_ref();
    let by_value = stale.clone();
    by_value.wake();
    stale_stored.wake();
    assert_eq!(second.poll(0).unwrap().tasks_polled, 0);
    assert_eq!((old.polls.get(), new.polls.get()), (1, 1));
}

#[test]
fn stale_wake_into_a_reserved_slot_returns_before_any_executor_access() {
    #[switch::task]
    fn wake_while_reserved(state: Rc<State>, stale: Option<Waker>) -> impl Future<Output = ()> {
        // Factory code runs with this call's slot reserved but not yet LIVE.
        if let Some(stale) = stale {
            assert_eq!(unsafe { TaskHeader::flags(header(&stale)).get() }, 0);
            stale.wake_by_ref();
        }
        Parked(state)
    }
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let old = state();
    spawner
        .spawn(wake_while_reserved(old.clone(), None))
        .unwrap();
    runtime.poll(0).unwrap();
    let stale = waker(&old);
    finish(&mut runtime, &old);
    let new = state();
    spawner
        .spawn(wake_while_reserved(new.clone(), Some(stale.clone())))
        .unwrap();
    let report = runtime.poll(0).unwrap();
    assert_eq!(report.tasks_polled, 1);
    assert_eq!(header(&waker(&new)), header(&stale));
    assert_eq!(new.polls.get(), 1);
    assert_eq!(runtime.poll(0).unwrap().tasks_polled, 0);
}

// ---- will_wake ----

#[test]
fn will_wake_identifies_slots() {
    let mut runtime = Runtime::new(0);
    let states = spawn_parked(&mut runtime, 2);
    let first = waker(&states[0]);
    let clone = first.clone();
    assert!(first.will_wake(&clone));
    // Across polls of one task.
    first.wake_by_ref();
    runtime.poll(0).unwrap();
    assert!(waker(&states[0]).will_wake(&first));
    // Between tasks, of the same and of another function.
    assert!(!first.will_wake(&waker(&states[1])));
    let other = state();
    runtime
        .spawner()
        .spawn(parked_other(other.clone()))
        .unwrap();
    runtime.poll(0).unwrap();
    assert!(!first.will_wake(&waker(&other)));
    // Another executor's task.
    let mut elsewhere = Runtime::new(0);
    let foreign = spawn_parked(&mut elsewhere, 1).remove(0);
    assert!(!first.will_wake(&waker(&foreign)));
    // A stale clone against the newer task in its slot.
    finish(&mut runtime, &states[0]);
    let newer = spawn_parked(&mut runtime, 1).remove(0);
    assert!(clone.will_wake(&waker(&newer)));
    let mut target = waker(&states[1]);
    target.clone_from(&first);
    assert!(target.will_wake(&first));
}

// ---- Foreign thread after owner exit ----

/// After the owner thread dropped its runtime, waker operations from another thread do
/// nothing: clone and drop touch nothing, and the closed mailbox discards the wake.
#[test]
fn foreign_thread_ops_after_owner_thread_exits_do_nothing() {
    let wakers = std::thread::spawn(|| {
        let mut runtime = Runtime::new(0);
        let states = spawn_parked(&mut runtime, 1);
        let waker = waker(&states[0]);
        let wakers: Vec<Waker> = (0..4).map(|_| waker.clone()).collect();
        drop(runtime);
        assert_eq!(states[0].drops.get(), 1);
        drop(states);
        wakers
    })
    .join()
    .unwrap();
    for (operation, waker) in wakers.into_iter().enumerate() {
        let target = header(&waker);
        match operation {
            0 => waker.wake_by_ref(),
            1 => drop(Waker::clone(&waker)),
            2 => waker.wake(),
            _ => drop(waker),
        }
        assert_eq!(unsafe { TaskHeader::flags(target).get() }, 0);
    }
}

// ---- Pools ----

#[test]
fn runtimes_on_one_thread_use_disjoint_blocks() {
    let mut a = Runtime::new(0);
    let mut b = Runtime::new(0);
    let in_a = spawn_big(&mut a, 2);
    let a_block = meta(&waker(&in_a[0]));
    for state in &in_a {
        finish(&mut a, state);
    }
    // `a` has free slots, yet `b` never takes one.
    let in_b = spawn_big(&mut b, 7);
    assert!(in_b.iter().all(|s| meta(&waker(s)) != a_block));
    let b_executor = unsafe { Meta::executor(meta(&waker(&in_b[0]))) };
    assert_ne!(unsafe { Meta::executor(a_block) }, b_executor);
}

#[test]
fn a_later_runtime_allocates_its_own_blocks() {
    let mut first = Runtime::new(0);
    let states = spawn_big(&mut first, 7);
    let blocks: Vec<_> = states.iter().step_by(3).map(|s| meta(&waker(s))).collect();
    drop(first);
    let mut second = Runtime::new(0);
    let later = spawn_big(&mut second, 7);
    assert!(later.iter().all(|s| !blocks.contains(&meta(&waker(s)))));
}

#[test]
fn live_future_never_moves_when_blocks_grow() {
    let mut runtime = Runtime::new(0);
    let first = spawn_big(&mut runtime, 1).remove(0);
    let addr = first.addr.get();
    let _many = spawn_big(&mut runtime, 6);
    waker(&first).wake_by_ref();
    runtime.poll(0).unwrap();
    assert_eq!(first.polls.get(), 2);
    assert_eq!(first.addr.get(), addr);
}

/// A `Parked` with another layout: a second future type of a different size.
struct Padded(Parked, #[allow(dead_code)] [u8; 4096]);

impl Future for Padded {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        Pin::new(&mut self.get_mut().0).poll(cx)
    }
}

macro_rules! argument {
    () => { impl Future<Output = ()> + 'static };
}

/// The type macro makes the task generic over its argument's future type.
#[switch::task]
fn run_argument(future: argument!()) -> impl Future<Output = ()> {
    future
}

#[test]
fn type_macro_argument_runs_each_future() {
    let count = Rc::new(Cell::new(0));
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let small = count.clone();
    spawner
        .spawn(run_argument(async move { small.set(small.get() + 1) }))
        .unwrap();
    let large = count.clone();
    spawner
        .spawn(run_argument(async move {
            let bulk = [0u8; 4096];
            std::future::ready(()).await;
            std::hint::black_box(&bulk);
            large.set(large.get() + 10);
        }))
        .unwrap();
    while runtime.poll(0).unwrap().live_tasks != 0 {}
    assert_eq!(count.get(), 11);
}

#[test]
fn type_macro_instantiations_use_distinct_pools() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let states: Vec<_> = (0..3).map(|_| state()).collect();
    spawner
        .spawn(run_argument(Parked(states[0].clone())))
        .unwrap();
    spawner
        .spawn(run_argument(Parked(states[1].clone())))
        .unwrap();
    spawner
        .spawn(run_argument(Padded(Parked(states[2].clone()), [0; 4096])))
        .unwrap();
    runtime.poll(0).unwrap();
    let pools: Vec<_> = states.iter().map(|s| runtime_pool(&waker(s))).collect();
    assert_eq!(pools[0], pools[1]);
    assert_ne!(pools[0], pools[2]);
}

#[test]
fn token_with_shared_marker_splits_by_future_type() {
    #[derive(Clone, Copy)]
    struct Shared;
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let states: Vec<_> = (0..3).map(|_| state()).collect();
    for state in &states[..2] {
        let state = state.clone();
        spawner
            .spawn(crate::__private::token(Shared, move || Parked(state)))
            .unwrap();
    }
    let padded = states[2].clone();
    spawner
        .spawn(crate::__private::token(Shared, move || {
            Padded(Parked(padded), [0; 4096])
        }))
        .unwrap();
    runtime.poll(0).unwrap();
    assert!(states.iter().all(|state| state.polls.get() == 1));
    let pools: Vec<_> = states.iter().map(|s| runtime_pool(&waker(s))).collect();
    assert_eq!(pools[0], pools[1]);
    assert_ne!(pools[0], pools[2]);
}

#[test]
fn same_future_type_functions_keep_separate_pools() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let (first, second) = (state(), state());
    spawner.spawn(parked(first.clone())).unwrap();
    spawner.spawn(parked_other(second.clone())).unwrap();
    runtime.poll(0).unwrap();
    assert_ne!(runtime_pool(&waker(&first)), runtime_pool(&waker(&second)));
}

#[test]
fn detached_and_joined_spawns_share_one_pool() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let (detached, joined) = (state(), state());
    spawner.spawn(parked(detached.clone())).unwrap();
    let _handle = spawner.spawn_join(parked(joined.clone())).unwrap();
    runtime.poll(0).unwrap();
    assert_eq!(runtime_pool(&waker(&detached)), runtime_pool(&waker(&joined)));
}

#[switch::task]
async fn per_type<T: 'static>(value: T, state: Rc<State>) {
    Parked(state).await;
    drop(value);
}

/// A pool per instantiation also when the future type does not depend on the parameter:
/// a type parameter, a const parameter, an `impl Trait` argument.
#[test]
fn instantiations_with_one_future_type_have_their_own_pools() {
    trait Tag {}
    impl Tag for u8 {}
    impl Tag for u16 {}
    #[switch::task]
    fn by_type<L: Tag + 'static>(state: Rc<State>) -> impl Future<Output = ()> {
        Parked(state)
    }
    #[switch::task]
    fn by_const<const N: u32>(state: Rc<State>) -> impl Future<Output = ()> {
        Parked(state)
    }
    #[switch::task]
    fn by_argument(_unused: impl Tag + 'static, state: Rc<State>) -> impl Future<Output = ()> {
        Parked(state)
    }
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let states: Vec<_> = (0..6).map(|_| state()).collect();
    spawner.spawn(by_type::<u8>(states[0].clone())).unwrap();
    spawner.spawn(by_type::<u16>(states[1].clone())).unwrap();
    spawner.spawn(by_const::<2>(states[2].clone())).unwrap();
    spawner.spawn(by_const::<3>(states[3].clone())).unwrap();
    spawner.spawn(by_argument(1u8, states[4].clone())).unwrap();
    spawner.spawn(by_argument(1u16, states[5].clone())).unwrap();
    runtime.poll(0).unwrap();
    let pools: Vec<_> = states.iter().map(|s| runtime_pool(&waker(s))).collect();
    for (i, pool) in pools.iter().enumerate() {
        assert!(!pools[i + 1..].contains(pool), "instantiation {i} shares a pool");
    }
}

// ---- Token and spawn unwinding ----

struct DropFlag(Rc<Cell<usize>>, bool);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
        if self.1 {
            panic_any(55usize);
        }
    }
}

#[switch::task]
async fn holds(flag: DropFlag) {
    let _flag = flag;
}

#[test]
fn unspawned_token_drops_only_its_arguments() {
    let drops = Rc::new(Cell::new(0));
    let token = holds(DropFlag(drops.clone(), false));
    assert_eq!(drops.get(), 0);
    drop(token);
    assert_eq!(drops.get(), 1);
}

#[test]
fn token_destructor_panic_leaves_pool_usable() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let drops = Rc::new(Cell::new(0));
    let token = holds(DropFlag(drops.clone(), true));
    let payload = catch_unwind(AssertUnwindSafe(|| drop(token))).unwrap_err();
    assert_eq!(payload.downcast_ref::<usize>(), Some(&55));
    runtime.shutdown();
    let error = spawner
        .spawn(holds(DropFlag(drops.clone(), true)))
        .unwrap_err();
    assert!(catch_unwind(AssertUnwindSafe(|| drop(error))).is_err());
    assert_eq!(drops.get(), 2);
    let mut runtime = Runtime::new(0);
    runtime
        .spawner()
        .spawn(holds(DropFlag(drops.clone(), false)))
        .unwrap();
    assert_eq!(runtime.poll(0).unwrap().tasks_polled, 1);
    assert_eq!(drops.get(), 3);
}

#[test]
fn factory_panic_releases_its_reserved_slot() {
    #[switch::task]
    fn factory(state: Rc<State>, panic: bool) -> impl Future<Output = ()> {
        if panic {
            panic_any(77usize);
        }
        Parked(state)
    }
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let held = state();
    spawner.spawn(factory(held.clone(), false)).unwrap();
    runtime.poll(0).unwrap();
    let failed = state();
    let payload = catch_unwind(AssertUnwindSafe(|| {
        spawner.spawn(factory(failed.clone(), true))
    }))
    .unwrap_err();
    assert_eq!(payload.downcast_ref::<usize>(), Some(&77));
    let report = runtime.poll(0).unwrap();
    assert_eq!((report.tasks_polled, report.live_tasks), (0, 1));
    // The failed reservation released its slot: the next spawn takes it.
    let next = state();
    spawner.spawn(factory(next.clone(), false)).unwrap();
    runtime.poll(0).unwrap();
    assert_eq!(failed.polls.get(), 0);
    let next_meta = meta(&waker(&next));
    assert_eq!(unsafe { Meta::position(next_meta, header(&waker(&next))) }, 1);
    drop(runtime);
    assert_eq!(
        (held.drops.get(), next.drops.get(), failed.drops.get()),
        (1, 1, 0)
    );
}

#[switch::task]
fn recursive_factory(
    spawner: Spawner,
    states: Rc<RefCell<Vec<Rc<State>>>>,
    depth: u8,
) -> impl Future<Output = ()> {
    let state = state();
    states.borrow_mut().push(state.clone());
    if depth > 0 {
        spawner
            .spawn(recursive_factory(
                spawner.clone(),
                states.clone(),
                depth - 1,
            ))
            .unwrap();
    }
    Parked(state)
}

#[test]
fn factory_may_spawn_recursively() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let states = Rc::new(RefCell::new(Vec::new()));
    spawner
        .spawn(recursive_factory(spawner.clone(), states.clone(), 70))
        .unwrap();
    let report = runtime.poll(0).unwrap();
    assert_eq!((report.tasks_polled, report.live_tasks), (71, 71));
    let mut headers: Vec<_> = states.borrow().iter().map(|s| header(&waker(s))).collect();
    headers.sort_unstable();
    headers.dedup();
    assert_eq!(headers.len(), 71);
    drop(runtime);
    assert!(states.borrow().iter().all(|s| s.drops.get() == 1));
}

#[switch::task]
fn shutdown_in_factory(
    runtime: Rc<RefCell<Runtime>>,
    state: Rc<State>,
    panic: bool,
) -> impl Future<Output = ()> {
    runtime.borrow_mut().shutdown();
    assert_eq!(
        state.drops.get(),
        0,
        "cleanup must wait for the reservation"
    );
    if panic {
        panic_any(91usize);
    }
    Parked(state)
}

#[test]
fn shutdown_from_a_factory_completes_when_the_spawn_returns() {
    let runtime = Rc::new(RefCell::new(Runtime::new(0)));
    let spawner = runtime.borrow().spawner();
    let earlier = spawn_parked(&mut runtime.borrow_mut(), 1).remove(0);
    let state = state();
    spawner
        .spawn(shutdown_in_factory(runtime.clone(), state.clone(), false))
        .unwrap();
    // The runtime closed inside the factory; its tasks, the new one included,
    // were dropped as the spawn returned.
    assert_eq!(
        (earlier.drops.get(), state.drops.get(), state.polls.get()),
        (1, 1, 0)
    );
    assert_eq!(runtime.borrow_mut().poll(0), Err(RuntimeError::Closed));
    let rejected = spawner.spawn(parked(self::state())).unwrap_err();
    assert_eq!(rejected.reason, RuntimeError::Closed);
}

#[test]
fn shutdown_then_panic_in_a_factory_releases_the_slot_then_cleans_up() {
    let runtime = Rc::new(RefCell::new(Runtime::new(0)));
    let spawner = runtime.borrow().spawner();
    let earlier = spawn_parked(&mut runtime.borrow_mut(), 1).remove(0);
    let state = state();
    let payload = catch_unwind(AssertUnwindSafe(|| {
        spawner.spawn(shutdown_in_factory(runtime.clone(), state.clone(), true))
    }))
    .unwrap_err();
    assert_eq!(payload.downcast_ref::<usize>(), Some(&91));
    assert_eq!((earlier.drops.get(), state.drops.get()), (1, 0));
    assert_eq!(runtime.borrow_mut().poll(0), Err(RuntimeError::Closed));
}

#[test]
fn deferred_shutdown_finishes_after_a_destructor_panic() {
    struct PanicOnDrop(Rc<State>);
    impl Future for PanicOnDrop {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            Poll::Pending
        }
    }
    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            self.0.drops.set(self.0.drops.get() + 1);
            panic_any(13usize);
        }
    }
    #[switch::task]
    fn panics_on_drop(state: Rc<State>) -> impl Future<Output = ()> {
        PanicOnDrop(state)
    }
    #[switch::task]
    fn drops_runtime(
        owner: Rc<RefCell<Option<Runtime>>>,
        state: Rc<State>,
    ) -> impl Future<Output = ()> {
        drop(owner.borrow_mut().take());
        Parked(state)
    }
    let owner = Rc::new(RefCell::new(Some(Runtime::new(0))));
    let spawner = owner.borrow().as_ref().unwrap().spawner();
    let panicking = state();
    spawner.spawn(panics_on_drop(panicking.clone())).unwrap();
    let others: Vec<_> = (0..3).map(|_| state()).collect();
    for state in &others {
        spawner.spawn(parked(state.clone())).unwrap();
    }
    owner.borrow_mut().as_mut().unwrap().poll(0).unwrap();
    let last = state();
    let payload = catch_unwind(AssertUnwindSafe(|| {
        spawner.spawn(drops_runtime(owner.clone(), last.clone()))
    }))
    .unwrap_err();
    assert_eq!(payload.downcast_ref::<usize>(), Some(&13));
    assert!(owner.borrow().is_none());
    // Every task was dropped once, the panicking one included.
    assert_eq!(panicking.drops.get(), 1);
    assert!(others.iter().all(|state| state.drops.get() == 1));
    assert_eq!(last.drops.get(), 1);
    assert_eq!(
        spawner.spawn(parked(state())).unwrap_err().reason,
        RuntimeError::Closed
    );
}

// ---- Cleanup ----

#[test]
fn cleanup_drops_every_task_of_every_block_and_function_once() {
    struct Ordered(u32, Rc<RefCell<Vec<u32>>>, #[allow(dead_code)] [u8; 20_000]);
    impl Future for Ordered {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            Poll::Pending
        }
    }
    impl Drop for Ordered {
        fn drop(&mut self) {
            self.1.borrow_mut().push(self.0);
        }
    }
    #[switch::task]
    fn first_fn(id: u32, log: Rc<RefCell<Vec<u32>>>) -> impl Future<Output = ()> {
        Ordered(id, log, [0; 20_000])
    }
    #[switch::task]
    fn second_fn(id: u32, log: Rc<RefCell<Vec<u32>>>) -> impl Future<Output = ()> {
        Ordered(id, log, [0; 20_000])
    }
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let log = Rc::new(RefCell::new(Vec::new()));
    for id in 0..5 {
        spawner.spawn(first_fn(id, log.clone())).unwrap();
    }
    for id in 1000..1002 {
        spawner.spawn(second_fn(id, log.clone())).unwrap();
    }
    runtime.poll(0).unwrap();
    runtime.shutdown();
    // The drop order is unspecified; each future is dropped exactly once.
    let mut dropped = log.borrow().clone();
    dropped.sort_unstable();
    let mut expected: Vec<u32> = (0..5).collect();
    expected.extend(1000..1002);
    assert_eq!(dropped, expected);
}

// ---- Timers ----

#[test]
fn sleep_expiry_wakes_a_task_in_a_later_block() {
    #[switch::task]
    async fn sleeper(spawner: Spawner, woke: Rc<Cell<bool>>) {
        let bulk = [0u8; 20_000];
        spawner.sleep_until(5).unwrap().await.unwrap();
        std::hint::black_box(&bulk);
        woke.set(true);
    }
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let flags: Vec<_> = (0..7).map(|_| Rc::new(Cell::new(false))).collect();
    for woke in &flags {
        spawner
            .spawn(sleeper(spawner.clone(), woke.clone()))
            .unwrap();
    }
    assert_eq!(runtime.poll(0).unwrap().live_tasks, 7);
    let report = runtime.poll(5).unwrap();
    assert_eq!((report.tasks_polled, report.live_tasks), (7, 0));
    assert!(flags.iter().all(|woke| woke.get()));
}

// ---- StoredWaker ----

#[switch::task]
fn stores(state: Rc<State>, stored: Rc<Cell<Option<StoredWaker>>>) -> impl Future<Output = ()> {
    std::future::poll_fn(move |cx| {
        stored.set(Some(StoredWaker::new(cx)));
        *state.waker.borrow_mut() = Some(cx.waker().clone());
        state.polls.set(state.polls.get() + 1);
        if state.done.get() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
}

#[test]
fn stored_waker_wake_polls_the_task_once() {
    let mut runtime = Runtime::new(0);
    let state = state();
    let stored = Rc::new(Cell::new(None));
    runtime
        .spawner()
        .spawn(stores(state.clone(), stored.clone()))
        .unwrap();
    runtime.poll(0).unwrap();
    let handle = stored.get().unwrap();
    handle.wake();
    handle.wake();
    assert_eq!(runtime.poll(0).unwrap().tasks_polled, 1);
    assert_eq!(runtime.poll(0).unwrap().tasks_polled, 0);
}

#[test]
fn stale_stored_waker_after_shutdown_or_drop_has_no_effect() {
    for drop_runtime in [false, true] {
        let mut runtime = Runtime::new(0);
        let state = state();
        let stored = Rc::new(Cell::new(None));
        runtime
            .spawner()
            .spawn(stores(state.clone(), stored.clone()))
            .unwrap();
        runtime.poll(0).unwrap();
        let stale = stored.get().unwrap();
        let block = meta(&waker(&state));
        if drop_runtime {
            drop(runtime);
        } else {
            runtime.shutdown();
        }
        // Only the flags check stands before the freed (or closed) executor.
        assert_eq!(
            unsafe { TaskHeader::flags(Meta::header(block, 0)).get() },
            0
        );
        stale.wake();
        stale.wake_deferred();
        assert_eq!(state.polls.get(), 1);
    }
}

#[test]
fn stale_stored_waker_after_dropping_a_panicked_runtime_with_queued_wakes() {
    use super::header::{LIVE, QUEUED, TAKEN};
    #[switch::task]
    async fn panics() {
        panic_any(7usize)
    }
    let mut runtime = Runtime::new(0);
    let state = state();
    let stored = Rc::new(Cell::new(None));
    runtime
        .spawner()
        .spawn(stores(state.clone(), stored.clone()))
        .unwrap();
    runtime.poll(0).unwrap();
    let stale = stored.get().unwrap();
    let flags = unsafe { TaskHeader::flags(header(&waker(&state))) };
    runtime.spawner().spawn(panics()).unwrap();
    assert!(catch_unwind(AssertUnwindSafe(|| runtime.poll(0))).is_err());
    // The panicked executor queues the wakes: deferred, then upgraded.
    stale.wake_deferred();
    stale.wake();
    assert_eq!(flags.get() & !TAKEN, LIVE | QUEUED);
    drop(runtime);
    // Cleanup cleared every flag before freeing the executor and its queues.
    assert_eq!(flags.get(), 0);
    stale.wake();
    stale.wake_deferred();
    assert_eq!((state.polls.get(), flags.get()), (1, 0));
}

#[test]
fn stored_waker_rejects_a_foreign_waker() {
    let cx = Context::from_waker(Waker::noop());
    let payload = catch_unwind(AssertUnwindSafe(|| StoredWaker::new(&cx))).unwrap_err();
    assert_eq!(
        payload.downcast_ref::<&str>().copied(),
        Some("StoredWaker needs a switch task waker")
    );
}

#[test]
fn stored_waker_new_rejects_a_foreign_thread() {
    let mut runtime = Runtime::new(0);
    let state = spawn_parked(&mut runtime, 1).remove(0);
    let waker = waker(&state);
    std::thread::scope(|scope| {
        scope.spawn(|| {
            let cx = Context::from_waker(&waker);
            let payload = catch_unwind(AssertUnwindSafe(|| StoredWaker::new(&cx))).unwrap_err();
            assert_eq!(
                payload.downcast_ref::<&str>().copied(),
                Some("waker used on foreign thread")
            );
        });
    });
}

// ---- Thread-local destruction ----

struct OnTlsDrop(Arc<AtomicUsize>);

struct Counted(Arc<AtomicUsize>);

impl Future for Counted {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        Poll::Pending
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[switch::task]
fn counted_in_tls(drops: Arc<AtomicUsize>) -> impl Future<Output = ()> {
    Counted(drops)
}

#[switch::task]
fn first_used_in_tls_drop(drops: Arc<AtomicUsize>) -> impl Future<Output = ()> {
    Counted(drops)
}

impl Drop for OnTlsDrop {
    fn drop(&mut self) {
        // First use of this function's pool on this thread, during TLS destruction.
        let mut runtime = Runtime::new(0);
        runtime
            .spawner()
            .spawn(first_used_in_tls_drop(self.0.clone()))
            .unwrap();
        assert_eq!(runtime.poll(0).unwrap().live_tasks, 1);
    }
}

thread_local! {
    static HELD_RUNTIME: RefCell<Option<Runtime>> = const { RefCell::new(None) };
    static SPAWN_ON_DROP: RefCell<Option<OnTlsDrop>> = const { RefCell::new(None) };
}

#[test]
fn runtime_dropped_and_spawned_from_thread_local_destructors() {
    let held = Arc::new(AtomicUsize::new(0));
    let late = Arc::new(AtomicUsize::new(0));
    let (held_in, late_in) = (held.clone(), late.clone());
    std::thread::spawn(move || {
        let mut runtime = Runtime::new(0);
        runtime.spawner().spawn(counted_in_tls(held_in)).unwrap();
        runtime.poll(0).unwrap();
        HELD_RUNTIME.with(|slot| *slot.borrow_mut() = Some(runtime));
        SPAWN_ON_DROP.with(|slot| *slot.borrow_mut() = Some(OnTlsDrop(late_in)));
    })
    .join()
    .unwrap();
    assert_eq!(held.load(Ordering::SeqCst), 1);
    assert_eq!(late.load(Ordering::SeqCst), 1);
}
