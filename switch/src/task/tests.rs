//! Slab, pool, token and task-waker behavior (TASKS_DESIGN.md §7: T2-T6, T10, T15).

use std::cell::{Cell, RefCell};
use std::mem::ManuallyDrop;
use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use super::TaskHeader;
use super::pool::{FunctionPool, RuntimePool};
use super::slab::{META, Meta, SLOTS, Shape, TaskRef};
use crate::runtime::{Runtime, Spawner};

#[derive(Default)]
struct State {
    polls: Cell<usize>,
    drops: Cell<usize>,
    done: Cell<bool>,
    waker: RefCell<Option<Waker>>,
    /// Future address at the latest poll.
    addr: Cell<usize>,
}

/// Records each poll and keeps a clone of its latest waker; ready once `done`.
struct Parked(Rc<State>);

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

task! {
    fn parked(state: Rc<State>) -> impl Future<Output = ()> {
        Parked(state)
    }
}

task! {
    fn parked_other(state: Rc<State>) -> impl Future<Output = ()> {
        Parked(state)
    }
}

fn state() -> Rc<State> {
    Rc::new(State::default())
}

fn waker(state: &State) -> Waker {
    state.waker.borrow().clone().expect("task was polled")
}

fn header(waker: &Waker) -> *const TaskHeader {
    waker.data().cast()
}

fn meta(waker: &Waker) -> *mut Meta {
    Shape::<Parked>::meta_of(header(waker))
}

/// Pool index (`slab number * 64 + slot`) of the task that produced `waker`.
fn index(waker: &Waker) -> u32 {
    unsafe { RuntimePool::index_of(header(waker), meta(waker)) }
}

fn function_pool(waker: &Waker) -> *mut FunctionPool {
    unsafe { RuntimePool::function_pool(Meta::pool(meta(waker)).get()) }
}

/// Spawn `n` parked tasks and poll them once.
fn spawn_parked(runtime: &mut Runtime, n: usize) -> Vec<Rc<State>> {
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
    assert_eq!(runtime.poll(0).unwrap().tasks_completed, 1);
}

// ---- T10 layout ----

#[test]
fn slot_starts_are_16_aligned_and_mask_round_trips() {
    fn check<Fut>() {
        assert_eq!(Shape::<Fut>::STRIDE % 16, 0);
        for slot in 0..SLOTS {
            assert_eq!(
                (Shape::<Fut>::DATA_OFF + slot * Shape::<Fut>::STRIDE) % 16,
                0
            );
        }
        assert!(Shape::<Fut>::SLAB_ALIGN.is_power_of_two());
        assert!(Shape::<Fut>::SLAB_ALIGN >= Shape::<Fut>::SLAB_SIZE);
    }
    check::<()>();
    check::<u8>();
    check::<[u8; 9]>();
    check::<[u64; 3]>();
    check::<[u64; 10]>();
    check::<Aligned128>();

    let mut runtime = Runtime::new(0);
    let states = spawn_parked(&mut runtime, SLOTS);
    let base = meta(&waker(&states[0]));
    assert_eq!(base.addr() % Shape::<Parked>::SLAB_ALIGN, 0);
    for slot in [0, SLOTS - 1] {
        let waker = waker(&states[slot]);
        assert_eq!(meta(&waker), base);
        assert_eq!(index(&waker), slot as u32);
        let task = unsafe { TaskRef::new(base, slot as u32) };
        assert_eq!((task.meta(), task.slot()), (base, slot as u32));
        assert_eq!(unsafe { task.header() }, header(&waker));
        assert_eq!(header(&waker).addr() % 16, 0);
    }
}

#[test]
fn eighty_byte_future_slab_is_6208_bytes_aligned_to_8192() {
    type Fut = [u64; 10];
    assert_eq!(Shape::<Fut>::STRIDE, 96);
    assert_eq!(Shape::<Fut>::DATA_OFF, META);
    assert_eq!(Shape::<Fut>::SLAB_SIZE, 6208);
    assert_eq!(Shape::<Fut>::SLAB_ALIGN, 8192);
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

task! {
    fn aligned_128(state: Rc<State>) -> impl Future<Output = ()> {
        Aligned128 { state, _bulk: [0; 40] }
    }
}

#[test]
fn over_aligned_future_uses_one_offset_in_both_directions_through_ring_eviction() {
    assert_eq!(Shape::<Aligned128>::DATA_OFF, 128);
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    // 20 tasks; finishing all of them overflows the 16-entry ring.
    let states: Vec<_> = (0..20).map(|_| state()).collect();
    for state in &states {
        spawner.spawn(aligned_128(state.clone())).unwrap();
    }
    runtime.poll(0).unwrap();
    for state in &states {
        let header = header(&waker(state));
        let meta = Shape::<Aligned128>::meta_of(header);
        let slot = u32::from(unsafe { TaskHeader::slot(header) });
        assert_eq!(unsafe { TaskRef::new(meta, slot).header() }, header);
        state.done.set(true);
        waker(state).wake_by_ref();
    }
    assert_eq!(runtime.poll(0).unwrap().tasks_completed, 20);
    let again: Vec<_> = (0..20).map(|_| state()).collect();
    for state in &again {
        spawner.spawn(aligned_128(state.clone())).unwrap();
    }
    runtime.poll(0).unwrap();
    for state in &again {
        state.done.set(true);
        waker(state).wake_by_ref();
    }
    assert_eq!(runtime.poll(0).unwrap().tasks_completed, 20);
    assert!(
        states
            .iter()
            .chain(&again)
            .all(|s| s.drops.get() == 1 && s.polls.get() == 2)
    );
}

// ---- T2 stale wakers ----

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
fn stale_waker_of_reused_slot_gives_newer_task_one_extra_poll() {
    let mut runtime = Runtime::new(0);
    let old = spawn_parked(&mut runtime, 1).remove(0);
    let stale = waker(&old);
    finish(&mut runtime, &old);
    let new = spawn_parked(&mut runtime, 1).remove(0);
    assert_eq!(header(&waker(&new)), header(&stale));
    assert_eq!(new.polls.get(), 1);
    stale.wake_by_ref();
    stale.wake_by_ref();
    assert_eq!(runtime.poll(0).unwrap().tasks_polled, 1);
    assert_eq!(new.polls.get(), 2);
    finish(&mut runtime, &new);
    assert_eq!((new.polls.get(), new.drops.get()), (3, 1));
}

#[test]
fn stale_waker_of_slab_lent_to_later_runtime_polls_there_once() {
    let mut first = Runtime::new(0);
    let old = spawn_parked(&mut first, 1).remove(0);
    let stale = waker(&old);
    drop(first);
    assert_eq!(old.drops.get(), 1);
    let mut second = Runtime::new(0);
    let new = spawn_parked(&mut second, 1).remove(0);
    assert_eq!(header(&waker(&new)), header(&stale));
    stale.wake_by_ref();
    assert_eq!(second.poll(0).unwrap().tasks_polled, 1);
    assert_eq!(new.polls.get(), 2);
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
        for waker in &stale {
            assert!(!unsafe { TaskHeader::is_live(header(waker)) });
            assert!(unsafe { Meta::executor(meta(waker)).get() }.is_null());
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

#[test]
fn stale_wake_into_a_reserved_slot_returns_before_any_executor_access() {
    task! {
        fn wake_while_reserved(state: Rc<State>, stale: Option<Waker>) -> impl Future<Output = ()> {
            // Factory code runs with this call's slot reserved but not yet LIVE.
            if let Some(stale) = stale {
                assert_eq!(unsafe { TaskHeader::flags(header(&stale)).get() }, 0);
                stale.wake_by_ref();
            }
            Parked(state)
        }
    }
    let mut first = Runtime::new(0);
    let spawner = first.spawner();
    let old = state();
    spawner
        .spawn(wake_while_reserved(old.clone(), None))
        .unwrap();
    first.poll(0).unwrap();
    let stale = waker(&old);
    drop(first);
    let mut second = Runtime::new(0);
    let new = state();
    second
        .spawner()
        .spawn(wake_while_reserved(new.clone(), Some(stale.clone())))
        .unwrap();
    let report = second.poll(0).unwrap();
    assert_eq!((report.tasks_polled, report.ready_tasks), (1, 0));
    assert_eq!(header(&waker(&new)), header(&stale));
    assert_eq!(new.polls.get(), 1);
}

// ---- T3 will_wake ----

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

// ---- T4 foreign thread after owner exit ----

#[test]
fn foreign_thread_ops_panic_after_owner_thread_exits() {
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
        // Never dropped here unless dropping is the operation under test.
        let waker = ManuallyDrop::new(waker);
        let payload = catch_unwind(AssertUnwindSafe(move || match operation {
            0 => waker.wake_by_ref(),
            1 => drop(Waker::clone(&waker)),
            2 => ManuallyDrop::into_inner(waker).wake(),
            _ => drop(ManuallyDrop::into_inner(waker)),
        }))
        .unwrap_err();
        assert_eq!(
            payload.downcast_ref::<&str>().copied(),
            Some("waker used on foreign thread")
        );
    }
}

// ---- T5 pool ----

#[test]
fn allocation_order_ring_then_lowest_then_spare_then_new() {
    let mut runtime = Runtime::new(0);
    let states = spawn_parked(&mut runtime, 4);
    let indices: Vec<_> = states.iter().map(|s| index(&waker(s))).collect();
    assert_eq!(indices, [0, 1, 2, 3]);
    finish(&mut runtime, &states[0]);
    finish(&mut runtime, &states[2]);
    let pool = unsafe { Meta::pool(meta(&waker(&states[1]))).get() };
    // Newest ring entry first, then the older one.
    let a = spawn_parked(&mut runtime, 1).remove(0);
    assert_eq!(index(&waker(&a)), 2);
    let b = spawn_parked(&mut runtime, 1).remove(0);
    assert_eq!(index(&waker(&b)), 0);
    // A ring entry whose slot was reserved again is skipped. The pool never
    // produces one, so force it: free 3, then mark slot 3 in use behind the ring.
    finish(&mut runtime, &states[3]);
    let slab = meta(&waker(&states[1]));
    let alloc = unsafe { Meta::alloc(slab) };
    alloc.set(alloc.get() | 1 << 3);
    let c = spawn_parked(&mut runtime, 1).remove(0);
    assert_eq!(index(&waker(&c)), 4);
    alloc.set(alloc.get() & !(1 << 3));
    // Lowest free index once the ring is empty.
    let d = spawn_parked(&mut runtime, 1).remove(0);
    assert_eq!(index(&waker(&d)), 3);
    assert_eq!(unsafe { RuntimePool::slab_count(pool) }, 1);
    // Fill the slab; the next spawn allocates a new slab.
    let fp = function_pool(&waker(&states[1]));
    let fill = spawn_parked(&mut runtime, SLOTS - 5);
    assert_eq!(index(&waker(fill.last().unwrap())), SLOTS as u32 - 1);
    let grown = spawn_parked(&mut runtime, 1).remove(0);
    assert_eq!(index(&waker(&grown)), SLOTS as u32);
    assert_ne!(meta(&waker(&grown)), slab);
    assert_eq!(unsafe { FunctionPool::counts(fp) }, (1, 0));
    // Shutdown returns both slabs to spare and frees the runtime pool.
    drop(runtime);
    assert_eq!(unsafe { FunctionPool::counts(fp) }, (0, 2));
    // A later runtime takes spare slabs before allocating.
    let mut later = Runtime::new(0);
    let reused = spawn_parked(&mut later, 1).remove(0);
    assert_eq!(unsafe { FunctionPool::counts(fp) }, (1, 1));
    let reused_meta = meta(&waker(&reused));
    assert!(reused_meta == slab || reused_meta == meta(&waker(&grown)));
    assert_eq!(index(&waker(&reused)), 0);
}

#[test]
fn live_tasks_pack_into_lowest_slabs_after_frees() {
    let mut runtime = Runtime::new(0);
    let states = spawn_parked(&mut runtime, 3 * SLOTS);
    for state in &states[..3 * SLOTS - 1] {
        state.done.set(true);
        waker(state).wake_by_ref();
    }
    runtime.poll(0).unwrap();
    // 16 spawns reuse the ring; the rest take the lowest indices in order.
    let refill = spawn_parked(&mut runtime, SLOTS);
    let rest: Vec<_> = refill[16..].iter().map(|s| index(&waker(s))).collect();
    let expected: Vec<_> = (0..(SLOTS as u32 - 16)).collect();
    assert_eq!(rest, expected);
}

#[test]
fn runtimes_on_one_thread_use_disjoint_slabs() {
    let mut a = Runtime::new(0);
    let mut b = Runtime::new(0);
    let in_a = spawn_parked(&mut a, 2);
    let a_slab = meta(&waker(&in_a[0]));
    for state in &in_a {
        finish(&mut a, state);
    }
    // `a` has two free slots, yet `b` never takes one.
    let in_b = spawn_parked(&mut b, SLOTS + 1);
    assert!(in_b.iter().all(|s| meta(&waker(s)) != a_slab));
    let b_executor = unsafe { Meta::executor(meta(&waker(&in_b[0]))).get() };
    assert_ne!(unsafe { Meta::executor(a_slab).get() }, b_executor);
}

#[test]
fn live_future_never_moves_when_slabs_grow() {
    let mut runtime = Runtime::new(0);
    let first = spawn_parked(&mut runtime, 1).remove(0);
    let addr = first.addr.get();
    let _many = spawn_parked(&mut runtime, 20 * SLOTS);
    waker(&first).wake_by_ref();
    runtime.poll(0).unwrap();
    assert_eq!(first.polls.get(), 2);
    assert_eq!(first.addr.get(), addr);
}

#[test]
fn one_function_on_two_threads_uses_disjoint_slabs() {
    let spawn_on_thread = || {
        std::thread::spawn(|| {
            let mut runtime = Runtime::new(0);
            let states = spawn_parked(&mut runtime, 1);
            let waker = waker(&states[0]);
            (meta(&waker).addr(), function_pool(&waker).addr())
        })
        .join()
        .unwrap()
    };
    let (slab_a, pool_a) = spawn_on_thread();
    let (slab_b, pool_b) = spawn_on_thread();
    assert_ne!(slab_a, slab_b);
    assert_ne!(pool_a, pool_b);
}

// ---- T6 token, and spawn unwinding ----

struct DropFlag(Rc<Cell<usize>>, bool);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
        if self.1 {
            panic_any(55usize);
        }
    }
}

task! {
    async fn holds(flag: DropFlag) {
        let _flag = flag;
    }
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
    assert_eq!(runtime.poll(0).unwrap().tasks_completed, 1);
    assert_eq!(drops.get(), 3);
}

task! {
    fn factory(state: Rc<State>, panic: bool) -> impl Future<Output = ()> {
        if panic {
            panic_any(77usize);
        }
        Parked(state)
    }
}

#[test]
fn factory_panic_releases_its_reserved_slot() {
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
    let next = state();
    spawner.spawn(factory(next.clone(), false)).unwrap();
    runtime.poll(0).unwrap();
    assert_eq!(index(&waker(&next)), 1);
    assert_eq!(failed.polls.get(), 0);
    drop(runtime);
    assert_eq!(
        (held.drops.get(), next.drops.get(), failed.drops.get()),
        (1, 1, 0)
    );
}

task! {
    fn recursive_factory(spawner: Spawner, states: Rc<RefCell<Vec<Rc<State>>>>, depth: u8) -> impl Future<Output = ()> {
        let state = state();
        states.borrow_mut().push(state.clone());
        if depth > 0 {
            spawner
                .spawn(recursive_factory(spawner.clone(), states.clone(), depth - 1))
                .unwrap();
        }
        Parked(state)
    }
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
    let indices: Vec<_> = states.borrow().iter().map(|s| index(&waker(s))).collect();
    // The outermost call reserved first; each nested call took the next index.
    assert_eq!(indices, (0..71).collect::<Vec<_>>());
    drop(runtime);
    assert!(states.borrow().iter().all(|s| s.drops.get() == 1));
}

task! {
    fn shutdown_in_factory(runtime: Rc<RefCell<Runtime>>, state: Rc<State>) -> impl Future<Output = ()> {
        runtime.borrow_mut().shutdown();
        Parked(state)
    }
}

#[test]
fn shutdown_from_a_factory_is_rejected_as_reentrant() {
    let runtime = Rc::new(RefCell::new(Runtime::new(0)));
    let spawner = runtime.borrow().spawner();
    let state = state();
    let payload = catch_unwind(AssertUnwindSafe(|| {
        spawner.spawn(shutdown_in_factory(runtime.clone(), state.clone()))
    }))
    .unwrap_err();
    assert_eq!(
        payload.downcast_ref::<&str>().copied(),
        Some("executor reentrancy")
    );
    let report = runtime.borrow_mut().poll(0).unwrap();
    assert_eq!(report.live_tasks, 0);
    runtime.borrow_mut().shutdown();
    assert_eq!(state.drops.get(), 0);
}

// ---- T7 cleanup order ----

#[test]
fn cleanup_drops_last_function_first_and_highest_slot_first() {
    struct Ordered(u32, Rc<RefCell<Vec<u32>>>);
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
    task! {
        fn first_fn(id: u32, log: Rc<RefCell<Vec<u32>>>) -> impl Future<Output = ()> {
            Ordered(id, log)
        }
    }
    task! {
        fn second_fn(id: u32, log: Rc<RefCell<Vec<u32>>>) -> impl Future<Output = ()> {
            Ordered(id, log)
        }
    }
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let log = Rc::new(RefCell::new(Vec::new()));
    for id in 0..(SLOTS as u32 + 2) {
        spawner.spawn(first_fn(id, log.clone())).unwrap();
    }
    for id in 1000..1002 {
        spawner.spawn(second_fn(id, log.clone())).unwrap();
    }
    runtime.poll(0).unwrap();
    runtime.shutdown();
    let mut expected = vec![1001, 1000];
    expected.extend((0..(SLOTS as u32 + 2)).rev());
    assert_eq!(*log.borrow(), expected);
}

// ---- T8 timers through TaskRef ----

#[test]
fn sleep_expiry_wakes_a_task_in_a_later_slab() {
    task! {
        async fn sleeper(spawner: Spawner, woke: Rc<Cell<bool>>) {
            spawner.sleep_until(5).unwrap().await.unwrap();
            woke.set(true);
        }
    }
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let flags: Vec<_> = (0..(SLOTS + 6))
        .map(|_| Rc::new(Cell::new(false)))
        .collect();
    for woke in &flags {
        spawner
            .spawn(sleeper(spawner.clone(), woke.clone()))
            .unwrap();
    }
    assert_eq!(runtime.poll(0).unwrap().live_tasks, SLOTS + 6);
    let report = runtime.poll(5).unwrap();
    assert_eq!(
        (report.timers_expired, report.tasks_completed),
        (SLOTS + 6, SLOTS + 6)
    );
    assert!(flags.iter().all(|woke| woke.get()));
}

// ---- T15 thread-local destruction ----

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

task! {
    fn counted_in_tls(drops: Arc<AtomicUsize>) -> impl Future<Output = ()> {
        Counted(drops)
    }
}

task! {
    fn first_used_in_tls_drop(drops: Arc<AtomicUsize>) -> impl Future<Output = ()> {
        Counted(drops)
    }
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
