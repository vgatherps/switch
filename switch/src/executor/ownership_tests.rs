use super::*;
use crate::StoredWaker;
use crate::task::header::{DEFERRED, DROP_STARTED, LIVE, QUEUED, TAKEN};
use std::cell::Cell;
use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

#[path = "waker_tests.rs"]
mod waker_tests;

#[path = "registration_tests.rs"]
mod registration_tests;

struct Owner(*mut Executor);

#[test]
fn constructor_reserves_ready_storage_and_uses_initial_timer_tick() {
    let ptr = Box::into_raw(Box::new(Executor::with_capacity(123, 17, 23)));
    let owner = Owner(ptr);
    unsafe {
        assert!((*ptr).run_queue.capacity() >= 17);
        assert!((*ptr).incoming.capacity() >= 17);
        assert!((*ptr).deferred.capacity() >= 17);
        assert_eq!((*ptr).timers.current_tick(), 123);
    }
    owner.cleanup();
}
impl Owner {
    fn new() -> Self {
        Self(Box::into_raw(Box::new(Executor::new())))
    }
    fn spawn<T: Task>(&self, token: SpawnToken<T>) {
        assert!(unsafe { Executor::spawn(self.0, token, None) }.is_ok());
    }
    /// The most recently spawned task (spawn pushes onto `incoming`).
    fn last(&self) -> *const TaskHeader {
        unsafe { *(*self.0).incoming.last().expect("no queued task") }
    }
    fn poll(&self) -> bool {
        unsafe { Executor::poll(self.0) }
    }
    fn counts(&self) -> (usize, usize) {
        unsafe { ((*self.0).task_count, (*self.0).allocated) }
    }
    fn cleanup(&self) {
        unsafe { Executor::cleanup(self.0) }
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.cleanup();
        unsafe {
            drop(Box::from_raw(self.0));
        }
    }
}

/// Task state flags, without the slot-in-use bit.
fn flags(task: *const TaskHeader) -> u8 {
    unsafe { TaskHeader::flags(task).get() & !TAKEN }
}

/// Owner-thread wake of a task by reference (as timer expiry does).
fn wake(task: *const TaskHeader) {
    unsafe { Executor::wake(task) }
}

#[switch::task]
async fn empty() {}

#[switch::task]
async fn count_task(count: Rc<Cell<usize>>) {
    count.set(count.get() + 1);
}

#[switch::task]
fn probe(task: Probe) -> impl Future<Output = ()> {
    task
}

struct Probe {
    polls: Rc<Cell<usize>>,
    drops: Rc<Cell<usize>>,
    wakes: usize,
    pending: bool,
    panic_poll: bool,
    panic_drop: bool,
    wake_drop: bool,
    waker: Option<Waker>,
}
impl Probe {
    fn new() -> Self {
        Self {
            polls: Rc::new(Cell::new(0)),
            drops: Rc::new(Cell::new(0)),
            wakes: 0,
            pending: false,
            panic_poll: false,
            panic_drop: false,
            wake_drop: false,
            waker: None,
        }
    }
}
impl Future for Probe {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.polls.set(self.polls.get() + 1);
        self.waker = Some(cx.waker().clone());
        for _ in 0..self.wakes {
            cx.waker().wake_by_ref();
        }
        if self.panic_poll {
            panic_any(1234usize);
        }
        if self.pending {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}
impl Drop for Probe {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
        if self.wake_drop
            && let Some(waker) = &self.waker
        {
            // Destructor-local wake while the slot is deliberately still owned.
            waker.wake_by_ref();
        }
        if self.panic_drop {
            panic_any(5678usize);
        }
    }
}

#[test]
fn self_wake_ready_retains_header_until_claim_drains() {
    let owner = Owner::new();
    let mut task = Probe::new();
    task.wakes = 3;
    let polls = task.polls.clone();
    let drops = task.drops.clone();
    owner.spawn(probe(task));
    assert!(owner.poll());
    assert_eq!(owner.counts(), (0, 1));
    assert_eq!((polls.get(), drops.get()), (1, 1));
    assert!(owner.poll());
    assert_eq!(owner.counts(), (0, 0));
    assert_eq!((polls.get(), drops.get()), (1, 1));
    assert!(!owner.poll());
}

#[test]
fn idle_future_remains_owned_until_cleanup() {
    let owner = Owner::new();
    let mut task = Probe::new();
    task.pending = true;
    let drops = task.drops.clone();
    owner.spawn(probe(task));
    owner.poll();
    assert_eq!(owner.counts(), (1, 1));
    assert!(!owner.poll());
    owner.cleanup();
    owner.cleanup();
    assert_eq!(owner.counts(), (0, 0));
    assert_eq!(drops.get(), 1);
}

#[test]
fn duplicate_wakes_make_one_next_batch_claim() {
    let owner = Owner::new();
    let mut task = Probe::new();
    task.wakes = 10;
    task.pending = true;
    let polls = task.polls.clone();
    let drops = task.drops.clone();
    owner.spawn(probe(task));
    owner.poll();
    owner.poll();
    assert_eq!(polls.get(), 2);
    unsafe {
        assert_eq!((*owner.0).incoming.len(), 1);
    }
    owner.cleanup();
    assert_eq!(drops.get(), 1);
}

#[test]
fn nested_spawn_grows_both_vectors_without_repolling_current_batch() {
    let owner = Owner::new();
    let ptr = owner.0;
    let inner = Rc::new(Cell::new(0));
    #[switch::task]
    async fn spawn_many(ptr: *mut Executor, inner: Rc<Cell<usize>>) {
        for _ in 0..(DEFAULT_CAPACITY * 3) {
            unsafe {
                assert!(Executor::spawn(ptr, count_task(inner.clone()), None).is_ok());
            }
        }
    }
    owner.spawn(spawn_many(ptr, inner.clone()));
    owner.spawn(empty());
    owner.poll();
    assert_eq!(inner.get(), 0);
    unsafe {
        assert!((*owner.0).run_queue.capacity() >= (*owner.0).allocated);
        assert!((*owner.0).incoming.capacity() >= (*owner.0).allocated);
        assert!((*owner.0).deferred.capacity() >= (*owner.0).allocated);
    }
    owner.poll();
    assert_eq!(inner.get(), DEFAULT_CAPACITY * 3);
    assert_eq!(owner.counts(), (0, 0));
}

#[test]
fn ready_destructor_wake_is_terminal_noop() {
    let owner = Owner::new();
    let mut task = Probe::new();
    task.wake_drop = true;
    let drops = task.drops.clone();
    owner.spawn(probe(task));
    owner.poll();
    assert_eq!(owner.counts(), (0, 0));
    assert!(!owner.poll());
    assert_eq!(drops.get(), 1);
}

#[test]
fn ready_self_wake_destructor_panic_keeps_incoming_header() {
    ready_drop_panic(true);
}
#[test]
fn ready_destructor_panic_without_claim_frees_header() {
    ready_drop_panic(false);
}
fn ready_drop_panic(self_wake: bool) {
    let owner = Owner::new();
    let mut task = Probe::new();
    task.wakes = usize::from(self_wake);
    task.wake_drop = true;
    task.panic_drop = true;
    let drops = task.drops.clone();
    owner.spawn(probe(task));
    let payload = catch_unwind(AssertUnwindSafe(|| owner.poll())).unwrap_err();
    assert_eq!(payload.downcast_ref::<usize>(), Some(&5678));
    assert_eq!(owner.counts(), (0, usize::from(self_wake)));
    unsafe {
        assert_eq!((*owner.0).lifecycle, Lifecycle::Panicked);
        assert!(!(*owner.0).active);
        assert_eq!((*owner.0).incoming.len(), usize::from(self_wake));
        if self_wake {
            assert_ne!(flags((&(*owner.0).incoming)[0]) & DROP_STARTED, 0);
        }
    }
    owner.cleanup();
    assert_eq!(owner.counts(), (0, 0));
    assert_eq!(drops.get(), 1);
}

#[test]
fn failed_poll_preserves_payload_future_and_both_queues() {
    let owner = Owner::new();
    let mut task = Probe::new();
    task.wakes = 1;
    task.panic_poll = true;
    let drops = task.drops.clone();
    let remaining = Rc::new(Cell::new(0));
    owner.spawn(probe(task));
    owner.spawn(count_task(remaining.clone()));
    let payload = catch_unwind(AssertUnwindSafe(|| owner.poll())).unwrap_err();
    assert_eq!(payload.downcast_ref::<usize>(), Some(&1234));
    assert_eq!(owner.counts(), (1, 2));
    assert_eq!(drops.get(), 0);
    unsafe {
        assert_eq!((*owner.0).run_queue.len(), 2);
        assert_eq!((*owner.0).incoming.len(), 1);
        assert_eq!((*owner.0).lifecycle, Lifecycle::Panicked);
    }
    assert!(catch_unwind(AssertUnwindSafe(|| owner.poll())).is_err());
    assert!(catch_unwind(AssertUnwindSafe(|| owner.spawn(empty()))).is_err());
    owner.cleanup();
    assert_eq!(drops.get(), 1);
    assert_eq!(remaining.get(), 0);
    assert_eq!(owner.counts(), (0, 0));
}

#[test]
fn cleanup_resumes_after_one_destructor_panics() {
    let owner = Owner::new();
    let first = Probe::new();
    let first_drops = first.drops.clone();
    owner.spawn(probe(first));
    let mut failing = Probe::new();
    failing.panic_drop = true;
    let failing_drops = failing.drops.clone();
    owner.spawn(probe(failing));
    let payload = catch_unwind(AssertUnwindSafe(|| owner.cleanup())).unwrap_err();
    assert_eq!(payload.downcast_ref::<usize>(), Some(&5678));
    // Drop order is unspecified: the other task is either dropped and freed, or
    // still allocated for the retry.
    assert_eq!(failing_drops.get(), 1);
    assert_eq!(owner.counts(), (0, 1 - first_drops.get()));
    owner.cleanup();
    owner.cleanup();
    assert_eq!(owner.counts(), (0, 0));
    assert_eq!((first_drops.get(), failing_drops.get()), (1, 1));
}

#[test]
fn over_aligned_future_has_exactly_one_drop() {
    #[repr(align(256))]
    struct Aligned(Rc<Cell<usize>>);
    impl Future for Aligned {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            assert_eq!(std::ptr::from_ref(&*self).addr() % 256, 0);
            Poll::Ready(())
        }
    }
    impl Drop for Aligned {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }
    #[switch::task]
    fn aligned(drops: Rc<Cell<usize>>) -> impl Future<Output = ()> {
        Aligned(drops)
    }
    let owner = Owner::new();
    let drops = Rc::new(Cell::new(0));
    owner.spawn(aligned(drops.clone()));
    owner.poll();
    owner.cleanup();
    assert_eq!(drops.get(), 1);
    assert_eq!(owner.counts(), (0, 0));
}

#[test]
fn recursive_kernel_poll_rejects_before_vector_mutation_and_keeps_remainder() {
    struct Recursive {
        root: *mut Executor,
        drops: Rc<Cell<usize>>,
    }
    impl Future for Recursive {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            unsafe { Executor::poll(self.root) };
            Poll::Ready(())
        }
    }
    impl Drop for Recursive {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }
    let owner = Owner::new();
    let drops = Rc::new(Cell::new(0));
    let remaining_polls = Rc::new(Cell::new(0));
    let remaining_drops = Rc::new(Cell::new(0));
    let mut task = Probe::new();
    task.polls = remaining_polls.clone();
    task.drops = remaining_drops.clone();
    #[switch::task]
    fn recursive(task: Recursive) -> impl Future<Output = ()> {
        task
    }
    owner.spawn(recursive(Recursive {
        root: owner.0,
        drops: drops.clone(),
    }));
    owner.spawn(probe(task));
    let (data, incoming) = unsafe { ((*owner.0).run_queue.as_ptr(), (*owner.0).incoming.as_ptr()) };
    let payload = catch_unwind(AssertUnwindSafe(|| owner.poll())).unwrap_err();
    assert_eq!(
        payload.downcast_ref::<&str>().copied(),
        Some("executor reentrancy")
    );
    unsafe {
        assert_eq!((*owner.0).run_queue.as_ptr(), incoming);
        assert_eq!((*owner.0).incoming.as_ptr(), data);
        assert_eq!((*owner.0).run_queue.len(), 2);
        assert!((*owner.0).incoming.is_empty());
        assert_eq!((*owner.0).task_count, 1);
        assert_eq!((*owner.0).allocated, 2);
        assert!(!(*owner.0).active);
        assert_eq!((*owner.0).lifecycle, Lifecycle::Panicked);
    }
    assert_eq!(
        (drops.get(), remaining_polls.get(), remaining_drops.get()),
        (0, 0, 0)
    );
    owner.cleanup();
    assert_eq!(
        (drops.get(), remaining_polls.get(), remaining_drops.get()),
        (1, 0, 1)
    );
}

#[test]
fn waking_another_current_batch_task_does_not_add_a_next_batch_claim() {
    let owner = Owner::new();
    #[switch::task]
    async fn wake_target(target: Rc<Cell<Option<*const TaskHeader>>>) {
        wake(target.get().unwrap());
    }
    let target = Rc::new(Cell::new(None));
    owner.spawn(wake_target(target.clone()));
    let mut task = Probe::new();
    task.pending = true;
    let polls = task.polls.clone();
    let drops = task.drops.clone();
    owner.spawn(probe(task));
    target.set(Some(owner.last()));
    owner.poll();
    assert_eq!(polls.get(), 1);
    assert_eq!(owner.counts(), (1, 1));
    assert!(!owner.poll());
    assert_eq!(polls.get(), 1);
    unsafe {
        assert!((*owner.0).incoming.is_empty());
    }
    assert_eq!(flags(target.get().unwrap()), LIVE);
    owner.cleanup();
    assert_eq!(drops.get(), 1);
}

#[test]
fn ready_destructor_spawns_and_wakes_an_idle_task_during_reclamation() {
    destructor_spawn_and_wake(true, false);
    destructor_spawn_and_wake(true, true);
}

#[test]
fn ready_destructor_spawns_and_wakes_a_current_batch_task_during_reclamation() {
    destructor_spawn_and_wake(false, false);
    destructor_spawn_and_wake(false, true);
}

fn destructor_spawn_and_wake(target_idle: bool, self_wake: bool) {
    struct SpawnDuringDrop {
        owner: *mut Executor,
        target: Rc<Cell<Option<*const TaskHeader>>>,
        completed: Rc<Cell<usize>>,
        drops: Rc<Cell<usize>>,
        self_wake: bool,
    }
    impl Future for SpawnDuringDrop {
        type Output = ();
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.self_wake {
                cx.waker().wake_by_ref();
            }
            Poll::Ready(())
        }
    }
    impl Drop for SpawnDuringDrop {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
            for _ in 0..(DEFAULT_CAPACITY * 3) {
                unsafe {
                    assert!(
                        Executor::spawn(self.owner, count_task(self.completed.clone()), None)
                            .is_ok()
                    );
                }
            }
            // Target slot remains live whether idle or in this batch.
            wake(self.target.get().unwrap());
        }
    }
    #[switch::task]
    fn spawn_during_drop(task: SpawnDuringDrop) -> impl Future<Output = ()> {
        task
    }
    let owner = Owner::new();
    let target = Rc::new(Cell::new(None));
    let completed = Rc::new(Cell::new(0));
    let drops = Rc::new(Cell::new(0));
    let drop_spawner = SpawnDuringDrop {
        owner: owner.0,
        target: target.clone(),
        completed: completed.clone(),
        drops: drops.clone(),
        self_wake,
    };
    let mut task = Probe::new();
    task.pending = true;
    let target_polls = task.polls.clone();
    let target_drops = task.drops.clone();
    if target_idle {
        owner.spawn(probe(task));
        target.set(Some(owner.last()));
        owner.poll();
        owner.spawn(spawn_during_drop(drop_spawner));
    } else {
        owner.spawn(spawn_during_drop(drop_spawner));
        owner.spawn(probe(task));
        target.set(Some(owner.last()));
    }
    owner.poll();
    assert_eq!(drops.get(), 1);
    assert_eq!(completed.get(), 0);
    assert_eq!(target_polls.get(), 1);
    assert_eq!(
        owner.counts(),
        (
            DEFAULT_CAPACITY * 3 + 1,
            DEFAULT_CAPACITY * 3 + 1 + usize::from(self_wake)
        )
    );
    unsafe {
        assert_eq!(
            (*owner.0).incoming.len(),
            DEFAULT_CAPACITY * 3 + usize::from(target_idle) + usize::from(self_wake)
        );
        assert!((*owner.0).run_queue.capacity() >= (*owner.0).allocated);
        assert!((*owner.0).incoming.capacity() >= (*owner.0).allocated);
        assert!((*owner.0).deferred.capacity() >= (*owner.0).allocated);
    }
    owner.poll();
    assert_eq!(completed.get(), DEFAULT_CAPACITY * 3);
    assert_eq!(target_polls.get(), 1 + usize::from(target_idle));
    assert_eq!(owner.counts(), (1, 1));
    owner.cleanup();
    assert_eq!((drops.get(), target_drops.get()), (1, 1));
}

#[test]
fn middle_batch_panic_allows_only_cleanup_with_exact_drops() {
    for preceding_self_wake in [false, true] {
        let owner = Owner::new();
        let mut preceding = Probe::new();
        preceding.wakes = usize::from(preceding_self_wake);
        let preceding_polls = preceding.polls.clone();
        let preceding_drops = preceding.drops.clone();
        let mut interrupted = Probe::new();
        interrupted.wakes = 1;
        interrupted.panic_poll = true;
        let interrupted_polls = interrupted.polls.clone();
        let interrupted_drops = interrupted.drops.clone();
        let unprocessed = [Probe::new(), Probe::new()];
        let unprocessed_polls = unprocessed.each_ref().map(|task| task.polls.clone());
        let unprocessed_drops = unprocessed.each_ref().map(|task| task.drops.clone());
        owner.spawn(probe(preceding));
        owner.spawn(probe(interrupted));
        for task in unprocessed {
            owner.spawn(probe(task));
        }
        let payload = catch_unwind(AssertUnwindSafe(|| owner.poll())).unwrap_err();
        assert_eq!(payload.downcast_ref::<usize>(), Some(&1234));
        assert_eq!((preceding_polls.get(), preceding_drops.get()), (1, 1));
        assert_eq!((interrupted_polls.get(), interrupted_drops.get()), (1, 0));
        assert_eq!(
            unprocessed_polls.each_ref().map(|count| count.get()),
            [0, 0]
        );
        assert_eq!(
            unprocessed_drops.each_ref().map(|count| count.get()),
            [0, 0]
        );
        assert_eq!(owner.counts(), (2, 3 + usize::from(preceding_self_wake)));
        unsafe {
            assert_eq!((*owner.0).run_queue.len(), 4);
            assert_eq!(
                (*owner.0).incoming.len(),
                1 + usize::from(preceding_self_wake)
            );
            assert_eq!((*owner.0).lifecycle, Lifecycle::Panicked);
            assert!(!(*owner.0).active);
        }
        assert!(catch_unwind(AssertUnwindSafe(|| owner.poll())).is_err());
        assert!(catch_unwind(AssertUnwindSafe(|| owner.spawn(empty()))).is_err());
        owner.cleanup();
        owner.cleanup();
        assert_eq!(owner.counts(), (0, 0));
        assert_eq!((preceding_polls.get(), preceding_drops.get()), (1, 1));
        assert_eq!((interrupted_polls.get(), interrupted_drops.get()), (1, 1));
        assert_eq!(
            unprocessed_polls.each_ref().map(|count| count.get()),
            [0, 0]
        );
        assert_eq!(
            unprocessed_drops.each_ref().map(|count| count.get()),
            [1, 1]
        );
    }
}

#[test]
fn active_selfwake_and_cross_task_wakes_keep_batch_order_and_claims() {
    struct Ordered {
        id: usize,
        targets: Rc<[Cell<Option<*const TaskHeader>>; 3]>,
        trace: Rc<std::cell::RefCell<Vec<usize>>>,
        drops: Rc<[Cell<usize>; 3]>,
        first_poll: bool,
    }
    impl Future for Ordered {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            self.trace.borrow_mut().push(self.id);
            if !self.first_poll {
                return Poll::Ready(());
            }
            self.first_poll = false;
            if self.id == 0 || self.id == 2 {
                for _ in 0..8 {
                    cx.waker().wake_by_ref();
                }
            }
            if self.id == 0 || self.id == 2 {
                // A wakes queued B before its poll; C wakes idle B after its poll.
                for _ in 0..8 {
                    wake(self.targets[1].get().unwrap());
                }
            }
            if self.id == 2 {
                for _ in 0..8 {
                    wake(self.targets[0].get().unwrap());
                }
            }
            Poll::Pending
        }
    }
    impl Drop for Ordered {
        fn drop(&mut self) {
            self.drops[self.id].set(self.drops[self.id].get() + 1);
        }
    }
    #[switch::task]
    fn ordered(task: Ordered) -> impl Future<Output = ()> {
        task
    }
    let owner = Owner::new();
    let targets = Rc::new(std::array::from_fn(|_| Cell::new(None)));
    let trace = Rc::new(std::cell::RefCell::new(Vec::new()));
    let drops = Rc::new(std::array::from_fn(|_| Cell::new(0)));
    for id in 0..3 {
        owner.spawn(ordered(Ordered {
            id,
            targets: targets.clone(),
            trace: trace.clone(),
            drops: drops.clone(),
            first_poll: true,
        }));
        targets[id].set(Some(owner.last()));
    }
    owner.poll();
    assert_eq!(*trace.borrow(), [0, 1, 2]);
    assert_eq!(owner.counts(), (3, 3));
    let target = |id: usize| targets[id].get().unwrap();
    unsafe {
        assert_eq!(
            (*owner.0).incoming.as_slice(),
            &[target(0), target(2), target(1)]
        );
    }
    for id in 0..3 {
        assert_eq!(flags(target(id)), LIVE | QUEUED);
    }
    owner.poll();
    assert_eq!(*trace.borrow(), [0, 1, 2, 0, 2, 1]);
    assert_eq!(owner.counts(), (0, 0));
    assert!(!owner.poll());
    assert_eq!(drops.each_ref().map(|count| count.get()), [1, 1, 1]);
}

#[test]
fn multiple_selfwakes_then_panic_keep_one_claim_and_original_payload() {
    let owner = Owner::new();
    let mut task = Probe::new();
    task.wakes = 8;
    task.panic_poll = true;
    let polls = task.polls.clone();
    let drops = task.drops.clone();
    let remainder = Probe::new();
    let remainder_polls = remainder.polls.clone();
    let remainder_drops = remainder.drops.clone();
    owner.spawn(probe(task));
    owner.spawn(probe(remainder));
    let payload = catch_unwind(AssertUnwindSafe(|| owner.poll())).unwrap_err();
    assert_eq!(payload.downcast_ref::<usize>(), Some(&1234));
    assert_eq!(owner.counts(), (1, 2));
    assert_eq!(
        (
            polls.get(),
            drops.get(),
            remainder_polls.get(),
            remainder_drops.get()
        ),
        (1, 0, 0, 0)
    );
    unsafe {
        assert_eq!((*owner.0).incoming.len(), 1);
        assert_eq!(flags((&(*owner.0).incoming)[0]) & LIVE, 0);
    }
    assert!(catch_unwind(AssertUnwindSafe(|| owner.poll())).is_err());
    owner.cleanup();
    assert_eq!(owner.counts(), (0, 0));
    assert_eq!(
        (
            polls.get(),
            drops.get(),
            remainder_polls.get(),
            remainder_drops.get()
        ),
        (1, 1, 0, 1)
    );
}

#[switch::task]
fn rejected(task: RejectedWakeOnDrop) -> impl Future<Output = ()> {
    task
}

struct RejectedWakeOnDrop {
    target: *const TaskHeader,
    drops: Rc<Cell<usize>>,
}
impl Future for RejectedWakeOnDrop {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        Poll::Ready(())
    }
}
impl Drop for RejectedWakeOnDrop {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
        // Target remains allocated while rejected spawn destroys its argument.
        wake(self.target);
    }
}

#[test]
fn panicked_lifecycle_rejects_work_and_queues_destructor_wake_to_idle_target() {
    let owner = Owner::new();
    let mut idle = Probe::new();
    idle.pending = true;
    idle.wake_drop = true;
    let idle_drops = idle.drops.clone();
    let idle_polls = idle.polls.clone();
    owner.spawn(probe(idle));
    let target = owner.last();
    owner.poll();
    let mut failing = Probe::new();
    failing.panic_poll = true;
    failing.wake_drop = true;
    let failing_drops = failing.drops.clone();
    owner.spawn(probe(failing));
    let payload = catch_unwind(AssertUnwindSafe(|| owner.poll())).unwrap_err();
    assert_eq!(payload.downcast_ref::<usize>(), Some(&1234));
    unsafe {
        assert_eq!((*owner.0).lifecycle, Lifecycle::Panicked);
    }
    assert!(catch_unwind(AssertUnwindSafe(|| owner.poll())).is_err());
    let rejected_drops = Rc::new(Cell::new(0));
    assert!(
        catch_unwind(AssertUnwindSafe(|| owner.spawn(rejected(
            RejectedWakeOnDrop {
                target,
                drops: rejected_drops.clone()
            }
        ))))
        .is_err()
    );
    assert_eq!(rejected_drops.get(), 1);
    // The executor takes the wake, but never polls again.
    unsafe {
        assert_eq!(*(*owner.0).incoming, [target]);
        assert_eq!((*owner.0).lifecycle, Lifecycle::Panicked);
    }
    assert_eq!(flags(target), LIVE | QUEUED);
    assert!(catch_unwind(AssertUnwindSafe(|| owner.poll())).is_err());
    assert_eq!(idle_polls.get(), 1);
    assert_eq!(owner.counts(), (1, 2));
    owner.cleanup();
    owner.cleanup();
    assert_eq!(owner.counts(), (0, 0));
    assert_eq!((idle_drops.get(), failing_drops.get()), (1, 1));
    unsafe {
        assert_eq!((*owner.0).lifecycle, Lifecycle::Closing);
    }
}

#[test]
fn closing_lifecycle_survives_destructor_panic_and_cleanup_retry() {
    struct DuringCleanup {
        owner: *mut Executor,
        target: *const TaskHeader,
        drops: Rc<Cell<usize>>,
        rejected_drops: Rc<Cell<usize>>,
    }
    impl Future for DuringCleanup {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            Poll::Pending
        }
    }
    impl Drop for DuringCleanup {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
            unsafe {
                assert_eq!((*self.owner).lifecycle, Lifecycle::Closing);
                assert!(catch_unwind(AssertUnwindSafe(|| Executor::poll(self.owner))).is_err());
                assert!(
                    catch_unwind(AssertUnwindSafe(|| Executor::spawn(
                        self.owner,
                        rejected(RejectedWakeOnDrop {
                            target: self.target,
                            drops: self.rejected_drops.clone()
                        }),
                        None
                    )))
                    .is_err()
                );
                wake(self.target);
                assert!((*self.owner).incoming.is_empty());
            }
            panic_any(5678usize);
        }
    }
    let owner = Owner::new();
    let mut target = Probe::new();
    target.pending = true;
    target.wake_drop = true;
    let target_drops = target.drops.clone();
    owner.spawn(probe(target));
    let target = owner.last();
    owner.poll();
    let drops = Rc::new(Cell::new(0));
    let rejected_drops = Rc::new(Cell::new(0));
    #[switch::task]
    fn during_cleanup(task: DuringCleanup) -> impl Future<Output = ()> {
        task
    }
    owner.spawn(during_cleanup(DuringCleanup {
        owner: owner.0,
        target,
        drops: drops.clone(),
        rejected_drops: rejected_drops.clone(),
    }));
    let payload = catch_unwind(AssertUnwindSafe(|| owner.cleanup())).unwrap_err();
    assert_eq!(payload.downcast_ref::<usize>(), Some(&5678));
    // Drop order is unspecified: the target is either dropped and freed, or
    // still allocated for the retry.
    assert_eq!((drops.get(), rejected_drops.get()), (1, 1));
    assert_eq!(owner.counts(), (0, 1 - target_drops.get()));
    unsafe {
        assert_eq!((*owner.0).lifecycle, Lifecycle::Closing);
        assert!(!(*owner.0).active);
    }
    assert!(catch_unwind(AssertUnwindSafe(|| owner.poll())).is_err());
    assert!(
        catch_unwind(AssertUnwindSafe(|| owner.spawn(rejected(
            RejectedWakeOnDrop {
                target,
                drops: rejected_drops.clone()
            }
        ))))
        .is_err()
    );
    unsafe {
        assert!((*owner.0).incoming.is_empty());
    }
    assert_eq!(flags(target) & QUEUED, 0);
    owner.cleanup();
    owner.cleanup();
    assert_eq!(owner.counts(), (0, 0));
    assert_eq!(
        (drops.get(), rejected_drops.get(), target_drops.get()),
        (1, 2, 1)
    );
    unsafe {
        assert_eq!((*owner.0).lifecycle, Lifecycle::Closing);
    }
}

/// Parks forever after publishing its `StoredWaker`.
struct StoredPark {
    stored: Rc<Cell<Option<StoredWaker>>>,
    polls: Rc<Cell<usize>>,
    drops: Rc<Cell<usize>>,
}
impl StoredPark {
    fn new() -> Self {
        Self {
            stored: Rc::default(),
            polls: Rc::default(),
            drops: Rc::default(),
        }
    }
}
impl Future for StoredPark {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.polls.set(self.polls.get() + 1);
        self.stored.set(Some(StoredWaker::new(cx)));
        Poll::Pending
    }
}
impl Drop for StoredPark {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

#[switch::task]
fn stored_park(task: StoredPark) -> impl Future<Output = ()> {
    task
}

#[test]
fn stored_wakes_in_a_panicked_executor_queue_and_never_poll() {
    stored_wakes_after_open(Lifecycle::Panicked);
}

#[test]
fn stored_wakes_in_a_closing_executor_queue_and_never_poll() {
    stored_wakes_after_open(Lifecycle::Closing);
}

/// `wake`, `wake_deferred` and an upgrade still queue a LIVE task once the
/// executor left Open; it is never polled again, and cleanup drops it once.
fn stored_wakes_after_open(lifecycle: Lifecycle) {
    let owner = Owner::new();
    let (eager, deferred) = (StoredPark::new(), StoredPark::new());
    let stored = [eager.stored.clone(), deferred.stored.clone()];
    let polls = [eager.polls.clone(), deferred.polls.clone()];
    let drops = [eager.drops.clone(), deferred.drops.clone()];
    owner.spawn(stored_park(eager));
    let eager_header = owner.last();
    owner.spawn(stored_park(deferred));
    let deferred_header = owner.last();
    owner.poll();
    match lifecycle {
        Lifecycle::Panicked => {
            let mut failing = Probe::new();
            failing.panic_poll = true;
            owner.spawn(probe(failing));
            assert!(catch_unwind(AssertUnwindSafe(|| owner.poll())).is_err());
        }
        Lifecycle::Closing => unsafe { Executor::close(owner.0) },
        Lifecycle::Open => unreachable!(),
    }
    unsafe {
        assert_eq!((*owner.0).lifecycle, lifecycle);
    }
    let [eager_waker, deferred_waker] = stored.map(|stored| stored.get().unwrap());
    eager_waker.wake();
    deferred_waker.wake_deferred();
    assert_eq!(flags(eager_header), LIVE | QUEUED);
    assert_eq!(flags(deferred_header), LIVE | QUEUED | DEFERRED);
    unsafe {
        assert_eq!(*(*owner.0).incoming, [eager_header]);
        assert_eq!(*(*owner.0).deferred, [deferred_header]);
    }
    deferred_waker.wake();
    assert_eq!(flags(deferred_header), LIVE | QUEUED);
    unsafe {
        assert_eq!(*(*owner.0).incoming, [eager_header, deferred_header]);
        assert!((*owner.0).deferred.is_empty());
    }
    assert!(catch_unwind(AssertUnwindSafe(|| owner.poll())).is_err());
    assert_eq!(polls.each_ref().map(|polls| polls.get()), [1, 1]);
    owner.cleanup();
    assert_eq!(owner.counts(), (0, 0));
    assert_eq!(drops.each_ref().map(|drops| drops.get()), [1, 1]);
    unsafe {
        assert!((*owner.0).incoming.is_empty() && (*owner.0).deferred.is_empty());
    }
}

/// The longest the wake queues get: every slot in use has an entry, one of them
/// the self-wake of a finished task, whose slot stays in use until it drains.
#[test]
fn wake_queues_hold_at_most_one_entry_per_slot_in_use() {
    let owner = Owner(Box::into_raw(Box::new(Executor::with_capacity(0, 0, 0))));
    let mut finishing = Probe::new();
    finishing.wakes = 1;
    owner.spawn(probe(finishing));
    let stale = owner.last();
    let parked: Vec<StoredPark> = (0..4).map(|_| StoredPark::new()).collect();
    let stored: Vec<_> = parked.iter().map(|task| task.stored.clone()).collect();
    for task in parked {
        owner.spawn(stored_park(task));
    }
    owner.poll();
    assert_eq!(flags(stale) & (LIVE | QUEUED), QUEUED);
    unsafe {
        assert_eq!(*(*owner.0).incoming, [stale]);
    }
    owner.spawn(probe(Probe::new()));
    assert_ne!(owner.last(), stale);
    assert_eq!(owner.counts(), (5, 6));
    for (index, stored) in stored.iter().enumerate() {
        let waker = stored.get().unwrap();
        if index % 2 == 0 {
            waker.wake();
        } else {
            waker.wake_deferred();
        }
    }
    unsafe {
        let (incoming, deferred) = ((*owner.0).incoming.len(), (*owner.0).deferred.len());
        assert_eq!((incoming, deferred), (4, 2));
        assert_eq!(incoming + deferred, (*owner.0).allocated);
        assert!(incoming <= (*owner.0).incoming.capacity());
        assert!(deferred <= (*owner.0).deferred.capacity());
    }
    // The stale entry drains and frees its slot; the new task completes.
    owner.poll();
    assert_eq!(owner.counts(), (4, 4));
}
