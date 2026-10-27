use super::*;
use std::cell::Cell;
use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

#[path = "clock_tests.rs"]
mod clock_tests;

task! {
    async fn empty() {}
}

task! {
    fn counted(task: Counted) -> impl Future<Output = ()> {
        task
    }
}

task! {
    async fn count_task(count: Rc<Cell<usize>>) {
        count.set(count.get() + 1);
    }
}

enum Finish {
    Ready,
    Pending,
    Panic,
}

struct Counted {
    polls: Rc<Cell<usize>>,
    drops: Rc<Cell<usize>>,
    finish: Finish,
    panic_drop: bool,
    held_spawner: Option<Spawner>,
}
impl Counted {
    fn new(finish: Finish) -> Self {
        Self {
            polls: Rc::new(Cell::new(0)),
            drops: Rc::new(Cell::new(0)),
            finish,
            panic_drop: false,
            held_spawner: None,
        }
    }
}
impl Future for Counted {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        self.polls.set(self.polls.get() + 1);
        match self.finish {
            Finish::Ready => Poll::Ready(()),
            Finish::Pending => Poll::Pending,
            Finish::Panic => panic_any(1234usize),
        }
    }
}
impl Drop for Counted {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
        if let Some(spawner) = &self.held_spawner {
            assert_eq!(
                spawner.spawn(empty()).unwrap_err().reason,
                RuntimeError::Closed
            );
        }
        if self.panic_drop {
            panic_any(5678usize);
        }
    }
}

fn core(runtime: &Runtime) -> *mut Executor {
    runtime.control.open_core().unwrap()
}

#[test]
fn moving_owner_with_live_task_keeps_root_stable() {
    let runtime = Runtime::new(123);
    let spawner = runtime.spawner();
    let task = Counted::new(Finish::Ready);
    let polls = task.polls.clone();
    let drops = task.drops.clone();
    spawner.spawn(counted(task)).unwrap();
    let root = core(&runtime);
    let mut runtime = Box::new(runtime);
    assert_eq!(core(&runtime), root);
    assert_eq!(runtime.poll(123).unwrap().tasks_polled, 1);
    assert_eq!((polls.get(), drops.get()), (1, 1));
    drop(runtime);
    assert_eq!(
        spawner.spawn(empty()).unwrap_err().reason,
        RuntimeError::Closed
    );
}

#[test]
fn two_owners_have_independent_tasks_and_teardown() {
    let mut a = Runtime::new(0);
    let mut b = Runtime::new(u64::MAX);
    let sa = a.spawner();
    let sb = b.spawner();
    let task_a = Counted::new(Finish::Ready);
    let task_b = Counted::new(Finish::Ready);
    let ran = [task_a.polls.clone(), task_b.polls.clone()];
    sa.spawn(counted(task_a)).unwrap();
    sb.spawn(counted(task_b)).unwrap();
    assert_ne!(core(&a), core(&b));
    assert_eq!(a.poll(0).unwrap().tasks_polled, 1);
    assert_eq!((ran[0].get(), ran[1].get()), (1, 0));
    a.shutdown();
    assert_eq!(sa.spawn(empty()).unwrap_err().reason, RuntimeError::Closed);
    assert_eq!(b.poll(u64::MAX).unwrap().tasks_polled, 1);
    assert_eq!((ran[0].get(), ran[1].get()), (1, 1));
}

#[test]
fn explicit_shutdown_is_idempotent_and_escaped_spawner_stays_closed() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    spawner.spawn(empty()).unwrap(); // async blocks need no Debug implementation.
    runtime.shutdown();
    runtime.shutdown();
    assert_eq!(
        spawner.spawn(empty()).unwrap_err().reason,
        RuntimeError::Closed
    );
    drop(runtime);
    assert_eq!(
        spawner.spawn(empty()).unwrap_err().reason,
        RuntimeError::Closed
    );
}

#[test]
fn rejected_future_is_returned_without_hidden_drop() {
    struct Identified {
        _identity: Rc<()>,
        drops: Rc<Cell<usize>>,
    }
    impl Future for Identified {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            Poll::Ready(())
        }
    }
    impl Drop for Identified {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    runtime.shutdown();
    let identity = Rc::new(());
    let drops = Rc::new(Cell::new(0));
    task! {
        fn identified(task: Identified) -> impl Future<Output = ()> {
            task
        }
    }
    let error = spawner
        .spawn(identified(Identified {
            _identity: identity.clone(),
            drops: drops.clone(),
        }))
        .unwrap_err();
    assert_eq!(error.reason, RuntimeError::Closed);
    // The returned token still holds the original argument.
    assert_eq!(Rc::strong_count(&identity), 2);
    assert_eq!(drops.get(), 0);
    assert!(format!("{error:?}").contains("Closed"));
    drop(error.token);
    assert_eq!(drops.get(), 1);
    assert_eq!(Rc::strong_count(&identity), 1);
}

#[test]
fn panicked_core_rejects_submission_and_still_shuts_down() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let failing = Counted::new(Finish::Panic);
    let drops = failing.drops.clone();
    spawner.spawn(counted(failing)).unwrap();
    let payload = catch_unwind(AssertUnwindSafe(|| runtime.poll(0))).unwrap_err();
    assert_eq!(payload.downcast_ref::<usize>(), Some(&1234));
    let rejected = Counted::new(Finish::Ready);
    let rejected_drops = rejected.drops.clone();
    let error = spawner.spawn(counted(rejected)).unwrap_err();
    assert_eq!(error.reason, RuntimeError::Panicked);
    assert_eq!(rejected_drops.get(), 0);
    drop(error.token);
    assert_eq!(rejected_drops.get(), 1);
    runtime.shutdown();
    assert_eq!(drops.get(), 1);
    assert_eq!(
        spawner.spawn(empty()).unwrap_err().reason,
        RuntimeError::Closed
    );
}

#[test]
fn nested_spawner_grows_queues_without_repolling_current_batch() {
    let mut runtime = Runtime::with_capacity(
        9,
        Capacity {
            tasks: 0,
            timers: 0,
        },
    );
    let spawner = runtime.spawner();
    task! {
        async fn spawn_many(spawner: Spawner, count: Rc<Cell<usize>>) {
            for _ in 0..(DEFAULT_CAPACITY * 3) {
                spawner.spawn(count_task(count.clone())).unwrap();
            }
        }
    }
    let count = Rc::new(Cell::new(0));
    spawner
        .spawn(spawn_many(spawner.clone(), count.clone()))
        .unwrap();
    spawner.spawn(empty()).unwrap();
    let root = core(&runtime);
    assert_eq!(runtime.poll(9).unwrap().tasks_polled, 2);
    assert_eq!(count.get(), 0);
    unsafe {
        assert_eq!(Executor::task_count(root), DEFAULT_CAPACITY * 3);
    }
    assert_eq!(runtime.poll(9).unwrap().tasks_polled, DEFAULT_CAPACITY * 3);
    assert_eq!(count.get(), DEFAULT_CAPACITY * 3);
    unsafe {
        assert_eq!(Executor::task_count(root), 0);
    }
}

#[test]
fn woken_and_spawned_tasks_poll_in_the_same_turn() {
    struct Parked {
        wake: Rc<Cell<Option<Waker>>>,
        polls: Rc<Cell<usize>>,
    }
    impl Future for Parked {
        type Output = ();
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            self.polls.set(self.polls.get() + 1);
            if self.polls.get() == 1 {
                self.wake.set(Some(cx.waker().clone()));
                Poll::Pending
            } else {
                self.wake.take();
                Poll::Ready(())
            }
        }
    }

    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let wake = Rc::new(Cell::new(None));
    let parked_polls = Rc::new(Cell::new(0));
    task! {
        fn parked(task: Parked) -> impl Future<Output = ()> {
            task
        }
    }
    spawner
        .spawn(parked(Parked {
            wake: wake.clone(),
            polls: parked_polls.clone(),
        }))
        .unwrap();
    assert_eq!(runtime.poll(0).unwrap().tasks_polled, 1);
    wake.take().unwrap().wake();
    let woken = runtime.poll(0).unwrap();
    assert_eq!((woken.tasks_polled, woken.tasks_completed), (1, 1));
    assert_eq!(parked_polls.get(), 2);

    let spawned_polls = Rc::new(Cell::new(0));
    spawner.spawn(count_task(spawned_polls.clone())).unwrap();
    let spawned = runtime.poll(0).unwrap();
    assert_eq!((spawned.tasks_polled, spawned.tasks_completed), (1, 1));
    assert_eq!(spawned_polls.get(), 1);
}

#[test]
fn idle_tasks_remain_owned_and_task_held_control_cycle_is_broken() {
    let mut runtime = Runtime::new(0);
    let weak = Rc::downgrade(&runtime.control);
    let mut idle = Counted::new(Finish::Pending);
    idle.held_spawner = Some(runtime.spawner());
    let drops = idle.drops.clone();
    let polls = idle.polls.clone();
    runtime.spawner().spawn(counted(idle)).unwrap();
    assert_eq!(runtime.poll(0).unwrap().tasks_polled, 1);
    assert_eq!(runtime.poll(0).unwrap().tasks_polled, 0);
    assert_eq!((polls.get(), drops.get()), (1, 0));
    runtime.shutdown();
    assert_eq!(drops.get(), 1);
    assert!(weak.upgrade().is_some());
    drop(runtime);
    assert!(weak.upgrade().is_none());
}

#[test]
fn explicit_shutdown_resumes_after_original_destructor_panic() {
    let mut runtime = Runtime::new(0);
    let survivor = Counted::new(Finish::Pending);
    let mut failing = Counted::new(Finish::Pending);
    failing.panic_drop = true;
    let drops = [survivor.drops.clone(), failing.drops.clone()];
    let spawner = runtime.spawner();
    spawner.spawn(counted(survivor)).unwrap();
    spawner.spawn(counted(failing)).unwrap();
    let root = core(&runtime);
    let payload = catch_unwind(AssertUnwindSafe(|| runtime.shutdown())).unwrap_err();
    assert_eq!(payload.downcast_ref::<usize>(), Some(&5678));
    assert_eq!((drops[0].get(), drops[1].get()), (0, 1));
    assert_eq!(runtime.control.root.get(), root);
    assert_eq!(
        spawner.spawn(empty()).unwrap_err().reason,
        RuntimeError::Closed
    );
    runtime.shutdown();
    runtime.shutdown();
    assert!(runtime.control.root.get().is_null());
    assert_eq!((drops[0].get(), drops[1].get()), (1, 1));
}

#[test]
fn owner_drop_continues_cleanup_and_preserves_original_payload() {
    let runtime = Runtime::new(0);
    let escaped = runtime.spawner();
    let mut survivor = Counted::new(Finish::Pending);
    survivor.held_spawner = Some(escaped.clone());
    let mut failing = Counted::new(Finish::Pending);
    failing.panic_drop = true;
    let drops = [survivor.drops.clone(), failing.drops.clone()];
    escaped.spawn(counted(survivor)).unwrap();
    escaped.spawn(counted(failing)).unwrap();
    let payload = catch_unwind(AssertUnwindSafe(|| drop(runtime))).unwrap_err();
    assert_eq!(payload.downcast_ref::<usize>(), Some(&5678));
    assert_eq!((drops[0].get(), drops[1].get()), (1, 1));
    assert!(escaped.control.root.get().is_null());
    assert_eq!(
        escaped.spawn(empty()).unwrap_err().reason,
        RuntimeError::Closed
    );
}

#[test]
fn owner_drop_during_existing_unwind_cleans_nonpanicking_futures() {
    let mut runtime = Runtime::new(0);
    let escaped = runtime.spawner();
    let mut task = Counted::new(Finish::Pending);
    task.held_spawner = Some(escaped.clone());
    let drops = task.drops.clone();
    escaped.spawn(counted(task)).unwrap();
    assert_eq!(runtime.poll(0).unwrap().tasks_polled, 1);
    let payload = catch_unwind(AssertUnwindSafe(move || {
        let _owner = runtime;
        panic_any(999usize);
    }))
    .unwrap_err();
    assert_eq!(payload.downcast_ref::<usize>(), Some(&999));
    assert_eq!(drops.get(), 1);
    assert!(escaped.control.root.get().is_null());
    assert_eq!(
        escaped.spawn(empty()).unwrap_err().reason,
        RuntimeError::Closed
    );
}
