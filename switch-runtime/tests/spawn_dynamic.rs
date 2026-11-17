//! `Spawner::spawn_dynamic`, through real switch tasks.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::rc::Rc;

use switch_runtime::oneshot::{self, Canceled};
use switch_runtime::{Runtime, RuntimeError, yield_now};

type Slot<T> = Rc<RefCell<Option<T>>>;

fn slot<T>() -> Slot<T> {
    Rc::new(RefCell::new(None))
}

/// Awaits `future` and stores its output.
#[switch_runtime::task]
async fn store<F>(future: F, out: Slot<F::Output>)
where
    F: Future + 'static,
    F::Output: 'static,
{
    *out.borrow_mut() = Some(future.await);
}

/// Counts its drops.
struct Counted(Rc<Cell<u32>>);

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

fn run(runtime: &mut Runtime) {
    for _ in 0..4 {
        runtime.poll(0).unwrap();
    }
}

#[test]
fn handle_resolves_to_output_of_non_send_future() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let shared = Rc::new(Cell::new(20u32));
    let captured = shared.clone();
    let handle = spawner
        .spawn_dynamic(async move {
            yield_now().await;
            captured.get() + 1
        })
        .unwrap();
    let out = slot();
    spawner.spawn(store(handle, out.clone())).unwrap();
    run(&mut runtime);
    assert_eq!(*out.borrow(), Some(Ok(21)));
    assert_eq!(Rc::strong_count(&shared), 1);
}

#[test]
fn different_output_types_resolve_in_one_runtime() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let number = spawner.spawn_dynamic(async { 7u64 }).unwrap();
    let text = spawner.spawn_dynamic(async { String::from("seven") }).unwrap();
    let number_out = slot();
    let text_out = slot();
    spawner.spawn(store(number, number_out.clone())).unwrap();
    spawner.spawn(store(text, text_out.clone())).unwrap();
    run(&mut runtime);
    assert_eq!(*number_out.borrow(), Some(Ok(7u64)));
    assert_eq!(*text_out.borrow(), Some(Ok(String::from("seven"))));
}

#[test]
fn future_first_runs_in_the_next_poll() {
    let mut runtime = Runtime::new(0);
    let polled = Rc::new(Cell::new(false));
    let flag = polled.clone();
    let _handle = runtime
        .spawner()
        .spawn_dynamic(async move { flag.set(true) })
        .unwrap();
    assert!(!polled.get());
    runtime.poll(0).unwrap();
    assert!(polled.get());
}

#[test]
fn future_larger_than_a_task_block_completes() {
    const SIZE: usize = 128 * 1024;
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let handle = spawner
        .spawn_dynamic(async {
            let mut buffer = [0u8; SIZE];
            buffer[SIZE - 1] = 9;
            yield_now().await;
            std::hint::black_box(&mut buffer);
            buffer[SIZE - 1]
        })
        .unwrap();
    let out = slot();
    spawner.spawn(store(handle, out.clone())).unwrap();
    run(&mut runtime);
    assert_eq!(*out.borrow(), Some(Ok(9u8)));
    assert_eq!(runtime.poll(0).unwrap().live_tasks, 0);
}

#[test]
fn spawn_on_closed_runtime_keeps_the_future_unpolled_in_the_error() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    runtime.shutdown();
    let drops = Rc::new(Cell::new(0));
    let polled = Rc::new(Cell::new(false));
    let counted = Counted(drops.clone());
    let flag = polled.clone();
    let error = spawner
        .spawn_dynamic(async move {
            flag.set(true);
            drop(counted);
        })
        .unwrap_err();
    assert_eq!(error.reason, RuntimeError::Closed);
    assert_eq!(drops.get(), 0);
    drop(error);
    assert_eq!(drops.get(), 1);
    assert!(!polled.get());
}

#[test]
fn token_from_a_refused_spawn_runs_on_another_runtime() {
    let mut closed = Runtime::new(0);
    let spawner = closed.spawner();
    closed.shutdown();
    let error = spawner.spawn_dynamic(async { 5u32 }).unwrap_err();
    let mut open = Runtime::new(0);
    let handle = open.spawner().spawn_join(error.token).unwrap();
    let out = slot();
    open.spawner().spawn(store(handle, out.clone())).unwrap();
    run(&mut open);
    assert_eq!(*out.borrow(), Some(Ok(5u32)));
}

#[test]
fn result_outlives_a_temporary_spawner() {
    let mut runtime = Runtime::new(0);
    let result = runtime.spawner().spawn_dynamic(async { 7u32 });
    let handle = result.unwrap();
    let out = slot();
    runtime.spawner().spawn(store(handle, out.clone())).unwrap();
    run(&mut runtime);
    assert_eq!(*out.borrow(), Some(Ok(7u32)));
}

#[test]
fn shutdown_before_finish_cancels_the_handle() {
    let mut first = Runtime::new(0);
    let mut second = Runtime::new(0);
    let (_open, gate) = oneshot::channel::<()>();
    let handle = first
        .spawner()
        .spawn_dynamic(async move {
            let _ = gate.await;
            1u32
        })
        .unwrap();
    run(&mut first);
    let out = slot();
    second.spawner().spawn(store(handle, out.clone())).unwrap();
    run(&mut second);
    assert_eq!(*out.borrow(), None);
    first.shutdown();
    run(&mut second);
    assert_eq!(*out.borrow(), Some(Err(Canceled)));
}
