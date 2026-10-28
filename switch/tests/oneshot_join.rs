//! `switch::oneshot` and `Spawner::spawn_join`, through real switch tasks.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::rc::Rc;

use switch::oneshot::{self, Canceled, Receiver};
use switch::{Runtime, RuntimeError};

type Slot<T> = Rc<RefCell<Option<T>>>;

fn slot<T>() -> Slot<T> {
    Rc::new(RefCell::new(None))
}

/// Awaits `future` and stores its output.
#[switch::task]
async fn store<F>(future: F, out: Slot<F::Output>)
where
    F: Future + 'static,
    F::Output: 'static,
{
    *out.borrow_mut() = Some(future.await);
}

/// Returns `value` once `gate` resolves (sent or canceled).
#[switch::task]
async fn gated<T: 'static>(gate: Receiver<()>, value: T) -> T {
    let _ = gate.await;
    value
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

// ---- oneshot ----

#[test]
fn send_reaches_a_receiver_that_awaited_first() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let (sender, receiver) = oneshot::channel();
    let out = slot();
    spawner.spawn(store(receiver, out.clone())).unwrap();
    run(&mut runtime);
    assert_eq!(*out.borrow(), None);
    assert_eq!(sender.send(7u32), Ok(()));
    run(&mut runtime);
    assert_eq!(*out.borrow(), Some(Ok(7)));
}

#[test]
fn receiver_awaited_after_send_is_ready() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let (sender, receiver) = oneshot::channel();
    assert_eq!(sender.send(String::from("sent")), Ok(()));
    let out = slot();
    spawner.spawn(store(receiver, out.clone())).unwrap();
    runtime.poll(0).unwrap();
    assert_eq!(*out.borrow(), Some(Ok(String::from("sent"))));
}

#[test]
fn dropped_sender_cancels_the_receiver() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let (waiting_sender, waiting) = oneshot::channel::<u32>();
    let (late_sender, late) = oneshot::channel::<u32>();
    let (waiting_out, late_out) = (slot(), slot());
    spawner.spawn(store(waiting, waiting_out.clone())).unwrap();
    run(&mut runtime);
    drop(waiting_sender);
    drop(late_sender);
    spawner.spawn(store(late, late_out.clone())).unwrap();
    run(&mut runtime);
    assert_eq!(*waiting_out.borrow(), Some(Err(Canceled)));
    assert_eq!(*late_out.borrow(), Some(Err(Canceled)));
}

#[test]
fn send_to_a_dropped_receiver_returns_the_value() {
    let (sender, receiver) = oneshot::channel();
    drop(receiver);
    assert_eq!(sender.send(5u8), Err(5));
}

// ---- JoinHandle ----

#[test]
fn joined_value_reaches_another_task() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let (open, gate) = oneshot::channel();
    let handle = spawner.spawn_join(gated(gate, 41u32)).unwrap();
    let out = slot();
    spawner.spawn(store(handle, out.clone())).unwrap();
    run(&mut runtime);
    assert_eq!(*out.borrow(), None);
    open.send(()).unwrap();
    run(&mut runtime);
    assert_eq!(*out.borrow(), Some(Ok(41)));
}

#[test]
fn handle_awaited_after_its_task_finished() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let (open, gate) = oneshot::channel();
    open.send(()).unwrap();
    let handle = spawner.spawn_join(gated(gate, 9u32)).unwrap();
    run(&mut runtime);
    assert_eq!(runtime.poll(0).unwrap().live_tasks, 0);
    let out = slot();
    spawner.spawn(store(handle, out.clone())).unwrap();
    runtime.poll(0).unwrap();
    assert_eq!(*out.borrow(), Some(Ok(9)));
}

#[test]
fn dropped_handle_drops_the_value() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let drops = Rc::new(Cell::new(0));
    // Dropped before completion: the value drops when the task completes.
    let (open, gate) = oneshot::channel();
    let handle = spawner
        .spawn_join(gated(gate, Counted(drops.clone())))
        .unwrap();
    run(&mut runtime);
    drop(handle);
    assert_eq!(drops.get(), 0);
    open.send(()).unwrap();
    run(&mut runtime);
    assert_eq!(drops.get(), 1);
    // Dropped after completion: the value drops with the handle.
    let (open, gate) = oneshot::channel();
    open.send(()).unwrap();
    let handle = spawner
        .spawn_join(gated(gate, Counted(drops.clone())))
        .unwrap();
    run(&mut runtime);
    assert_eq!(drops.get(), 1);
    drop(handle);
    assert_eq!(drops.get(), 2);
}

#[test]
fn detached_spawn_drops_the_value_at_completion() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let drops = Rc::new(Cell::new(0));
    let (open, gate) = oneshot::channel();
    spawner.spawn(gated(gate, Counted(drops.clone()))).unwrap();
    run(&mut runtime);
    assert_eq!(drops.get(), 0);
    open.send(()).unwrap();
    runtime.poll(0).unwrap();
    assert_eq!(drops.get(), 1);
}

#[switch::task(capacity = 2)]
async fn limited(gate: Receiver<()>) -> u32 {
    let _ = gate.await;
    3
}

#[test]
fn detached_and_joined_spawns_share_one_pool() {
    let runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let (_open_a, gate_a) = oneshot::channel();
    let (_open_b, gate_b) = oneshot::channel();
    let (_open_c, gate_c) = oneshot::channel();
    let (_open_d, gate_d) = oneshot::channel();
    spawner.spawn(limited(gate_a)).unwrap();
    let _handle = spawner.spawn_join(limited(gate_b)).unwrap();
    let detached = spawner.spawn(limited(gate_c)).unwrap_err();
    assert_eq!(detached.reason, RuntimeError::Exhausted);
    let joined = spawner.spawn_join(limited(gate_d)).unwrap_err();
    assert_eq!(joined.reason, RuntimeError::Exhausted);
}

#[test]
fn failed_spawn_join_returns_the_token() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let (open_a, gate_a) = oneshot::channel();
    let (_open_b, gate_b) = oneshot::channel();
    let (open_c, gate_c) = oneshot::channel();
    spawner.spawn(limited(gate_a)).unwrap();
    spawner.spawn(limited(gate_b)).unwrap();
    let error = spawner.spawn_join(limited(gate_c)).unwrap_err();
    assert_eq!(error.reason, RuntimeError::Exhausted);
    open_a.send(()).unwrap();
    run(&mut runtime);
    // The returned token still holds its argument: the gate it was built with.
    let handle = spawner.spawn_join(error.token).unwrap();
    let out = slot();
    spawner.spawn(store(handle, out.clone())).unwrap();
    open_c.send(()).unwrap();
    run(&mut runtime);
    assert_eq!(*out.borrow(), Some(Ok(3)));
}

#[test]
fn shutdown_before_finish_cancels_the_handle() {
    let mut first = Runtime::new(0);
    let mut second = Runtime::new(0);
    let (_open, gate) = oneshot::channel();
    let handle = first.spawner().spawn_join(gated(gate, 1u32)).unwrap();
    run(&mut first);
    let out = slot();
    second.spawner().spawn(store(handle, out.clone())).unwrap();
    run(&mut second);
    assert_eq!(*out.borrow(), None);
    first.shutdown();
    run(&mut second);
    assert_eq!(*out.borrow(), Some(Err(Canceled)));
}

#[test]
fn handle_polled_after_shutdown_is_canceled() {
    let first = Runtime::new(0);
    let mut second = Runtime::new(0);
    let (_open, gate) = oneshot::channel();
    let handle = first.spawner().spawn_join(gated(gate, 1u32)).unwrap();
    drop(first);
    let out = slot();
    second.spawner().spawn(store(handle, out.clone())).unwrap();
    second.poll(0).unwrap();
    assert_eq!(*out.borrow(), Some(Err(Canceled)));
}

/// Panics on its first poll.
#[switch::task]
async fn panics(_argument: Counted) -> u32 {
    panic!("joined task panics");
}

#[test]
fn panic_cancels_the_handle_at_once() {
    let mut a = Runtime::new(0);
    let mut b = Runtime::new(0);
    let drops = Rc::new(Cell::new(0));
    let handle = a
        .spawner()
        .spawn_join(panics(Counted(drops.clone())))
        .unwrap();
    let out = slot();
    b.spawner().spawn(store(handle, out.clone())).unwrap();
    run(&mut b);
    assert_eq!(*out.borrow(), None);
    let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| a.poll(0))).unwrap_err();
    assert_eq!(payload.downcast_ref::<&str>(), Some(&"joined task panics"));
    b.poll(0).unwrap();
    assert_eq!(*out.borrow(), Some(Err(Canceled)));
    // The unwind dropped the async block's locals, the argument among them; the
    // poisoned future stays in its slot until shutdown.
    assert_eq!(drops.get(), 1);
    a.shutdown();
    assert_eq!(drops.get(), 1);
    drop(a);
    assert_eq!(drops.get(), 1);
}

#[derive(Debug, PartialEq)]
struct Odd(u32);

#[switch::task]
async fn checked_async(value: u32) -> Result<u32, Odd> {
    if value % 2 == 1 {
        return Err(Odd(value));
    }
    Ok(value / 2)
}

#[switch::task]
fn checked_plain(value: u32) -> impl Future<Output = Result<u32, Odd>> {
    async move {
        if value % 2 == 1 {
            return Err(Odd(value));
        }
        Ok(value / 2)
    }
}

#[test]
fn result_returning_tasks_in_both_forms() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let outs: Vec<_> = (0..4).map(|_| slot()).collect();
    let handles = [
        spawner.spawn_join(checked_async(4)).unwrap(),
        spawner.spawn_join(checked_async(5)).unwrap(),
        spawner.spawn_join(checked_plain(6)).unwrap(),
        spawner.spawn_join(checked_plain(7)).unwrap(),
    ];
    for (handle, out) in handles.into_iter().zip(&outs) {
        spawner.spawn(store(handle, out.clone())).unwrap();
    }
    run(&mut runtime);
    let results: Vec<_> = outs.iter().map(|out| out.borrow_mut().take()).collect();
    assert_eq!(
        results,
        [
            Some(Ok(Ok(2))),
            Some(Ok(Err(Odd(5)))),
            Some(Ok(Ok(3))),
            Some(Ok(Err(Odd(7)))),
        ]
    );
}
