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

// ---- oneshot across threads ----

/// Polls until `out` is set or `turns` polls ran.
fn run_until<T>(runtime: &mut Runtime, out: &Slot<T>, turns: usize) {
    for _ in 0..turns {
        if out.borrow().is_some() {
            return;
        }
        runtime.poll(0).unwrap();
        std::thread::yield_now();
    }
}

#[test]
fn a_send_from_another_thread_delivers_the_value() {
    let mut runtime = Runtime::new(0);
    let (sender, receiver) = oneshot::channel();
    let out = slot();
    runtime
        .spawner()
        .spawn(store(receiver, out.clone()))
        .unwrap();
    runtime.poll(0).unwrap();
    std::thread::spawn(move || sender.send(String::from("across")).unwrap())
        .join()
        .unwrap();
    // The wake went through the mailbox: one poll delivers it.
    runtime.poll(0).unwrap();
    assert_eq!(*out.borrow(), Some(Ok(String::from("across"))));
}

#[test]
fn a_sender_dropped_on_another_thread_cancels() {
    let mut runtime = Runtime::new(0);
    let (sender, receiver) = oneshot::channel::<u32>();
    let out = slot();
    runtime
        .spawner()
        .spawn(store(receiver, out.clone()))
        .unwrap();
    runtime.poll(0).unwrap();
    std::thread::spawn(move || drop(sender)).join().unwrap();
    runtime.poll(0).unwrap();
    assert_eq!(*out.borrow(), Some(Err(Canceled)));
}

#[test]
fn a_send_from_another_thread_to_a_dropped_receiver_returns_the_value() {
    let (sender, receiver) = oneshot::channel::<Vec<u8>>();
    drop(receiver);
    let back = std::thread::spawn(move || sender.send(vec![1, 2]))
        .join()
        .unwrap();
    assert_eq!(back, Err(vec![1, 2]));
}

#[test]
fn a_send_racing_the_first_poll_is_never_lost() {
    for _ in 0..8 {
        let mut runtime = Runtime::new(0);
        let (sender, receiver) = oneshot::channel();
        let out = slot();
        runtime
            .spawner()
            .spawn(store(receiver, out.clone()))
            .unwrap();
        let sending = std::thread::spawn(move || sender.send(5u32).unwrap());
        // Each poll either sees the value or registered before the send's wake.
        run_until(&mut runtime, &out, 1000);
        sending.join().unwrap();
        run_until(&mut runtime, &out, 2);
        assert_eq!(*out.borrow(), Some(Ok(5)));
    }
}

#[test]
fn a_send_racing_the_receiver_drop_frees_once() {
    for _ in 0..8 {
        let (sender, receiver) = oneshot::channel::<Vec<u8>>();
        let sending = std::thread::spawn(move || {
            let _ = sender.send(vec![7]);
        });
        drop(receiver);
        sending.join().unwrap();
    }
}

#[test]
fn a_sender_drop_racing_the_receiver_drop_frees_once() {
    for _ in 0..8 {
        let (sender, receiver) = oneshot::channel::<Vec<u8>>();
        let dropping = std::thread::spawn(move || drop(sender));
        drop(receiver);
        dropping.join().unwrap();
    }
}

#[test]
fn a_sender_drop_racing_the_first_poll_cancels() {
    for _ in 0..8 {
        let mut runtime = Runtime::new(0);
        let (sender, receiver) = oneshot::channel::<u32>();
        let out = slot();
        runtime
            .spawner()
            .spawn(store(receiver, out.clone()))
            .unwrap();
        let dropping = std::thread::spawn(move || drop(sender));
        run_until(&mut runtime, &out, 1000);
        dropping.join().unwrap();
        run_until(&mut runtime, &out, 2);
        assert_eq!(*out.borrow(), Some(Err(Canceled)));
    }
}

#[test]
fn a_sender_dropped_after_its_receiver_does_not_wake_the_slot() {
    /// Polls its receiver until `done`, then drops it and completes. Keeps its
    /// waker in `waker`.
    #[switch::task(capacity = 1)]
    fn waits(
        receiver: Receiver<u32>,
        done: Rc<Cell<bool>>,
        polls: Rc<Cell<u32>>,
        waker: Slot<std::task::Waker>,
    ) -> impl Future<Output = ()> {
        let mut receiver = Some(receiver);
        std::future::poll_fn(move |cx| {
            polls.set(polls.get() + 1);
            *waker.borrow_mut() = Some(cx.waker().clone());
            if done.get() {
                drop(receiver.take());
                return std::task::Poll::Ready(());
            }
            let held = receiver.as_mut().unwrap();
            assert!(std::pin::Pin::new(held).poll(cx).is_pending());
            std::task::Poll::Pending
        })
    }
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let (old_sender, old_receiver) = oneshot::channel();
    let (done, old_polls, waker) = (Rc::new(Cell::new(false)), Rc::new(Cell::new(0)), slot());
    spawner
        .spawn(waits(
            old_receiver,
            done.clone(),
            old_polls.clone(),
            waker.clone(),
        ))
        .unwrap();
    runtime.poll(0).unwrap();
    // The receiver registered its task; the task then drops it and completes.
    done.set(true);
    waker.borrow_mut().take().unwrap().wake();
    runtime.poll(0).unwrap();
    assert_eq!(old_polls.get(), 2);
    // Capacity 1: a newer task takes the old one's slot.
    let (_new_sender, new_receiver) = oneshot::channel();
    let new_polls = Rc::new(Cell::new(0));
    spawner
        .spawn(waits(
            new_receiver,
            Rc::new(Cell::new(false)),
            new_polls.clone(),
            slot(),
        ))
        .unwrap();
    runtime.poll(0).unwrap();
    assert_eq!(new_polls.get(), 1);
    drop(old_sender);
    assert_eq!(runtime.poll(0).unwrap().tasks_polled, 0);
    assert_eq!(new_polls.get(), 1);
}

#[test]
fn the_waiter_moves_with_the_receiver_to_another_task() {
    /// Polls the receiver once, then hands it over.
    #[switch::task]
    fn polls_once(
        receiver: Receiver<u32>,
        handoff: Slot<Receiver<u32>>,
        polls: Rc<Cell<u32>>,
    ) -> impl Future<Output = ()> {
        let mut receiver = Some(receiver);
        std::future::poll_fn(move |cx| {
            polls.set(polls.get() + 1);
            let mut held = receiver.take().unwrap();
            assert!(std::pin::Pin::new(&mut held).poll(cx).is_pending());
            *handoff.borrow_mut() = Some(held);
            std::task::Poll::Ready(())
        })
    }
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let (sender, receiver) = oneshot::channel();
    let (handoff, polls, out) = (slot(), Rc::new(Cell::new(0)), slot());
    spawner
        .spawn(polls_once(receiver, handoff.clone(), polls.clone()))
        .unwrap();
    runtime.poll(0).unwrap();
    let receiver = handoff.borrow_mut().take().unwrap();
    spawner.spawn(store(receiver, out.clone())).unwrap();
    runtime.poll(0).unwrap();
    std::thread::spawn(move || sender.send(9).unwrap())
        .join()
        .unwrap();
    runtime.poll(0).unwrap();
    assert_eq!(*out.borrow(), Some(Ok(9)));
    assert_eq!(polls.get(), 1);
}

#[test]
fn a_receiver_polled_outside_a_switch_task_panics() {
    let (_sender, mut receiver) = oneshot::channel::<u32>();
    let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        let _ = std::pin::Pin::new(&mut receiver).poll(&mut cx);
    }))
    .unwrap_err();
    assert_eq!(
        payload.downcast_ref::<&str>(),
        Some(&"RemoteWaker needs a switch task waker")
    );
}

fn assert_send<T: Send>() {}

#[test]
fn the_sender_is_send() {
    assert_send::<oneshot::Sender<Vec<u8>>>();
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
