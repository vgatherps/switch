use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use switch::Runtime;

struct WakeOnce(bool);

impl Future for WakeOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.0 {
            Poll::Ready(())
        } else {
            self.0 = true;
            // Deferred: a plain self-wake would run again in this same poll.
            switch::StoredWaker::new(cx).wake_deferred();
            Poll::Pending
        }
    }
}

#[switch::task]
async fn push_ready(trace: Rc<RefCell<Vec<&'static str>>>) {
    trace.borrow_mut().push("ready");
}

#[switch::task]
async fn push_around_wake(trace: Rc<RefCell<Vec<&'static str>>>) {
    trace.borrow_mut().push("pending first poll");
    WakeOnce(false).await;
    trace.borrow_mut().push("pending second poll");
}

#[test]
fn ready_and_self_waking_pending_task_run_in_two_turns() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let trace = Rc::new(RefCell::new(Vec::new()));

    spawner.spawn(push_ready(Rc::clone(&trace))).unwrap();
    spawner.spawn(push_around_wake(Rc::clone(&trace))).unwrap();

    let first = runtime.poll(0).unwrap();
    assert_eq!((first.tasks_polled, first.live_tasks), (2, 1));
    assert_eq!(*trace.borrow(), ["ready", "pending first poll"]);

    let second = runtime.poll(0).unwrap();
    assert_eq!((second.tasks_polled, second.live_tasks), (1, 0));
    assert_eq!(
        *trace.borrow(),
        ["ready", "pending first poll", "pending second poll"]
    );
}
