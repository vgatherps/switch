use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use switch::{PollReport, Runtime, RuntimeError, Sleep};

struct YieldingSleep {
    sleep: Sleep,
    polls: Rc<Cell<usize>>,
    drops: Rc<Cell<usize>>,
    trace: Rc<RefCell<Vec<&'static str>>>,
}

impl Future for YieldingSleep {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.polls.set(self.polls.get() + 1);
        match Pin::new(&mut self.sleep).poll(cx) {
            Poll::Pending => {
                self.trace.borrow_mut().push("pending");
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Poll::Ready(result) => {
                result.unwrap();
                self.trace.borrow_mut().push("sleep ready");
                Poll::Ready(())
            }
        }
    }
}

impl Drop for YieldingSleep {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
        self.trace.borrow_mut().push("drop");
    }
}

#[test]
fn equal_ticks_refresh_then_expiry_deduplicates_and_callback_spawn_runs_this_turn() {
    let mut runtime = Runtime::new(10);
    let spawner = runtime.spawner();
    let polls = Rc::new(Cell::new(0));
    let drops = Rc::new(Cell::new(0));
    let trace = Rc::new(RefCell::new(Vec::new()));
    spawner
        .spawn(YieldingSleep {
            sleep: spawner.sleep_until(20).unwrap(),
            polls: polls.clone(),
            drops: drops.clone(),
            trace: trace.clone(),
        })
        .unwrap();
    for _ in 0..2 {
        assert_eq!(
            runtime.poll(10, || {}).unwrap(),
            PollReport {
                tasks_polled: 1,
                tasks_completed: 0,
                timers_expired: 0,
                ready_tasks: 1,
                live_tasks: 1,
            }
        );
    }
    assert_eq!((polls.get(), drops.get()), (2, 0));
    let callback_trace = trace.clone();
    let spawned_trace = trace.clone();
    let expired = runtime
        .poll(20, || {
            callback_trace.borrow_mut().push("callback");
            spawner
                .spawn(async move {
                    spawned_trace.borrow_mut().push("spawned ready");
                })
                .unwrap();
        })
        .unwrap();
    assert_eq!(
        expired,
        PollReport {
            tasks_polled: 2,
            tasks_completed: 2,
            timers_expired: 1,
            ready_tasks: 0,
            live_tasks: 0,
        }
    );
    assert_eq!((polls.get(), drops.get()), (3, 1));
    assert_eq!(
        *trace.borrow(),
        [
            "pending",
            "pending",
            "callback",
            "sleep ready",
            "drop",
            "spawned ready"
        ]
    );
    let callback = Cell::new(false);
    assert_eq!(
        runtime.poll(19, || callback.set(true)),
        Err(RuntimeError::TimeRegressed {
            current: 20,
            supplied: 19
        })
    );
    assert!(!callback.get());
    assert_eq!((polls.get(), drops.get()), (3, 1));
    assert_eq!(
        runtime.poll(20, || callback.set(true)).unwrap(),
        PollReport::default()
    );
    assert!(callback.get());
}

#[test]
fn empty_long_jump_accepts_max_and_keeps_clock_on_regression() {
    let mut runtime = Runtime::new(17);
    let spawner = runtime.spawner();
    let callbacks = Cell::new(0);
    let report = runtime
        .poll(u64::MAX, || {
            callbacks.set(callbacks.get() + 1);
            assert_eq!(
                spawner.sleep(1).err(),
                Some(switch::SleepError::DeadlineOverflow)
            );
        })
        .unwrap();
    assert_eq!(report, PollReport::default());
    assert_eq!(callbacks.get(), 1);
    assert_eq!(
        runtime.poll(u64::MAX - 1, || callbacks.set(callbacks.get() + 1)),
        Err(RuntimeError::TimeRegressed {
            current: u64::MAX,
            supplied: u64::MAX - 1
        })
    );
    assert_eq!(callbacks.get(), 1);
    assert_eq!(
        spawner.sleep(1).err(),
        Some(switch::SleepError::DeadlineOverflow)
    );
    let mut due = spawner.sleep(0).unwrap();
    assert_eq!(
        Pin::new(&mut due).poll(&mut Context::from_waker(std::task::Waker::noop())),
        Poll::Ready(Ok(()))
    );
    assert_eq!(
        runtime
            .poll(u64::MAX, || callbacks.set(callbacks.get() + 1))
            .unwrap(),
        PollReport::default()
    );
    assert_eq!(callbacks.get(), 2);
}
