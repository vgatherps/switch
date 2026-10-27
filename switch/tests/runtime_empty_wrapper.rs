use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use switch::{PollReport, Runtime, Sleep, StoredWaker};

struct ParkedSleep {
    sleep: Sleep,
    wake: Rc<RefCell<Option<StoredWaker>>>,
    polls: Rc<Cell<usize>>,
    drops: Rc<Cell<usize>>,
}

impl Future for ParkedSleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.polls.set(self.polls.get() + 1);
        match Pin::new(&mut self.sleep).poll(cx) {
            Poll::Pending => {
                if self.wake.borrow().is_none() {
                    // Owner-thread use only while this task is parked. Ready and Drop
                    // clear the shared handle before the task allocation can be freed.
                    *self.wake.borrow_mut() = Some(unsafe { StoredWaker::new(cx) });
                }
                Poll::Pending
            }
            Poll::Ready(result) => {
                self.wake.borrow_mut().take();
                result.unwrap();
                Poll::Ready(())
            }
        }
    }
}

impl Drop for ParkedSleep {
    fn drop(&mut self) {
        self.wake.borrow_mut().take();
        self.drops.set(self.drops.get() + 1);
    }
}

#[test]
fn callback_duplicate_wakes_poll_parked_sleep_in_same_turn_then_expiry_drops_once() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let wake = Rc::new(RefCell::new(None));
    let polls = Rc::new(Cell::new(0));
    let drops = Rc::new(Cell::new(0));
    spawner
        .spawn(ParkedSleep {
            sleep: spawner.sleep_until(10).unwrap(),
            wake: wake.clone(),
            polls: polls.clone(),
            drops: drops.clone(),
        })
        .unwrap();
    let polled_pending = PollReport {
        tasks_polled: 1,
        live_tasks: 1,
        ..PollReport::default()
    };
    assert_eq!(runtime.poll(0, || {}).unwrap(), polled_pending);
    assert!(wake.borrow().is_some());
    let callbacks = Cell::new(0);
    assert_eq!(
        runtime
            .poll(0, || {
                callbacks.set(callbacks.get() + 1);
                assert_eq!((polls.get(), drops.get()), (1, 0));
                // Copy ends before polling resumes; no handle survives completion.
                let stored = wake.borrow().as_ref().copied().unwrap();
                stored.wake();
                stored.wake();
            })
            .unwrap(),
        polled_pending
    );
    assert_eq!((callbacks.get(), polls.get(), drops.get()), (1, 2, 0));
    assert_eq!(
        runtime.poll(0, || {}).unwrap(),
        PollReport {
            live_tasks: 1,
            ..PollReport::default()
        }
    );
    assert_eq!((polls.get(), drops.get()), (2, 0));
    assert_eq!(
        runtime.poll(10, || {}).unwrap(),
        PollReport {
            tasks_polled: 1,
            tasks_completed: 1,
            timers_expired: 1,
            ..PollReport::default()
        }
    );
    assert!(wake.borrow().is_none());
    assert_eq!((polls.get(), drops.get()), (3, 1));
    runtime.shutdown();
    assert_eq!(drops.get(), 1);
}
