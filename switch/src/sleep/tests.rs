use super::*;
use crate::runtime::{Runtime, RuntimeError};
use std::cell::{Cell, RefCell};
use std::mem::ManuallyDrop;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::task::Waker;

fn poll_noop(sleep: &mut Sleep) -> Poll<Result<(), SleepError>> {
    Pin::new(sleep).poll(&mut Context::from_waker(Waker::noop()))
}

struct Holder {
    sleep: Rc<RefCell<Option<Sleep>>>,
    complete: Rc<Cell<bool>>,
    forget: Option<Rc<RefCell<Option<ManuallyDrop<Sleep>>>>>,
    wake: Option<Rc<Cell<Option<crate::StoredWaker>>>>,
}
impl Future for Holder {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if let Some(wake) = &this.wake {
            wake.set(Some(unsafe { crate::StoredWaker::new(cx) }));
        }
        if this.complete.get() {
            if let Some(target) = &this.forget {
                *target.borrow_mut() = this.sleep.borrow_mut().take().map(ManuallyDrop::new);
            }
            return Poll::Ready(());
        }
        let mut slot = this.sleep.borrow_mut();
        let sleep = slot.as_mut().unwrap();
        // Reconstructed Context has identical Waker identity and must be accepted.
        let mut reconstructed = Context::from_waker(cx.waker());
        let result = Pin::new(sleep).poll(&mut reconstructed);
        if result.is_ready() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}
fn holder(sleep: Rc<RefCell<Option<Sleep>>>, complete: Rc<Cell<bool>>) -> Holder {
    Holder {
        sleep,
        complete,
        forget: None,
        wake: None,
    }
}

#[test]
fn construction_tick_callback_and_due_cached_noop() {
    let mut runtime = Runtime::new(7);
    let spawner = runtime.spawner();
    let mut earlier = spawner.sleep(3).unwrap();
    let captured = Rc::new(RefCell::new(None));
    let copy = captured.clone();
    runtime
        .poll(10, || *copy.borrow_mut() = Some(spawner.sleep(3).unwrap()))
        .unwrap();
    assert_eq!(earlier.deadline, 10);
    assert_eq!(poll_noop(&mut earlier), Poll::Ready(Ok(())));
    runtime.shutdown();
    assert_eq!(poll_noop(&mut earlier), Poll::Ready(Ok(())));
    assert_eq!(captured.borrow().as_ref().unwrap().deadline, 13);
}

#[test]
fn availability_precedes_overflow_and_escaped_sleep() {
    let mut runtime = Runtime::new(u64::MAX);
    let spawner = runtime.spawner();
    assert!(matches!(
        spawner.sleep(1),
        Err(SleepError::DeadlineOverflow)
    ));
    let mut sleep = spawner.sleep_until(u64::MAX).unwrap();
    runtime.shutdown();
    assert!(matches!(spawner.sleep(1), Err(SleepError::Closed)));
    assert_eq!(poll_noop(&mut sleep), Poll::Ready(Err(SleepError::Closed)));
    let mut runtime = Runtime::new(u64::MAX);
    let spawner = runtime.spawner();
    let mut sleep = spawner.sleep_until(u64::MAX).unwrap();
    assert!(
        catch_unwind(AssertUnwindSafe(
            || runtime.poll(u64::MAX, || panic!("original"))
        ))
        .is_err()
    );
    assert!(matches!(spawner.sleep(1), Err(SleepError::Panicked)));
    assert_eq!(
        poll_noop(&mut sleep),
        Poll::Ready(Err(SleepError::Panicked))
    );
}

#[test]
fn wrong_context_cancels_original_subscription_and_is_terminal() {
    let mut runtime = Runtime::new(0);
    let slot = Rc::new(RefCell::new(Some(
        runtime.spawner().sleep_until(10).unwrap(),
    )));
    runtime
        .spawner()
        .spawn(holder(slot.clone(), Rc::new(Cell::new(false))))
        .unwrap();
    assert_eq!(runtime.poll(0, || {}).unwrap().tasks_polled, 1);
    assert!(slot.borrow().as_ref().unwrap().token.is_some());
    assert_eq!(
        poll_noop(slot.borrow_mut().as_mut().unwrap()),
        Poll::Ready(Err(SleepError::WrongRuntime))
    );
    assert!(slot.borrow().as_ref().unwrap().token.is_none());
    assert_eq!(runtime.poll(10, || {}).unwrap().timers_expired, 0);
    assert_eq!(
        poll_noop(slot.borrow_mut().as_mut().unwrap()),
        Poll::Ready(Err(SleepError::WrongRuntime))
    );
}

#[test]
fn other_runtime_context_rejects_without_migration() {
    let mut a = Runtime::new(0);
    let mut b = Runtime::new(0);
    let slot = Rc::new(RefCell::new(Some(a.spawner().sleep_until(5).unwrap())));
    a.spawner()
        .spawn(holder(slot.clone(), Rc::new(Cell::new(false))))
        .unwrap();
    a.poll(0, || {}).unwrap();
    b.spawner()
        .spawn(holder(slot.clone(), Rc::new(Cell::new(false))))
        .unwrap();
    assert_eq!(b.poll(0, || {}).unwrap().tasks_completed, 1);
    assert_eq!(
        slot.borrow().as_ref().unwrap().result,
        Some(Err(SleepError::WrongRuntime))
    );
    assert_eq!(a.poll(5, || {}).unwrap().timers_expired, 0);
    assert_eq!(b.poll(5, || {}).unwrap().timers_expired, 0);
}

#[test]
fn transfer_before_retirement_and_stale_after_retirement() {
    for transfer_first in [false, true] {
        let mut runtime = Runtime::new(0);
        let spawner = runtime.spawner();
        let slot = Rc::new(RefCell::new(Some(spawner.sleep_until(8).unwrap())));
        let done = Rc::new(Cell::new(false));
        let wake = Rc::new(Cell::new(None));
        let mut original = holder(slot.clone(), done.clone());
        original.wake = Some(wake.clone());
        spawner.spawn(original).unwrap();
        runtime.poll(0, || {}).unwrap();
        let old = slot.borrow().as_ref().unwrap().token.unwrap();
        if transfer_first {
            spawner
                .spawn(holder(slot.clone(), Rc::new(Cell::new(false))))
                .unwrap();
            runtime.poll(0, || {}).unwrap();
            assert_eq!(slot.borrow().as_ref().unwrap().token, Some(old));
        }
        done.set(true);
        wake.take().unwrap().wake();
        assert_eq!(runtime.poll(0, || {}).unwrap().tasks_completed, 1);
        if !transfer_first {
            spawner
                .spawn(holder(slot.clone(), Rc::new(Cell::new(false))))
                .unwrap();
            runtime.poll(0, || {}).unwrap();
            assert_ne!(slot.borrow().as_ref().unwrap().token, Some(old));
        }
        let report = runtime.poll(8, || {}).unwrap();
        assert_eq!((report.timers_expired, report.tasks_completed), (1, 1));
    }
}

#[test]
fn drop_cancels_and_reused_slot_does_not_accept_old_token() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let slot = Rc::new(RefCell::new(Some(spawner.sleep_until(5).unwrap())));
    spawner
        .spawn(holder(slot.clone(), Rc::new(Cell::new(false))))
        .unwrap();
    runtime.poll(0, || {}).unwrap();
    let old = slot.borrow().as_ref().unwrap().token.unwrap();
    drop(slot.borrow_mut().take());
    let next = Rc::new(RefCell::new(Some(spawner.sleep_until(6).unwrap())));
    spawner
        .spawn(holder(next.clone(), Rc::new(Cell::new(false))))
        .unwrap();
    runtime.poll(0, || {}).unwrap();
    assert_ne!(next.borrow().as_ref().unwrap().token, Some(old));
    assert!(!unsafe {
        (*next.borrow().as_ref().unwrap().control.open_core().unwrap())
            .timers
            .cancel(old)
    });
    assert_eq!(runtime.poll(5, || {}).unwrap().timers_expired, 0);
    assert_eq!(runtime.poll(6, || {}).unwrap().timers_expired, 1);
}

#[test]
fn retirement_cleans_undropped_pending_and_expired_sleep() {
    for expired in [false, true] {
        let mut runtime = Runtime::new(0);
        let spawner = runtime.spawner();
        let slot = Rc::new(RefCell::new(Some(spawner.sleep_until(5).unwrap())));
        let done = Rc::new(Cell::new(false));
        let retained = Rc::new(RefCell::new(None));
        let wake = Rc::new(Cell::new(None));
        spawner
            .spawn(Holder {
                sleep: slot.clone(),
                complete: done.clone(),
                forget: Some(retained.clone()),
                wake: Some(wake.clone()),
            })
            .unwrap();
        runtime.poll(0, || {}).unwrap();
        done.set(true);
        if !expired {
            wake.take().unwrap().wake();
        }
        assert_eq!(
            runtime
                .poll(if expired { 5 } else { 0 }, || {})
                .unwrap()
                .tasks_completed,
            1
        );
        let held = ManuallyDrop::into_inner(retained.borrow_mut().take().unwrap());
        let token = held.token.unwrap();
        let root = held.control.open_core().unwrap();
        assert!(!unsafe { (*root).timers.cancel(token) });
        let replacement = Rc::new(RefCell::new(Some(spawner.sleep_until(9).unwrap())));
        spawner
            .spawn(holder(replacement.clone(), Rc::new(Cell::new(false))))
            .unwrap();
        runtime.poll(if expired { 5 } else { 0 }, || {}).unwrap();
        assert_ne!(replacement.borrow().as_ref().unwrap().token, Some(token));
        drop(held); // Stale cancellation must not cancel the replacement.
        assert_eq!(runtime.poll(9, || {}).unwrap().timers_expired, 1);
    }
}

#[test]
fn maximum_equal_and_regressed_ticks() {
    let mut runtime = Runtime::new(u64::MAX - 1);
    let slot = Rc::new(RefCell::new(Some(runtime.spawner().sleep(1).unwrap())));
    runtime
        .spawner()
        .spawn(holder(slot, Rc::new(Cell::new(false))))
        .unwrap();
    assert_eq!(runtime.poll(u64::MAX - 1, || {}).unwrap().tasks_polled, 1);
    assert!(matches!(
        runtime.poll(u64::MAX - 2, || panic!("must not run")),
        Err(RuntimeError::TimeRegressed { .. })
    ));
    let report = runtime.poll(u64::MAX, || {}).unwrap();
    assert_eq!((report.timers_expired, report.tasks_completed), (1, 1));
    assert_eq!(runtime.poll(u64::MAX, || {}).unwrap().timers_expired, 0);
}

#[test]
fn foreign_runtime_frame_rejected_while_original_frame_live() {
    let mut a = Runtime::new(0);
    let b = Rc::new(RefCell::new(Runtime::new(0)));
    let slot = Rc::new(RefCell::new(Some(a.spawner().sleep_until(5).unwrap())));
    struct Nested {
        sleep: Rc<RefCell<Option<Sleep>>>,
        other: Rc<RefCell<Runtime>>,
    }
    impl Future for Nested {
        type Output = ();
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            assert!(
                Pin::new(self.sleep.borrow_mut().as_mut().unwrap())
                    .poll(cx)
                    .is_pending()
            );
            self.other
                .borrow()
                .spawner()
                .spawn(holder(self.sleep.clone(), Rc::new(Cell::new(false))))
                .unwrap();
            assert_eq!(
                self.other
                    .borrow_mut()
                    .poll(0, || {})
                    .unwrap()
                    .tasks_completed,
                1
            );
            assert_eq!(
                self.sleep.borrow().as_ref().unwrap().result,
                Some(Err(SleepError::WrongRuntime))
            );
            Poll::Ready(())
        }
    }
    a.spawner()
        .spawn(Nested {
            sleep: slot,
            other: b.clone(),
        })
        .unwrap();
    assert_eq!(a.poll(0, || {}).unwrap().tasks_completed, 1);
    assert_eq!(a.poll(5, || {}).unwrap().timers_expired, 0);
    assert_eq!(b.borrow_mut().poll(5, || {}).unwrap().timers_expired, 0);
}

#[test]
fn active_frame_clears_pending_ready_and_unwind_before_destructor() {
    struct Inspect {
        root: Rc<Cell<*mut crate::executor::Executor>>,
        polls: u8,
        panic: bool,
        drops: Rc<Cell<usize>>,
    }
    impl Future for Inspect {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            let task = crate::waker::task_from_waker(cx.waker());
            let root = unsafe {
                (*task.as_ptr())
                    .executor
                    .cast::<crate::executor::Executor>()
            };
            self.root.set(root);
            assert_eq!(
                unsafe { (*root).active_frame.cast::<()>() },
                cx.waker().data()
            );
            if self.panic {
                panic!("poll payload");
            }
            self.polls += 1;
            if self.polls == 1 {
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        }
    }
    impl Drop for Inspect {
        fn drop(&mut self) {
            assert!(unsafe { (*self.root.get()).active_frame.is_null() });
            self.drops.set(self.drops.get() + 1);
        }
    }
    for panic in [false, true] {
        let mut runtime = Runtime::new(0);
        let root = Rc::new(Cell::new(std::ptr::null_mut()));
        let drops = Rc::new(Cell::new(0));
        runtime
            .spawner()
            .spawn(Inspect {
                root: root.clone(),
                polls: 0,
                panic,
                drops: drops.clone(),
            })
            .unwrap();
        if panic {
            let payload = catch_unwind(AssertUnwindSafe(|| runtime.poll(0, || {}))).unwrap_err();
            assert_eq!(payload.downcast_ref::<&str>(), Some(&"poll payload"));
            assert!(unsafe { (*root.get()).active_frame.is_null() });
            assert_eq!(drops.get(), 0);
            runtime.shutdown();
        } else {
            runtime.poll(0, || {}).unwrap();
            assert!(unsafe { (*root.get()).active_frame.is_null() });
            runtime.poll(0, || {}).unwrap();
            assert!(unsafe { (*root.get()).active_frame.is_null() });
        }
        assert_eq!(drops.get(), 1);
    }
}

#[test]
fn unavailable_poll_cancels_other_live_task_subscription() {
    let mut runtime = Runtime::new(0);
    let slot = Rc::new(RefCell::new(Some(
        runtime.spawner().sleep_until(9).unwrap(),
    )));
    runtime
        .spawner()
        .spawn(holder(slot.clone(), Rc::new(Cell::new(false))))
        .unwrap();
    runtime.poll(0, || {}).unwrap();
    let token = slot.borrow().as_ref().unwrap().token.unwrap();
    let root = slot.borrow().as_ref().unwrap().control.open_core().unwrap();
    assert!(catch_unwind(AssertUnwindSafe(|| runtime.poll(0, || panic!("callback")))).is_err());
    assert_eq!(
        poll_noop(slot.borrow_mut().as_mut().unwrap()),
        Poll::Ready(Err(SleepError::Panicked))
    );
    assert!(!unsafe { (*root).timers.cancel(token) });
    runtime.shutdown();
    assert_eq!(
        poll_noop(slot.borrow_mut().as_mut().unwrap()),
        Poll::Ready(Err(SleepError::Panicked))
    );
}
