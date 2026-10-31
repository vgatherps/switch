use super::*;
use crate::runtime::{Runtime, RuntimeError};
use crate::task::TaskHeader;
use crate::task::block::Meta;
use std::cell::{Cell, RefCell};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::task::Waker;

fn poll_noop(sleep: &mut Sleep) -> Poll<Result<(), SleepError>> {
    Pin::new(sleep).poll(&mut Context::from_waker(Waker::noop()))
}
struct Holder {
    sleep: Rc<RefCell<Option<Sleep>>>,
    complete: Rc<Cell<bool>>,
    wake: Option<Rc<Cell<Option<Waker>>>>,
}
impl Future for Holder {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if let Some(wake) = &this.wake {
            wake.set(Some(cx.waker().clone()));
        }
        if this.complete.get() {
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
fn new_holder(sleep: Rc<RefCell<Option<Sleep>>>, complete: Rc<Cell<bool>>) -> Holder {
    Holder {
        sleep,
        complete,
        wake: None,
    }
}

#[switch::task]
fn holding(task: Holder) -> impl Future<Output = ()> {
    task
}

#[switch::task]
fn holder(sleep: Rc<RefCell<Option<Sleep>>>, complete: Rc<Cell<bool>>) -> impl Future<Output = ()> {
    new_holder(sleep, complete)
}

#[switch::task]
async fn panic_task(payload: &'static str) {
    std::panic::panic_any(payload);
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
    spawner.spawn(panic_task("original")).unwrap();
    assert!(catch_unwind(AssertUnwindSafe(|| runtime.poll(u64::MAX))).is_err());
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
    assert_eq!(runtime.poll(0).unwrap().tasks_polled, 1);
    assert!(slot.borrow().as_ref().unwrap().token.is_some());
    assert_eq!(
        poll_noop(slot.borrow_mut().as_mut().unwrap()),
        Poll::Ready(Err(SleepError::WrongRuntime))
    );
    assert!(slot.borrow().as_ref().unwrap().token.is_none());
    assert_eq!(runtime.poll(10).unwrap().tasks_polled, 0);
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
    a.poll(0).unwrap();
    b.spawner()
        .spawn(holder(slot.clone(), Rc::new(Cell::new(false))))
        .unwrap();
    assert_eq!(b.poll(0).unwrap().tasks_polled, 1);
    assert_eq!(
        slot.borrow().as_ref().unwrap().result,
        Some(Err(SleepError::WrongRuntime))
    );
    assert_eq!(a.poll(5).unwrap().tasks_polled, 0);
    assert_eq!(b.poll(5).unwrap().tasks_polled, 0);
}

#[test]
fn transfer_keeps_registration_before_and_after_original_task_ends() {
    for transfer_first in [false, true] {
        let mut runtime = Runtime::new(0);
        let spawner = runtime.spawner();
        let slot = Rc::new(RefCell::new(Some(spawner.sleep_until(8).unwrap())));
        let done = Rc::new(Cell::new(false));
        let wake = Rc::new(Cell::new(None));
        let mut original = new_holder(slot.clone(), done.clone());
        original.wake = Some(wake.clone());
        spawner.spawn(holding(original)).unwrap();
        runtime.poll(0).unwrap();
        let old = slot.borrow().as_ref().unwrap().token.unwrap();
        if transfer_first {
            spawner
                .spawn(holder(slot.clone(), Rc::new(Cell::new(false))))
                .unwrap();
            runtime.poll(0).unwrap();
            assert_eq!(slot.borrow().as_ref().unwrap().token, Some(old));
        }
        done.set(true);
        wake.take().unwrap().wake();
        assert_eq!(runtime.poll(0).unwrap().tasks_polled, 1);
        if !transfer_first {
            spawner
                .spawn(holder(slot.clone(), Rc::new(Cell::new(false))))
                .unwrap();
            runtime.poll(0).unwrap();
            assert_eq!(slot.borrow().as_ref().unwrap().token, Some(old));
        }
        let report = runtime.poll(8).unwrap();
        assert_eq!(report.tasks_polled, 1);
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
    runtime.poll(0).unwrap();
    let old = slot.borrow().as_ref().unwrap().token.unwrap();
    drop(slot.borrow_mut().take());
    let next = Rc::new(RefCell::new(Some(spawner.sleep_until(6).unwrap())));
    spawner
        .spawn(holder(next.clone(), Rc::new(Cell::new(false))))
        .unwrap();
    runtime.poll(0).unwrap();
    assert_ne!(next.borrow().as_ref().unwrap().token, Some(old));
    assert!(!unsafe {
        (*next.borrow().as_ref().unwrap().control.open_core().unwrap())
            .timers
            .cancel(old)
    });
    assert_eq!(runtime.poll(5).unwrap().tasks_polled, 0);
    assert_eq!(runtime.poll(6).unwrap().tasks_polled, 1);
}

#[test]
fn maximum_equal_and_regressed_ticks() {
    let mut runtime = Runtime::new(u64::MAX - 1);
    let slot = Rc::new(RefCell::new(Some(runtime.spawner().sleep(1).unwrap())));
    runtime
        .spawner()
        .spawn(holder(slot, Rc::new(Cell::new(false))))
        .unwrap();
    assert_eq!(runtime.poll(u64::MAX - 1).unwrap().tasks_polled, 1);
    assert!(matches!(
        runtime.poll(u64::MAX - 2),
        Err(RuntimeError::TimeRegressed { .. })
    ));
    let report = runtime.poll(u64::MAX).unwrap();
    assert_eq!((report.tasks_polled, report.live_tasks), (1, 0));
    assert_eq!(runtime.poll(u64::MAX).unwrap().tasks_polled, 0);
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
            assert_eq!(self.other.borrow_mut().poll(0).unwrap().tasks_polled, 1);
            assert_eq!(
                self.sleep.borrow().as_ref().unwrap().result,
                Some(Err(SleepError::WrongRuntime))
            );
            Poll::Ready(())
        }
    }
    #[switch::task]
    fn nested(task: Nested) -> impl Future<Output = ()> {
        task
    }
    a.spawner()
        .spawn(nested(Nested {
            sleep: slot,
            other: b.clone(),
        }))
        .unwrap();
    assert_eq!(a.poll(0).unwrap().tasks_polled, 1);
    assert_eq!(a.poll(5).unwrap().tasks_polled, 0);
    assert_eq!(b.borrow_mut().poll(5).unwrap().tasks_polled, 0);
}

#[test]
fn current_task_clears_pending_ready_and_unwind_before_destructor() {
    struct Inspect {
        root: Rc<Cell<*mut crate::executor::Executor>>,
        polls: u8,
        panic: bool,
        drops: Rc<Cell<usize>>,
    }
    impl Future for Inspect {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            let meta = TaskHeader::meta(cx.waker().data().cast());
            let root = unsafe { Meta::executor(meta) };
            self.root.set(root);
            assert_eq!(unsafe { (*root).current.cast::<()>() }, cx.waker().data());
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
            assert!(unsafe { (*self.root.get()).current.is_null() });
            self.drops.set(self.drops.get() + 1);
        }
    }
    #[switch::task]
    fn inspect(task: Inspect) -> impl Future<Output = ()> {
        task
    }
    for panic in [false, true] {
        let mut runtime = Runtime::new(0);
        let root = Rc::new(Cell::new(std::ptr::null_mut()));
        let drops = Rc::new(Cell::new(0));
        runtime
            .spawner()
            .spawn(inspect(Inspect {
                root: root.clone(),
                polls: 0,
                panic,
                drops: drops.clone(),
            }))
            .unwrap();
        if panic {
            let payload = catch_unwind(AssertUnwindSafe(|| runtime.poll(0))).unwrap_err();
            assert_eq!(payload.downcast_ref::<&str>(), Some(&"poll payload"));
            assert!(unsafe { (*root.get()).current.is_null() });
            assert_eq!(drops.get(), 0);
            runtime.shutdown();
        } else {
            runtime.poll(0).unwrap();
            assert!(unsafe { (*root.get()).current.is_null() });
            runtime.poll(0).unwrap();
            assert!(unsafe { (*root.get()).current.is_null() });
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
    let spawner = runtime.spawner();
    spawner
        .spawn(holder(slot.clone(), Rc::new(Cell::new(false))))
        .unwrap();
    runtime.poll(0).unwrap();
    let token = slot.borrow().as_ref().unwrap().token.unwrap();
    let root = slot.borrow().as_ref().unwrap().control.open_core().unwrap();
    spawner.spawn(panic_task("task")).unwrap();
    assert!(catch_unwind(AssertUnwindSafe(|| runtime.poll(0))).is_err());
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
