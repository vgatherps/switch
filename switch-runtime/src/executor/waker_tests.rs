use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

fn block_owner(task: *const TaskHeader) -> u16 {
    unsafe { Meta::owner(TaskHeader::meta(task)) }
}

/// Parks once; another thread clones, drops and wakes its waker.
#[switch_runtime::task]
fn woken_from_another_thread(ptr: *mut Executor, polls: Rc<Cell<usize>>) -> impl Future<Output = ()> {
    std::future::poll_fn(move |cx| {
        polls.set(polls.get() + 1);
        if polls.get() > 1 {
            return Poll::Ready(());
        }
        let waker = cx.waker();
        let owned = waker.clone();
        std::thread::scope(|scope| {
            scope
                .spawn(move || {
                    drop(waker.clone());
                    drop(owned);
                    waker.wake_by_ref();
                })
                .join()
                .unwrap();
        });
        // The foreign wake waits in the mailbox: nothing queued here yet.
        unsafe { assert!((*ptr).incoming.is_empty()) };
        Poll::Pending
    })
}

/// Cloning and dropping a task waker on another thread does not panic, and a wake
/// from there reaches the task at the runtime's next poll.
#[test]
fn a_task_waker_cloned_and_dropped_on_another_thread_does_not_panic() {
    let owner = Owner::new();
    let polls = Rc::new(Cell::new(0));
    owner.spawn(woken_from_another_thread(owner.0, polls.clone()));
    assert_eq!(owner.poll(), Ok(1));
    assert_eq!(owner.poll(), Ok(1));
    assert_eq!(polls.get(), 2);
    assert_eq!(owner.counts(), (0, 0));
}

/// A wake through a task `Waker` on another thread, while the task is parked,
/// polls it once at the next poll.
#[test]
fn a_foreign_wake_through_a_task_waker_reaches_the_task() {
    #[switch_runtime::task]
    fn parks(slot: Rc<RefCell<Option<std::task::Waker>>>, polls: Rc<Cell<usize>>) -> impl Future<Output = ()> {
        std::future::poll_fn(move |cx| {
            polls.set(polls.get() + 1);
            *slot.borrow_mut() = Some(cx.waker().clone());
            Poll::<()>::Pending
        })
    }
    let owner = Owner::new();
    let (slot, polls) = (Rc::new(RefCell::new(None)), Rc::new(Cell::new(0)));
    owner.spawn(parks(slot.clone(), polls.clone()));
    assert_eq!(owner.poll(), Ok(1));
    assert_eq!(owner.poll(), Ok(0));
    let waker = slot.borrow_mut().take().unwrap();
    std::thread::spawn(move || waker.wake()).join().unwrap();
    assert_eq!(owner.poll(), Ok(1));
    assert_eq!(polls.get(), 2);
    assert_eq!(owner.poll(), Ok(0));
}

#[test]
fn runtimes_on_one_thread_share_a_nonzero_tag_and_other_threads_differ() {
    #[switch_runtime::task]
    async fn idle() {
        std::future::pending::<()>().await;
    }
    let first = Owner::new();
    let second = Owner::new();
    first.spawn(idle());
    second.spawn(idle());
    let first_tag = block_owner(first.last());
    assert_ne!(first_tag, 0);
    assert_eq!(first_tag, block_owner(second.last()));
    assert_ne!(
        TaskHeader::meta(first.last()),
        TaskHeader::meta(second.last())
    );
    let other_tag = std::thread::spawn(|| {
        let owner = Owner::new();
        owner.spawn(idle());
        block_owner(owner.last())
    })
    .join()
    .unwrap();
    assert_ne!(other_tag, 0);
    assert_ne!(first_tag, other_tag);
}

#[test]
fn local_ordinary_and_cloned_wakes_share_one_claim() {
    #[switch_runtime::task]
    fn wake_three_ways(polls: Rc<Cell<usize>>) -> impl Future<Output = ()> {
        let mut first = true;
        std::future::poll_fn(move |cx| {
            polls.set(polls.get() + 1);
            if first {
                first = false;
                cx.waker().wake_by_ref();
                let by_value = cx.waker().clone();
                by_value.wake();
                let clone = cx.waker().clone();
                clone.wake_by_ref();
                // End the poll here to see the one queue entry the three wakes made.
                request_turn();
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
    }
    let owner = Owner::new();
    let polls = Rc::new(Cell::new(0));
    owner.spawn(wake_three_ways(polls.clone()));
    assert_eq!(owner.poll(), Ok(1));
    unsafe {
        assert_eq!((*owner.0).incoming.len(), 1);
    }
    assert_eq!(owner.poll(), Ok(1));
    assert_eq!(polls.get(), 2);
    assert_eq!(owner.counts(), (0, 0));
}

#[test]
fn borrowed_waker_survives_scoped_join_during_poll_unwind() {
    struct SignalOnUnwind(std::sync::mpsc::Sender<()>);
    impl Drop for SignalOnUnwind {
        fn drop(&mut self) {
            assert!(std::thread::panicking());
            self.0.send(()).unwrap();
        }
    }
    #[switch_runtime::task]
    fn unwind_with_foreign_use(observed: Arc<AtomicUsize>) -> impl Future<Output = ()> {
        std::future::poll_fn(move |cx| {
            let (send, receive) = std::sync::mpsc::channel();
            let waker = cx.waker();
            std::thread::scope(|scope| {
                let observed = &observed;
                scope.spawn(move || {
                    receive.recv().unwrap();
                    waker.wake_by_ref();
                    drop(waker.clone());
                    observed.fetch_add(1, Ordering::SeqCst);
                });
                let _signal = SignalOnUnwind(send);
                panic_any(314usize);
            })
        })
    }
    let owner = Owner::new();
    let observed = Arc::new(AtomicUsize::new(0));
    owner.spawn(unwind_with_foreign_use(observed.clone()));
    let payload = catch_unwind(AssertUnwindSafe(|| owner.poll())).unwrap_err();
    assert_eq!(payload.downcast_ref::<usize>(), Some(&314));
    assert_eq!(observed.load(Ordering::SeqCst), 1);
    unsafe {
        assert_eq!((*owner.0).lifecycle, Lifecycle::Panicked);
        assert!(!(*owner.0).active);
        assert!((*owner.0).incoming.is_empty());
    }
    owner.cleanup();
    assert_eq!(owner.counts(), (0, 0));
}
