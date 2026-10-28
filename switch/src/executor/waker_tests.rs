use super::*;
use crate::waker::this_thread_tag;
use std::sync::{
    Arc, Barrier,
    atomic::{AtomicUsize, Ordering},
};

fn block_owner(task: *const TaskHeader) -> u16 {
    unsafe { Meta::owner(TaskHeader::meta(task)) }
}

#[switch::task]
fn foreign_ops(ptr: *mut Executor) -> impl Future<Output = ()> {
    std::future::poll_fn(move |cx| {
        let owner_tag = this_thread_tag();
        let barrier = Barrier::new(4);
        let waker = cx.waker();
        std::thread::scope(|scope| {
            let mut joins = Vec::new();
            for operation in 0..4 {
                let barrier = &barrier;
                // Made on the owner thread; only its use crosses.
                let owned = (operation >= 2).then(|| waker.clone());
                joins.push(scope.spawn(move || {
                    assert_ne!(this_thread_tag(), 0);
                    assert_ne!(this_thread_tag(), owner_tag);
                    barrier.wait();
                    let rejected = catch_unwind(AssertUnwindSafe(move || match operation {
                        0 => waker.wake_by_ref(),
                        1 => drop(waker.clone()),
                        2 => owned.unwrap().wake(),
                        _ => drop(owned.unwrap()),
                    }))
                    .unwrap_err();
                    assert_eq!(
                        rejected.downcast_ref::<&str>().copied(),
                        Some("waker used on foreign thread")
                    );
                }));
            }
            for join in joins {
                join.join().unwrap();
            }
        });
        unsafe {
            assert!((*ptr).incoming.is_empty());
            assert_eq!(((*ptr).task_count, (*ptr).allocated), (1, 1));
            assert_eq!(TaskHeader::flags((*ptr).current).get() & !TAKEN, LIVE);
        }
        Poll::Ready(())
    })
}

#[test]
fn foreign_wake_clone_and_drop_reject_without_scheduling() {
    let owner = Owner::new();
    owner.spawn(foreign_ops(owner.0));
    owner.poll();
    assert_eq!(owner.counts(), (0, 0));
}

#[test]
fn runtimes_on_one_thread_share_a_nonzero_tag_and_other_threads_differ() {
    #[switch::task]
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
    #[switch::task]
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
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
    }
    let owner = Owner::new();
    let polls = Rc::new(Cell::new(0));
    owner.spawn(wake_three_ways(polls.clone()));
    owner.poll();
    unsafe {
        assert_eq!((*owner.0).incoming.len(), 1);
    }
    owner.poll();
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
    #[switch::task]
    fn unwind_with_foreign_use(observed: Arc<AtomicUsize>) -> impl Future<Output = ()> {
        std::future::poll_fn(move |cx| {
            let (send, receive) = std::sync::mpsc::channel();
            let waker = cx.waker();
            std::thread::scope(|scope| {
                let observed = &observed;
                scope.spawn(move || {
                    receive.recv().unwrap();
                    assert!(catch_unwind(AssertUnwindSafe(|| waker.wake_by_ref())).is_err());
                    assert!(catch_unwind(AssertUnwindSafe(|| waker.clone())).is_err());
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
