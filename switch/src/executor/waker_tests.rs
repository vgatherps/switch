use super::*;
use crate::waker::{StoredWaker, current_task};
use std::sync::{
    Arc, Barrier,
    atomic::{AtomicUsize, Ordering},
};
use std::task::Waker;

#[test]
fn foreign_borrowed_wake_and_helpers_reject_without_scheduling() {
    let owner = Owner::new();
    let ptr = owner.0;
    let owner_tag = unsafe { (*ptr).owner };
    owner.spawn(std::future::poll_fn(move |cx| {
        let barrier = Barrier::new(3);
        let waker = cx.waker();
        std::thread::scope(|scope| {
            let mut joins = Vec::new();
            for operation in 0..3 {
                let barrier = &barrier;
                joins.push(scope.spawn(move || {
                    let foreign_owner = Owner::new();
                    assert_ne!(unsafe { (*foreign_owner.0).owner }, 0);
                    assert_ne!(unsafe { (*foreign_owner.0).owner }, owner_tag);
                    barrier.wait();
                    let rejected = catch_unwind(AssertUnwindSafe(|| {
                        let foreign = Context::from_waker(waker);
                        match operation {
                            0 => waker.wake_by_ref(),
                            1 => current_task(&foreign).wake(),
                            _ => unsafe { StoredWaker::new(&foreign).wake() },
                        }
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
            let task = crate::waker::task_from_waker(waker).as_ptr();
            assert!(!(*task).queued);
            assert_eq!((*task).state, TaskState::Idle);
        }
        Poll::Ready(())
    }));
    owner.poll();
    assert_eq!(owner.counts(), (0, 0));
}

#[test]
fn runtimes_on_one_thread_share_a_nonzero_tag_and_other_threads_differ() {
    let first = Owner::new();
    let second = Owner::new();
    let first_tag = unsafe { (*first.0).owner };
    assert_ne!(first_tag, 0);
    assert_eq!(first_tag, unsafe { (*second.0).owner });
    let other_tag = std::thread::spawn(|| {
        let owner = Owner::new();
        unsafe { (*owner.0).owner }
    })
    .join()
    .unwrap();
    assert_ne!(other_tag, 0);
    assert_ne!(first_tag, other_tag);
}

#[test]
fn local_ordinary_thin_and_stored_wakes_share_one_claim() {
    let owner = Owner::new();
    let mut first = true;
    let polls = Rc::new(Cell::new(0));
    let count = polls.clone();
    owner.spawn(std::future::poll_fn(move |cx| {
        count.set(count.get() + 1);
        if first {
            first = false;
            cx.waker().wake_by_ref();
            current_task(cx).wake();
            unsafe { StoredWaker::new(cx).wake() };
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }));
    owner.poll();
    unsafe {
        assert_eq!((*owner.0).incoming.len(), 1);
    }
    owner.poll();
    assert_eq!(polls.get(), 2);
    assert_eq!(owner.counts(), (0, 0));
}

#[test]
fn clone_and_clone_from_cannot_export_frame_waker() {
    let owner = Owner::new();
    owner.spawn(std::future::poll_fn(|cx| {
        let payload = catch_unwind(AssertUnwindSafe(|| cx.waker().clone())).unwrap_err();
        assert!(
            payload
                .downcast_ref::<&str>()
                .unwrap()
                .contains("Waker::clone()")
        );
        let mut destination = Waker::noop().clone();
        let original_data = destination.data();
        let original_vtable = destination.vtable();
        assert!(catch_unwind(AssertUnwindSafe(|| destination.clone_from(cx.waker()))).is_err());
        assert_eq!(destination.data(), original_data);
        assert!(std::ptr::eq(destination.vtable(), original_vtable));
        destination.wake_by_ref();
        Poll::Ready(())
    }));
    owner.poll();
    assert_eq!(owner.counts(), (0, 0));
}

#[test]
fn borrowed_frame_survives_scoped_join_during_poll_unwind() {
    struct SignalOnUnwind(std::sync::mpsc::Sender<()>);
    impl Drop for SignalOnUnwind {
        fn drop(&mut self) {
            assert!(std::thread::panicking());
            self.0.send(()).unwrap();
        }
    }
    let owner = Owner::new();
    let observed = Arc::new(AtomicUsize::new(0));
    let child_observed = observed.clone();
    owner.spawn(std::future::poll_fn(move |cx| {
        let (send, receive) = std::sync::mpsc::channel();
        let waker = cx.waker();
        std::thread::scope(|scope| {
            let observed = &child_observed;
            scope.spawn(move || {
                receive.recv().unwrap();
                assert!(catch_unwind(AssertUnwindSafe(|| waker.wake_by_ref())).is_err());
                let foreign = Context::from_waker(waker);
                assert!(catch_unwind(AssertUnwindSafe(|| current_task(&foreign))).is_err());
                observed.fetch_add(1, Ordering::SeqCst);
            });
            let _signal = SignalOnUnwind(send);
            panic_any(314usize);
        })
    }));
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
