use super::*;
use crate::intrusive::NIL;
use crate::timer_registration::Token;
use std::cell::RefCell;

#[derive(Clone, Copy)]
enum Finish {
    Ready,
    Pending,
    Panic,
}

task! {
    fn registered(task: Registered) -> impl Future<Output = ()> {
        task
    }
}

struct Registered {
    owner: *mut Executor,
    header: *const TaskHeader,
    tokens: Rc<RefCell<Vec<Token>>>,
    drops: Rc<Cell<usize>>,
    finish: Finish,
    self_wake: bool,
    panic_drop: bool,
    registrations: usize,
}
impl Registered {
    fn new(owner: &Owner, finish: Finish) -> Self {
        Self {
            owner: owner.0,
            header: std::ptr::null(),
            tokens: Rc::new(RefCell::new(Vec::new())),
            drops: Rc::new(Cell::new(0)),
            finish,
            self_wake: false,
            panic_drop: false,
            registrations: 2,
        }
    }
}
impl Future for Registered {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let current = unsafe { (*self.owner).current };
        assert_eq!(cx.waker().data(), current.cast());
        self.header = current;
        if self.tokens.borrow().is_empty() {
            for i in 0..self.registrations {
                let token = unsafe {
                    (*self.owner)
                        .timers
                        .register(current, 10 + i as u64)
                        .unwrap()
                };
                self.tokens.borrow_mut().push(token);
            }
        }
        if self.self_wake {
            cx.waker().wake_by_ref();
        }
        match self.finish {
            Finish::Ready => Poll::Ready(()),
            Finish::Pending => Poll::Pending,
            Finish::Panic => panic_any(1234usize),
        }
    }
}
impl Drop for Registered {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
        if !self.header.is_null() {
            unsafe {
                assert_eq!(TaskHeader::reg_head(self.header).get(), NIL);
                for &token in self.tokens.borrow().iter() {
                    assert!(!(*self.owner).timers.cancel(token));
                }
            }
        }
        if self.panic_drop {
            panic_any(5678usize);
        }
    }
}

#[test]
fn ready_retires_real_subscriptions_before_drop_with_or_without_queue_claim() {
    for self_wake in [false, true] {
        let owner = Owner::new();
        let mut future = Registered::new(&owner, Finish::Ready);
        future.self_wake = self_wake;
        let drops = future.drops.clone();
        let tokens = future.tokens.clone();
        owner.spawn(registered(future));
        assert!(owner.poll());
        assert_eq!(drops.get(), 1);
        assert_eq!(owner.counts(), (0, usize::from(self_wake)));
        for &token in tokens.borrow().iter() {
            assert!(!unsafe { (*owner.0).timers.cancel(token) });
        }
        assert_eq!(owner.poll(), self_wake);
        assert_eq!(owner.counts(), (0, 0));
    }
}

#[test]
fn interrupted_poll_retires_before_unwind_without_dropping_future() {
    let owner = Owner::new();
    let mut future = Registered::new(&owner, Finish::Panic);
    future.self_wake = true;
    let drops = future.drops.clone();
    let tokens = future.tokens.clone();
    owner.spawn(registered(future));
    let payload = catch_unwind(AssertUnwindSafe(|| owner.poll())).unwrap_err();
    assert_eq!(payload.downcast_ref::<usize>(), Some(&1234));
    assert_eq!(drops.get(), 0);
    assert_eq!(owner.counts(), (0, 1));
    for &token in tokens.borrow().iter() {
        assert!(!unsafe { (*owner.0).timers.cancel(token) });
    }
    owner.cleanup();
    assert_eq!(drops.get(), 1);
    assert_eq!(owner.counts(), (0, 0));
}

#[test]
fn shutdown_retires_pending_and_expired_before_any_drop_and_survives_drop_panic() {
    for expired in [false, true] {
        for panic_drop in [false, true] {
            let owner = Owner::new();
            let first = Registered::new(&owner, Finish::Pending);
            let mut second = Registered::new(&owner, Finish::Pending);
            second.panic_drop = panic_drop;
            let drops = [first.drops.clone(), second.drops.clone()];
            let tokens = [first.tokens.clone(), second.tokens.clone()];
            owner.spawn(registered(first));
            owner.spawn(registered(second));
            owner.poll();
            if expired {
                unsafe {
                    (*owner.0).timers.advance_to(11).unwrap();
                    loop {
                        let task = (*owner.0).timers.pop_expired();
                        let Some(task) = task else {
                            break;
                        };
                        Executor::wake(task);
                    }
                }
            }
            let result = catch_unwind(AssertUnwindSafe(|| owner.cleanup()));
            assert_eq!(result.is_err(), panic_drop);
            if let Err(payload) = result {
                assert_eq!(payload.downcast_ref::<usize>(), Some(&5678));
            }
            for ts in &tokens {
                for &token in ts.borrow().iter() {
                    assert!(!unsafe { (*owner.0).timers.cancel(token) });
                }
            }
            owner.cleanup();
            assert_eq!(owner.counts(), (0, 0));
            assert!(drops.iter().all(|count| count.get() == 1));
        }
    }
}

#[test]
fn registration_growth_during_poll_preserves_future_and_header_aliases() {
    let owner = Owner::new();
    let mut future = Registered::new(&owner, Finish::Ready);
    future.registrations = 256;
    let drops = future.drops.clone();
    owner.spawn(registered(future));
    assert!(owner.poll());
    assert_eq!(drops.get(), 1);
    assert_eq!(owner.counts(), (0, 0));
}
