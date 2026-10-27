use super::*;
use crate::intrusive::NIL;
use crate::task::TaskHeader;
use crate::timer_registration::Token;
use std::cell::RefCell;

fn report(
    polled: usize,
    completed: usize,
    expired: usize,
    ready: usize,
    live: usize,
) -> PollReport {
    PollReport {
        tasks_polled: polled,
        tasks_completed: completed,
        timers_expired: expired,
        ready_tasks: ready,
        live_tasks: live,
    }
}
struct TimedTask {
    root: *mut Executor,
    deadline: u64,
    registrations: usize,
    tokens: Rc<RefCell<Vec<Token>>>,
    header: Rc<Cell<*mut TaskHeader>>,
    polls: Rc<Cell<usize>>,
    drops: Rc<Cell<usize>>,
}
impl TimedTask {
    fn new(runtime: &Runtime, deadline: u64, registrations: usize) -> Self {
        Self {
            root: core(runtime),
            deadline,
            registrations,
            tokens: Rc::new(RefCell::new(Vec::new())),
            header: Rc::new(Cell::new(std::ptr::null_mut())),
            polls: Rc::new(Cell::new(0)),
            drops: Rc::new(Cell::new(0)),
        }
    }
}
impl Future for TimedTask {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.polls.set(self.polls.get() + 1);
        let header = crate::waker::task_from_waker(cx.waker()).as_ptr();
        self.header.set(header);
        unsafe {
            if (*self.root).timers.current_tick() >= self.deadline {
                return Poll::Ready(());
            }
            if self.tokens.borrow().is_empty() {
                for _ in 0..self.registrations {
                    let token = (*self.root).timers.register(header, self.deadline).unwrap();
                    self.tokens.borrow_mut().push(token);
                }
            }
        }
        Poll::Pending
    }
}
impl Drop for TimedTask {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
        if !self.header.get().is_null() {
            unsafe {
                assert_eq!((*self.header.get()).reg_head, NIL);
            }
        }
    }
}

#[test]
fn initial_equal_repeated_and_full_range_ticks_leave_empty_runtime_idle() {
    for tick in [0, 123, u64::MAX] {
        let mut runtime = Runtime::new(tick);
        for now in [tick, tick, u64::MAX, u64::MAX] {
            let r = runtime.poll(now).unwrap();
            assert_eq!(r, PollReport::default());
        }
    }
}

#[test]
fn regressed_time_preserves_clock_timer_and_task_state() {
    let mut runtime = Runtime::new(100);
    let task = TimedTask::new(&runtime, 110, 2);
    let polls = task.polls.clone();
    let drops = task.drops.clone();
    runtime.spawner().spawn(task).unwrap();
    assert_eq!(runtime.poll(100).unwrap(), report(1, 0, 0, 0, 1));
    for now in [99, 0] {
        assert_eq!(
            runtime.poll(now),
            Err(RuntimeError::TimeRegressed {
                current: 100,
                supplied: now
            })
        );
        assert_eq!(unsafe { (*core(&runtime)).timers.current_tick() }, 100);
        assert_eq!((polls.get(), drops.get()), (1, 0));
    }
    assert_eq!(runtime.poll(100).unwrap(), report(0, 0, 0, 0, 1));
    assert_eq!(runtime.poll(110).unwrap(), report(1, 1, 2, 0, 0));
    assert_eq!((polls.get(), drops.get()), (2, 1));
}

#[test]
fn spawned_task_enters_current_snapshot_and_future_spawn_selfwake_enter_next() {
    struct Parent {
        spawner: Spawner,
        step: usize,
        ran: Rc<Cell<usize>>,
    }
    impl Future for Parent {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.step == 0 {
                self.step = 1;
                let ran = self.ran.clone();
                self.spawner
                    .spawn(async move {
                        ran.set(ran.get() + 1);
                    })
                    .unwrap();
                cx.waker().wake_by_ref();
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        }
    }
    let mut runtime = Runtime::new(7);
    let spawner = runtime.spawner();
    let ran = Rc::new(Cell::new(0));
    let parent = Parent {
        spawner: spawner.clone(),
        step: 0,
        ran: ran.clone(),
    };
    spawner.spawn(parent).unwrap();
    assert_eq!(runtime.poll(7).unwrap(), report(1, 0, 0, 2, 2));
    assert_eq!(ran.get(), 0);
    assert_eq!(runtime.poll(7).unwrap(), report(2, 2, 0, 0, 0));
    assert_eq!(ran.get(), 1);
}

#[test]
fn terminal_selfwake_claim_is_not_ready_and_drains_with_zero_future_polls() {
    struct ReadyWake(Rc<Cell<usize>>);
    impl Future for ReadyWake {
        type Output = ();
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            self.0.set(self.0.get() + 1);
            cx.waker().wake_by_ref();
            Poll::Ready(())
        }
    }
    let mut runtime = Runtime::new(0);
    let polls = Rc::new(Cell::new(0));
    runtime.spawner().spawn(ReadyWake(polls.clone())).unwrap();
    assert_eq!(runtime.poll(0).unwrap(), report(1, 1, 0, 0, 0));
    assert_eq!(runtime.poll(0).unwrap(), PollReport::default());
    assert_eq!(polls.get(), 1);
    assert_eq!(runtime.poll(0).unwrap(), PollReport::default());
}

#[test]
fn closed_runtime_reports_closed_for_regressed_and_equal_ticks() {
    let mut runtime = Runtime::new(u64::MAX);
    runtime.shutdown();
    for now in [0, u64::MAX] {
        assert_eq!(runtime.poll(now), Err(RuntimeError::Closed));
    }
}
