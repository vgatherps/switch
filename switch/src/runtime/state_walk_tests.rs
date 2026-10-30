use super::*;
use crate::RemoteWaker;
use std::cell::RefCell;
use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Where {
    Poll,
    Destructor,
    Factory,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Action {
    Nothing,
    Panic,
    Shutdown,
    DropRuntime,
    Spawn,
    RemoteWake,
    ReentrantPoll,
}

#[derive(Clone, Copy, Debug)]
struct Event(Where, Action);

struct State {
    owner: RefCell<Option<Runtime>>,
    control: Rc<Control>,
    mailbox: *const Mailbox,
    remote: Cell<Option<RemoteWaker>>,
    plan: Vec<Event>,
    cursor: Cell<usize>,
    armed: Cell<bool>,
    log: RefCell<Vec<String>>,
    drops: RefCell<Vec<Rc<Cell<usize>>>>,
    pending_panic: Cell<Option<usize>>,
    first_panic: Cell<Option<usize>>,
    first_escaped: Cell<Option<usize>>,
}

impl State {
    fn closed(&self) {
        if self.control.open_core() != Err(Unavailable::Closed) {
            return;
        }
        unsafe {
            assert!(Mailbox::is_closed(self.mailbox), "mailbox open after Closing");
            assert!(!Mailbox::pending(self.mailbox));
        }
        let spawner = Spawner {
            control: self.control.clone(),
        };
        assert_eq!(spawner.spawn(empty()).unwrap_err().reason, RuntimeError::Closed);
        let root = self.control.root.get();
        if !root.is_null() {
            let before = unsafe { Executor::queue_lengths(root) };
            assert!(catch_unwind(AssertUnwindSafe(|| unsafe { Executor::poll(root) })).is_err());
            assert_eq!(unsafe { Executor::queue_lengths(root) }, before);
        }
    }

    fn counted(self: &Rc<Self>) -> Probe {
        let drops = Rc::new(Cell::new(0));
        self.drops.borrow_mut().push(drops.clone());
        Probe {
            state: self.clone(),
            drops,
        }
    }

    fn spawn(self: &Rc<Self>) {
        let spawner = Spawner {
            control: self.control.clone(),
        };
        let open = self.control.open_core();
        let result = spawner.spawn(probe(self.counted(), true));
        match open {
            Ok(_) => result.unwrap(),
            Err(reason) => {
                assert_eq!(result.unwrap_err().reason, RuntimeError::from(reason));
            }
        }
    }

    fn at(self: &Rc<Self>, place: Where) {
        if !self.armed.get() {
            return;
        }
        while let Some(&Event(next, action)) = self.plan.get(self.cursor.get()) {
            if next != place {
                break;
            }
            let index = self.cursor.get();
            self.cursor.set(index + 1);
            self.log.borrow_mut().push(format!("{index}: {place:?} {action:?}"));
            self.closed();
            match action {
                Action::Nothing => {}
                Action::Panic => {
                    let payload = 100 + index;
                    self.pending_panic.set(Some(payload));
                    if self.first_panic.get().is_none() {
                        self.first_panic.set(Some(payload));
                    }
                    panic_any(payload);
                }
                Action::Shutdown => {
                    let root = self.control.root.get();
                    let reject = !root.is_null() && unsafe {
                        Executor::active(root) && Executor::spawning(root) == 0
                    };
                    let result = catch_unwind(AssertUnwindSafe(|| self.control.shutdown()));
                    if reject {
                        let payload = result.unwrap_err();
                        assert_eq!(payload.downcast_ref::<&str>(), Some(&"executor reentrancy"));
                    } else if let Err(payload) = result {
                        std::panic::resume_unwind(payload);
                    }
                }
                Action::DropRuntime => {
                    let runtime = self.owner.borrow_mut().take();
                    drop(runtime);
                }
                Action::Spawn => self.spawn(),
                Action::RemoteWake => {
                    let remote = self.remote.get().unwrap();
                    std::thread::spawn(move || remote.wake()).join().unwrap();
                    if self.control.open_core() == Err(Unavailable::Closed) {
                        assert!(!unsafe { Mailbox::pending(self.mailbox) });
                    }
                }
                Action::ReentrantPoll => {
                    if let Ok(root) = self.control.open_core() {
                        let active = unsafe { Executor::active(root) };
                        let before = unsafe { Executor::queue_lengths(root) };
                        let result = catch_unwind(AssertUnwindSafe(|| unsafe {
                            Executor::poll(root)
                        }));
                        if active {
                            let payload = result.unwrap_err();
                            assert_eq!(payload.downcast_ref::<&str>(), Some(&"executor reentrancy"));
                            assert_eq!(unsafe { Executor::queue_lengths(root) }, before);
                        } else if let Err(payload) = result {
                            std::panic::resume_unwind(payload);
                        }
                    } else {
                        self.closed();
                    }
                }
            }
            self.closed();
        }
    }

    fn caught(&self, operation: impl FnOnce()) {
        let result = catch_unwind(AssertUnwindSafe(operation));
        match (result, self.pending_panic.take()) {
            (Ok(()), None) => {}
            (Err(payload), Some(expected)) => {
                assert_eq!(payload.downcast_ref::<usize>(), Some(&expected));
                if self.first_escaped.get().is_none() {
                    self.first_escaped.set(Some(expected));
                }
            }
            (Err(payload), None) => std::panic::resume_unwind(payload),
            (Ok(()), Some(expected)) => panic!("lost panic {expected}"),
        }
        self.closed();
    }
}

struct Probe {
    state: Rc<State>,
    drops: Rc<Cell<usize>>,
}

impl Future for Probe {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.state.remote.get().is_none() {
            self.state.remote.set(Some(RemoteWaker::new(cx)));
        }
        self.state.at(Where::Poll);
        Poll::Pending
    }
}

impl Drop for Probe {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
        let root = self.state.control.root.get();
        if !root.is_null() && unsafe {
            Executor::lifecycle(root) == Lifecycle::Closing && Executor::spawning(root) == 0
        } {
            self.state.at(Where::Destructor);
        }
    }
}

#[switch::task]
fn probe(task: Probe, factory: bool) -> impl Future<Output = ()> {
    if factory {
        task.state.at(Where::Factory);
    }
    task
}

#[switch::task]
async fn empty() {}

fn aborts(plan: &[Event], drop_cleanup: bool) -> bool {
    // Skip Runtime drops from an active poll. Its cleanup and the drop's
    // CleanupContinuation both panic. The same applies to a destructor
    // dropping a still-owned Runtime during explicit or deferred shutdown.
    // A poll that spawns a factory which closes the Runtime also aborts:
    // DeferredShutdown and CleanupContinuation both try active cleanup.
    // During Runtime::drop, two cleanup destructors that panic would abort.
    if matches!(plan.first(), Some(Event(Where::Poll, Action::DropRuntime))) {
        return true;
    }
    if matches!(plan.get(1), Some(Event(Where::Poll, Action::DropRuntime))) {
        match plan[0] {
            Event(Where::Poll, action) if action != Action::Panic => return true,
            Event(Where::Factory, action)
                if !matches!(action, Action::Shutdown | Action::DropRuntime) =>
            {
                return true;
            }
            _ => {}
        }
    }
    if (!drop_cleanup || matches!(plan.first(), Some(Event(Where::Factory, Action::Shutdown))))
        && plan.iter().any(|event| {
            matches!(event, Event(Where::Destructor, Action::DropRuntime))
        })
        && !matches!(plan.first(), Some(Event(Where::Factory, Action::DropRuntime)))
    {
        return true;
    }
    if matches!(plan, [
        Event(Where::Poll, Action::Spawn),
        Event(Where::Factory, Action::Shutdown | Action::DropRuntime),
    ]) {
        return true;
    }
    drop_cleanup && matches!(plan, [
        Event(Where::Destructor, Action::Panic),
        Event(Where::Destructor, Action::Panic),
    ])
}

fn scenario(plan: Vec<Event>, drop_cleanup: bool) {
    let runtime = Runtime::with_capacity(0, Capacity {
        tasks: 0,
        timers: 0,
    });
    let control = runtime.control.clone();
    let mailbox = unsafe { Executor::mailbox(control.root.get()) };
    let state = Rc::new(State {
        owner: RefCell::new(Some(runtime)),
        control,
        mailbox,
        remote: Cell::new(None),
        plan,
        cursor: Cell::new(0),
        armed: Cell::new(false),
        log: RefCell::new(Vec::new()),
        drops: RefCell::new(Vec::new()),
        pending_panic: Cell::new(None),
        first_panic: Cell::new(None),
        first_escaped: Cell::new(None),
    });
    let result = catch_unwind(AssertUnwindSafe(|| {
        let spawner = Spawner {
            control: state.control.clone(),
        };
        for _ in 0..3 {
            spawner.spawn(probe(state.counted(), false)).unwrap();
        }
        state.owner.borrow_mut().as_mut().unwrap().poll(0).unwrap();
        let remote = state.remote.get().unwrap();
        std::thread::spawn(move || remote.wake()).join().unwrap();
        assert!(unsafe { Mailbox::pending(mailbox) });
        state.armed.set(true);
        while let Some(&Event(place, _)) = state.plan.get(state.cursor.get()) {
            let before = state.cursor.get();
            state.caught(|| match (place, state.control.open_core()) {
                (Where::Destructor, _) if !state.control.root.get().is_null() => {
                    if drop_cleanup {
                        let runtime = state.owner.borrow_mut().take();
                        drop(runtime);
                    } else {
                        state.control.shutdown();
                    }
                }
                (Where::Poll, Ok(_)) => {
                    remote.wake();
                    state.owner.borrow_mut().as_mut().unwrap().poll(0).unwrap();
                }
                (Where::Factory, Ok(_)) => state.spawn(),
                (_, Err(reason)) => {
                    let expected = RuntimeError::from(reason);
                    assert_eq!(spawner.spawn(empty()).unwrap_err().reason, expected);
                    if let Some(runtime) = state.owner.borrow_mut().as_mut() {
                        assert_eq!(runtime.poll(0), Err(expected));
                    }
                    state.log.borrow_mut().push(format!(
                        "{before}: {place:?} rejected {reason:?}"
                    ));
                    state.cursor.set(before + 1);
                }
                (Where::Destructor, Ok(_)) => unreachable!(),
            });
            assert!(state.cursor.get() > before, "event was neither run nor rejected");
        }
        state.caught(|| {
            let runtime = state.owner.borrow_mut().take();
            drop(runtime);
        });
        assert!(state.control.root.get().is_null());
        state.closed();
        for drops in state.drops.borrow().iter() {
            assert_eq!(drops.get(), 1, "future not dropped exactly once");
        }
        assert_eq!(state.first_escaped.get(), state.first_panic.get());
    }));
    if let Err(payload) = result {
        eprintln!(
            "plan={:?}, drop_cleanup={drop_cleanup}, events={:?}",
            state.plan, state.log.borrow()
        );
        std::panic::resume_unwind(payload);
    }
}

#[test]
fn runtime_lifecycle_matrix() {
    let mut events = Vec::new();
    for place in [Where::Poll, Where::Destructor, Where::Factory] {
        for action in [
            Action::Nothing,
            Action::Panic,
            Action::Shutdown,
            Action::DropRuntime,
            Action::Spawn,
            Action::RemoteWake,
            Action::ReentrantPoll,
        ] {
            events.push(Event(place, action));
        }
    }
    for drop_cleanup in [false, true] {
        for &first in &events {
            for plan in std::iter::once(vec![first])
                .chain(events.iter().map(|&second| vec![first, second]))
            {
                if !aborts(&plan, drop_cleanup) {
                    scenario(plan, drop_cleanup);
                }
            }
        }
    }
}
