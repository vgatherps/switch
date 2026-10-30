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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Event(Where, Action);

/// What the executed actions say the runtime must report.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Life {
    Open,
    Panicked,
    Closed,
}

struct State {
    owner: RefCell<Option<Runtime>>,
    control: Rc<Control>,
    mailbox: *const Mailbox,
    /// A foreign-thread handle for the probe `target`.
    remote: Cell<Option<RemoteWaker>>,
    target: Cell<Option<usize>>,
    target_polls: Cell<usize>,
    next_probe: Cell<usize>,
    plan: Vec<Event>,
    cursor: Cell<usize>,
    armed: Cell<bool>,
    log: RefCell<Vec<String>>,
    drops: RefCell<Vec<Rc<Cell<usize>>>>,
    pending_panic: Cell<Option<usize>>,
    first_panic: Cell<Option<usize>>,
    first_escaped: Cell<Option<usize>>,
    life: Cell<Life>,
    /// A foreign wake of `target` is queued: the next turn must poll it.
    remote_pending: Cell<bool>,
    /// The harness is inside a runtime poll.
    polling: Cell<bool>,
}

impl State {
    fn close(&self) {
        self.life.set(Life::Closed);
        self.remote_pending.set(false);
    }

    /// The runtime reports what the executed actions say; once Closed, it takes no work.
    fn check_life(&self) {
        let expected = match self.life.get() {
            Life::Open => None,
            Life::Panicked => Some(Unavailable::Panicked),
            Life::Closed => Some(Unavailable::Closed),
        };
        assert_eq!(self.control.open_core().err(), expected, "runtime availability");
        let spawner = Spawner {
            control: self.control.clone(),
        };
        match self.life.get() {
            Life::Open => {}
            Life::Panicked => {
                assert_eq!(spawner.spawn(empty()).unwrap_err().reason, RuntimeError::Panicked);
            }
            Life::Closed => {
                unsafe {
                    assert!(Mailbox::is_closed(self.mailbox), "mailbox open after Closing");
                    assert!(!Mailbox::pending(self.mailbox));
                }
                assert_eq!(spawner.spawn(empty()).unwrap_err().reason, RuntimeError::Closed);
                let root = self.control.root.get();
                if !root.is_null() {
                    let before = unsafe { Executor::queue_lengths(root) };
                    assert!(catch_unwind(AssertUnwindSafe(|| unsafe { Executor::poll(root) })).is_err());
                    assert_eq!(unsafe { Executor::queue_lengths(root) }, before);
                }
            }
        }
    }

    fn counted(self: &Rc<Self>) -> Probe {
        let drops = Rc::new(Cell::new(0));
        self.drops.borrow_mut().push(drops.clone());
        let id = self.next_probe.get();
        self.next_probe.set(id + 1);
        Probe {
            state: self.clone(),
            drops,
            id,
        }
    }

    fn spawn(self: &Rc<Self>) {
        let spawner = Spawner {
            control: self.control.clone(),
        };
        let life = self.life.get();
        let result = spawner.spawn(probe(self.counted(), true));
        match life {
            Life::Open => result.unwrap(),
            Life::Panicked => assert_eq!(result.unwrap_err().reason, RuntimeError::Panicked),
            Life::Closed => assert_eq!(result.unwrap_err().reason, RuntimeError::Closed),
        }
    }

    /// Asserts `operation` is refused with "executor reentrancy" before any change.
    fn refused(&self, operation: impl FnOnce()) {
        let root = self.control.root.get();
        let before = unsafe { Executor::queue_lengths(root) };
        let payload = catch_unwind(AssertUnwindSafe(operation)).unwrap_err();
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"executor reentrancy"));
        assert_eq!(unsafe { Executor::queue_lengths(root) }, before);
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
            self.check_life();
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
                Action::Shutdown => match place {
                    // A poll is running: cleanup would free the task being polled.
                    Where::Poll => self.refused(|| self.control.shutdown()),
                    // A spawn is reserving: the runtime closes now, cleanup when it ends.
                    Where::Factory => {
                        self.control.shutdown();
                        self.close();
                    }
                    // A cleanup is running.
                    Where::Destructor => self.refused(|| self.control.shutdown()),
                },
                Action::DropRuntime => {
                    let runtime = self.owner.borrow_mut().take();
                    if runtime.is_some() && place == Where::Factory {
                        drop(runtime);
                        self.close();
                    } else {
                        drop(runtime);
                    }
                }
                Action::Spawn => self.spawn(),
                Action::RemoteWake => {
                    let remote = self.remote.get().unwrap();
                    std::thread::spawn(move || remote.wake()).join().unwrap();
                    let queued = self.life.get() != Life::Closed;
                    assert_eq!(unsafe { Mailbox::pending(self.mailbox) }, queued);
                    self.remote_pending.set(queued);
                }
                Action::ReentrantPoll => match (self.life.get(), self.polling.get()) {
                    (Life::Open, true) => self.refused(|| unsafe {
                        Executor::poll(self.control.root.get());
                    }),
                    (Life::Open, false) => {
                        self.polling.set(true);
                        unsafe { Executor::poll(self.control.root.get()) };
                        self.polling.set(false);
                    }
                    _ => {}
                },
            }
            self.check_life();
        }
    }

    /// Runs one harness step that `driver` started. A planned panic must come out of it, and a
    /// panic that leaves a runtime poll marks an open runtime Panicked.
    fn caught(&self, driver: Where, operation: impl FnOnce()) {
        let result = catch_unwind(AssertUnwindSafe(operation));
        let unwound = self.polling.replace(false);
        match (result, self.pending_panic.take()) {
            (Ok(()), None) => {}
            (Err(payload), Some(expected)) => {
                assert_eq!(payload.downcast_ref::<usize>(), Some(&expected));
                if self.first_escaped.get().is_none() {
                    self.first_escaped.set(Some(expected));
                }
                if driver == Where::Poll && unwound && self.life.get() == Life::Open {
                    self.life.set(Life::Panicked);
                }
            }
            (Err(payload), None) => std::panic::resume_unwind(payload),
            (Ok(()), Some(expected)) => panic!("lost panic {expected}"),
        }
        self.check_life();
    }
}

struct Probe {
    state: Rc<State>,
    drops: Rc<Cell<usize>>,
    id: usize,
}

impl Future for Probe {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.state.target.get().is_none() {
            self.state.target.set(Some(self.id));
            self.state.remote.set(Some(RemoteWaker::new(cx)));
        }
        if self.state.target.get() == Some(self.id) {
            self.state.target_polls.set(self.state.target_polls.get() + 1);
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
            assert_eq!(self.state.life.get(), Life::Closed, "a cleanup runs on an open runtime");
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

/// Plans this harness cannot run in-process. The aborting ones are checked to abort, in a
/// child process each, by `lifecycle_matrix_exclusions_abort`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Excluded {
    /// A task drops the Runtime during a runtime poll. Safe code cannot: `Runtime::poll` holds
    /// `&mut Runtime` (here the owner's `RefCell` is borrowed).
    DropDuringPoll,
    /// A task destructor drops the Runtime while it is still owned during a cleanup (an
    /// explicit shutdown, or one a factory deferred). Dropping the Runtime inside its own
    /// cleanup aborts: the drop's cleanup is refused, then retried while unwinding.
    AbortDropInCleanup,
    /// A task factory inside a poll shuts the runtime down, so cleanup is deferred to the end of
    /// a spawn inside that poll. Safe code cannot (the factory needs the polled `Runtime`); here
    /// the factory calls `Control::shutdown` and the process aborts: the deferred cleanup is
    /// refused and retried while unwinding.
    AbortShutdownInPollFactory,
    /// Two task destructors panic while `Runtime::drop` cleans up: the second panic happens
    /// while the first unwinds and aborts, as for any drop.
    AbortTwoDestructorPanics,
}

impl Excluded {
    fn aborts(self) -> bool {
        self != Excluded::DropDuringPoll
    }
}

fn excluded(plan: &[Event], drop_cleanup: bool) -> Option<Excluded> {
    // The first event runs in a poll unless the runtime closed or panicked before it.
    let second_in_poll = match plan.first() {
        Some(Event(Where::Poll, action)) => *action != Action::Panic,
        Some(Event(Where::Factory, action)) => !matches!(action, Action::Shutdown | Action::DropRuntime),
        _ => false,
    };
    if matches!(plan.first(), Some(Event(Where::Poll, Action::DropRuntime)))
        || (second_in_poll && matches!(plan.get(1), Some(Event(Where::Poll, Action::DropRuntime))))
    {
        return Some(Excluded::DropDuringPoll);
    }
    // The Runtime is still owned when a cleanup runs: an explicit one, or one deferred by a
    // factory's shutdown. A factory's drop, or `Runtime::drop`, has already taken it.
    let owned_in_cleanup = match plan.first() {
        Some(Event(Where::Factory, Action::Shutdown)) => true,
        Some(Event(Where::Factory, Action::DropRuntime)) => false,
        _ => !drop_cleanup,
    };
    if owned_in_cleanup && plan.contains(&Event(Where::Destructor, Action::DropRuntime)) {
        return Some(Excluded::AbortDropInCleanup);
    }
    match plan {
        [Event(Where::Poll, Action::Spawn), Event(Where::Factory, Action::DropRuntime)] => {
            Some(Excluded::DropDuringPoll)
        }
        [Event(Where::Poll, Action::Spawn), Event(Where::Factory, Action::Shutdown)] => {
            Some(Excluded::AbortShutdownInPollFactory)
        }
        [Event(Where::Destructor, Action::Panic), Event(Where::Destructor, Action::Panic)] if drop_cleanup => {
            Some(Excluded::AbortTwoDestructorPanics)
        }
        _ => None,
    }
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
        target: Cell::new(None),
        target_polls: Cell::new(0),
        next_probe: Cell::new(0),
        plan,
        cursor: Cell::new(0),
        armed: Cell::new(false),
        log: RefCell::new(Vec::new()),
        drops: RefCell::new(Vec::new()),
        pending_panic: Cell::new(None),
        first_panic: Cell::new(None),
        first_escaped: Cell::new(None),
        life: Cell::new(Life::Open),
        remote_pending: Cell::new(false),
        polling: Cell::new(false),
    });
    let result = catch_unwind(AssertUnwindSafe(|| {
        let spawner = Spawner {
            control: state.control.clone(),
        };
        for _ in 0..3 {
            spawner.spawn(probe(state.counted(), false)).unwrap();
        }
        state.owner.borrow_mut().as_mut().unwrap().poll(0).unwrap();
        assert_eq!(state.target_polls.get(), 1);
        state.armed.set(true);
        while let Some(&Event(place, _)) = state.plan.get(state.cursor.get()) {
            let before = state.cursor.get();
            state.caught(place, || match (place, state.life.get()) {
                (Where::Destructor, _) if !state.control.root.get().is_null() => {
                    state.close();
                    if drop_cleanup {
                        let runtime = state.owner.borrow_mut().take();
                        drop(runtime);
                    } else {
                        state.control.shutdown();
                    }
                }
                (Where::Poll, Life::Open) => {
                    // Only a foreign wake, delivered through the mailbox, polls the target.
                    if !state.remote_pending.get() {
                        let remote = state.remote.get().unwrap();
                        std::thread::spawn(move || remote.wake()).join().unwrap();
                        assert!(unsafe { Mailbox::pending(mailbox) });
                    }
                    state.remote_pending.set(false);
                    let polls = state.target_polls.get();
                    state.polling.set(true);
                    state.owner.borrow_mut().as_mut().unwrap().poll(0).unwrap();
                    state.polling.set(false);
                    assert_eq!(state.target_polls.get(), polls + 1, "the foreign wake polled its target once");
                }
                (Where::Factory, Life::Open) => state.spawn(),
                (_, life) => {
                    let expected = match life {
                        Life::Panicked => RuntimeError::Panicked,
                        _ => RuntimeError::Closed,
                    };
                    assert_eq!(spawner.spawn(empty()).unwrap_err().reason, expected);
                    if let Some(runtime) = state.owner.borrow_mut().as_mut() {
                        assert_eq!(runtime.poll(0), Err(expected));
                    }
                    state.log.borrow_mut().push(format!("{before}: {place:?} rejected {life:?}"));
                    state.cursor.set(before + 1);
                }
            });
            assert!(state.cursor.get() > before, "event was neither run nor rejected");
        }
        state.caught(Where::Destructor, || {
            let runtime = state.owner.borrow_mut().take();
            if runtime.is_some() {
                state.close();
            }
            drop(runtime);
        });
        assert!(state.control.root.get().is_null());
        state.close();
        state.check_life();
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

const PLACES: [Where; 3] = [Where::Poll, Where::Destructor, Where::Factory];
const ACTIONS: [Action; 7] = [
    Action::Nothing,
    Action::Panic,
    Action::Shutdown,
    Action::DropRuntime,
    Action::Spawn,
    Action::RemoteWake,
    Action::ReentrantPoll,
];

/// Every plan of one or two events, for both cleanup drivers.
fn plans() -> Vec<(Vec<Event>, bool)> {
    let events: Vec<Event> =
        PLACES.iter().flat_map(|&place| ACTIONS.iter().map(move |&action| Event(place, action))).collect();
    let mut plans = Vec::new();
    for drop_cleanup in [false, true] {
        for &first in &events {
            plans.push((vec![first], drop_cleanup));
            for &second in &events {
                plans.push((vec![first, second], drop_cleanup));
            }
        }
    }
    plans
}

#[test]
fn runtime_lifecycle_matrix() {
    for (plan, drop_cleanup) in plans() {
        if excluded(&plan, drop_cleanup).is_none() {
            scenario(plan, drop_cleanup);
        }
    }
}

fn encode(plan: &[Event], drop_cleanup: bool) -> String {
    let index = |event: &Event| {
        let place = PLACES.iter().position(|&place| place == event.0).unwrap();
        let action = ACTIONS.iter().position(|&action| action == event.1).unwrap();
        (place * ACTIONS.len() + action).to_string()
    };
    let mut parts: Vec<String> = plan.iter().map(index).collect();
    parts.push(u8::from(drop_cleanup).to_string());
    parts.join(",")
}

fn decode(text: &str) -> (Vec<Event>, bool) {
    let mut parts: Vec<usize> = text.split(',').map(|part| part.parse().unwrap()).collect();
    let drop_cleanup = parts.pop().unwrap() == 1;
    let plan = parts
        .into_iter()
        .map(|index| Event(PLACES[index / ACTIONS.len()], ACTIONS[index % ACTIONS.len()]))
        .collect();
    (plan, drop_cleanup)
}

/// Each plan excluded as aborting does abort when run, in a child process each.
#[cfg(all(unix, not(miri)))]
#[test]
fn lifecycle_matrix_exclusions_abort() {
    use std::os::unix::process::ExitStatusExt;
    let aborting: Vec<_> = plans()
        .into_iter()
        .filter(|(plan, drop_cleanup)| {
            excluded(plan, *drop_cleanup).is_some_and(Excluded::aborts)
        })
        .collect();
    assert!(!aborting.is_empty());
    for (plan, drop_cleanup) in aborting {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "runtime::state_walk_tests::lifecycle_abort_child", "--ignored", "--nocapture"])
            .env("SWITCH_LIFECYCLE_PLAN", encode(&plan, drop_cleanup))
            .env("RUST_BACKTRACE", "0")
            .output()
            .unwrap();
        assert_eq!(
            output.status.signal(),
            Some(6),
            "plan {plan:?}, drop_cleanup {drop_cleanup}: {}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[cfg(all(unix, not(miri)))]
#[test]
#[ignore = "invoked only by lifecycle_matrix_exclusions_abort, with SWITCH_LIFECYCLE_PLAN"]
fn lifecycle_abort_child() {
    let (plan, drop_cleanup) = decode(&std::env::var("SWITCH_LIFECYCLE_PLAN").unwrap());
    scenario(plan, drop_cleanup);
}
