use super::*;
use crate::StoredWaker;
use std::cell::{Cell, RefCell};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

impl Executor {
    unsafe fn check(this: *mut Self) {
        unsafe {
            assert!((*this).run_queue.is_empty());
            let slots: Vec<_> = Self::slots(this).collect();
            let queued: Vec<_> = (*this).incoming.iter()
                .chain(&(*this).deferred).copied().collect();
            let mut allocated = (*this).spawning;
            let mut live = 0;
            // reserve leaves 0; commit sets TAKEN | LIVE | QUEUED. Poll clears
            // QUEUED | DEFERRED, terminal clears LIVE, and drop sets DROP_STARTED.
            // release clears the whole byte.
            let legal = [
                0,
                TAKEN | LIVE,
                TAKEN | LIVE | QUEUED,
                TAKEN | LIVE | QUEUED | DEFERRED,
                TAKEN,
                TAKEN | DROP_STARTED,
                TAKEN | QUEUED,
                TAKEN | QUEUED | DROP_STARTED,
                TAKEN | QUEUED | DEFERRED,
                TAKEN | QUEUED | DEFERRED | DROP_STARTED,
            ];
            for &header in &slots {
                let flags = TaskHeader::flags(header).get();
                assert!(legal.contains(&flags), "illegal flags {flags:#x}");
                assert_eq!(
                    queued.iter().filter(|&&entry| entry == header).count(),
                    usize::from(flags & QUEUED != 0)
                );
                allocated += usize::from(flags != 0);
                live += usize::from(flags & LIVE != 0);
            }
            for header in queued {
                assert!(slots.contains(&header));
            }
            assert_eq!((*this).allocated, allocated);
            assert_eq!((*this).task_count, live);
            for capacity in [
                (*this).incoming.capacity(),
                (*this).deferred.capacity(),
                (*this).run_queue.capacity(),
            ] {
                assert!(allocated <= capacity, "allocated {allocated}, capacity {capacity}");
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Behavior {
    Park,
    Complete,
    SelfWake,
    WakeOther(usize),
}

/// The model's view of one live task's wake.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Pending {
    None,
    /// Woken eagerly: runs in the next round.
    Eager,
    /// Woken deferred and not yet undeferred: runs only after the next `undefer`.
    Deferred,
    /// In the current round's snapshot: polled exactly once in this round.
    Round,
}

/// What the harness is running: one round without `undefer`, or a whole turn
/// (`undefer`, then rounds until nothing is eagerly woken or the budget is spent).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum Mode {
    #[default]
    Idle,
    Round,
    Turn,
}

struct Record {
    header: Cell<*const TaskHeader>,
    waker: Cell<Option<StoredWaker>>,
    polls: Cell<usize>,
    drops: Cell<usize>,
    pending: Cell<Pending>,
}

#[derive(Default)]
struct Model {
    records: RefCell<Vec<Rc<Record>>>,
    mode: Cell<Mode>,
}

impl Model {
    /// The live task in `header`'s slot; stale handles may name a reused slot.
    fn live(&self, header: *const TaskHeader) -> Option<Rc<Record>> {
        self.records.borrow().iter().find(|record| record.header.get() == header && record.drops.get() == 0).cloned()
    }

    fn wake(&self, header: *const TaskHeader) {
        if let Some(record) = self.live(header) {
            // A deferred task moves to the next round (upgrade); one in the
            // current snapshot is still polled once in it.
            if matches!(record.pending.get(), Pending::None | Pending::Deferred) {
                record.pending.set(Pending::Eager);
            }
        }
    }

    fn defer(&self, header: *const TaskHeader) {
        if let Some(record) = self.live(header)
            && record.pending.get() == Pending::None
        {
            record.pending.set(Pending::Deferred);
        }
    }

    /// A task's poll: it must be due in what the harness runs.
    fn polled(&self, record: &Record) {
        let pending = record.pending.get();
        let due = match self.mode.get() {
            Mode::Round => pending == Pending::Round,
            Mode::Turn => pending == Pending::Eager,
            Mode::Idle => false,
        };
        assert!(due, "spurious poll: {:?} task polled in {:?}", pending, self.mode.get());
        record.pending.set(Pending::None);
        record.polls.set(record.polls.get() + 1);
    }

    fn live_with(&self, pending: Pending) -> usize {
        self.records.borrow().iter().filter(|record| record.drops.get() == 0 && record.pending.get() == pending).count()
    }
}

struct Probe {
    record: Rc<Record>,
    model: Rc<Model>,
    behavior: Behavior,
}

impl Future for Probe {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let model = &self.model;
        model.polled(&self.record);
        let own = self.record.header.get();
        let waker = StoredWaker::new(cx);
        match self.behavior {
            Behavior::Park => Poll::Pending,
            Behavior::Complete => Poll::Ready(()),
            Behavior::SelfWake if self.record.polls.get() == 1 => {
                waker.wake();
                model.wake(own);
                Poll::Pending
            }
            Behavior::SelfWake => {
                waker.wake();
                model.wake(own);
                Poll::Ready(())
            }
            Behavior::WakeOther(index) => {
                waker.wake_deferred();
                model.defer(own);
                let target = model.records.borrow()[index].clone();
                let target_waker = target.waker.get().unwrap();
                target_waker.wake_deferred();
                model.defer(target.header.get());
                target_waker.wake();
                model.wake(target.header.get());
                Poll::Pending
            }
        }
    }
}

impl Drop for Probe {
    fn drop(&mut self) {
        self.record.drops.set(self.record.drops.get() + 1);
    }
}

#[switch::task]
fn probe(task: Probe) -> impl Future<Output = ()> {
    task
}

struct Owner(*mut Executor);

impl Drop for Owner {
    fn drop(&mut self) {
        unsafe {
            Executor::cleanup(self.0);
            drop(Box::from_raw(self.0));
        }
    }
}

#[derive(Debug)]
enum Event {
    Spawn(Behavior),
    /// One round without `undefer`.
    Round,
    /// A runtime turn's scheduling: `undefer`, then rounds.
    Turn,
    Wake(usize),
    Defer(usize),
    Shutdown,
}

fn random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn apply(owner: &Owner, model: &Rc<Model>, event: &Event) {
    let root = owner.0;
    match *event {
        Event::Spawn(behavior) => {
            let record = Rc::new(Record {
                header: Cell::new(std::ptr::null()),
                waker: Cell::new(None),
                polls: Cell::new(0),
                drops: Cell::new(0),
                pending: Cell::new(Pending::Eager),
            });
            let task = Probe {
                record: record.clone(),
                model: model.clone(),
                behavior,
            };
            assert!(unsafe { Executor::spawn(root, probe(task), None) }.is_ok());
            unsafe {
                let header = *(*root).incoming.last().unwrap();
                record.header.set(header);
                let waker = Waker::from_raw(crate::waker::raw(header));
                record.waker.set(Some(StoredWaker::new(&Context::from_waker(&waker))));
            }
            model.records.borrow_mut().push(record);
        }
        Event::Round => {
            for record in model.records.borrow().iter() {
                if record.drops.get() == 0 && record.pending.get() == Pending::Eager {
                    record.pending.set(Pending::Round);
                }
            }
            model.mode.set(Mode::Round);
            unsafe { Executor::poll(root) };
            model.mode.set(Mode::Idle);
            assert_eq!(model.live_with(Pending::Round), 0, "lost wake: a task of the round was not polled");
        }
        Event::Turn => {
            for record in model.records.borrow().iter() {
                if record.drops.get() == 0 && record.pending.get() == Pending::Deferred {
                    record.pending.set(Pending::Eager);
                }
            }
            model.mode.set(Mode::Turn);
            let polled = unsafe {
                Executor::check_turn(root);
                let mut turn = Executor::enter_turn(root);
                Executor::undefer(root);
                let polled = Executor::poll_batch(&mut turn);
                turn.finish();
                polled
            };
            model.mode.set(Mode::Idle);
            if polled < POLL_BUDGET {
                assert_eq!(model.live_with(Pending::Eager), 0, "lost wake: an eager task did not run in the turn");
            }
        }
        Event::Wake(index) | Event::Defer(index) => {
            let record = model.records.borrow()[index].clone();
            let waker = record.waker.get().unwrap();
            if matches!(event, Event::Wake(_)) {
                waker.wake();
                model.wake(record.header.get());
            } else {
                waker.wake_deferred();
                model.defer(record.header.get());
            }
        }
        Event::Shutdown => unsafe { Executor::cleanup(root) },
    }
    unsafe { Executor::check(root) };
    for record in model.records.borrow().iter() {
        assert!(record.drops.get() <= 1);
    }
}

#[test]
fn executor_state_walk() {
    let (seeds, steps) = if cfg!(miri) { (3, 20) } else { (384, 60) };
    for seed in 1..=seeds {
        let owner = Owner(Box::into_raw(Box::new(Executor::with_capacity(0, 0, 0))));
        let model = Rc::new(Model::default());
        let mut events = Vec::new();
        let result = catch_unwind(AssertUnwindSafe(|| {
            let mut rng = seed;
            let prefix = [
                Event::Spawn(Behavior::WakeOther(1)),
                Event::Spawn(Behavior::Park),
                Event::Turn,
                Event::Defer(1),
                Event::Round,
                Event::Turn,
            ];
            let random_steps = steps - prefix.len();
            for event in prefix {
                events.push(event);
                apply(&owner, &model, events.last().unwrap());
            }
            let shutdown_at = random_steps - 4 + (random(&mut rng) as usize % 4);
            for step in 0..random_steps {
                let count = model.records.borrow().len();
                let index = random(&mut rng) as usize % count;
                let event = if step == shutdown_at {
                    Event::Shutdown
                } else if step > shutdown_at {
                    if random(&mut rng) & 1 == 0 {
                        Event::Wake(index)
                    } else {
                        Event::Defer(index)
                    }
                } else {
                    match random(&mut rng) % 8 {
                        0..=2 => Event::Spawn(match random(&mut rng) % 4 {
                            0 => Behavior::Park,
                            1 => Behavior::Complete,
                            2 => Behavior::SelfWake,
                            _ => Behavior::WakeOther(index),
                        }),
                        3 => Event::Round,
                        4 => Event::Turn,
                        5 => Event::Wake(index),
                        _ => Event::Defer(index),
                    }
                };
                events.push(event);
                apply(&owner, &model, events.last().unwrap());
            }
            for record in model.records.borrow().iter() {
                assert_eq!(record.drops.get(), 1);
            }
        }));
        if let Err(payload) = result {
            eprintln!("seed={seed}, events={events:?}");
            resume_unwind(payload);
        }
    }
}
