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
                assert!(flags & DEFERRED == 0 || flags & QUEUED != 0);
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
            assert!((*this).incoming.len() + (*this).deferred.len() <= allocated);
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

struct Record {
    header: Cell<*const TaskHeader>,
    waker: Cell<Option<StoredWaker>>,
    polls: Cell<usize>,
    drops: Cell<usize>,
    due: Cell<bool>,
}

type Records = Rc<RefCell<Vec<Rc<Record>>>>;

struct Probe {
    record: Rc<Record>,
    records: Records,
    behavior: Behavior,
}

impl Future for Probe {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.record.polls.set(self.record.polls.get() + 1);
        self.record.due.set(false);
        let waker = StoredWaker::new(cx);
        match self.behavior {
            Behavior::Park => Poll::Pending,
            Behavior::Complete => Poll::Ready(()),
            Behavior::SelfWake if self.record.polls.get() == 1 => {
                waker.wake();
                self.record.due.set(true);
                Poll::Pending
            }
            Behavior::SelfWake => {
                waker.wake();
                Poll::Ready(())
            }
            Behavior::WakeOther(index) => {
                waker.wake_deferred();
                self.record.due.set(true);
                let records = self.records.borrow();
                let target = &records[index];
                let target_waker = target.waker.get().unwrap();
                target_waker.wake_deferred();
                target_waker.wake();
                for current in records.iter().filter(|current| current.header == target.header) {
                    if current.drops.get() == 0 {
                        current.due.set(true);
                    }
                }
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
    Poll,
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

fn apply(owner: &Owner, records: &Records, event: &Event) {
    let root = owner.0;
    match *event {
        Event::Spawn(behavior) => {
            let record = Rc::new(Record {
                header: Cell::new(std::ptr::null()),
                waker: Cell::new(None),
                polls: Cell::new(0),
                drops: Cell::new(0),
                due: Cell::new(true),
            });
            let task = Probe {
                record: record.clone(),
                records: records.clone(),
                behavior,
            };
            assert!(unsafe { Executor::spawn(root, probe(task), None) }.is_ok());
            unsafe {
                let header = *(*root).incoming.last().unwrap();
                record.header.set(header);
                let waker = Waker::from_raw(crate::waker::raw(header));
                record.waker.set(Some(StoredWaker::new(&Context::from_waker(&waker))));
            }
            records.borrow_mut().push(record);
        }
        Event::Poll => {
            let expected: Vec<_> = records.borrow().iter()
                .filter(|task| task.drops.get() == 0 && task.due.get())
                .map(|task| (task.clone(), task.polls.get())).collect();
            unsafe {
                Executor::undefer(root);
                Executor::check(root);
                Executor::poll(root);
            }
            for (task, before) in expected {
                assert_eq!(task.polls.get(), before + 1, "lost wake");
            }
        }
        Event::Wake(index) | Event::Defer(index) => {
            let records = records.borrow();
            let task = &records[index];
            let waker = task.waker.get().unwrap();
            if matches!(event, Event::Wake(_)) {
                waker.wake();
            } else {
                waker.wake_deferred();
            }
            // Stale handles may name a reused slot. Mark its current future.
            for current in records.iter().filter(|current| current.header == task.header) {
                if current.drops.get() == 0 {
                    current.due.set(true);
                }
            }
        }
        Event::Shutdown => unsafe { Executor::cleanup(root) },
    }
    unsafe { Executor::check(root) };
    for task in records.borrow().iter() {
        assert!(task.drops.get() <= 1);
    }
}

#[test]
fn executor_state_walk() {
    let (seeds, steps) = if cfg!(miri) { (3, 20) } else { (384, 60) };
    for seed in 1..=seeds {
        let owner = Owner(Box::into_raw(Box::new(Executor::with_capacity(0, 0, 0))));
        let records = Rc::new(RefCell::new(Vec::new()));
        let mut events = Vec::new();
        let result = catch_unwind(AssertUnwindSafe(|| {
            let mut rng = seed;
            let prefix = [
                Event::Spawn(Behavior::WakeOther(1)),
                Event::Spawn(Behavior::Park),
                Event::Poll,
                Event::Defer(1),
                Event::Poll,
            ];
            let random_steps = steps - prefix.len();
            for event in prefix {
                events.push(event);
                apply(&owner, &records, events.last().unwrap());
            }
            let shutdown_at = random_steps - 4 + (random(&mut rng) as usize % 4);
            for step in 0..random_steps {
                let count = records.borrow().len();
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
                    match random(&mut rng) % 7 {
                        0..=2 => Event::Spawn(match random(&mut rng) % 4 {
                            0 => Behavior::Park,
                            1 => Behavior::Complete,
                            2 => Behavior::SelfWake,
                            _ => Behavior::WakeOther(index),
                        }),
                        3 | 4 => Event::Poll,
                        5 => Event::Wake(index),
                        _ => Event::Defer(index),
                    }
                };
                events.push(event);
                apply(&owner, &records, events.last().unwrap());
            }
            for task in records.borrow().iter() {
                assert_eq!(task.drops.get(), 1);
            }
        }));
        if let Err(payload) = result {
            eprintln!("seed={seed}, events={events:?}");
            resume_unwind(payload);
        }
    }
}
