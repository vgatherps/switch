use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use crate::StoredWaker;
use crate::intrusive::{IntrusiveLinks, Linked, ListHead, ListLinked, NIL, Timed};
use crate::runtime::Runtime;
use crate::timer_wheel::{InsertOutcome, TimerWheel, WheelEntry, WheelPosition};

#[derive(Default)]
struct Entry {
    links: IntrusiveLinks,
    deadline: u64,
    position: WheelPosition,
}

impl Linked for Entry {
    fn links(&self) -> &IntrusiveLinks {
        &self.links
    }

    fn links_mut(&mut self) -> &mut IntrusiveLinks {
        &mut self.links
    }
}

impl Timed for Entry {
    fn deadline(&self) -> u64 {
        self.deadline
    }
}

impl WheelEntry for Entry {
    fn wheel_position(&self) -> WheelPosition {
        self.position
    }
    fn set_wheel_position(&mut self, position: WheelPosition) {
        self.position = position;
    }
}

fn entries(deadlines: &[u64]) -> Vec<Entry> {
    deadlines
        .iter()
        .map(|&deadline| Entry {
            links: IntrusiveLinks::new(),
            deadline,
            ..Entry::default()
        })
        .collect()
}

#[test]
fn wheel_deadline_three_expires_after_deadline_five_is_cancelled() {
    let mut entries = entries(&[3, 5]);
    let mut wheel: TimerWheel = TimerWheel::new(0);
    assert_eq!(wheel.insert(&mut entries, 0), InsertOutcome::Linked);
    assert_eq!(wheel.insert(&mut entries, 1), InsertOutcome::Linked);

    wheel.remove(&mut entries, 1);
    let mut expired = Vec::new();
    wheel.advance_to(&mut entries, 3, &mut expired).unwrap();

    assert_eq!(expired, [0]);
    assert!(!wheel.has_pending());
}

#[test]
fn intrusive_list_initializes_nil_and_pushes_then_pops() {
    let mut entries = entries(&[0]);
    let mut list = ListHead::new();

    assert!(list.is_empty());
    assert_eq!(entries[0].prev(), NIL);
    assert_eq!(entries[0].next(), NIL);

    list.push_back(&mut entries, 0);
    assert_eq!(list.first, 0);
    assert_eq!(list.last, 0);
    assert_eq!(list.pop_front(&mut entries), Some(0));
    assert!(list.is_empty());
    assert_eq!(entries[0].prev(), NIL);
    assert_eq!(entries[0].next(), NIL);
}

struct StoredWake {
    slot: Rc<Cell<Option<StoredWaker>>>,
    polls: Rc<Cell<usize>>,
}

impl Future for StoredWake {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let count = this.polls.get();
        this.polls.set(count + 1);
        if count == 0 {
            // SAFETY: this task remains alive until the second poll below, and
            // the stored waker is used on this executor thread before then.
            this.slot.set(Some(unsafe { StoredWaker::new(cx) }));
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

#[test]
fn wheel_expiry_can_drive_an_existing_stored_executor_waker() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let mut entries = entries(&[3]);
    let mut wheel: TimerWheel = TimerWheel::new(0);
    assert_eq!(wheel.insert(&mut entries, 0), InsertOutcome::Linked);

    let slot = Rc::new(Cell::new(None));
    let polls = Rc::new(Cell::new(0));
    spawner
        .spawn(StoredWake {
            slot: Rc::clone(&slot),
            polls: Rc::clone(&polls),
        })
        .unwrap();
    let first = runtime.poll(0, || {}).unwrap();
    assert_eq!((first.tasks_polled, first.live_tasks), (1, 1));
    assert_eq!(polls.get(), 1);

    let mut expired = Vec::new();
    wheel.advance_to(&mut entries, 3, &mut expired).unwrap();
    assert_eq!(expired, [0]);
    slot.take().expect("first poll stored a wake").wake();
    let report = runtime.poll(0, || {}).unwrap();
    assert_eq!(
        (
            report.tasks_polled,
            report.tasks_completed,
            report.live_tasks
        ),
        (1, 1, 0)
    );
    assert_eq!(polls.get(), 2);
}
