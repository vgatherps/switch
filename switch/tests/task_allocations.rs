//! T1: after a pool reaches its peak, task operations do not allocate.

// This test uses part of the module.
#[allow(dead_code)]
#[path = "support/counting.rs"]
mod counting;
#[macro_use]
#[path = "support/task_macro.rs"]
mod task_macro;

use counting::{CountingGlobalAlloc, measure, no_alloc};
use std::cell::{Cell, RefCell};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use switch::{Capacity, Runtime};

#[global_allocator]
static ALLOCATOR: CountingGlobalAlloc = CountingGlobalAlloc;

const BULK: usize = 1024;
/// Lower bound of one slab of `Bulky` tasks: 64 slots of more than `BULK` bytes.
const SLAB_BYTES: usize = 64 * BULK;

#[derive(Default)]
struct State {
    polls: Cell<usize>,
    done: Cell<bool>,
    waker: RefCell<Option<Waker>>,
}

struct Bulky {
    state: Rc<State>,
    _bulk: [u8; BULK],
}

impl Future for Bulky {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let state = &self.state;
        state.polls.set(state.polls.get() + 1);
        let mut stored = state.waker.borrow_mut();
        match stored.as_mut() {
            Some(waker) => waker.clone_from(cx.waker()),
            None => *stored = Some(cx.waker().clone()),
        }
        if state.done.get() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

task! {
    fn bulky(state: Rc<State>) -> impl Future<Output = ()> {
        Bulky { state, _bulk: [0; BULK] }
    }
}

fn runtime() -> Runtime {
    Runtime::with_capacity(
        0,
        Capacity {
            tasks: 256,
            timers: 0,
        },
    )
}

#[test]
fn steady_state_task_operations_do_not_allocate() {
    let mut runtime = runtime();
    let spawner = runtime.spawner();
    // Warm-up: reach a peak of 64 live tasks, then finish them all.
    let warm: Vec<_> = (0..64).map(|_| Rc::new(State::default())).collect();
    for state in &warm {
        spawner.spawn(bulky(state.clone())).unwrap();
    }
    runtime.poll(0).unwrap();
    for state in &warm {
        state.done.set(true);
        state.waker.borrow().as_ref().unwrap().wake_by_ref();
    }
    assert_eq!(runtime.poll(0).unwrap().tasks_completed, 64);

    let state = Rc::new(State::default());
    let (token, make) = measure(|| bulky(state.clone()));
    no_alloc(make);
    let (_, spawn) = measure(|| spawner.spawn(token).unwrap());
    no_alloc(spawn);
    let (_, first_poll) = measure(|| runtime.poll(0).unwrap());
    no_alloc(first_poll);
    let waker = state.waker.borrow().clone().unwrap();
    let (clone, clone_stats) = measure(|| waker.clone());
    no_alloc(clone_stats);
    let (_, drop_stats) = measure(|| drop(clone));
    no_alloc(drop_stats);
    let mut target = warm[0].waker.borrow().clone().unwrap();
    let (_, clone_from) = measure(|| target.clone_from(&waker));
    no_alloc(clone_from);
    assert!(target.will_wake(&waker));
    let (_, wake) = measure(|| {
        waker.wake_by_ref();
        target.wake();
    });
    no_alloc(wake);
    let (report, repoll) = measure(|| runtime.poll(0).unwrap());
    no_alloc(repoll);
    assert_eq!(report.tasks_polled, 1);
    state.done.set(true);
    waker.wake_by_ref();
    let (report, completion) = measure(|| runtime.poll(0).unwrap());
    no_alloc(completion);
    assert_eq!(report.tasks_completed, 1);
    let (_, token_drop) = measure(|| drop(bulky(state.clone())));
    no_alloc(token_drop);
    assert_eq!(state.polls.get(), 3);
}

#[test]
fn growth_allocates_only_slabs_vectors_and_runtime_pools() {
    let mut first = runtime();
    let spawner = first.spawner();
    let states: Vec<_> = (0..65).map(|_| Rc::new(State::default())).collect();
    // First spawn on this thread: function pool, runtime pool, one slab.
    let (_, initial) = measure(|| spawner.spawn(bulky(states[0].clone())).unwrap());
    assert!(initial.allocated_bytes >= SLAB_BYTES, "{initial:?}");
    // The rest of the slab allocates nothing.
    for state in &states[1..64] {
        let (_, fill) = measure(|| spawner.spawn(bulky(state.clone())).unwrap());
        no_alloc(fill);
    }
    // The 65th task needs a new slab: the slab plus bookkeeping Vec growth.
    let (_, grow) = measure(|| spawner.spawn(bulky(states[64].clone())).unwrap());
    assert!(grow.allocated_bytes >= SLAB_BYTES, "{grow:?}");
    assert!(grow.allocated_bytes < 2 * SLAB_BYTES, "{grow:?}");
    first.poll(0).unwrap();
    let (_, shutdown) = measure(|| first.shutdown());
    no_alloc(shutdown);

    // A later runtime reuses the spare slabs; its first spawn allocates only its
    // runtime pool and vectors, never a slab.
    let mut second = runtime();
    let spawner = second.spawner();
    let state = Rc::new(State::default());
    let (_, first_in_runtime) = measure(|| spawner.spawn(bulky(state.clone())).unwrap());
    assert!(first_in_runtime.allocations > 0, "{first_in_runtime:?}");
    assert!(
        first_in_runtime.allocated_bytes < BULK,
        "{first_in_runtime:?}"
    );
    let next = Rc::new(State::default());
    let (_, second_in_runtime) = measure(|| spawner.spawn(bulky(next.clone())).unwrap());
    no_alloc(second_in_runtime);
    second.poll(0).unwrap();
}
