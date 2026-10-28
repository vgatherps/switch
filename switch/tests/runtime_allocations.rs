#[path = "support/counting.rs"]
mod counting;
use counting::{CountingGlobalAlloc, measure, no_alloc};
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use switch::{Capacity, Runtime, Sleep};

#[macro_use]
#[path = "support/task_macro.rs"]
mod task_macro;

#[global_allocator]
static ALLOCATOR: CountingGlobalAlloc = CountingGlobalAlloc;

struct Holder {
    sleep: Rc<RefCell<Option<Sleep>>>,
    stop: Rc<Cell<bool>>,
    drops: Rc<Cell<usize>>,
}
impl Future for Holder {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.stop.get() {
            return Poll::Ready(());
        }
        if let Some(sleep) = self.sleep.borrow_mut().as_mut()
            && Pin::new(sleep).poll(cx).is_ready()
        {
            return Poll::Ready(());
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}
impl Drop for Holder {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}
fn holder(
    sleep: &Rc<RefCell<Option<Sleep>>>,
    stop: &Rc<Cell<bool>>,
    drops: &Rc<Cell<usize>>,
) -> Holder {
    Holder {
        sleep: sleep.clone(),
        stop: stop.clone(),
        drops: drops.clone(),
    }
}

#[test]
fn known_box_control_disabled_window_and_reset() {
    let (_, stats) = measure(|| {
        let value = Box::new([13u8; 256]);
        std::hint::black_box(&value);
        drop(value);
    });
    assert_eq!(
        (stats.allocations, stats.deallocations, stats.reallocations),
        (1, 1, 0)
    );
    assert_eq!(
        (
            stats.allocated_bytes,
            stats.deallocated_bytes,
            stats.net_bytes,
            stats.peak_extra_bytes
        ),
        (256, 256, 0, 256)
    );
    let value = Box::new([17u8; 128]);
    std::hint::black_box(&value);
    drop(value);
    assert_eq!(counting::snapshot(), stats);
    let (_, empty) = measure(|| std::hint::black_box(7));
    assert_eq!(empty, counting::Stats::default());
}

#[test]
fn reserved_real_sleep_phases_and_task_allocation_are_distinct() {
    let (mut runtime, startup) = measure(|| {
        Runtime::with_capacity(
            100,
            Capacity {
                tasks: 4,
                timers: 4,
            },
        )
    });
    assert!(startup.allocations > 0);
    // Local to this test so its pool starts empty on whatever thread runs it.
    task! {
        fn spawn_holder(holder: Holder) -> impl Future<Output = ()> {
            holder
        }
    }
    let spawner = runtime.spawner();
    let drops = Rc::new(Cell::new(0));
    let stop_a = Rc::new(Cell::new(false));
    let stop_b = Rc::new(Cell::new(false));
    let slot = Rc::new(RefCell::new(Some(spawner.sleep_until(110).unwrap())));
    let task = spawn_holder(holder(&slot, &stop_a, &drops));
    let (_, spawn) = measure(|| spawner.spawn(task).unwrap());
    // First spawn of this function on this thread: function pool, runtime pool,
    // executor pool list, depot (5 arrays), block, block list, spare capacity.
    assert_eq!((spawn.allocations, spawn.reallocations), (11, 0));
    let (r, register) = measure(|| runtime.poll(100).unwrap());
    assert_eq!((r.tasks_polled, r.ready_tasks, r.live_tasks), (1, 1, 1));
    no_alloc(register);
    let (_, refresh) = measure(|| runtime.poll(100).unwrap());
    no_alloc(refresh);
    let task = spawn_holder(holder(&slot, &stop_b, &drops));
    let (_, spawn_b) = measure(|| spawner.spawn(task).unwrap());
    assert_eq!((spawn_b.allocations, spawn_b.reallocations), (0, 0));
    let (r, transfer) = measure(|| runtime.poll(100).unwrap());
    assert_eq!(r.tasks_polled, 2);
    no_alloc(transfer);
    stop_a.set(true);
    let (r, retire_a) = measure(|| runtime.poll(100).unwrap());
    assert_eq!((r.tasks_completed, r.live_tasks), (1, 1));
    no_alloc(retire_a);
    let (r, expiry) = measure(|| runtime.poll(110).unwrap());
    assert_eq!(
        (
            r.timers_expired,
            r.tasks_polled,
            r.tasks_completed,
            r.live_tasks
        ),
        (1, 1, 1, 0)
    );
    no_alloc(expiry);
    assert_eq!(drops.get(), 2);
    let (_, cached_drop) = measure(|| drop(slot.borrow_mut().take()));
    no_alloc(cached_drop);

    let cancel_slot = Rc::new(RefCell::new(Some(spawner.sleep_until(120).unwrap())));
    let stop = Rc::new(Cell::new(false));
    let task = spawn_holder(holder(&cancel_slot, &stop, &drops));
    spawner.spawn(task).unwrap();
    let (_, register_reuse) = measure(|| runtime.poll(110).unwrap());
    no_alloc(register_reuse);
    let (_, cancel) = measure(|| drop(cancel_slot.borrow_mut().take()));
    no_alloc(cancel);
    assert_eq!(runtime.poll(120).unwrap().timers_expired, 0);
    let (_, shutdown) = measure(|| runtime.shutdown());
    no_alloc(shutdown);
    assert_eq!(drops.get(), 3);
    println!(
        "startup={startup:?}\nspawn={spawn:?}\nregister={register:?}\nrefresh={refresh:?}\nspawn_b={spawn_b:?}\ntransfer={transfer:?}\nretire_a={retire_a:?}\nexpiry={expiry:?}\ncancel={cancel:?}\nshutdown={shutdown:?}"
    );
}

#[test]
fn zero_reservation_growth_is_reported_separately() {
    let mut runtime = Runtime::with_capacity(
        0,
        Capacity {
            tasks: 0,
            timers: 0,
        },
    );
    // Local to this test so its pool starts empty on whatever thread runs it.
    task! {
        fn spawn_holder(holder: Holder) -> impl Future<Output = ()> {
            holder
        }
    }
    let spawner = runtime.spawner();
    let slot = Rc::new(RefCell::new(Some(spawner.sleep_until(2).unwrap())));
    let stop = Rc::new(Cell::new(false));
    let drops = Rc::new(Cell::new(0));
    let task = spawn_holder(holder(&slot, &stop, &drops));
    let (_, spawn_growth) = measure(|| spawner.spawn(task).unwrap());
    assert!(spawn_growth.allocations + spawn_growth.reallocations >= 3);
    let (_, timer_growth) = measure(|| runtime.poll(0).unwrap());
    assert!(timer_growth.allocations + timer_growth.reallocations >= 2);
    let (_, steady) = measure(|| runtime.poll(0).unwrap());
    no_alloc(steady);
    let (_, shutdown) = measure(|| runtime.shutdown());
    no_alloc(shutdown);
    assert_eq!(drops.get(), 1);
    println!(
        "spawn_growth={spawn_growth:?}\ntimer_growth={timer_growth:?}\nsteady={steady:?}\nshutdown={shutdown:?}"
    );
}
