use std::cell::Cell;
use std::rc::Rc;

use switch::{Capacity, Runtime, SleepError, Spawner};

#[switch::task]
async fn sleep_then_set(spawner: Spawner, completed: Rc<Cell<bool>>) {
    spawner.sleep(10).unwrap().await.unwrap();
    completed.set(true);
}

#[switch::task]
async fn sleep_until_then_set(spawner: Spawner, completed: Rc<Cell<bool>>) {
    spawner.sleep_until(110).unwrap().await.unwrap();
    completed.set(true);
}

struct Interface {
    calls: usize,
}

impl Interface {
    fn poll_nonblocking(&mut self) -> usize {
        self.calls += 1;
        self.calls
    }
}

#[test]
fn public_runtime_drives_sleep_with_caller_ticks_and_prepolled_events() {
    let mut runtime = Runtime::with_capacity(
        100,
        Capacity {
            tasks: 2,
            timers: 2,
        },
    );
    let spawner = runtime.spawner();
    let sleep_spawner = spawner.clone();
    let completed = Rc::new(Cell::new(false));

    spawner
        .spawn(sleep_then_set(sleep_spawner, completed.clone()))
        .unwrap();

    let mut interface = Interface { calls: 0 };
    interface.poll_nonblocking();
    let first = runtime.poll(100).unwrap();
    assert_eq!((first.tasks_polled, first.live_tasks), (1, 1));
    assert!(!completed.get());

    interface.poll_nonblocking();
    let repeated = runtime.poll(100).unwrap();
    assert_eq!(repeated.tasks_polled, 0);

    interface.poll_nonblocking();
    let expired = runtime.poll(110).unwrap();
    assert_eq!((expired.tasks_polled, expired.live_tasks), (1, 0));
    assert_eq!(interface.calls, 3);
    assert!(completed.get());

    let due_spawner = spawner.clone();
    let due_completed = Rc::new(Cell::new(false));
    spawner
        .spawn(sleep_until_then_set(due_spawner, due_completed.clone()))
        .unwrap();
    let due = runtime.poll(110).unwrap();
    assert_eq!((due.tasks_polled, due.live_tasks), (1, 0));
    assert!(due_completed.get());
    assert_eq!(
        spawner.sleep(u64::MAX).err(),
        Some(SleepError::DeadlineOverflow)
    );
}
