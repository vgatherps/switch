use std::cell::Cell;
use std::rc::Rc;

use switch::{Capacity, Runtime, SleepError};

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
fn public_runtime_drives_sleep_with_caller_ticks_and_unit_event_callback() {
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
    let task_completed = completed.clone();

    spawner
        .spawn(async move {
            sleep_spawner.sleep(10).unwrap().await.unwrap();
            task_completed.set(true);
        })
        .unwrap();

    let mut interface = Interface { calls: 0 };
    let first = runtime
        .poll(100, || {
            interface.poll_nonblocking();
        })
        .unwrap();
    assert_eq!(
        (first.tasks_polled, first.tasks_completed, first.live_tasks),
        (1, 0, 1)
    );
    assert!(!completed.get());

    let repeated = runtime
        .poll(100, || {
            interface.poll_nonblocking();
        })
        .unwrap();
    assert_eq!(repeated.tasks_polled, 0);

    let expired = runtime
        .poll(110, || {
            interface.poll_nonblocking();
        })
        .unwrap();
    assert_eq!(
        (
            expired.timers_expired,
            expired.tasks_polled,
            expired.tasks_completed
        ),
        (1, 1, 1)
    );
    assert_eq!(expired.live_tasks, 0);
    assert_eq!(interface.calls, 3);
    assert!(completed.get());

    let due_spawner = spawner.clone();
    let due_completed = Rc::new(Cell::new(false));
    let task_due_completed = due_completed.clone();
    spawner
        .spawn(async move {
            due_spawner.sleep_until(110).unwrap().await.unwrap();
            task_due_completed.set(true);
        })
        .unwrap();
    let due = runtime.poll(110, || {}).unwrap();
    assert_eq!(
        (due.tasks_polled, due.tasks_completed, due.timers_expired),
        (1, 1, 0)
    );
    assert!(due_completed.get());
    assert_eq!(
        spawner.sleep(u64::MAX).err(),
        Some(SleepError::DeadlineOverflow)
    );
}
