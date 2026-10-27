//! Manual event and timer driving with caller-supplied ticks.

use std::collections::VecDeque;
use switch::{Runtime, Spawner};

#[macro_use]
#[path = "../tests/support/task_macro.rs"]
mod task_macro;

task! {
    async fn print_event(event: &'static str) {
        println!("{event}");
    }
}

task! {
    async fn print_after_sleep(spawner: Spawner) {
        spawner.sleep(10).unwrap().await.unwrap();
        println!("deadline reached");
    }
}

struct EventQueue {
    pending: VecDeque<&'static str>,
}

impl EventQueue {
    fn poll_nonblocking(&mut self, spawner: &Spawner) -> usize {
        let mut dispatched = 0;
        while let Some(event) = self.pending.pop_front() {
            spawner.spawn(print_event(event)).unwrap();
            dispatched += 1;
        }
        dispatched
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queued_events_schedule_once_in_the_current_turn() {
        let mut runtime = Runtime::new(0);
        let spawner = runtime.spawner();
        let mut events = EventQueue {
            pending: VecDeque::from(["one", "two"]),
        };
        assert_eq!(events.poll_nonblocking(&spawner), 2);
        let first = runtime.poll(0).unwrap();
        assert_eq!((first.tasks_polled, first.tasks_completed), (2, 2));
        assert_eq!(first.live_tasks, 0);
        assert_eq!(events.poll_nonblocking(&spawner), 0);
        let second = runtime.poll(0).unwrap();
        assert_eq!(second, switch::PollReport::default());
    }
}

fn main() {
    let mut runtime = Runtime::new(100);
    let spawner = runtime.spawner();
    let task_spawner = spawner.clone();
    spawner.spawn(print_after_sleep(task_spawner)).unwrap();

    let mut events = EventQueue {
        pending: VecDeque::from(["first event"]),
    };
    for now in [100, 100, 110] {
        if now == 110 {
            events.pending.push_back("event at deadline");
        }
        events.poll_nonblocking(&spawner);
        let report = runtime.poll(now).unwrap();
        println!("tick {now}: {report:?}");
    }
}
