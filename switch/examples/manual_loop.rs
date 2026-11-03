//! Manual event and timer driving with caller-supplied ticks.

use std::collections::VecDeque;
use switch::{Runtime, Spawner};

#[switch::task]
async fn print_event(event: &'static str) {
    println!("{event}");
}

#[switch::task]
async fn print_after_sleep(spawner: Spawner) {
    spawner.sleep(10).unwrap().await.unwrap();
    println!("deadline reached");
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
