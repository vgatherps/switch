//! Manual event and timer driving with caller-supplied ticks.
//!
//! Run: `cargo run -p switch-examples --example manual_loop`. No peer.

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

fn main() {
    let mut runtime = Runtime::new(100);
    let spawner = runtime.spawner();
    let task_spawner = spawner.clone();
    spawner.spawn(print_after_sleep(task_spawner)).unwrap();

    let mut events = VecDeque::from(["first event"]);
    for now in [100, 100, 110] {
        if now == 110 {
            events.push_back("event at deadline");
        }
        while let Some(event) = events.pop_front() {
            spawner.spawn(print_event(event)).unwrap();
        }
        let report = runtime.poll(now).unwrap();
        println!("tick {now}: {report:?}");
    }
}
