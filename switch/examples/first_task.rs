//! Your first task: spawn a task, get a value back, and wait on a timer, all on a
//! runtime that only runs when `main` polls it.

use switch::{JoinHandle, Runtime, Spawner};

// ANCHOR: hello
#[switch::task]
async fn hello(name: &'static str) {
    println!("Hello, {name}!");
}
// ANCHOR_END: hello

// ANCHOR: join_tasks
#[switch::task]
async fn double(value: u32) -> u32 {
    value * 2
}

#[switch::task]
async fn report(handle: JoinHandle<u32>) {
    let value = handle.await.unwrap();
    println!("double(21) = {value}");
}
// ANCHOR_END: join_tasks

// ANCHOR: timer_task
#[switch::task]
async fn wake_at_ten(spawner: Spawner) {
    spawner.sleep(10).unwrap().await.unwrap();
    println!("ten ticks passed");
}
// ANCHOR_END: timer_task

fn main() {
    // ANCHOR: run_hello
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    spawner.spawn(hello("switch")).unwrap();
    while runtime.poll(0).unwrap().live_tasks != 0 {}
    // ANCHOR_END: run_hello

    // ANCHOR: run_join
    let handle = spawner.spawn_join(double(21)).unwrap();
    spawner.spawn(report(handle)).unwrap();
    while runtime.poll(0).unwrap().live_tasks != 0 {}
    // ANCHOR_END: run_join

    // ANCHOR: run_timer
    spawner.spawn(wake_at_ten(spawner.clone())).unwrap();
    for tick in [0, 5, 9, 10] {
        let report = runtime.poll(tick).unwrap();
        println!(
            "tick {tick}: polled {}, live {}",
            report.tasks_polled, report.live_tasks
        );
    }
    // ANCHOR_END: run_timer
}
