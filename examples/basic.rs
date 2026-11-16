//! Spawn an immediate task and a task that yields once.
//!
//! Run: `cargo run -p switch-examples --example basic`. No peer.

#[switch::task]
async fn immediate() {
    println!("Task 1: immediate");
}

#[switch::task]
async fn yield_once() {
    println!("Task 2: start");
    switch::yield_now().await;
    println!("Task 2: after yield");
}

fn main() {
    println!("=== switch runtime example ===\n");
    let mut runtime = switch::Runtime::new(0);
    let spawner = runtime.spawner();

    spawner.spawn(immediate()).unwrap();
    spawner.spawn(yield_once()).unwrap();

    loop {
        let report = runtime.poll(0).unwrap();
        println!(
            "Polled {} tasks; {} live",
            report.tasks_polled, report.live_tasks
        );
        if report.live_tasks == 0 {
            break;
        }
    }
    println!("\nDone.");
}
