//! A runtime driven by a real clock: ticks are nanoseconds since the program
//! started, and the loop sleeps briefly between polls.
//!
//! Run: `cargo run -p switch-examples --example clock_loop`. No peer.

use std::time::{Duration, Instant};

use switch::{Runtime, Spawner};

#[switch::task]
async fn sum_in_slices(values: Vec<u64>) {
    let mut total = 0;
    for chunk in values.chunks(1024) {
        total += chunk.iter().sum::<u64>();
        // Let the loop and the other tasks run before the next slice.
        switch::yield_now().await;
    }
    println!("sum = {total}");
}

#[switch::task]
async fn wait_two_ms(spawner: Spawner) {
    let ticks = u64::try_from(Duration::from_millis(2).as_nanos()).unwrap();
    spawner.sleep(ticks).unwrap().await.unwrap();
    println!("2 ms passed");
}

fn main() {
    let start = Instant::now();
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    spawner.spawn(wait_two_ms(spawner.clone())).unwrap();
    spawner.spawn(sum_in_slices((0..10_000).collect())).unwrap();
    loop {
        let tick = u64::try_from(start.elapsed().as_nanos()).unwrap();
        let report = runtime.poll(tick).unwrap();
        if report.live_tasks == 0 {
            break;
        }
        std::thread::sleep(Duration::from_micros(100));
    }
}
