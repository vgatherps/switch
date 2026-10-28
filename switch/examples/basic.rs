//! Basic example.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

struct YieldOnce(bool);

impl Future for YieldOnce {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

#[switch::task]
async fn immediate() {
    println!("Task 1: immediate");
}

#[switch::task]
async fn yield_once() {
    println!("Task 2: start");
    YieldOnce(false).await;
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
