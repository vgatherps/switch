//! Waking a switch task from another thread with a `RemoteWaker`.
//!
//! Run: `cargo run -p switch-examples --example remote_wake`. The peer is the worker thread.

use std::future::poll_fn;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::Poll;
use std::time::Duration;

use switch::{RemoteWaker, Runtime};

// ANCHOR: task
/// Waits until another thread stores a nonzero value, then prints it.
#[switch::task]
async fn wait_for_value(value: Arc<AtomicU32>, waker: Arc<OnceLock<RemoteWaker>>) {
    let seen = poll_fn(|cx| {
        let _ = waker.set(RemoteWaker::new(cx));
        match value.load(Ordering::Acquire) {
            0 => Poll::Pending,
            seen => Poll::Ready(seen),
        }
    })
    .await;
    println!("woken with {seen}");
}
// ANCHOR_END: task

fn main() {
    // ANCHOR: setup
    let mut runtime = Runtime::new(0);
    let value = Arc::new(AtomicU32::new(0));
    let waker = Arc::new(OnceLock::new());
    let task = wait_for_value(value.clone(), waker.clone());
    runtime.spawner().spawn(task).unwrap();
    // The first poll runs the task once: it publishes its RemoteWaker and parks.
    runtime.poll(0).unwrap();
    // ANCHOR_END: setup

    // ANCHOR: thread
    let worker = std::thread::spawn(move || {
        value.store(7, Ordering::Release);
        waker.get().expect("the task was polled once").wake();
    });
    // ANCHOR_END: thread

    // ANCHOR: loop
    while runtime.poll(0).unwrap().live_tasks != 0 {
        std::thread::sleep(Duration::from_micros(50));
    }
    worker.join().unwrap();
    // ANCHOR_END: loop
}
