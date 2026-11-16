//! Handing one value from one task to another with a oneshot channel.
//!
//! Run: `cargo run -p switch-examples --example oneshot`. No peer.

use switch::oneshot::{self, Receiver, Sender};
use switch::{Runtime, Spawner};

// ANCHOR: tasks
#[switch::task]
async fn producer(spawner: Spawner, sender: Sender<u32>) {
    spawner.sleep(5).unwrap().await.unwrap();
    if sender.send(42).is_err() {
        println!("the receiver is gone");
    }
}

#[switch::task]
async fn consumer(receiver: Receiver<u32>) {
    match receiver.await {
        Ok(value) => println!("received {value}"),
        Err(oneshot::Canceled) => println!("the sender was dropped without sending"),
    }
}
// ANCHOR_END: tasks

fn main() {
    // ANCHOR: run
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let (sender, receiver) = oneshot::channel();
    spawner.spawn(consumer(receiver)).unwrap();
    spawner.spawn(producer(spawner.clone(), sender)).unwrap();
    let mut tick = 0;
    while runtime.poll(tick).unwrap().live_tasks != 0 {
        tick += 1;
    }
    // ANCHOR_END: run
}
