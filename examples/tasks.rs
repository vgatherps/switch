//! Spawn from a task, then spawn a plain function's returned future.
//!
//! Run: `cargo run -p switch-examples --example tasks`. No peer.

use switch::{Runtime, Spawner};

#[switch::task]
async fn parent(spawner: Spawner) {
    println!("parent polled");
    spawner.spawn(child()).unwrap();
}

#[switch::task]
async fn child() {
    println!("child polled");
}

#[switch::task]
fn plain(message: &'static str) -> impl Future<Output = ()> {
    println!("plain function called at spawn time");
    async move {
        println!("plain future polled: {message}");
    }
}

fn main() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();

    spawner.spawn(parent(spawner.clone())).unwrap();
    while runtime.poll(0).unwrap().live_tasks != 0 {}

    spawner.spawn(plain("hello")).unwrap();
    println!("before polling the plain future");
    while runtime.poll(0).unwrap().live_tasks != 0 {}
}
