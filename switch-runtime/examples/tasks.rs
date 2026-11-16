use switch_runtime::{Runtime, Spawner};

// ANCHOR: async_task
#[switch_runtime::task]
async fn parent(spawner: Spawner) {
    println!("parent polled");
    spawner.spawn(child()).unwrap();
}

#[switch_runtime::task]
async fn child() {
    println!("child polled");
}
// ANCHOR_END: async_task

// ANCHOR: plain_fn
#[switch_runtime::task]
fn plain(message: &'static str) -> impl Future<Output = ()> {
    println!("plain function called at spawn time");
    async move {
        println!("plain future polled: {message}");
    }
}
// ANCHOR_END: plain_fn

fn main() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();

    spawner.spawn(parent(spawner.clone())).unwrap();
    while runtime.poll(0).unwrap().live_tasks != 0 {}

    spawner.spawn(plain("hello")).unwrap();
    println!("before polling the plain future");
    while runtime.poll(0).unwrap().live_tasks != 0 {}
}
