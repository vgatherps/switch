# Your first task

switch is used from a clone of its repository, so the dependency in `Cargo.toml` is a path into the clone:

```toml
[dependencies]
switch = { path = "<clone>/switch" }
```

With no features enabled, `switch` is the runtime alone and builds on any machine with cargo. The three programs below are combined in the example `first_task`:

```sh
cargo run -p switch-examples --example first_task
```

## Hello, task

A task is one call of a function marked [`#[switch::task]`](macro@crate::task), spawned on a [`Runtime`](crate::Runtime).

```rust
use switch::Runtime;

#[switch::task]
async fn hello(name: &'static str) {
    println!("Hello, {name}!");
}

fn main() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    spawner.spawn(hello("switch")).unwrap();
    while runtime.poll(0).unwrap().live_tasks != 0 {}
}
```

```text
Hello, switch!
```

Calling a `#[switch::task]` function returns a token instead of running the body, and [`Spawner::spawn`](crate::Spawner) turns the token into a task. Tasks run only inside [`runtime.poll`](crate::Runtime::poll), which returns a [`PollReport`](crate::PollReport) whose `live_tasks` field counts the tasks that have not finished. The runtime has no threads of its own, so nothing happens between two calls to `poll`.

## Getting a value back

`spawn` discards the task's return value. `spawn_join` returns a [`JoinHandle`](crate::JoinHandle) instead, which resolves to that value when another task awaits it:

```rust
use switch::{JoinHandle, Runtime};

#[switch::task]
async fn double(value: u32) -> u32 {
    value * 2
}

#[switch::task]
async fn report(handle: JoinHandle<u32>) {
    let value = handle.await.unwrap();
    println!("double(21) = {value}");
}

fn main() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let handle = spawner.spawn_join(double(21)).unwrap();
    spawner.spawn(report(handle)).unwrap();
    while runtime.poll(0).unwrap().live_tasks != 0 {}
}
```

```text
double(21) = 42
```

`double` runs only while `main` is polling, so `main` cannot wait for the handle itself and a second task awaits it.

## A timer, and the tick

The `u64` passed to `Runtime::new` and to every `poll` is the tick, the runtime's only clock, in a unit the program chooses. It may repeat from one poll to the next but must never decrease.

A task sleeps through its spawner:

```rust
use switch::{Runtime, Spawner};

#[switch::task]
async fn wake_at_ten(spawner: Spawner) {
    spawner.sleep(10).unwrap().await.unwrap();
    println!("ten ticks passed");
}

fn main() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    spawner.spawn(wake_at_ten(spawner.clone())).unwrap();
    for tick in [0, 5, 9, 10] {
        let report = runtime.poll(tick).unwrap();
        println!(
            "tick {tick}: polled {}, live {}",
            report.tasks_polled, report.live_tasks
        );
    }
}
```

```text
tick 0: polled 1, live 1
tick 5: polled 0, live 1
tick 9: polled 0, live 1
ten ticks passed
tick 10: polled 1, live 0
```

`spawner.sleep(10)` creates a timer that is due ten ticks after the latest tick the runtime was given, and the sleep ends in the first poll whose tick has reached that deadline. A program reads a clock once per iteration and passes the reading as the tick; the network programs in this guide use nanoseconds since the program started.

Network programs need more than cargo to build. Next: [Setting up for network programs](crate::guide::setup).
