# Your first task

A switch program needs one dependency. switch is used from a clone of its repository, so the line in your `Cargo.toml` points at the clone:

```toml
[dependencies]
switch = { path = "<clone>/switch" }
```

With no features enabled, `switch` has tasks and timers and no network, and it builds on any machine with cargo. The example `first_task` does the three things below in one `main`; here each is shown as a small program of its own. From the clone, run the example with:

```sh
cargo run -p switch-examples --example first_task
```

## Hello, task

A task is one call of a function marked [`#[switch::task]`](macro@crate::task), started on a [`Runtime`](crate::Runtime).

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

`Runtime::new(0)` creates the runtime. `runtime.spawner()` returns a [`Spawner`](crate::Spawner), a handle that starts tasks and can be cloned freely. Calling `hello("switch")` does not run the body. It packs the argument into a token, and `spawner.spawn` starts the task from that token.

The task still has not run. Tasks run inside [`runtime.poll`](crate::Runtime::poll), which runs the tasks that are ready and returns a [`PollReport`](crate::PollReport). The report's `live_tasks` field counts the tasks that have not finished, so this loop polls until `hello` is done. The runtime never runs anything by itself: it has no threads, and between two calls to `poll` nothing happens.

## Getting a value back

`spawn` drops whatever the task returns. `spawn_join` keeps it and gives you a [`JoinHandle`](crate::JoinHandle), which another task awaits:

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

The handle is awaited in a task because `main` cannot wait for it: `main` is the code that polls, and `double` only runs while it does.

## A timer, and the tick

The number passed to `Runtime::new` and to every `poll` is the tick. It is the runtime's only clock: a `u64` that tells the runtime what time it is now. You choose the unit. A tick may repeat from one poll to the next, and it must never go down.

A task waits for time to pass with a sleep from its spawner:

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

`spawner.sleep(10)` makes a timer that is due ten ticks after the latest tick the runtime was given, here at tick 10. At tick 0 the task runs as far as the sleep and waits. The polls at ticks 5 and 9 find nothing to run. The poll at tick 10 wakes the task, and it prints and finishes.

The two earlier loops passed 0 on every poll. That works for tasks that never sleep. Had `hello` slept, its loop would have spun forever at tick 0, because a sleep ends only when a poll passes a tick that has reached its deadline.

A real program reads a clock once each time round its loop and passes the reading as the tick. The network programs in this guide use nanoseconds since the program started.

Network programs need more than cargo to build. Next: [Setting up for network programs](crate::guide::setup).
