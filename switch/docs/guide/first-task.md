# Your first task

switch is used from a clone of its repository, so the dependency in `Cargo.toml` is a path into the clone:

```toml
[dependencies]
switch = { path = "<clone>/switch" }
```

With no features enabled, `switch` is the runtime alone, tasks and timers without the network, and builds on any machine with cargo. The three programs below are combined into one `main` in the example `first_task`, which runs from the clone with:

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

`runtime.spawner()` returns a [`Spawner`](crate::Spawner), a handle that starts tasks and can be cloned freely. Calling a `#[switch::task]` function returns a token holding the arguments instead of running the body, and `spawner.spawn` turns that token into a task. The task runs only inside [`runtime.poll`](crate::Runtime::poll), which runs the tasks that are ready and returns a [`PollReport`](crate::PollReport). The report's `live_tasks` field counts the tasks that have not finished, so the loop above polls until `hello` is done. The runtime has no threads of its own, so nothing happens between two calls to `poll`.

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

`spawner.sleep(10)` creates a timer that is due ten ticks after the latest tick the runtime was given, here at tick 10. The task runs as far as the sleep at tick 0, the polls at ticks 5 and 9 find nothing to run, and the poll at tick 10 wakes it.

A sleep ends only when a poll passes a tick that has reached its deadline. The two earlier loops pass 0 on every poll, which works for tasks that never sleep. Had `hello` slept, its loop would have spun forever at tick 0.

A real program reads a clock once per iteration of its loop and passes the reading as the tick. The network programs in this guide use nanoseconds since the program started.

Network programs need more than cargo to build. Next: [Setting up for network programs](crate::guide::setup).
