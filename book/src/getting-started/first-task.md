# Your first task

Before any network code, it helps to see the switch runtime on its own. This page runs async code with no NIC, no DPDK and no special permissions. Everything here builds and runs on macOS and on Linux as an ordinary user, outside the Docker container from [Setting up a build environment](environment.md).

By the end you will have written three small tasks, run them, and seen the one idea that shapes every switch program: **your code owns the loop**. The runtime does nothing until you call it, and it never decides what time it is.

## Add the dependency

The runtime is the `switch` crate. Inside this workspace a package depends on it with:

```toml
[dependencies]
switch.workspace = true
```

That is the only dependency. The `#[switch::task]` attribute used below comes with it.

The code on this page is one program, `switch/examples/first_task.rs`. Run it with:

```sh
cargo run -p switch --example first_task
```

## Hello, task

A *task* is one call of a function marked `#[switch::task]`, started on the runtime. Here is the function:

```rust,ignore
{{#include ../../../switch/examples/first_task.rs:hello}}
```

To run it, `main` creates a runtime, gets a *spawner* from it, spawns the task and then polls the runtime until no task is left:

```rust,ignore
{{#include ../../../switch/examples/first_task.rs:run_hello}}
```

Line by line:

- `Runtime::new(0)` creates the runtime. The `0` is its starting *tick*, the clock value the runtime believes it is at. More on ticks below.
- `runtime.spawner()` returns a `Spawner`, a cheap, cloneable handle whose job is to start tasks (and, later, to create timers).
- `hello("switch")` does not run the function body. It packages the argument into a token that describes the call. `spawner.spawn(..)` takes that token and starts the task.
- `runtime.poll(0)` is one *poll* of the runtime: it runs every task that is ready, then returns a `PollReport`. Its `live_tasks` field counts the tasks that have not finished yet.

The program prints `Hello, switch!` during the first poll. The task finishes, `live_tasks` is 0, and the loop ends.

Nothing ran when `spawn` was called. The task ran inside `poll`, because `main` called it. That is true of every task in every switch program.

## Getting a value back

A task can return a value. `spawn` throws the value away; `spawn_join` keeps it and gives you a `JoinHandle` that resolves to it.

```rust,ignore
{{#include ../../../switch/examples/first_task.rs:join_tasks}}
```

```rust,ignore
{{#include ../../../switch/examples/first_task.rs:run_join}}
```

The handle is awaited inside `report`, which is itself a task. A `JoinHandle` must be awaited by a switch task: `main` cannot block on it, because nothing would run the task it is waiting for. The handle resolves to `Ok(value)`, or to `Err(Canceled)` if the task ended without producing a value (for example because the runtime was shut down first). [Writing and spawning tasks](../runtime/tasks.md) covers both cases.

## A timer: the tick is your clock

The runtime has no clock of its own. Each call to `poll` passes in a tick, a `u64` that says what time it is now. You choose the unit. Ticks may stay the same from one poll to the next, but they must never go backwards.

A task waits for time to pass with a sleep created by the spawner:

```rust,ignore
{{#include ../../../switch/examples/first_task.rs:timer_task}}
```

`spawner.sleep(10)` makes a timer whose deadline is ten ticks after the latest tick the runtime has accepted. The sleep finishes during the first poll whose tick reaches that deadline:

```rust,ignore
{{#include ../../../switch/examples/first_task.rs:run_timer}}
```

The latest tick the runtime has seen is 0, so the deadline is 10. The output is:

```text
tick 0: polled 1, live 1
tick 5: polled 0, live 1
tick 9: polled 0, live 1
ten ticks passed
tick 10: polled 1, live 0
```

At tick 0 the task runs, creates its sleep and parks: it is still live but not ready. At ticks 5 and 9 there is nothing to run. At tick 10 the deadline is reached, the task wakes, prints and finishes.

This is why the hello loop above, `while poll(0).live_tasks != 0 {}`, is only safe for tasks that finish without timers. Had `hello` slept, that loop would run forever with the tick stuck at 0. A task that waits on a sleep needs a loop that moves the tick forward.

In a real program the tick comes from a real clock, read once per loop pass. Programs that use the network pass nanoseconds since program start; [Driving the runtime](../runtime/driving.md#ticks) explains why.

## What you just saw

Every switch program has this shape:

```text
   your loop                         the runtime
   ---------                         -----------
   read the clock -> tick
   poll(tick)  ------------------->  wake timers whose deadline <= tick
                                     run every ready task until it awaits
               <-------------------  return PollReport
   (do other work: turn the network,
    sleep a little, or go straight
    back to the top)
```

The runtime does not sleep, read a clock or start threads. Nothing happens between two calls to `poll`. You decide how often to call it, what tick to pass, and what else the loop does between calls. In a network program, the other thing the loop does is move packets, and the next page adds exactly that.

Next: [Your first network program](first-connection.md) puts a TCP client on this loop. For the full story of the runtime, read [The switch runtime](../runtime/index.md); for timers in depth, [Time and sleeping](../runtime/time.md).
