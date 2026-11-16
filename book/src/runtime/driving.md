# Driving the runtime

Your loop drives the runtime by calling `Runtime::poll(tick)`. The previous page, [Writing and spawning tasks](tasks.md), showed how tasks get into the runtime; this page explains what one poll does with them, what the tick must be, how tasks share the poll, and how to write the loop around it.

## A small program to follow

`switch/examples/basic.rs` spawns two tasks and polls until both finish, printing what each poll reports:

```rust,ignore
{{#include ../../../switch/examples/basic.rs}}
```

Run it with `cargo run -p switch --example basic`. It prints:

```text
=== switch runtime example ===

Task 1: immediate
Task 2: start
Polled 2 tasks; 1 live
Task 2: after yield
Polled 1 tasks; 0 live

Done.
```

The first poll runs both tasks. `immediate` finishes. `yield_once` prints, then awaits `yield_now()`, which asks to be run again in the *next* poll, so the first poll ends with one task still alive. The second poll runs `yield_once` to the end.

The `PollReport` that `poll` returns has two fields. `tasks_polled` counts how many times this poll called into a task's future. `live_tasks` counts every task that has not finished, including tasks parked on a timer or waiting for a wake.

## One poll, step by step

```text
 poll(tick)
   |
   |-- tick lower than the last accepted tick?  -> Err(TimeRegressed), nothing changes
   |-- runtime shut down or panicked?           -> Err(Closed) / Err(Panicked)
   |
   |-- finish the pass that request_turn() cut short last time, if any
   |-- deliver wakes that other threads queued in the mailbox
   |-- move the tick forward; every sleep whose deadline <= tick wakes its task
   |
   |   pass 1: tasks woken or spawned since the last poll,
   |           then tasks that yielded during the last poll,
   |           then tasks whose sleep just expired
   |   pass 2: tasks woken or spawned during pass 1
   |   pass 3: ...
   |   (a new pass starts only while fewer than 1024 task polls were made)
   |
   '-- return PollReport { tasks_polled, live_tasks }
```

Each pass takes the set of ready tasks as it stands when the pass starts, and runs each one until it returns `Pending` or finishes. If a running task wakes or spawns another task, that task waits for the next pass of the same poll. Passes continue until no task is ready.

The 1024 limit keeps one poll from running forever when tasks keep waking each other. It is checked between passes: a pass that has started always finishes, so one poll can make somewhat more than 1024 task polls. Work left over when the limit stops the poll is not lost; it runs in the next poll.

Calling `poll` again while a poll is running, for example from inside a task that somehow reaches the `Runtime`, panics.

## Ticks

The tick is the only clock the runtime has. You give it a starting tick in `Runtime::new(initial_tick)`, and a new one on every `poll`.

The rule for ticks is simple: they must never go backwards. A tick equal to the last one is fine; the runtime just has no new timers to fire. A tick lower than the last accepted one makes `poll` return `Err(RuntimeError::TimeRegressed { current, supplied })`, where `current` is the runtime's tick and `supplied` the one you passed. Nothing else happens: no task runs and no state changes, so the loop can pass a correct tick next time.

The unit is yours. Tests often use small numbers, as on this page. Real programs read a monotonic clock. Once a program uses [`switch_net::within`](../net/timeouts.md) or sleeps measured in `Duration`s, its ticks must be nanoseconds, because those functions turn a `Duration` into ticks by counting nanoseconds. Every program in this repository that touches the network passes nanoseconds since a `std::time::Instant` taken at startup. [The drive loop](../net/drive-loop.md) shows how one clock sample gives both the runtime's tick and the network stack's time.

## Tasks woken before a poll run in it

Anything that wakes or spawns a task between two polls is picked up by the next poll. This is how the network fits in: the loop first moves packets, which wakes the tasks waiting on them, and then polls, which runs those tasks.

The test in `switch/examples/manual_loop.rs` shows the rule with a stand-in for the network, an `EventQueue` that spawns one task per pending event:

```rust,ignore
{{#include ../../../switch/examples/manual_loop.rs:events_before_poll}}
```

Two events are dispatched before the first poll, and that poll runs both tasks once. With no new events, the second poll has nothing to do and returns an empty report.

The opposite is also true. Work your loop does *after* a poll is not seen by any task until the following poll.

## Fairness: no task is interrupted

Tasks are never interrupted. Once the runtime calls into a task, that task keeps the thread until it returns `Pending` at an `.await` or finishes. The runtime only decides the order in which ready tasks run.

So a task that computes for a long time without awaiting holds up every other task and the whole loop, including the network. Nothing moves packets until it returns. A task with a long piece of work should split it into slices and call `switch::yield_now().await` between them, as this task from `switch/examples/clock_loop.rs` does:

```rust,ignore
{{#include ../../../switch/examples/clock_loop.rs:sliced_work}}
```

`yield_now` makes the task ready again for the *next* poll, not for a later pass of the current one. So between two slices, the current poll finishes, your loop does its other work, and the next poll resumes the task. [Tasks working together](cooperating.md#yielding) has more on yielding.

## request_turn

Sometimes a task knows that the loop should get control back now, before the rest of the ready tasks run. `switch::request_turn()`, called from inside a task, ends the current poll as soon as that task returns. The poll reports what it did so far. The next poll first finishes the tasks that were left in the cut-short pass, then carries on as usual.

The request lasts for that one poll. Outside a poll, `request_turn()` does nothing.

Application code rarely calls it. switch-net calls it for you: on the local backend, once tasks have handed the network stack a full burst of frames to send, it ends the poll so that the loop sends them sooner. [The drive loop](../net/drive-loop.md) describes when that happens.

## Writing the loop

Here is a complete loop that runs tasks against a real clock, from `switch/examples/clock_loop.rs`:

```rust,ignore
{{#include ../../../switch/examples/clock_loop.rs:loop}}
```

The tick is the time since `start` in nanoseconds, read once per pass. After each poll the loop sleeps for 100 µs. That sleep is the loop's *idle strategy*, and it is up to you. There are two shapes in this repository.

*Busy polling* goes straight back to the top of the loop. The thread never sleeps, so a woken task runs within one loop pass and a timer fires on the first poll at or after its deadline. This is what latency-sensitive programs do, on a CPU set aside for the loop (see [The pinned CPU and helper threads](../nic/cpus.md)). The benchmark host in `switch-bench` works this way: each pass turns the network and polls the runtime, with no sleep.

*Sleeping when idle* checks whether the last pass did anything and, if not, sleeps the thread for a short fixed time. `switch-binance-feed` sleeps 1 ms after a pass where nothing moved:

```rust,ignore
if !moved {
    std::thread::sleep(IDLE_SLEEP);
}
```

The runtime has no call that tells you when its next timer is due. A sleeping loop therefore cannot sleep exactly until the next deadline; a timer fires at the first poll after its deadline, which can be up to one idle sleep late. Pick the idle sleep with that in mind, or busy poll. On the local backend, `Net::poll_at()` tells you when the network stack's own timers are next due, and [The drive loop](../net/drive-loop.md) shows how to bound the sleep with it.

## Where the network comes in

A network program's loop does one more thing each time through: before polling the runtime, it turns the network, which receives packets, sends queued frames and wakes the tasks that were waiting on them. switch-net packages "turn, then poll" as one call, `switch_net::round`, and the loop that calls it is the [drive loop](../net/drive-loop.md). Everything on this page applies to it unchanged.

Next: [Time and sleeping](time.md) explains timers in detail.
