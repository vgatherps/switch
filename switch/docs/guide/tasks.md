# Tasks

All the tasks of a runtime share one thread. A task runs until it reaches an `.await` that is not ready, and only then does the next task run; nothing interrupts a task in between. Tasks wait for time, for each other and for other threads with the tools below. Each is shown with a runnable example: `cargo run -p switch-examples --example NAME`.

## Writing and spawning

A task function is an `async fn` with [`#[switch::task]`](macro@crate::task) on it. Its arguments are moved into the task, so pass owned values, or shared ones such as `Rc`. A task that starts other tasks takes a [`Spawner`](crate::Spawner) as an argument (example `tasks`):

```rust
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

fn main() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    spawner.spawn(parent(spawner.clone())).unwrap();
    while runtime.poll(0).unwrap().live_tasks != 0 {}
}
```

```text
parent polled
child polled
```

`spawn` fails only when the runtime has been shut down or a task has panicked, and then it gives the token back in the error. A panic in a task is not caught: it comes out of `runtime.poll` into your loop.

To get a task's return value, spawn it with `spawn_join` and await the [`JoinHandle`](crate::JoinHandle) in another task, as in [Your first task](crate::guide::first_task). The handle gives `Err(Canceled)` if the task ended without a value, because it panicked or the runtime was shut down. Dropping the handle leaves the task running.

## Sleeping

`spawner.sleep(ticks)` waits that many ticks from the latest tick the runtime was given, and `spawner.sleep_until(tick)` waits for an absolute tick. A sleep ends during the first poll whose tick has reached its deadline, so it is as punctual as your loop: a loop that polls every millisecond ends sleeps up to a millisecond late.

With ticks in nanoseconds, a `Duration` converts directly (example `clock_loop`):

```rust
use std::time::Duration;
use switch::Spawner;

#[switch::task]
async fn wait_two_ms(spawner: Spawner) {
    let ticks = u64::try_from(Duration::from_millis(2).as_nanos()).unwrap();
    spawner.sleep(ticks).unwrap().await.unwrap();
    println!("2 ms passed");
}
```

Dropping a sleep cancels it. The errors a sleep can return are on [`SleepError`](crate::SleepError); with one runtime that stays open, none of them occurs.

## Yielding

A task that computes for a long time holds up every other task and, in a network program, the network too: packets are received and sent between polls. [`yield_now`](crate::yield_now) hands the thread back. The task stays ready and continues in the next call to `runtime.poll`, after the loop has been round once.

```rust
#[switch::task]
async fn sum_in_slices(values: Vec<u64>) {
    let mut total = 0;
    for chunk in values.chunks(1024) {
        total += chunk.iter().sum::<u64>();
        // Let the loop and the other tasks run before the next slice.
        switch::yield_now().await;
    }
    println!("sum = {total}");
}
```

## Passing one value to a task

A [`oneshot`](crate::oneshot) channel carries one value to one waiting task (example `oneshot`):

```rust
use switch::oneshot::{self, Receiver, Sender};
use switch::{Runtime, Spawner};

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

fn main() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let (sender, receiver) = oneshot::channel();
    spawner.spawn(consumer(receiver)).unwrap();
    spawner.spawn(producer(spawner.clone(), sender)).unwrap();
    let mut tick = 0;
    while runtime.poll(tick).unwrap().live_tasks != 0 {
        tick += 1;
    }
}
```

```text
received 42
```

`send` wakes the receiving task. If the receiver has been dropped, `send` returns the value. The sender does not have to be in a task: the loop in `main` can hold it and send between polls, which is a simple way to tell a task to stop. Both ends stay on the runtime's thread.

## Waking a task from another thread

Everything above stays on one thread, and the types enforce it: none of them is `Send`. The exception is [`RemoteWaker`](crate::RemoteWaker), which another thread uses to wake a task. The task makes one for itself while it is being polled, puts it where the other thread can find it, and checks for its data each time it is woken (example `remote_wake`):

```rust
use std::future::poll_fn;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::Poll;
use std::time::Duration;

use switch::{RemoteWaker, Runtime};

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

fn main() {
    let mut runtime = Runtime::new(0);
    let value = Arc::new(AtomicU32::new(0));
    let waker = Arc::new(OnceLock::new());
    runtime.spawner().spawn(wait_for_value(value.clone(), waker.clone())).unwrap();
    // The first poll runs the task once: it publishes its RemoteWaker and waits.
    runtime.poll(0).unwrap();

    let worker = std::thread::spawn(move || {
        value.store(7, Ordering::Release);
        waker.get().expect("the task was polled once").wake();
    });

    while runtime.poll(0).unwrap().live_tasks != 0 {
        std::thread::sleep(Duration::from_micros(50));
    }
    worker.join().unwrap();
}
```

```text
woken with 7
```

The other thread stores the data first and wakes second, so the task cannot miss it. The wake takes effect at the start of the next `runtime.poll`.

Next: [The drive loop](crate::guide::drive_loop).
