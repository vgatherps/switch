# Tasks

All the tasks of a runtime share one thread and are scheduled cooperatively. The code comes from runnable examples: `cargo run -p switch-examples --example NAME`.

## Writing and spawning

A task function is an `async fn` marked [`#[switch::task]`](macro@crate::task). Its future must be `'static` but need not be `Send`, so its arguments are owned values or shared ones such as `Rc`. A task that spawns other tasks takes a [`Spawner`](crate::Spawner) as an argument (example `tasks`):

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

`spawn` fails only when the runtime has been shut down or a task has panicked, and the error then returns the token. A panic in a task is not caught and propagates out of `runtime.poll` into the loop.

A task's return value comes from `spawn_join` and a [`JoinHandle`](crate::JoinHandle) awaited in another task, as in [Your first task](crate::guide::first_task). The handle resolves to `Err(Canceled)` if the task ended without a value, because it panicked or the runtime was shut down. Dropping the handle leaves the task running.

## Sleeping

[Your first task](crate::guide::first_task) slept with `spawner.sleep(ticks)`; `spawner.sleep_until(tick)` waits for an absolute tick. A sleep is only as punctual as the loop, so a loop that polls every millisecond can end a sleep a millisecond late.

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

Dropping a sleep cancels it. The errors a sleep can return are on [`SleepError`](crate::SleepError).

## Yielding

A task that runs for a long time without awaiting stalls the network as well as the other tasks, because packets are received and sent between polls. After [`yield_now`](crate::yield_now) the other ready tasks run, the loop goes round, and the task continues in the next `runtime.poll`, or sooner if something else wakes it first. This task is also in `clock_loop`:

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

[`oneshot`](crate::oneshot) is a single-threaded oneshot channel whose receiver is awaited in a task (example `oneshot`):

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

`send` wakes the receiving task, or returns the value if the receiver has been dropped. The sender does not have to be in a task. The loop in `main` can hold it and send between polls, which is one way to tell a task to stop.

## Waking a task from another thread

None of the types above is `Send`, so everything so far stays on the runtime's thread. The exception is [`RemoteWaker`](crate::RemoteWaker), which another thread uses to wake a task. The task creates one for itself while it is being polled, publishes it to the other thread, and checks for its data each time it is woken (example `remote_wake`):

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

The other thread stores the data before it calls `wake`, which rules out a lost wakeup. The wake takes effect at the start of the next `runtime.poll`.

Next: [The drive loop](crate::guide::drive_loop).
