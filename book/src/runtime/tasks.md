# Writing and spawning tasks

A task is one call of a function marked `#[switch::task]`, started on the runtime by a spawner, as introduced in [The switch runtime](index.md). This page covers what such a function may look like, what calling it does, and the two ways to start it.

## Marking a function as a task

The attribute goes on an `async fn`:

```rust,ignore
{{#include ../../../switch-macros/tests/ui/pass_forms.rs:countdown}}
```

or on a plain `fn` that returns `impl Future<Output = T>`, for the cases where you want to do some work before building the future, or return a hand-written future:

```rust,ignore
{{#include ../../../switch-macros/tests/ui/pass_forms.rs:plain_fn}}
```

The function keeps its name, visibility and generics. What changes is its return type: calling it now returns a `SpawnToken`, and the `T` of the future becomes the task's result.

The `countdown` example also shows the usual way a task gets hold of a spawner: it takes one as an argument. `Spawner` is cheap to clone, so a task that starts other tasks, or creates timers, receives its own clone when it is spawned.

## What the attribute accepts

The rules come from one requirement: a task's future lives inside the runtime for as long as the task runs, independent of the code that spawned it. So the future, and the value it produces, must be `'static`, and every argument is moved into it.

| You write | Result |
|---|---|
| `async fn f(a: A, b: B) -> T` | accepted |
| `fn f(a: A) -> impl Future<Output = T>` (optionally `+ 'static`) | accepted |
| `mut` on an argument: `async fn f(mut a: A)` | accepted |
| generic parameters, `where` clauses, const generics, `impl Trait` arguments | accepted |
| `&'static str` and other `'static` references as arguments | accepted |
| `Rc`, `RefCell` and other non-`Send` types in arguments or the future | accepted |
| a `self` receiver (`async fn run(&self)`) | compile error |
| `const fn`, `unsafe fn` or `extern fn` | compile error |
| an argument pattern that is not a plain name: `(a, b): (u8, u8)`, `ref a: u8` | compile error |
| any other return type, including `impl Future<..> + Send` and `Pin<Box<dyn Future<..>>>` | compile error |
| arguments that borrow for a shorter lifetime, such as `value: &u8` | compile error (the future is not `'static`) |
| arguments to the attribute itself, such as `#[switch::task(name = "x")]` | compile error |

The accepted forms are taken from `switch-macros/tests/ui/pass_forms.rs` and `pass_generics.rs`, and the rejected ones from the `fail_*.rs` files next to them; those files are compiled by the crate's tests, so the table matches what the compiler does.

One more limit is checked at compile time. A task's future, plus a small header the runtime keeps with it, must fit in 64 KiB. A task whose future is bigger, for example one that keeps a large array across an `.await`, fails to compile. Put large buffers in a `Box` or a `Vec` to keep them out of the future itself.

There is no limit on how many tasks can be alive at once.

## Arguments and when they are dropped

Each argument is moved into the token when you call the function, and from the token into the task when it is spawned.

For an `async fn`, every argument lives inside the future, exactly as it would for an ordinary `async fn`. An argument the body never uses is dropped when the task finishes.

For a plain `fn` returning a future, the body runs once at spawn time to build the future. Arguments that the returned future does not capture are dropped then, when `spawn` builds the future.

## Calling makes a token; spawning starts it

Calling a task function runs none of its code. It only packages the arguments:

```rust,ignore
let token = hello("switch");      // nothing has run yet
spawner.spawn(token).unwrap();    // the task exists now; the next poll runs it
```

The token is a `SpawnToken`. It is not `Send`, `Sync` or `Clone`, so it is spawned once, on the thread that made it. Dropping a token without spawning it drops its arguments and nothing else; for an `async fn`, no line of the body ever ran.

Spawning does not run the task either. `spawn` places the task in the runtime and marks it ready. It first runs during the next call to `Runtime::poll`, or later in the current poll if a running task spawned it (see [Driving the runtime](driving.md#one-poll-step-by-step)).

Here is the whole life of a task:

```text
 f(args) --> SpawnToken --spawn--> ready --(poll runs it)--> running
                 |                   ^                          |
             dropped:                |                  awaits something
          args dropped               |                  not yet available
                                  woken                         |
                                     |                          v
                                     +---------------------- parked
                                                                |
               running --returns Ready--> finished: future dropped,
                                           result to the JoinHandle (if any)

 side exits:  running --panics--> ended; the panic leaves Runtime::poll
              parked or ready --runtime shut down--> dropped
```

## Detached tasks and joined tasks

There are two ways to spawn a token.

`spawner.spawn(token)` starts a *detached* task. Its result is dropped when it finishes. Use it for tasks that report through other means, such as shared state, a channel, or the socket they write to.

`spawner.spawn_join(token)` starts the same task and also returns a `JoinHandle<T>`, a future that resolves to `Result<T, Canceled>`. It costs one small heap allocation per spawn (a oneshot channel, see [Tasks working together](cooperating.md#oneshot-channels)).

```rust,ignore
{{#include ../../../switch/examples/first_task.rs:join_tasks}}
```

```rust,ignore
{{#include ../../../switch/examples/first_task.rs:run_join}}
```

A `JoinHandle` resolves to `Ok(value)` when the task returns, at once if it had already finished. It resolves to `Err(Canceled)` when the task ended without a value: the runtime was shut down or dropped before the task finished, or the task panicked. A handle must be awaited by a switch task; polled anywhere else while the task is still running, it panics.

Dropping a `JoinHandle` does not stop the task. It detaches it, and the result is dropped when the task finishes (or right away, if it already did). To stop a task early, have it wait on something you control, such as a oneshot receiver raced against its work with [`first`](../net/timeouts.md#racing-a-receive-against-a-stop-signal).

## When spawning fails

`spawn` and `spawn_join` fail only when the runtime can no longer take work: after it was shut down, or after a task panicked. They return a `SpawnError`, which carries the reason and hands the token back:

```rust,ignore
pub struct SpawnError<T> {
    pub reason: RuntimeError,
    pub token: SpawnToken<T>,
}
```

The `reason` is `RuntimeError::Closed` or `RuntimeError::Panicked`. The token comes back unspawned, so you decide when its arguments are dropped. A full runtime is never a reason; task storage grows as needed ([How task storage works](errors.md#how-task-storage-works)). [Panics, errors and shutdown](errors.md) lists every error.

`SpawnError` and `RuntimeError` implement `Debug` but not `Display`, so programs print them with `{:?}`, as `switch-binance-feed` does:

```rust,ignore
.map_err(|error| format!("spawning the feed: {:?}", error.reason))?;
```

## Generic tasks

Task functions may be generic over types and constants, and may take `impl Trait` arguments:

```rust,ignore
#[switch::task]
async fn show<T: Display + 'static>(value: T, log: Log) {
    log.borrow_mut().push(value.to_string());
}
```

The `'static` bound is there because the value is held by the future. A type parameter the future does not hold need not be `'static`.

## Boxed futures

For an occasional spawn where writing a task function is not practical, `switch::boxed(future)` turns a `Pin<Box<dyn Future<Output = ()>>>` into a token. It costs one heap allocation per spawn and one dynamic call per poll, so ordinary code writes `#[switch::task]` functions.

Next: [Driving the runtime](driving.md) explains what happens to a spawned task inside `poll`.
