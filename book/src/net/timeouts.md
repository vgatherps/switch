# Timeouts and racing futures

The previous pages, from [Opening sockets](opening.md) to [UDP sockets](udp.md), showed socket operations that wait: a connect waits for the peer to answer, a `recv` waits for data, a `room` waits for space. None of them takes a time limit. When a task needs to bound a wait, or to stop waiting when something else happens, it uses two small functions from switch-net: `within` bounds a future by a time limit, and `first` races two futures against each other.

Both are plain async functions that work with any futures, not only sockets. They rely on two runtime behaviors covered earlier: [sleeps](../runtime/time.md), and the fact that dropping a future cancels whatever it was doing.

## Bounding a connect

```rust,ignore
pub async fn within<F: Future>(spawner: &Spawner, limit: Duration, work: F) -> Option<F::Output>
```

`within` runs `work` until it finishes or until `limit` has passed, whichever comes first. It returns `Some(output)` if the work finished in time and `None` if the limit passed first. In the `None` case the work future is dropped, and everything it held is dropped with it.

`switch-binance-feed` connects to a WebSocket server this way. It first wraps `within` so that a timeout becomes one of its own errors, naming the step that took too long:

```rust,ignore
{{#include ../../../conduit/crates/switch-binance-feed/src/lib.rs:within_step}}
```

Then it builds the whole connection, TCP followed by TLS and the WebSocket upgrade, as one async block, and bounds it with a single limit:

```rust,ignore
{{#include ../../../conduit/crates/switch-binance-feed/src/lib.rs:connect}}
```

Nothing in the `open` block has run when it is passed to `within`; it runs as `within` polls it. If the server never answers, the limit passes, `within` returns `None`, and the block is dropped. Whatever the block held at that moment goes with it. If it was still waiting in `tcp_connect`, the socket that connect was opening is closed: dropping an open in progress drops its socket (see [Opening sockets](opening.md)). If the TCP connection was already up and TLS was in progress, the TCP stream is dropped, which closes the connection.

The same pattern bounds any wait. [Closing connections and handling failures](../protocols/closing.md) uses it for the WebSocket close handshake, where a silent peer would otherwise keep `recv`, `close` and `linger` waiting forever.

### How the limit is measured

`within` creates a runtime sleep of `limit` converted to nanoseconds. Like every relative sleep, it counts from the runtime's latest accepted tick, at the moment the task first polls the `within` future. The limit has passed once a poll's tick reaches that deadline.

The `spawner` must belong to the runtime that runs the task; `within` panics otherwise. It also panics if `limit` does not fit in a `u64` count of nanoseconds, which is about 584 years.

## Racing a receive against a stop signal

```rust,ignore
pub async fn first<A: Future, B: Future>(a: A, b: B) -> Option<A::Output>
```

`first` polls both futures until one of them finishes. If `a` finishes, it returns `Some` with `a`'s output. If `b` finishes first, it returns `None`, and `b`'s output is discarded. Each time `first` is polled it polls `a` before `b`, so when both are ready in the same poll, `a` wins. The future that did not finish is dropped when `first` returns.

`switch-binance-feed` uses `first` to stop reading when the program is told to stop. The stop signal is a [oneshot receiver](../runtime/cooperating.md#oneshot-channels) that the drive loop sends on:

```rust,ignore
{{#include ../../../conduit/crates/switch-binance-feed/src/lib.rs:read_loop}}
```

On each pass through the loop, `first` races `wss.recv()` against the stop receiver. A received message wins and is handled; a stop signal ends the loop with `Ok(())`. Messages that are already waiting are handled before the stop, because `recv` is `a` and is polled first.

The stop receiver is passed as `&mut *stop`, a mutable reference, not by value. When `recv` wins, `first` drops only the reference, and the receiver itself survives for the next pass. Passing the receiver by value would consume it in the first race.

Dropping the losing `recv` is safe here because a WebSocket `recv` that is dropped while waiting loses nothing (see [Sending and receiving messages](../protocols/messages.md)). Before racing any other future against a stop signal, check what that future does when it is dropped part-way.

The two functions combine. `within` is itself built from `first`: it races `work` against a sleep. A task can also bound a whole phase and race it against a stop at the same time, as the feed does when it connects:

```rust,ignore
let result = match first(connecting, &mut stop).await {
```

Here `connecting` is the `within`-bounded connect from the previous section. The result is `None` if the stop came first, `Some(Err(..))` if the connect failed or timed out, and `Some(Ok(wss))` if it succeeded.

## The clock requirement

`within` turns its `Duration` into ticks by counting nanoseconds. That is only correct if the runtime's ticks are nanoseconds: its initial tick, the tick of every poll, and every relative sleep on it. The [drive loop](drive-loop.md) produces such ticks from one clock sample per round, as described in [Driving the runtime](../runtime/driving.md#ticks).

The deadline is exact in ticks. switch-net's own test of `within` starts a runtime at tick 1,000,037 and waits with a limit of 123,456 ns. The work is still pending one tick before the deadline and times out at the deadline itself:

```rust,ignore
{{#include ../../../switch-net/src/drive.rs:exact_deadline}}
```

In real time, a limit can pass up to one loop pass late, since it is noticed only when a poll's tick reaches it. A busy-polling loop keeps that delay to one round; a loop that sleeps when idle can add up to one idle sleep.

If the ticks are in another unit, every limit is off by the ratio between the units.

Next: [Errors](errors.md) lists every error the socket operations return.
