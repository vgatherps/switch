# The drive loop

A switch-net program has one loop that does all the work: it moves packets and runs tasks, over and over, on the pinned CPU. That loop holds the `Net` from [Sockets with switch-net](index.md), runs rounds, and decides when to rest. [Driving the runtime](../runtime/driving.md) covers the runtime half on its own; here we add the network.

## A round: receive, poll, send

A **round** has three steps, always in this order:

1. **receive**: `net.receive(now)` takes in packets and wakes the tasks whose sockets became ready.
2. **poll**: `rt.poll(tick)` runs the woken tasks until each waits again.
3. **send**: `net.send()` pushes out what the tasks wrote since the receive step.

`switch_net::round` does all three with one clock reading:

```rust,ignore
pub fn round(net: &mut Net, rt: &mut Runtime, now: Instant, tick: u64) -> Result<bool, RoundError>
```

Because send comes after poll, a reply that a woken task queues leaves in the same round as the packet that woke it. Only the receive and send steps move packets. A task that calls `try_send` only queues a frame; the frame leaves in the next send step. A task never sees new network state while it runs: data that arrives is taken in by a receive step, which wakes the task so the poll that follows runs it.

`round` never sleeps. It returns `true` if anything moved: the receive step took in or processed packets, packets are still waiting to be processed, the poll ran at least one task, or the send step put packets out or left some pending. A drive loop is a loop around it:

```rust,ignore
{{#include ../../../switch-net/examples/tcp_client_local.rs:drive}}
```

```text
   +--> clock.sample()
   |         |
   |         v
   |    net.receive(now) ---> rt.poll(tick) ---> net.send()
   |                              |                  ^
   |                              |                  |
   |              a task's send reaches the TX burst:
   |              the poll ends early, the send step comes sooner
   |                                                 |
   +---- busy polling: go again at once <---- moved? +
         sleeping: if nothing moved, sleep briefly (1 ms in the examples)
```

## What each step does on each backend

On the local backend the Stack runs inside your process. The receive step calls `Stack::receive(now, ingress_budget)`: it takes in up to `NetConfig::ingress_budget` received packets, lets the Stack answer what needs an immediate answer (acknowledgements, resets, ARP replies), and then wakes every task waiting on a socket that became ready. Only sockets that reported a change are checked, so an idle receive step costs the same with one parked task or a thousand. It also wakes tasks waiting for an empty frame: as many of the longest-waiting ones as the pool has free frames. The send step calls `Stack::send()`: TCP sends what sockets have queued, including retransmissions and window updates, and the packets go to the NIC. The repository uses an ingress budget of 32 in its tap examples and tests, and 128 in switch-bench and the market-data feed (`switch-binance-feed`).

On the junction backend the daemon runs the Stack on its own CPU. The receive step polls the session with the daemon once: it sends pending control messages, applies the daemon's events (a connection is established, the peer sent FIN, a socket closed), collects the packets the daemon delivered into the receive ring (the session's shared memory that the daemon writes received packets into), and wakes the tasks those changes concern. A task's `try_send` has already copied its bytes into the send ring, the shared memory that carries sends to the daemon, so the send step has nothing left to push. It reports how many records went to the daemon since the last send step, and reports it if the daemon is gone.

The receive step returns a `Turn` and the send step a `Sent`, two counter types:

| Type and field | Counts |
|---|---|
| `Turn::rx_received` | packets this receive step took from the NIC |
| `Turn::rx_rejected` | received packets dropped as unsupported or with a bad IPv4 header checksum |
| `Turn::rx_processed` | received packets handed to TCP/IP processing (on the junction backend: events plus packets) |
| `Turn::rx_staged` | received packets still waiting after the receive step, because the budget ran out |
| `Turn::tx_accepted`, `Turn::tx_pending` | packets the NIC accepted during the receive step, and packets still waiting for it afterwards |
| `Sent::tx_accepted` | packets the NIC accepted in the send step (on the junction backend: records sent to the daemon) |
| `Sent::tx_pending` | packets built but not yet accepted by the NIC |

On the junction backend `rx_staged`, `rx_rejected` and both `tx_pending` counters are always 0, and `Turn::tx_accepted` is 0.

## Time: one clock, two units

`net.receive` takes the Stack's time, a `switch_net::Instant` in microseconds. `rt.poll` takes a tick, a `u64`. The two must come from one clock reading so the network and the tasks agree on what time it is. `net.send` takes no time; it runs at the time of the last receive. The repository's programs take one `std::time::Instant` at startup and, every round, read the elapsed time once: the Stack gets it in microseconds and the runtime gets it in nanoseconds.

```rust,ignore
{{#include ../../../switch-net/examples/tcp_client_local.rs:clock}}
```

Ticks in nanoseconds are required as soon as any task uses `switch_net::within` or a `Duration`-based sleep, because those convert durations to nanoseconds; see [Ticks](../runtime/driving.md#ticks) and [Timeouts and racing futures](timeouts.md). Use this one clock for everything, including the clock you pass to `switch_open::open`. Do not use `Instant::now()` from smoltcp, the TCP/IP library the Stack is built on: it reads the wall clock, which can jump.

## Busy polling or sleeping

A loop either busy-polls or sleeps when idle:

**Busy polling** calls `round` again at once, whether or not anything moved. This gives the lowest latency and is the intended way to run on a pinned CPU that nothing else uses. switch-bench's loop busy-polls. [The pinned CPU and helper threads](../nic/cpus.md) explains how to give the loop a CPU to itself.

**Sleeping** pauses briefly when `round` returns `false`. The market-data feed and the first network program sleep 1 ms after an idle round, because there the CPU is shared with other work. On the local backend `Net::poll_at()` says when the Stack's next TCP timer is due. A loop that sleeps longer than a millisecond or so should wake by then. The examples sleep a fixed 1 ms, which keeps TCP's timers at most that late. On the junction backend `poll_at()` always returns `None`: TCP timers run in the daemon. Either way, a runtime sleep can fire up to one idle sleep late.

## When tasks send many frames

On the local backend, frames that tasks hand to the Stack wait in their sockets until the send step. If a task sends a lot in one runtime poll, those frames would sit there until every other ready task had run too. switch-net shortens that wait on its own. It counts the frames handed to the Stack (successful `try_send` and UDP `send` calls) since the last successful send step. From the send that reaches `Stack::tx_burst()`, the number of frames the NIC port takes in one burst, each `try_send` or UDP `send` asks the runtime to end the current poll after that task ([`request_turn`](../runtime/driving.md#request_turn)). The poll returns, the send step runs, and the frames leave sooner. A successful send step starts the count again. There is nothing to configure. The junction backend has no equivalent: `try_send` puts its bytes into the send ring at once.

## Errors from a round

`net.receive` and `net.send` both return `TurnError`, and `round` wraps either as `RoundError::Turn`:

- `TurnError::Stack` on the local backend: the Stack failed, for example `TimeWentBackwards` when `now` is earlier than the previous receive step's time, or a `Port` error from the NIC port. [Stack errors and panics](../stack/errors.md) lists them.
- `TurnError::HostLost` on the junction backend: the session with the daemon ended. See [When the daemon goes away](errors.md#when-the-daemon-goes-away).

A receive step that returns an error still wakes the tasks it found ready, because the error can come after packets were taken in. Tasks still waiting stay waiting. `round` returns the error at once, so that round has no poll and no send step. `RoundError` also carries `RuntimeError` from the poll. It implements only `Debug`, so print it with `{:?}`.

If a panic unwinds out of `receive` or `send`, the Net is unusable: drop it. Dropping the `Net` while sockets are still alive leaves those sockets with nothing to receive or send for them, and any task waiting on them is never woken; [Shutting down cleanly](shutdown.md) gives the right order.

Next: [Opening sockets](opening.md). For the same loop without switch-net, see [The Stack's drive loop](../stack/drive-loop.md).
