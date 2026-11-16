# Writing tests that use DPDK

Mark a test that uses DPDK `#[switch_test_support::pinned_test]` instead of `#[test]`, and start DPDK in it with `switch_test_kit::with_eal`. A test that opens a NIC port, builds a Stack or drives a `Net` needs both, because DPDK has rules that the standard Rust test harness breaks. This page explains the rule and the attribute, the fixtures in `switch-test-kit` that give tests fake NICs and a simulated wire, and the `testkit` module of `switch-net` for testing code against the Linux kernel as the peer.

These tests build and run where any DPDK code does: in the Docker image, or on Linux with DPDK installed ([Setting up a build environment](../getting-started/environment.md)). [Running the test suite](suite.md) shows how to run them. A crate outside this repository uses them as path dependencies of a clone:

```toml
[dev-dependencies]
switch-test-support = { path = "<clone>/conduit/switch-test-support" }
switch-test-kit = { path = "<clone>/conduit/switch-test-support/switch-test-kit" }
switch-net = { path = "<clone>/switch-net", features = ["testkit"] }
```

`switch-test-kit` is for tests of the Stack itself; `switch-net` with its `testkit` feature is for tests of code written with switch-net.

## Why tests need one thread

[EAL](../nic/index.md), DPDK's per-process setup, can be started only once per process, and the thread that starts it owns it. That thread becomes DPDK's main thread, pinned to the one CPU the process may use, and packet buffers and NIC ports must be used from it.

The standard test harness runs every test on a fresh thread. If each test started DPDK on its own thread, only the first could succeed, and the rest would run on threads that do not own EAL.

`#[pinned_test]` solves this by sending every test body to one long-lived worker thread per test binary. The worker starts EAL the first time a body asks for it, and every later body in that binary runs on the same thread with the same EAL:

```text
 libtest thread: test_a ---+                                +--> result or panic of test_a
 libtest thread: test_b ---+--> channel --> switch-test-worker thread (owns EAL)
 libtest thread: test_c ---+     one body at a time          runs body_a, body_b, body_c in turn
                                         |
                         body runs past its bound? ----> print the test's name, _exit(1)
```

The test harness still sees one test per function, with its own pass or fail. A panic in a body fails that test with its original payload, and its output shows the panic message, with the source location when the panic happened on the worker thread itself.

## The #[pinned_test] attribute

Write `#[switch_test_support::pinned_test]` where you would write `#[test]`. This test starts EAL with `with_eal`, described in the next section, and checks that it belongs to the worker thread:

```rust,ignore
{{#include ../../../conduit/switch-test-support/switch-test-kit/tests/book.rs:pinned}}
```

The attribute replaces `#[test]`; if the function also has a `#[test]`, it is removed. Other attributes, such as `#[ignore]`, stay. It also works inside `proptest!` blocks.

Each body has a time limit, 180 seconds by default (`switch_test_support::PINNED_TEST_BOUND`). `#[pinned_test(bound_ms = N)]` sets a different one. A body that runs past its limit ends the whole test process: the worker thread is stuck holding EAL, so no later test in that binary could run anyway. The process prints ``pinned test `NAME` did not finish within BOUND; exiting the test process`` to stderr and exits with status 1, without running exit handlers or flushing stdout, so the harness's own `test NAME ...` line may be missing. The `timeout` the Makefile wraps around every test step is the backstop if even that fails.

The test binary must run with `--test-threads=1`, as every DPDK target in the Makefile does, and its process must be allowed on exactly one CPU.

## EAL and fake NICs

`switch-test-kit` holds the fixtures the workspace's DPDK tests share. Its entry point is `with_eal`, which starts EAL on the current thread the first time and hands it to the closure, as in the test above. EAL starts with no PCI devices, no hugepages and 1 GiB of ordinary memory, with its main thread pinned to the one CPU the process may use; it panics if the process may use more than one. Call it from inside a `#[pinned_test]` body, so it always runs on the worker thread.

Instead of real NICs, tests use DPDK's virtual devices:

| Fixture | What it is |
|---|---|
| `ring_port::RingPort` | A NIC port backed by two in-memory rings, where the test itself is the peer: it puts frames on the receive ring and takes them off the transmit ring. |
| `tap::TapVdev` | A [tap device](../getting-started/first-connection.md), so the Linux kernel is the peer. Needs `CAP_NET_ADMIN` and `/dev/net/tun`. |
| `pcap::PcapVdev` | A device that reads received frames from one pcap file and writes sent frames to another. |

Fixtures that own a device or a ring have a `close` method. Declare them before the port they back, so the port is dropped first. A fixture dropped without `close` cleans up anyway, and then, unless the test is already panicking, fails the test. Fixture names (of rings and devices) must be unique among the fixtures alive at the same time.

## Two Stacks on a simulated wire

Most Stack tests need two endpoints talking to each other. `pair::Pair` holds two real Stacks, `a` and `b`, each on its own `RingPort`, joined by two `wire::Link`s that copy every frame from one side's transmit ring to the other side's receive ring. Both Stacks run on one clock that the test advances itself, so nothing sleeps and a test runs the same way every time.

```text
         Stack a                                             Stack b
  (10.0.0.1, Side::A)                                  (10.0.0.2, Side::B)
          |                                                   |
     RingPort A  --tx ring--> Link a_to_b --rx ring-->   RingPort B
                 <--rx ring-- Link b_to_a <--tx ring--
                                  |
                     one clock, advanced by the test
     a Pair round: a receives and sends, forward a_to_b, b receives and sends, forward b_to_a
```

A Pair round (`Pair::round`; not the same thing as switch-net's [`round`](../net/drive-loop.md), which drives a `Net` and the runtime) runs Stack `a`'s receive and send steps in one call (`Stack::turn`), forwards what `a` sent to `b`, does the same for `b`, and forwards what `b` sent back to `a`. `pair.drive(observe, app)` repeats Pair rounds and calls `app` with both Stacks after each, until `app` returns `App::Done`. When a Pair round moves nothing and `app` returns `App::Idle`, it advances the clock to the earliest timer of either Stack, so retransmissions and delayed ACKs happen without real waiting. Sixteen idle Pair rounds in a row, or 100,000 in all, fail the test.

This test establishes a TCP connection from `a` to `b`, sends four bytes from `a`, drives until `b` has them, and closes the pair:

```rust,ignore
{{#include ../../../conduit/switch-test-support/switch-test-kit/tests/book.rs:pair}}
```

`pair::establish(&mut pair, Side::A)` connects a client on side A to a listener on side B and drives until both are established. It returns the two socket handles in side order, A then B (here client, then server), and every frame it delivered on the way. `pair.close()` frees every frame still on the rings or held by the links, closes both ports and checks that every packet buffer came back; it returns how many frames it had to free, so a clean test expects 0.

The links can also damage the traffic. `Link::impair` takes a closure that sees each frame and returns a `wire::Verdict`: `Deliver`, `Duplicate`, `Drop`, or `Hold`, which keeps the frame back and delivers it right after the next one, reordering the two. From a teardown test in `switch-test-kit`:

```rust,ignore
let mut verdicts = [Verdict::Hold, Verdict::Deliver, Verdict::Hold].into_iter();
pair.a_to_b.impair(move |_| verdicts.next().unwrap());
```

## Testing switch-net code against the kernel

For code written with [switch-net](../net/index.md), the `testkit` module of `switch-net` (enabled by its `testkit` feature) sets up a `Net` on a tap device, with the Linux kernel at the other end. Your switch code runs as a [task](../getting-started/first-task.md); the peer is ordinary blocking Rust using `std::net` on a thread of its own.

Addresses are fixed: switch is `SWITCH_ADDR`, 192.0.2.2/24, and the kernel side of the tap is `KERNEL_ADDR`, 192.0.2.1, where the peer listens. The example below uses these imports and constants:

```rust,ignore
{{#include ../../../switch-net/tests/testkit_echo.rs:setup}}
```

A `TapConfig` names the tap device and sets the socket configuration and socket count:

```rust,ignore
{{#include ../../../switch-net/tests/testkit_echo.rs:tap}}
```

The name must be short and unique among taps alive at once: the device is `net_tap_<name>` and the Linux interface `cdtap_<name>`.

`client_server` runs one connection end to end. Its seven arguments are:

1. `backend`: `Backend::Conduit` or `Backend::Junction` (see the last section),
2. `tap`: the `TapConfig`,
3. `port`: the port the peer listens on, on 192.0.2.1,
4. `listener_setup`: a closure run on the peer's `TcpListener` before it accepts, for socket options (`|_| {}` for none),
5. `serve`: the peer, run on its own thread with the accepted `std::net::TcpStream`; its return value is what `client_server` returns,
6. `client`: a function (an `async fn` works) that takes the `NetHandle` and the connected `TcpStream` and returns the future `client_server` runs as a switch task; `client_server` opens the stream from a fresh local port, and drops what the future returns only after the peer has finished,
7. `each_round`: a closure called each time through `client_server`'s loop (`|| {}` for none).

Each time through its loop it runs one round by hand: the `Net`'s receive step, a poll of the runtime, the `Net`'s send step, then `each_round`. It stops when both sides are done. Here the peer reads one line and echoes it:

```rust,ignore
{{#include ../../../switch-net/tests/testkit_echo.rs:serve}}
```

and the client sends the line and reads until it is back:

```rust,ignore
{{#include ../../../switch-net/tests/testkit_echo.rs:client}}
```

The client returns its two stream halves. `client_server` drops what the client returns only after the peer has finished, so what the peer saw came from the client's sends, not from its socket closing early.

`with_tap_net(backend, &config, body)` is the lower-level form: it gives `body` a `TapNet`, whose `receive(now)` and `send()` you call yourself around your own runtime polls, and a `Clock`, and leaves the peer to you. Either way the body must drop every handle, socket and frame it made before it returns. Then `with_tap_net` takes the Stack back, closes the port with every packet buffer returned, and removes the tap device; anything still alive fails the test.

`Clock::now()` supplies the Stack's microsecond `Instant`; `Clock::ns()` supplies monotonic nanosecond ticks for `Runtime::poll` and relative sleeps. `client_server` polls its runtime in nanoseconds, so `within` limits use their written durations. It sleeps 50 µs each time through its loop, so the peer thread gets the CPU.

## Running one body on both backends

Both `with_tap_net` and `client_server` take a `Backend`. `Backend::Conduit` builds a `Net` on the [local backend](../net/backends.md). `Backend::Junction` runs the daemon's side of the junction backend (its Stack and the session's other end) on the same test thread, over the same tap device, and gives the test a `Net` on the junction backend, so the same body exercises both. The usual shape is one function that takes the backend and one pinned test per backend:

```rust,ignore
{{#include ../../../switch-net/tests/testkit_echo.rs:run}}
```

`switch-net`'s own tests generate such pairs with a small `macro_rules!` in `switch-net/tests/support/mod.rs`, which also has a two-backend fixture over ring ports, with a raw Stack as the peer instead of the kernel.

## Where to go next

[Protocol conformance: Autobahn and TLS-Anvil](conformance.md) covers the external suites that check the WebSocket and TLS implementations.
