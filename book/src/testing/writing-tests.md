# Writing tests that use DPDK

A test that opens a NIC port, builds a Stack or turns a `Net` has to start DPDK first, and DPDK has rules that the standard Rust test harness breaks. This page explains the rule, the `#[pinned_test]` attribute that works around it, the fixtures in `switch-test-kit` that give tests fake NICs and a simulated wire, and the `testkit` module of `switch-net` for testing code against the Linux kernel as the peer.

These tests build and run where any DPDK code does: in the Docker image, or on Linux with DPDK installed ([Setting up a build environment](../getting-started/environment.md)). [Running the test suite](suite.md) shows how to run them.

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

Add `switch-test-support` as a dev-dependency and write `#[switch_test_support::pinned_test]` where you would write `#[test]`:

```rust,ignore
{{#include ../../../conduit/switch-test-support/tests/pinned.rs:pinned}}
```

The attribute replaces `#[test]`; if the function also has a `#[test]`, it is removed. Other attributes, such as `#[ignore]`, stay. It also works inside `proptest!` blocks.

Each body has a time limit, 180 seconds by default (`switch_test_support::PINNED_TEST_BOUND`). `#[pinned_test(bound_ms = N)]` sets a different one. A body that runs past its limit ends the whole test process: the worker thread is stuck holding EAL, so no later test in that binary could run anyway. The process prints ``pinned test `NAME` did not finish within BOUND; exiting the test process`` to stderr and exits with status 1, without running exit handlers or flushing stdout, so the harness's own `test NAME ...` line may be missing. The `timeout` the Makefile wraps around every test step is the backstop if even that fails.

The test binary must run with `--test-threads=1`, as every target in the Makefile does, and its process must be allowed on exactly one CPU.

## EAL and fake NICs

`switch-test-kit` holds the fixtures the workspace's DPDK tests share. Its entry point is `with_eal`, which starts EAL on the current thread the first time and hands it to the closure:

```rust,ignore
switch_test_kit::with_eal(|eal| {
    // open ports and build Stacks with `eal`
});
```

EAL starts with no PCI devices, no hugepages and 1 GiB of ordinary memory, with its main thread pinned to the one CPU the process may use; it panics if the process may use more than one. Call it from inside a `#[pinned_test]` body, so it always runs on the worker thread.

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
     a Pair round: turn a, forward a_to_b, turn b, forward b_to_a
```

A Pair round (`Pair::round`; not the same thing as switch-net's [`round`](../net/drive-loop.md), which turns a `Net` and polls the runtime) turns `a`, forwards what it sent to `b`, turns `b`, and forwards what it sent back to `a`. `pair.drive(observe, app)` repeats Pair rounds and calls `app` with both Stacks after each, until `app` returns `App::Done`. When a Pair round moves nothing and `app` returns `App::Idle`, it advances the clock to the earliest timer of either Stack, so retransmissions and delayed ACKs happen without real waiting. Sixteen idle Pair rounds in a row, or 100,000 in all, fail the test.

This test from `dpdk-smoltcp` establishes a TCP connection, sends four bytes from `a` and drives until `b` has them:

```rust,ignore
{{#include ../../../conduit/crates/dpdk-smoltcp/tests/stack_sockets.rs:pair}}
```

`pair::establish` connects a client on one side to a listener on the other and drives until both are established. It returns the two socket handles, client first. At the end, `pair.close()` frees every frame still on the rings or held by the links, closes both ports and checks that every packet buffer came back; it returns how many frames it had to free:

```rust,ignore
assert_eq!(pair.close(), 0);
```

The links can also damage the traffic. `Link::impair` takes a closure that sees each frame and returns a `wire::Verdict`: `Deliver`, `Duplicate`, `Drop`, or `Hold`, which keeps the frame back and delivers it right after the next one, reordering the two. From a teardown test in `switch-test-kit`:

```rust,ignore
let mut verdicts = [Verdict::Hold, Verdict::Deliver, Verdict::Hold].into_iter();
pair.a_to_b.impair(move |_| verdicts.next().unwrap());
```

## Testing switch-net code against the kernel

For code written with [switch-net](../net/index.md), the `testkit` module of `switch-net` (enabled by its `testkit` feature) sets up a `Net` on a tap device, with the Linux kernel at the other end. Your switch code runs as a [task](../getting-started/first-task.md); the peer is ordinary blocking Rust using `std::net` on a thread of its own.

Addresses are fixed: switch is `SWITCH_ADDR`, 192.0.2.2/24, and the kernel side of the tap is `KERNEL_ADDR`, 192.0.2.1, where the peer listens. A `TapConfig` names the tap device and sets the socket configuration and socket count:

```rust,ignore
{{#include ../../../switch-net/tests/testkit_echo.rs:tap}}
```

The name must be short and unique among taps alive at once: the device is `net_tap_<name>` and the Linux interface `cdtap_<name>`.

`client_server` runs one connection end to end. It starts a listener on 192.0.2.1 and runs your `serve` function on a thread with the connection it accepts. It connects to that listener from the `Net`, from a fresh local port, and runs your `client` as a task with the `NetHandle` and the connected `TcpStream`. It turns the `Net` and polls the runtime until both are done, and returns what `serve` returned. Here the peer reads one line and echoes it:

```rust,ignore
{{#include ../../../switch-net/tests/testkit_echo.rs:serve}}
```

and the client sends the line and reads until it is back:

```rust,ignore
{{#include ../../../switch-net/tests/testkit_echo.rs:client}}
```

The client returns its two stream halves. `client_server` drops what the client returns only after the peer has finished, so what the peer saw came from the client's sends, not from its socket closing early.

`with_tap_net(backend, &config, body)` is the lower-level form: it gives `body` a `TapNet`, which you turn yourself, and a `Clock`, and leaves the peer to you. Either way the body must drop every handle, socket and frame it made before it returns. Then `with_tap_net` takes the Stack back, closes the port with every packet buffer returned, and removes the tap device; anything still alive fails the test.

On each pass through its loop, `client_server` accepts the peer's connection if it has arrived, turns the `Net` at the test `Clock`'s current time, polls the runtime with a tick from the same clock, and sleeps 50 µs so the peer thread gets the CPU. [Timeouts and racing futures](../net/timeouts.md#the-clock-requirement) explains which tick unit `within` and sleeps expect.

## Running one body on both backends

Both `with_tap_net` and `client_server` take a `Backend`. `Backend::Conduit` builds a `Net` on the [local backend](../net/backends.md). `Backend::Junction` runs the daemon's side of the junction backend (its Stack and the session's other end) on the same test thread, over the same tap device, and gives the test a `Net` on the junction backend, so the same body exercises both. The usual shape is one function that takes the backend and one pinned test per backend:

```rust,ignore
{{#include ../../../switch-net/tests/testkit_echo.rs:run}}
```

`switch-net`'s own tests generate such pairs with a small `macro_rules!` in `switch-net/tests/support/mod.rs`, which also has a two-backend fixture over ring ports, with a raw Stack as the peer instead of the kernel.

## Where to go next

[Protocol conformance: Autobahn and TLS-Anvil](conformance.md) covers the external suites that check the WebSocket and TLS implementations.
