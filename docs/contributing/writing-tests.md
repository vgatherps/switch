# Writing tests

Code that does not start DPDK is tested with plain `#[test]`. A test that starts DPDK, opens a network card port, builds a stack or drives a `Net` needs the `#[pinned_test]` attribute and the fixtures described here. These tests build and run where DPDK is installed: in the Docker image, or on Linux with DPDK 25.11.3. [Running the tests](testing.md) shows how to run them.

A crate in this workspace takes the helpers as dev-dependencies:

```toml
[dev-dependencies]
switch-test-support.workspace = true   # #[pinned_test]
switch-test-kit.workspace = true       # DPDK fixtures: fake ports, two-stack pairs
```

## One thread for every DPDK test

DPDK's per-process setup, EAL, starts once per process and belongs to the thread that started it; packet buffers and ports must be used from that thread. The standard test harness runs each test on a new thread, so only the first test could start EAL and the others would run on threads that do not own it.

`#[switch_test_support::pinned_test]` replaces `#[test]` and runs the test body on one long-lived worker thread per test binary. The first body that needs EAL starts it on that thread, and every later body in the binary uses the same one. The harness still reports each test separately, and a panic in a body fails that test with its own message.

```rust
use switch_test_kit::with_eal;

#[switch_test_support::pinned_test]
fn eal_belongs_to_the_test_worker() {
    with_eal(|eal| {
        assert_eq!(eal.owner_lcore_id(), 7);
    });
}
```

`with_eal` starts EAL the first time it is called and passes it to the closure. It starts with no PCI devices, no hugepages and 1 GiB of ordinary memory, with its main thread on the one CPU the process may use. That thread gets DPDK's thread number (lcore) 7 (`--lcores=7@<cpu>`), so the assertion above checks that the worker thread is the one that owns EAL. `with_eal` panics if the process may use more than one CPU, so run every DPDK test binary pinned to one CPU and with one test thread:

```sh
taskset -c 3 cargo test --locked -p dpdk-smoltcp -- --test-threads=1
```

Each body has a time limit, 180 seconds by default. `#[pinned_test(bound_ms = 5000)]` sets another. A body that runs past its limit ends the whole test process, because the stuck worker still owns EAL and no later test in the binary could run. The process prints

```text
pinned test `NAME` did not finish within 180s; exiting the test process
```

and exits with status 1. The harness's own `test NAME ...` line may be missing from the output.

Other attributes on the function, such as `#[ignore]`, are kept. The attribute also works inside `proptest!` blocks.

## Fake network ports

Tests do not use real cards. `switch-test-kit` has three DPDK virtual devices:

| Fixture | What it is |
|---|---|
| `ring_port::RingPort` | A port on two in-memory rings. The test is the peer: it puts frames on the receive ring and takes them off the transmit ring |
| `tap::TapVdev` | A tap device, with the Linux kernel as the peer. Needs `CAP_NET_ADMIN` and `/dev/net/tun`, which the Docker test containers have |
| `pcap::PcapVdev` | A port that reads received frames from one pcap file and writes sent frames to another |

Declare a fixture before the port it backs, so the port is dropped first, and call its `close` method at the end of the test. A fixture dropped without `close` cleans up and then fails the test, unless the test is already panicking. Names of fixtures alive at the same time must differ.

## Two stacks on a simulated wire

Most stack tests need two endpoints. `pair::Pair` holds two real stacks, `a` and `b`, each on a `RingPort`, joined by two `wire::Link`s that copy every frame from one side's transmit ring to the other side's receive ring. Both stacks run on a clock the test advances itself, so nothing sleeps and a test runs the same way every time.

This test opens a TCP connection from `a` to `b`, sends four bytes, and drives the pair until `b` has them:

```rust
use switch_test_kit::pair::{App, Pair, Side, T0, config, establish};
use switch_test_kit::with_eal;

#[switch_test_support::pinned_test]
fn four_bytes_reach_the_peer() {
    with_eal(|eal| {
        let mut pair = Pair::new(
            eal,
            ["book_a", "book_b"],
            [config(Side::A), config(Side::B)],
            T0,
        );
        let ([client, server], _) = establish(&mut pair, Side::A);
        pair.a.tcp_mut(&client).send_slice(b"book").unwrap();
        pair.drive(
            |_, _| {},
            |_, b| {
                if b.tcp(&server).recv_queue() == 4 {
                    App::Done
                } else {
                    App::Idle
                }
            },
        );
        // Let the client receive any acknowledgement sent by the server.
        pair.round(|_, _| {});
        assert_eq!(pair.close(), 0);
    });
}
```

`establish` connects a client on the given side to a listener on the other and drives until both ends are connected. It returns the two socket handles, side A first, and the frames it delivered on the way.

`pair.round` runs one step of each stack and moves what each sent to the other side. `pair.drive` repeats rounds and calls its second closure with both stacks after each, until the closure returns `App::Done`. When a round moves nothing and the closure returns `App::Idle`, the clock jumps to the earliest timer of either stack, so retransmissions and delayed ACKs happen without waiting. Sixteen idle rounds in a row fail the test, and so does round 100,000. The first closure sees every frame that crosses the wire.

`pair.close()` frees every frame still on the rings or held by the links, closes both ports, and checks that every packet buffer came back. It returns how many frames it had to free; a clean test expects 0.

A link can damage the traffic. `Link::impair` takes a closure that sees each frame and returns a `wire::Verdict`: `Deliver`, `Duplicate`, `Drop`, or `Hold`, which keeps one frame back and delivers it right after the next frame the link delivers. A link holds one frame at a time; a second `Hold` while it holds one panics. This drops every frame from `b` to `a`:

```rust
use switch_test_kit::wire::Verdict;

pair.b_to_a.impair(|_| Verdict::Drop);
```

## Testing switch-net code against the kernel

For code built on `switch-net`, its `testkit` module (feature `testkit`) sets up a `Net` on a tap device with the Linux kernel at the other end. The code under test runs as a switch task; the peer is ordinary Rust using `std::net` on its own thread. switch is at 192.0.2.2/24 and the kernel side at 192.0.2.1, where the peer listens.

```toml
[dev-dependencies]
switch-net = { workspace = true, features = ["testkit"] }
switch-test-support.workspace = true
```

`client_server` runs one TCP connection end to end. This test sends a line from switch and checks that the kernel peer echoes it twice: once with the `Net` owning the tap device (`Backend::Conduit`), and once sharing it through the daemon (`Backend::Junction`), whose side runs on the same test thread over the same tap device:

```rust
use std::io::{Read, Write};

use switch_net::testkit::{client_server, Backend, TapConfig};
use switch_net::{tcp, NetHandle, TcpStream};

const PORT: u16 = 7000;
const LINE: &[u8] = b"hello";

const TAP: TapConfig = TapConfig {
    name: "bookecho",
    socket: tcp::Config { rx_capacity: 64 * 1024, tx_capacity: 64 * 1024, mss_cap: None },
    max_sockets: 1,
    random_seed: 1,
};

/// The kernel side: reads one line and writes it back.
fn serve(mut stream: std::net::TcpStream) -> Vec<u8> {
    let mut line = vec![0; LINE.len()];
    stream.read_exact(&mut line).expect("read the line");
    stream.write_all(&line).expect("echo the line");
    line
}

/// The switch side, run as a task: sends the line and reads until it is back. Returns the
/// halves so the connection stays open until the peer is done.
async fn client(handle: NetHandle, stream: TcpStream) -> (switch_net::TcpRx, switch_net::TcpTx) {
    let (mut rx, mut tx) = stream.split();
    let mut frame = handle.tx_frame().await;
    frame.as_mut()[..LINE.len()].copy_from_slice(LINE);
    frame.truncate(LINE.len());
    tx.room(LINE.len()).await.expect("room for the line");
    tx.try_send(frame.freeze()).map_err(|(_, error)| error).expect("send the line");
    let mut echoed = Vec::new();
    while echoed.len() < LINE.len() {
        let frame = rx.recv().await.expect("receive the echo");
        echoed.extend_from_slice(frame.payload());
    }
    assert_eq!(echoed, LINE);
    (rx, tx)
}

fn echo(backend: Backend) {
    let seen = client_server(backend, &TAP, PORT, |_| {}, serve, client, || {});
    assert_eq!(seen, LINE);
}

#[switch_test_support::pinned_test]
fn echo_local_backend() {
    echo(Backend::Conduit);
}

#[switch_test_support::pinned_test]
fn echo_junction_backend() {
    echo(Backend::Junction);
}
```

The tap name must be short and unique among taps alive at once; the device becomes `net_tap_<name>` and the Linux interface `cdtap_<name>`.

`client_server(backend, tap, port, listener_setup, serve, client, each_round)` starts the peer listening on 192.0.2.1:`port`, calling `listener_setup` on its `TcpListener` first. It runs `serve` on the connection the peer accepts, and `client` as a switch task on a stream connected from a fresh local port. It drives the `Net` until both are done, calling `each_round` each time round, and returns what `serve` returned. It drops what `client` returned only after the peer has finished, so what the peer saw came from the client's sends, not from its socket closing. The whole run must finish within 60 seconds.

`with_tap_net(backend, &config, body)` is the lower-level form when the test needs its own loop: `body` gets a `TapNet`, whose `receive(now)` and `send()` it calls around its own runtime polls, and a `Clock`. `Clock::now()` is the time for `receive`; `Clock::ns()` is the tick for the runtime. Either way, the body must drop every handle, socket and frame it made before it returns; anything still alive when the tap is torn down fails the test.
