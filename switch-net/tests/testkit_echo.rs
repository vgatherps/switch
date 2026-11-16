//! A switch-net client against a kernel TCP echo peer on a tap device, on both backends:
//! the testkit example of the book's "Writing tests that use DPDK" page.
//! Needs CAP_NET_ADMIN and /dev/net/tun (the Docker test container).

// ANCHOR: setup
use std::io::{Read, Write};

use switch_net::testkit::{client_server, Backend, TapConfig};
use switch_net::{tcp, NetHandle, TcpStream};

const PORT: u16 = 7000;
const LINE: &[u8] = b"hello";
// ANCHOR_END: setup

// ANCHOR: tap
const TAP: TapConfig = TapConfig {
    name: "bookecho",
    socket: tcp::Config { rx_capacity: 64 * 1024, tx_capacity: 64 * 1024, mss_cap: None },
    max_sockets: 1,
    random_seed: 1,
};
// ANCHOR_END: tap

// ANCHOR: serve
/// The kernel side: reads one line and writes it back.
fn serve(mut stream: std::net::TcpStream) -> Vec<u8> {
    let mut line = vec![0; LINE.len()];
    stream.read_exact(&mut line).expect("read the line");
    stream.write_all(&line).expect("echo the line");
    line
}
// ANCHOR_END: serve

// ANCHOR: client
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
// ANCHOR_END: client

// ANCHOR: run
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
// ANCHOR_END: run

#[switch::task]
async fn timeout(spawner: switch::Spawner, done: std::rc::Rc<std::cell::Cell<bool>>) {
    assert!(switch_net::within(
        &spawner,
        std::time::Duration::from_millis(10),
        std::future::pending::<()>(),
    ).await.is_none());
    done.set(true);
}

fn written_timeout(backend: Backend) {
    switch_net::testkit::with_tap_net(backend, &TAP, |net, clock| {
        let mut runtime = switch::Runtime::new(clock.ns());
        let spawner = runtime.spawner();
        let done = std::rc::Rc::new(std::cell::Cell::new(false));
        spawner.spawn(timeout(spawner.clone(), done.clone())).unwrap();
        let start = clock.elapsed();
        while !done.get() {
            assert!(clock.elapsed() - start < std::time::Duration::from_secs(1), "10 ms timeout did not expire");
            net.turn(clock.now()).unwrap();
            runtime.poll(clock.ns()).unwrap();
            std::thread::sleep(switch_net::testkit::ROUND_SLEEP);
        }
        assert!(clock.elapsed() - start >= std::time::Duration::from_millis(10));
        runtime.shutdown();
    });
}

#[switch_test_support::pinned_test]
fn within_uses_written_duration_local_backend() {
    written_timeout(Backend::Conduit);
}

#[switch_test_support::pinned_test]
fn within_uses_written_duration_junction_backend() {
    written_timeout(Backend::Junction);
}
