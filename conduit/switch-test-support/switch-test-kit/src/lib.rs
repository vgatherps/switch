//! DPDK fixtures shared by the workspace's integration tests and benches.
//!
//! With these fixtures a test starts DPDK, opens NIC ports that need no hardware, and runs two real
//! `Stack`s against each other on a simulated wire, driven by a clock the test advances itself, so
//! nothing sleeps and every run is the same. Run each test body with
//! `#[switch_test_support::pinned_test]`, so it runs on the one thread that owns EAL.
//!
//! This test establishes a TCP connection, sends four bytes from side A, drives until side B has
//! them, and checks that every packet buffer came back:
//!
//! ```no_run
//! use switch_test_kit::pair::{App, Pair, Side, T0, config, establish};
//! use switch_test_kit::with_eal;
//!
//! with_eal(|eal| {
//!     let mut pair = Pair::new(eal, ["book_a", "book_b"], [config(Side::A), config(Side::B)], T0);
//!     let ([client, server], _) = establish(&mut pair, Side::A);
//!     pair.a.tcp_mut(&client).send_slice(b"book").unwrap();
//!     pair.drive(
//!         |_, _| {},
//!         |_, b| if b.tcp(&server).recv_queue() == 4 { App::Done } else { App::Idle },
//!     );
//!     pair.round(|_, _| {});
//!     assert_eq!(pair.close(), 0);
//! });
//! ```
//!
//! [`with_eal`] starts EAL on the calling thread the first time and passes it to the closure: no
//! PCI devices, no hugepages, 1 GiB of ordinary memory, the main thread pinned to the one CPU the
//! process may use ([`eal_args`] panics if it may use more).
//! [`ring_port::RingPort`] is a NIC port on two in-memory rings where the test is the peer.
//! [`pair::Pair`] is two `Stack`s on ring ports joined by copying [`wire::Link`]s, with a
//! [`wire::Verdict`] per frame to deliver, duplicate, drop or hold traffic.
//! [`many`] opens many connections over a `Pair`, and [`peers`] answers for on-link neighbors.
//! [`tap::TapVdev`] is a net_tap device with the Linux kernel as the peer, and [`pcap::PcapVdev`]
//! reads and writes pcap files.
//!
//! Declare fixtures that own a device or ring before the port they back, so the port drops first,
//! and `close` them. A fixture dropped without `close` cleans up and then, unless the test is
//! already panicking, fails the test. Fixture names must be unique among live fixtures.
//!
//! See the switch book, chapter "Writing tests that use DPDK".

use std::cell::OnceCell;

use dpdk_smoltcp::Eal;
use switch_dpdk_sys::switch_lcore_id;

pub mod many;
pub mod pair;
pub mod pcap;
pub mod peers;
pub mod ring_port;
pub mod tap;
pub mod wire;

/// Test EAL argv: no PCI, hugepages, shared config or telemetry; lcore 7 as
/// the main lcore, on the one CPU the calling thread may run on; 1 GiB of
/// (lazily touched) memory, because RX pools back every socket's receive
/// budget and the 2 x 10 240-connection benches need ~320 MiB of RX mbufs.
/// Panics unless exactly one CPU is allowed.
pub fn eal_args() -> Vec<String> {
    let cpu = switch_dpdk_sys::only_allowed_cpu().unwrap_or_else(|error| panic!("{error}"));
    [
        "switch-test",
        "--no-pci",
        "--no-huge",
        "--no-shconf",
        "--no-telemetry",
        "-m",
        "1024",
        format!("--lcores=7@{cpu}").as_str(),
        "--main-lcore=7",
    ]
    .map(str::to_owned)
    .into()
}

thread_local! {
    static EAL: OnceCell<Eal> = const { OnceCell::new() };
}

/// `body` with this thread's EAL, initialized on first use; the thread that
/// initialized EAL owns it.
pub fn with_eal<R>(body: impl FnOnce(&Eal) -> R) -> R {
    EAL.with(|owner| {
        let eal = owner.get_or_init(|| {
            let args = eal_args();
            Eal::init(&args.iter().map(String::as_str).collect::<Vec<_>>()).unwrap()
        });
        assert_eq!(eal.owner_lcore_id(), 7);
        assert_eq!(unsafe { switch_lcore_id() }, 7);
        body(eal)
    })
}
