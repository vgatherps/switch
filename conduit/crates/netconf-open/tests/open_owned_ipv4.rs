//! One `open` call lives in this test binary because EAL initializes once per process.

#[path = "open_tap/common.rs"]
mod common;

use std::ffi::CString;
use std::net::UdpSocket;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant as WallInstant};

use dpdk_smoltcp::smoltcp::time::Instant;
use zhida_open::open;

const IFACE: &str = "cdo7_owned";
/// Only in the config's `owned_ipv4`, not its `ipv4` (192.0.2.2/24).
const OWNED: &str = "192.0.2.50";
/// In neither.
const UNOWNED: &str = "192.0.2.51";

fn ip(args: &[&str]) -> String {
    let output = Command::new("ip").args(args).output().unwrap();
    assert!(
        output.status.success(),
        "ip {}: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

/// The kernel's neighbour entry for `addr` on the tap, as `ip neigh` prints it.
fn neighbour(addr: &str) -> String {
    ip(&["neigh", "show", "to", addr, "dev", IFACE])
}

/// With 192.0.2.50/32 in `owned_ipv4`, the Stack `open` builds answers the kernel's ARP request
/// for it with its own MAC; 192.0.2.51, owned by nobody, stays unresolved.
#[zhida_test_support::pinned_test]
fn open_owns_the_configured_owned_ipv4() {
    let mut config = common::config("net_tap_o7_owned", IFACE, common::EXPLICIT_NONE, false);
    config.owned_ipv4 = vec![format!("{OWNED}/32").parse().unwrap()];
    let started = WallInstant::now();
    let clock = || Instant::from_micros(i64::try_from(started.elapsed().as_micros()).unwrap());
    let setup = thread::spawn(|| {
        let name = CString::new(IFACE).unwrap();
        let deadline = WallInstant::now() + Duration::from_secs(2);
        // SAFETY: `name` is a live NUL-terminated interface name.
        while unsafe { libc::if_nametoindex(name.as_ptr()) } == 0 {
            assert!(
                WallInstant::now() < deadline,
                "tap interface {IFACE} was not created"
            );
            thread::sleep(Duration::from_millis(1));
        }
        ip(&["link", "set", "dev", IFACE, "up"]);
        ip(&["address", "add", "192.0.2.1/24", "dev", IFACE]);
    });
    // SAFETY: EAL creates this private net_tap vdev; this worker owns it and its packets, and the
    // vdev lives for the process lifetime.
    let mut opened = unsafe { open(&config, &common::sizing(), None, &mut || clock()) }.unwrap();
    setup.join().unwrap();
    // A datagram to each address makes the kernel ARP for it; the Stack is turned meanwhile.
    let socket = UdpSocket::bind("192.0.2.1:0").unwrap();
    for addr in [OWNED, UNOWNED] {
        socket.send_to(b"x", (addr, 9)).unwrap();
    }
    let deadline = WallInstant::now() + Duration::from_secs(3);
    while !neighbour(OWNED).contains("lladdr") {
        assert!(
            WallInstant::now() < deadline,
            "{OWNED} unresolved: {}",
            neighbour(OWNED)
        );
        opened.stack.turn(clock(), 32).unwrap();
        thread::sleep(Duration::from_millis(1));
    }
    assert!(
        neighbour(OWNED).contains("lladdr 02:00:00:00:00:fe"),
        "{}",
        neighbour(OWNED)
    );
    // both requests left together; give the unowned one 100 ms more of turns to be answered
    let settle = WallInstant::now() + Duration::from_millis(100);
    while WallInstant::now() < settle {
        opened.stack.turn(clock(), 32).unwrap();
        thread::sleep(Duration::from_millis(1));
    }
    assert!(
        !neighbour(UNOWNED).contains("lladdr"),
        "{}",
        neighbour(UNOWNED)
    );
    opened.stack.into_port().try_close().unwrap();
}
