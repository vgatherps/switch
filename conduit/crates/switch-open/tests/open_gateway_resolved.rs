//! One `open` call lives in this test binary because EAL initializes once per process.

#[path = "open_tap/common.rs"]
mod common;

use std::ffi::CString;
use std::process::Command;
use std::str::FromStr;
use std::thread;
use std::time::{Duration as WallDuration, Instant as WallInstant};

use switch_open::open;
use dpdk_smoltcp::smoltcp::time::Instant;
use dpdk_smoltcp::smoltcp::wire::{EthernetAddress, Ipv4Address};

fn configure_kernel_side(iface: &'static str) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let name = CString::new(iface).unwrap();
        let deadline = WallInstant::now() + WallDuration::from_secs(2);
        // SAFETY: `name` is a live NUL-terminated interface name.
        while unsafe { libc::if_nametoindex(name.as_ptr()) } == 0 {
            assert!(WallInstant::now() < deadline, "tap interface {iface} was not created");
            thread::sleep(WallDuration::from_millis(1));
        }
        let commands: [&[&str]; 2] = [
            &["link", "set", "dev", iface, "up"],
            &["address", "add", "192.0.2.1/24", "dev", iface],
        ];
        for args in commands {
            let output = Command::new("ip").args(args).output().unwrap();
            assert!(
                output.status.success(),
                "ip {}: {}",
                args.join(" "),
                String::from_utf8_lossy(&output.stderr)
            );
        }
    })
}

/// The kernel tap's MAC, read once `open` has returned: the tap PMD sets it to the `mac=`
/// devarg after the interface appears, so a read during setup can see the earlier random one.
fn kernel_mac(iface: &str) -> EthernetAddress {
    let address = std::fs::read_to_string(format!("/sys/class/net/{iface}/address")).unwrap();
    EthernetAddress::from_str(address.trim()).unwrap()
}

#[switch_test_support::pinned_test]
fn open_resolves_tap_kernel_gateway() {
    let iface = "cdo5_gw_ok";
    let setup = configure_kernel_side(iface);
    let config = common::config_with_gateway(
        "net_tap_o5_gw_ok",
        iface,
        common::EXPLICIT_NONE,
        false,
        r#""192.0.2.1""#,
    );
    let started = WallInstant::now();
    let mut clock = || {
        Instant::from_micros(i64::try_from(started.elapsed().as_micros()).unwrap())
    };
    let opened = unsafe { open(&config, &common::sizing(), None, &mut clock) }.unwrap();
    setup.join().unwrap();
    let kernel_mac = kernel_mac(iface);
    let Some((gateway, mac, waited)) = opened.report.gateway else {
        panic!("gateway was not reported")
    };
    assert_eq!(gateway, Ipv4Address::new(192, 0, 2, 1));
    assert_eq!(mac, kernel_mac);
    assert!(waited < dpdk_smoltcp::smoltcp::time::Duration::from_secs(3));
    assert!(opened.report.to_string().ends_with(&format!(
        "\ngateway: {gateway} at {mac} after {} ms",
        waited.total_millis()
    )));
    opened.stack.into_port().try_close().unwrap();
}
