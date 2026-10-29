//! The Stack's TX frame pool on net_ring: `Stack::tx_frame`, `Stack::new`'s
//! frame-pool admission, kit descriptor sizing, and frames outliving the Stack
//! and the Port.

use std::process::Command;

use dpdk_smoltcp::smoltcp::time::Instant;
use dpdk_smoltcp::smoltcp::wire::{EthernetAddress, Ipv4Address, Ipv4Cidr};
use dpdk_smoltcp::{
    tx_reserve, Port, PortError, Stack, StackConfig, StackError, DEFAULT_NEIGHBOR_CAPACITY,
};
use zhida_test_kit::ring_port::{self, RingPort};
use zhida_test_kit::with_eal;

const T: Instant = Instant::from_millis_const(10);

fn config(tx_pending: usize) -> StackConfig<'static> {
    StackConfig {
        max_udp_sockets: 0,
        ethernet: EthernetAddress([0x02, 0, 0, 0, 0, 0x01]),
        ipv4: Ipv4Cidr::new(Ipv4Address::new(10, 0, 0, 1), 24),
        gateway: None,
        paths: &[],
        random_seed: 1,
        neighbor_capacity: DEFAULT_NEIGHBOR_CAPACITY,
        max_sockets: 1,
        rx_staging: 8,
        tx_pending,
    }
}

fn stack(port: Port, tx_pending: usize) -> Stack {
    match Stack::new(port, config(tx_pending), T) {
        Ok(stack) => stack,
        Err((_, error)) => panic!("{error}"),
    }
}

/// Every pool back to zero and each object returned once, then close.
fn finish(fixture: RingPort, port: Port) {
    assert_eq!(fixture.in_use(), (0, 0));
    assert_eq!(fixture.frame_in_use(), 0);
    fixture.assert_returned_exactly_once();
    port.try_close().unwrap();
    fixture.close();
}

#[zhida_test_support::pinned_test]
fn tx_frame_allocates_empty_frames_until_the_pool_is_empty() {
    with_eal(|eal| {
        let fixture = RingPort::new("frame_alloc", 16, 16);
        let mut stack = stack(fixture.open(eal), 8);
        let mut frames = Vec::new();
        while let Some(frame) = stack.tx_frame() {
            assert_eq!(frame.len(), 0);
            // IP MTU 1500 − 28: the largest UDP payload.
            assert_eq!(frame.capacity(), 1472);
            frames.push(frame);
        }
        // The kit's frame pool holds 63.
        assert_eq!(frames.len(), 63);
        assert_eq!(fixture.frame_in_use(), 63);
        let origin = frames[0].origin();
        assert!(frames.iter().all(|frame| frame.origin() == origin));

        let mut frame = frames.pop().unwrap();
        let payload = b"frame payload";
        frame.room_mut()[..payload.len()].copy_from_slice(payload);
        frame.set_len(payload.len());
        assert_eq!(frame.payload(), payload);
        drop(frame);
        assert_eq!(fixture.frame_in_use(), 62);
        let again = stack.tx_frame().expect("a dropped frame returns to the pool");
        assert_eq!(again.len(), 0);
        assert!(stack.tx_frame().is_none());
        drop((again, frames));
        assert_eq!(fixture.frame_in_use(), 0);
        finish(fixture, stack.into_port());
    });
}

/// `Stack::new` needs TX descriptors + tx_pending + 1 frames; one fewer
/// returns the Port unchanged.
#[zhida_test_support::pinned_test]
fn frame_pool_admission_refuses() {
    with_eal(|eal| {
        assert_eq!(tx_reserve(16, 8), 24);
        for count in [24, 25] {
            let fixture = RingPort::new("frame_admit", 16, 16);
            let mut port_config = fixture.config();
            port_config.frame_pool.count = count;
            let port = fixture.open_with(eal, port_config);
            let identity = format!("{port:?}");
            match Stack::new(port, config(8), T) {
                Err((port, error)) => {
                    assert_eq!(count, 24);
                    assert_eq!(
                        error,
                        StackError::TxFramePoolTooSmall { count: 24, required: 25 }
                    );
                    assert_eq!(format!("{port:?}"), identity);
                    finish(fixture, port);
                }
                Ok(stack) => {
                    assert_eq!(count, 25);
                    finish(fixture, stack.into_port());
                }
            }
        }
    });
}

/// The kit opens net_ring with as many TX descriptors as its TX ring has
/// slots, so a ring full of sent frames leaves `tx_pending` + 1 TX packets in a
/// pool of exactly `tx_reserve`, and `Stack::new` accepts that pool.
#[zhida_test_support::pinned_test]
fn kit_ring_descriptors_match_ring_size() {
    with_eal(|eal| {
        const RING: u16 = 32;
        const PENDING: usize = 8;
        let fixture = RingPort::new("kit_desc", 16, u32::from(RING));
        let mut port_config = fixture.config();
        assert_eq!(port_config.tx_descriptors, RING);
        let reserve = tx_reserve(RING, PENDING);
        port_config.tx_pool.count = u32::try_from(reserve).unwrap();
        port_config.frame_pool.count = u32::try_from(reserve + 1).unwrap();
        let mut port = fixture.open_with(eal, port_config);
        assert_eq!(port.descriptor_counts().1, RING);

        // The ring holds RING − 1 sent frames; the PMD never holds more.
        let held = usize::from(RING) - 1;
        let frames: Vec<_> = (0..held).map(|i| ring_port::frame(i as u8)).collect();
        ring_port::send(&mut port, &frames);
        assert_eq!(fixture.in_use(), (0, held as u32));
        assert_eq!(reserve - held, PENDING + 1);

        let mut stack = stack(port, PENDING);
        let all: Vec<_> = std::iter::from_fn(|| stack.tx_frame()).collect();
        assert_eq!(all.len(), reserve + 1);
        drop(all);
        assert_eq!(fixture.take_tx(), frames);
        finish(fixture, stack.into_port());
    });
}

/// Frames the application holds keep the frame pool in use after the Stack is
/// gone: Port close refuses until they drop.
#[zhida_test_support::pinned_test]
fn frames_outlive_stack_via_into_port() {
    with_eal(|eal| {
        let fixture = RingPort::new("frame_life", 16, 16);
        let mut stack = stack(fixture.open(eal), 8);
        let mut frame = stack.tx_frame().unwrap();
        let port = stack.into_port();
        let (port, error) = port.try_close().unwrap_err();
        assert_eq!(error, PortError::OutstandingBuffers { rx: 0, tx: 0, frame: 1 });
        // The Port is closed; the frame is still the application's.
        frame.room_mut()[..4].copy_from_slice(b"live");
        frame.set_len(4);
        assert_eq!(frame.payload(), b"live");
        drop(frame);
        assert_eq!(fixture.frame_in_use(), 0);
        port.try_close().unwrap();
        fixture.close();
    });
}

const CHILD: &str = "ZHIDA_PORT_DROP_CHILD";

/// Dropping a Port while a frame is out retains its pools forever (the names
/// stay taken), so this runs in a child process of this test binary: the
/// child drops the Port with a frame out, writes and drops the frame, and
/// checks it returned to the retained pool.
#[zhida_test_support::pinned_test]
fn port_drop_retains_pools_with_frames_out() {
    if std::env::var_os(CHILD).is_some() {
        port_drop_child();
        return;
    }
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "port_drop_retains_pools_with_frames_out",
            "--test-threads=1",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "child failed\nstdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(stdout.contains("1 passed"), "child ran no test\nstdout:\n{stdout}");
    assert!(
        stderr.contains(
            "Zhida retained port backing: OutstandingBuffers { rx: 0, tx: 0, frame: 1 }"
        ),
        "stderr:\n{stderr}"
    );
}

fn port_drop_child() {
    with_eal(|eal| {
        let fixture = RingPort::new("frame_drop", 16, 16);
        let mut stack = stack(fixture.open(eal), 8);
        let mut frame = stack.tx_frame().unwrap();
        drop(stack);
        // Cleanup refused (one frame out) and retained every pool.
        assert!(fixture.pools_exist());
        assert_eq!(fixture.frame_in_use(), 1);
        frame.room_mut()[..4].copy_from_slice(b"kept");
        frame.set_len(4);
        assert_eq!(frame.payload(), b"kept");
        drop(frame);
        assert_eq!(fixture.frame_in_use(), 0);
        fixture.assert_returned_exactly_once();
        // The retained pools keep their names; this process ends here.
        std::mem::forget(fixture);
    });
}
