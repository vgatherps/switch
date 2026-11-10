//! TCP frames between two Stacks over copying links: a 1 MiB stream sent as
//! frames arrives byte-exact, and lost segments are recovered by the copy
//! and in-place retransmit paths. `Pair::close` checks every pool is idle and
//! each object returned once.

use dpdk_smoltcp::smoltcp::wire::TcpSeqNumber;
use dpdk_smoltcp::{frame_reserve, rx_reserve, Stack, TcpHandle};
use zhida_test_kit::many;
use zhida_test_kit::pair::{
    self, drain_bytes, establish, establish_sized, new_pair, pattern, segment, App, Dir, Pair,
    Side, T0,
};
use zhida_test_kit::wire::Verdict;
use zhida_test_kit::with_eal;

/// Queue `data[*sent..]` on `handle` in frames of at most one MSS while the
/// socket has byte room and the shared pool has frames; returns whether anything was queued.
fn send_frames(stack: &mut Stack, handle: &TcpHandle, data: &[u8], sent: &mut usize) -> bool {
    let start = *sent;
    while *sent < data.len() {
        let socket = stack.tcp(handle);
        let mss = socket.max_segment_payload().expect("established");
        let room = socket.frame_send_room();
        if room == 0 {
            break;
        }
        let Some(mut frame) = stack.tx_frame() else {
            break;
        };
        let len = (data.len() - *sent).min(mss).min(room);
        frame.as_mut()[..len].copy_from_slice(&data[*sent..*sent + len]);
        frame.truncate(len);
        let frame = frame.freeze();
        if let Err((_, error)) = stack.tcp_send_frame(handle, frame) {
            panic!("frame within frame_send_room refused: {error}");
        }
        *sent += len;
    }
    *sent != start
}

/// A's sequence number and payload length of each data segment in `frames`.
fn a_data(frames: &[(Dir, Vec<u8>)]) -> Vec<(TcpSeqNumber, Vec<u8>)> {
    frames
        .iter()
        .filter(|(dir, frame)| *dir == Dir::AToB && frame[12..14] == [0x08, 0x00])
        .map(|(_, frame)| segment(frame, Side::A))
        .filter(|tcp| !tcp.payload.is_empty())
        .map(|tcp| (tcp.seq_number, tcp.payload.to_vec()))
        .collect()
}

/// A's next sequence number, read off its final handshake ACK.
fn next_seq(handshake: &[(Dir, Vec<u8>)]) -> TcpSeqNumber {
    let (dir, frame) = handshake.last().expect("handshake frames");
    assert_eq!(*dir, Dir::AToB);
    segment(frame, Side::A).seq_number
}

/// Drive until B has read `expected` on `server`, with nothing more to send.
fn deliver(pair: &mut Pair, server: &TcpHandle, expected: &[u8]) -> Vec<(Dir, Vec<u8>)> {
    let mut frames = Vec::new();
    let mut received = Vec::new();
    pair.drive(
        |dir, frame| frames.push((dir, frame.to_vec())),
        |_, b| {
            let bytes = drain_bytes(b.tcp_mut(server)).unwrap();
            let moved = !bytes.is_empty();
            received.extend_from_slice(&bytes);
            if received.len() == expected.len() {
                App::Done
            } else if moved {
                App::Moved
            } else {
                App::Idle
            }
        },
    );
    assert!(received == expected, "B read different bytes");
    // B's last ACK can still be on its TX ring when the read completes.
    pair.drive_rounds(
        |dir, frame| frames.push((dir, frame.to_vec())),
        |last, _, _| if last.moved() { App::Moved } else { App::Done },
    );
    frames
}

/// 1 MiB sent from A as application frames over a 64 KiB socket arrives at B
/// byte for byte.
#[zhida_test_support::pinned_test]
fn tcp_frames_1mib_byte_exact() {
    with_eal(|eal| {
        const BUFFER: usize = 65536;
        let configs = [pair::config(Side::A), pair::config(Side::B)];
        let mut pair = Pair::with_ports(eal, ["mibfa", "mibfb"], configs, T0, 128, |side, port| {
            let stack = configs[side.index()];
            let full = pair::windows(port.mtu, [BUFFER]);
            port.rx_pool.count = pair::pool_count(
                rx_reserve(port.rx_descriptors, port.rx_burst, stack.rx_staging, port.rx_pool.cache_size),
                full,
            );
            port.frame_pool.count =
                pair::pool_count(frame_reserve(port.tx_descriptors, stack.tx_pending), full);
        });
        let ([client, server], _) = establish_sized(&mut pair, Side::A, BUFFER);
        let data = pattern(1 << 20, 11);
        let (mut sent, mut received) = (0, Vec::with_capacity(data.len()));
        pair.drive(
            |_, _| {},
            |a, b| {
                let queued = send_frames(a, &client, &data, &mut sent);
                let bytes = drain_bytes(b.tcp_mut(&server)).unwrap();
                let moved = queued || !bytes.is_empty();
                received.extend_from_slice(&bytes);
                if received.len() == data.len() && a.tcp(&client).send_queue() == 0 {
                    App::Done
                } else if moved {
                    App::Moved
                } else {
                    App::Idle
                }
            },
        );
        assert!(received == data, "B read different bytes");
        // The final ACK can still be in flight when the application reads the
        // last bytes; process it and drain the links before checking teardown.
        pair.drive_rounds(
            |_, _| {},
            |last, _, _| if last.moved() { App::Moved } else { App::Done },
        );
        for link in [&pair.a_to_b, &pair.b_to_a] {
            assert_eq!((link.ring_full, link.pool_empty, link.dropped, link.held), (0, 0, 0, 0));
        }
        assert_eq!(pair.close(), 0);
    });
}

/// Two frames queued together leave as two whole segments; both are lost.
/// The RTO retransmission is one copied segment carrying both, as the byte
/// ring would send, and B reads the bytes once.
#[zhida_test_support::pinned_test]
fn lost_segment_retransmits_by_coalesced_copy() {
    with_eal(|eal| {
        let mut pair = new_pair(eal, ["lsca", "lscb"]);
        let ([client, server], handshake) = establish(&mut pair, Side::A);
        let a_seq = next_seq(&handshake);
        let mut dropped = 0;
        pair.a_to_b.impair(move |frame| {
            if dropped < 2 && frame[12..14] == [0x08, 0x00] && !segment(frame, Side::A).payload.is_empty() {
                dropped += 1;
                Verdict::Drop
            } else {
                Verdict::Deliver
            }
        });
        let (one, two) = (pattern(64, 1), pattern(64, 2));
        let mut sent = 0;
        let both = [one.as_slice(), two.as_slice()].concat();
        assert!(send_frames(&mut pair.a, &client, &one, &mut sent));
        let mut sent = 0;
        assert!(send_frames(&mut pair.a, &client, &two, &mut sent));
        let first = pair.round(|_, _| {});
        assert_eq!(first.a.tx_accepted, 2);
        assert_eq!(pair.a_to_b.dropped, 2);
        let frames = deliver(&mut pair, &server, &both);
        assert_eq!(a_data(&frames), [(a_seq, both.clone())]);
        assert_eq!(pair.close(), 0);
    });
}

/// One frame cloned to two TCP connections; the first connection's segment
/// is lost. Its retransmission is a copy of the shared payload, the other
/// connection's segment is unaffected, and both receivers read the payload
/// once. The application's clone stays intact throughout.
#[zhida_test_support::pinned_test]
fn lost_broadcast_segment_retransmits_by_copy() {
    with_eal(|eal| {
        let configs = [many::config(Side::A, 2), many::config(Side::B, 2)];
        let mut pair = Pair::new(eal, ["lbca", "lbcb"], configs, T0);
        let conns = many::open(&mut pair, 0..2, 4096);
        let payload = pattern(500, 8);
        let mut frame = pair.a.tx_frame().unwrap();
        frame.as_mut()[..payload.len()].copy_from_slice(&payload);
        frame.truncate(payload.len());
        let frame = frame.freeze();
        let lost_port = many::CLIENT_BASE + conns[0].id;
        let mut dropped = false;
        pair.a_to_b.impair(move |bytes| {
            if !dropped && bytes[12..14] == [0x08, 0x00] {
                let tcp = segment(bytes, Side::A);
                if tcp.src_port == lost_port && !tcp.payload.is_empty() {
                    dropped = true;
                    return Verdict::Drop;
                }
            }
            Verdict::Deliver
        });
        for conn in &conns {
            if let Err((_, error)) = pair.a.tcp_send_frame(&conn.client, frame.clone()) {
                panic!("{error}");
            }
        }
        let mut received = [Vec::new(), Vec::new()];
        let mut retransmitted = Vec::new();
        pair.drive(
            |dir, bytes| {
                if dir == Dir::AToB && bytes[12..14] == [0x08, 0x00] {
                    let tcp = segment(bytes, Side::A);
                    if tcp.src_port == lost_port && !tcp.payload.is_empty() {
                        retransmitted.push(tcp.payload.to_vec());
                    }
                }
            },
            |_, b| {
                let mut moved = false;
                for (conn, received) in conns.iter().zip(&mut received) {
                    let bytes = drain_bytes(b.tcp_mut(&conn.server)).unwrap();
                    moved |= !bytes.is_empty();
                    received.extend_from_slice(&bytes);
                }
                if received.iter().all(|r| r.len() == payload.len()) {
                    App::Done
                } else if moved {
                    App::Moved
                } else {
                    App::Idle
                }
            },
        );
        assert_eq!(pair.a_to_b.dropped, 1);
        assert_eq!(retransmitted, std::slice::from_ref(&payload));
        assert!(received.iter().all(|r| *r == payload));
        assert_eq!(frame.as_ref(), payload);
        drop(frame);
        assert_eq!(pair.close(), 0);
    });
}

/// Four whole 1000-byte frames leave; the first is lost. B's duplicate ACKs
/// trigger a fast retransmission before any RTO: one MSS from the lost byte,
/// copied across the first two frames as the byte ring would send it, and B
/// reads every byte once.
#[zhida_test_support::pinned_test]
fn dupack_fast_retransmit() {
    with_eal(|eal| {
        let mut pair = new_pair(eal, ["dupfa", "dupfb"]);
        let ([client, server], handshake) = establish(&mut pair, Side::A);
        let a_seq = next_seq(&handshake);
        let mut dropped = false;
        pair.a_to_b.impair(move |frame| {
            if !dropped && frame[12..14] == [0x08, 0x00] && !segment(frame, Side::A).payload.is_empty() {
                dropped = true;
                Verdict::Drop
            } else {
                Verdict::Deliver
            }
        });
        let data = pattern(4000, 12);
        for chunk in data.chunks(1000) {
            let mut sent = 0;
            assert!(send_frames(&mut pair.a, &client, chunk, &mut sent));
        }
        let start = pair.now();
        let frames = deliver(&mut pair, &server, &data);
        let segments = a_data(&frames);
        assert_eq!(segments.len(), 4, "three after the loss and one retransmission");
        assert_eq!(segments[3], (a_seq, data[..1460].to_vec()));
        assert_eq!(pair.now(), start, "recovered without waiting for a timer");
        assert_eq!(pair.close(), 0);
    });
}
