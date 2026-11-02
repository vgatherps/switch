//! Validation rules, fail-fast UTF-8, control frames, and where
//! message bytes live.

use std::cell::Cell;
use std::rc::Rc;

use super::*;
use crate::rx::ProtocolError as P;

const KEY: [u8; 4] = [0x11, 0x22, 0x33, 0x44];

fn mask_for(role: Role) -> Option<[u8; 4]> {
    (role == Role::Server).then_some(KEY)
}

/// `frames` each pushed as one chunk; returns the run.
fn feed(rx: &mut Rx, frames: &[Vec<u8>]) -> Run {
    let chunks: Vec<&[u8]> = frames.iter().map(Vec::as_slice).collect();
    drive(rx, &chunks)
}

fn fails(role: Role, frames: &[Vec<u8>], want: P) {
    let run = feed(&mut rx(role), frames);
    assert_eq!(run.end, Some(WsError::Protocol(want)), "{frames:x?}");
    assert_eq!(run.end_chunk, Some(frames.len() - 1), "failed before the last chunk");
}

/// A frame's header only: failures found in the header need no payload.
fn header(b0: u8, mask: Option<[u8; 4]>, len: usize) -> Vec<u8> {
    let mut f = encode(b0, mask, &vec![0; len]);
    f.truncate(f.len() - len);
    f
}

#[test]
fn header_rules_fail_at_the_header() {
    for role in [Role::Client, Role::Server] {
        let m = mask_for(role);
        for rsv in [0x40, 0x20, 0x10] {
            fails(role, &[header(fin(BINARY) | rsv, m, 3)], P::ReservedBits);
        }
        for op in (3..=7).chain(11..=15) {
            fails(role, &[header(fin(op), m, 3)], P::UnknownOpcode(op));
        }
        for op in [PING, PONG, CLOSE] {
            fails(role, &[header(op, m, 0)], P::FragmentedControl);
            fails(role, &[header(fin(op), m, 126)], P::ControlTooBig);
        }
        fails(role, &[header(fin(CONT), m, 1)], P::UnexpectedContinuation);
        fails(role, &[header(CONT, m, 1)], P::UnexpectedContinuation);
        for op in [TEXT, BINARY] {
            fails(role, &[encode(TEXT, m, b"a"), header(fin(op), m, 1)], P::ExpectedContinuation);
            fails(role, &[encode(BINARY, m, b"a"), header(op, m, 1)], P::ExpectedContinuation);
        }
        let mut top = header(fin(BINARY), m, 70000);
        top[2] |= 0x80;
        fails(role, &[top], P::LengthTopBit);
    }
    fails(Role::Server, &[header(fin(BINARY), None, 3)], P::UnmaskedFrame);
    fails(Role::Client, &[header(fin(BINARY), Some(KEY), 3)], P::MaskedFrame);
}

#[test]
fn rules_check_in_tungstenite_order() {
    // Unknown opcode before size, size before mask, mask before reserved bits.
    let small = Config::new(Role::Server, Limits { max_message: 10, ..Limits::default() });
    let run = feed(&mut WsRx::new(Raw::new(), small), &[header(0x70 | 3, None, 11)]);
    assert_eq!(run.end, Some(WsError::Protocol(P::UnknownOpcode(3))));
    let run = feed(&mut WsRx::new(Raw::new(), small), &[header(0x70 | BINARY, None, 11)]);
    assert_eq!(run.end, Some(WsError::Protocol(P::MessageTooBig)));
    fails(Role::Server, &[header(0x70 | BINARY, None, 1)], P::UnmaskedFrame);
}

/// A message over `max_message` fails at the header that passes it: a single frame, or a
/// continuation (8 + 4 fits exactly, 8 + 5 fails at the second header).
#[test]
fn size_limits() {
    let cfg = Config::new(Role::Client, Limits { max_message: 12, ..Limits::default() });
    let run = feed(&mut WsRx::new(Raw::new(), cfg), &[header(fin(TEXT), None, 13)]);
    assert_eq!(run.end, Some(WsError::Protocol(P::MessageTooBig)));
    let run = feed(&mut WsRx::new(Raw::new(), cfg), &[encode(BINARY, None, &[1; 8]), encode(fin(CONT), None, &[2; 4])]);
    assert_eq!(run.end, None);
    assert_eq!(run.msgs[0].payload.len(), 12);
    let run = feed(&mut WsRx::new(Raw::new(), cfg), &[encode(BINARY, None, &[1; 8]), header(fin(CONT), None, 5)]);
    assert_eq!(run.end, Some(WsError::Protocol(P::MessageTooBig)));
    assert_eq!(run.end_chunk, Some(1));
}

/// A message within `max_message` is delivered in a single frame of any size (20 MiB here).
#[test]
#[cfg_attr(miri, ignore)]
fn one_frame_up_to_max_message_is_delivered() {
    let len = 20 << 20;
    let cfg = Config::new(Role::Client, Limits { max_message: len, ..Limits::default() });
    let run = feed(&mut WsRx::new(Raw::new(), cfg), &[encode(fin(BINARY), None, &vec![7; len])]);
    assert_eq!(run.end, None);
    assert_eq!(run.msgs.len(), 1);
    assert_eq!(run.msgs[0].payload.len(), len);
}

fn close(code: u16, reason: &[u8]) -> Vec<u8> {
    let mut p = code.to_be_bytes().to_vec();
    p.extend_from_slice(reason);
    encode(fin(CLOSE), None, &p)
}

/// `max_message` limits data frames only: with a 1-byte limit a 2-byte Ping is answered, a
/// Pong is taken, and a Close with status 1000 closes, while a 2-byte Binary fails.
#[test]
fn control_frames_are_not_held_to_max_message() {
    let cfg = Config::new(Role::Client, Limits { max_message: 1, ..Limits::default() });
    let run = feed(&mut WsRx::new(Raw::new(), cfg), &[encode(fin(PING), None, b"ab"), encode(fin(PONG), None, b"cd")]);
    assert_eq!((run.end, run.pong), (None, Some(b"ab".to_vec())));
    let mut r = WsRx::new(Raw::new(), cfg);
    assert_eq!(feed(&mut r, &[close(1000, b"")]).end, Some(WsError::Closed));
    assert_eq!(r.peer_close(), Some(&1000u16.to_be_bytes()[..]));
    let run = feed(&mut WsRx::new(Raw::new(), cfg), &[encode(fin(BINARY), None, b"xy")]);
    assert_eq!(run.end, Some(WsError::Protocol(P::MessageTooBig)));
}

#[test]
fn close_frames() {
    for code in [1000, 1003, 1007, 1013, 1014, 3000, 4999] {
        let mut r = rx(Role::Client);
        let run = feed(&mut r, &[close(code, b"bye")]);
        assert_eq!(run.end, Some(WsError::Closed), "{code}");
        let mut want = code.to_be_bytes().to_vec();
        want.extend_from_slice(b"bye");
        assert_eq!(r.peer_close(), Some(&want[..]));
    }
    let mut r = rx(Role::Client);
    assert_eq!(feed(&mut r, &[encode(fin(CLOSE), None, b"")]).end, Some(WsError::Closed));
    assert_eq!(r.peer_close(), Some(&[][..]));

    // a status code a peer may not send still closes, answered with 1002, whatever the reason
    for code in [0, 999, 1004, 1005, 1006, 1015, 1016, 2999, 5000, 65535] {
        let mut r = rx(Role::Client);
        let run = feed(&mut r, &[close(code, &[b'w', 0xff])]);
        assert_eq!(run.end, Some(WsError::Closed), "{code}");
        assert_eq!(r.peer_close(), Some(&1002u16.to_be_bytes()[..]), "{code}");
    }
    fails(Role::Client, &[encode(fin(CLOSE), None, &[3])], P::InvalidCloseLength);
    fails(Role::Client, &[close(1000, &[b'o', 0xff])], P::InvalidUtf8);
}

#[test]
fn errors_are_sticky() {
    let mut r = rx(Role::Client);
    let run = feed(&mut r, &[close(1000, b"")]);
    assert_eq!(run.end, Some(WsError::Closed));
    assert!(matches!(r.recv(&mut ()), Err(WsError::Closed)));

    let mut r = rx(Role::Client);
    feed(&mut r, &[header(fin(BINARY) | 0x40, None, 0)]);
    assert!(matches!(r.recv(&mut ()), Err(WsError::Protocol(P::ReservedBits))));
}

#[test]
fn messages_before_a_failure_are_delivered() {
    let run = feed(&mut rx(Role::Client), &[[encode(fin(TEXT), None, b"ok"), header(fin(9), None, 200)].concat()]);
    assert_eq!(run.msgs.len(), 1);
    assert_eq!(run.msgs[0].payload, b"ok");
    assert_eq!(run.end, Some(WsError::Protocol(P::ControlTooBig)));
}

/// Text cut into `chunk`-byte frames; which chunk's push returned the error.
fn utf8_fail_chunk(role: Role, wire: &[u8], chunk: usize) -> Option<usize> {
    let cuts: Vec<usize> = (1..).map(|i| i * chunk).take_while(|&c| c < wire.len()).collect();
    let run = drive(&mut rx(role), &split(wire, &cuts));
    match run.end {
        Some(WsError::Protocol(P::InvalidUtf8)) => run.end_chunk,
        None => None,
        Some(e) => panic!("{e:?}"),
    }
}

#[test]
fn utf8_fails_at_the_chunk_holding_the_bad_byte() {
    for role in [Role::Client, Role::Server] {
        let m = mask_for(role);
        let mut text = b"0123456789abcdefghijklmnopqrstuv".to_vec();
        let hdr = 2 + if m.is_some() { 4 } else { 0 };
        for bad in 0..text.len() {
            let mut t = text.clone();
            t[bad] = 0xff;
            let wire = encode(fin(TEXT), m, &t);
            assert_eq!(utf8_fail_chunk(role, &wire, 5), Some((hdr + bad) / 5), "{role:?} bad {bad}");
            // Whole frame in one chunk: the direct path checks it too.
            assert_eq!(utf8_fail_chunk(role, &wire, wire.len()), Some(0));
        }
        // E0 80: E0 alone is a valid prefix, the 80 makes it invalid.
        text[10] = 0xE0;
        text[11] = 0x80;
        let wire = encode(fin(TEXT), m, &text);
        let at = hdr + 11;
        assert_eq!(utf8_fail_chunk(role, &wire, at), Some(1));
        // A code point cut at the end of the message fails when the message ends.
        let wire = encode(fin(TEXT), m, &"ab€".as_bytes()[..4]);
        assert_eq!(utf8_fail_chunk(role, &wire, 3), Some((wire.len() - 1) / 3));
    }
}

#[test]
fn utf8_across_fragments() {
    let euro = "€".as_bytes();
    let ok = [encode(TEXT, None, &euro[..1]), encode(CONT, None, &euro[1..2]), encode(fin(CONT), None, &euro[2..])];
    let run = feed(&mut rx(Role::Client), &ok);
    assert_eq!(run.end, None);
    assert_eq!(run.msgs[0].payload, euro);
    // Second fragment breaks the sequence: fails at its chunk, before the message ends.
    let bad = [encode(TEXT, None, &euro[..1]), encode(CONT, None, b"x"), encode(fin(CONT), None, b"")];
    let run = feed(&mut rx(Role::Client), &bad);
    assert_eq!(run.end, Some(WsError::Protocol(P::InvalidUtf8)));
    assert_eq!(run.end_chunk, Some(1));
    // Binary messages are not checked.
    let run = feed(&mut rx(Role::Client), &[encode(fin(BINARY), None, &[0xff, 0xfe])]);
    assert_eq!(run.end, None);
}

#[test]
fn mask_phase_carries_across_chunks() {
    let payload: Vec<u8> = (0..11).collect();
    let wire = encode(fin(BINARY), Some(KEY), &payload);
    for cuts in [[7, 8, 9], [6, 9, 13], [10, 11, 17]] {
        let run = drive(&mut rx(Role::Server), &split(&wire, &cuts));
        assert_eq!(run.msgs.len(), 1);
        assert_eq!(run.msgs[0].payload, payload, "{cuts:?}");
        assert!(!run.msgs[0].direct);
    }
}

#[test]
fn masked_fragment_frames_restart_key_phase() {
    let first = encode(BINARY, Some(KEY), b"abc");
    let second = encode(fin(CONT), Some(KEY), b"defghijkl");
    let mut wire = first;
    wire.extend_from_slice(&second);
    let run = drive(&mut rx(Role::Server), &split(&wire, &[7, 8, 10, 16, 18]));
    assert_eq!(run.end, None);
    assert_eq!(run.msgs.len(), 1);
    assert_eq!(run.msgs[0].payload, b"abcdefghijkl");
}

#[test]
fn control_frames_between_fragments() {
    let frames = [
        encode(TEXT, None, b"ab"),
        encode(fin(PING), None, b"p1"),
        encode(fin(PONG), None, b"x"),
        encode(CONT, None, b"cd"),
        encode(fin(PING), None, b"p2"),
        encode(fin(CONT), None, b"ef"),
    ];
    let run = feed(&mut rx(Role::Client), &frames);
    assert_eq!(run.end, None);
    assert_eq!(run.msgs.len(), 1);
    assert_eq!(run.msgs[0].payload, b"abcdef");
    assert_eq!(run.msgs[0].stamp, stamp(0));
    assert_eq!(run.pong.as_deref(), Some(&b"p2"[..]));

    let mut r = rx(Role::Client);
    let run = feed(&mut r, &[encode(BINARY, None, b"ab"), close(1001, b"")]);
    assert_eq!(run.end, Some(WsError::Closed));
    assert_eq!(r.peer_close(), Some(&[0x03, 0xe9][..]));
}

#[test]
fn direct_message_borrows_the_frame() {
    let wire = [encode(fin(TEXT), None, b"first"), encode(fin(BINARY), None, &[9; 130])].concat();
    let mut r = rx(Role::Client);
    let f = VecFrame { data: wire, stamp: 42 };
    let base = f.data.as_ptr();
    r.push(f);
    let m = r.recv(&mut ()).unwrap().unwrap();
    assert_eq!(m.bytes().as_ptr(), base.wrapping_add(2));
    assert_eq!(m.text(), Some("first"));
    assert_eq!(m.rx_timestamp(), 42);
    let m = r.recv(&mut ()).unwrap().unwrap();
    assert_eq!(m.bytes().as_ptr(), base.wrapping_add(7 + 4));
    assert_eq!(m.kind(), Kind::Binary);
    assert!(r.recv(&mut ()).unwrap().is_none());
}

#[test]
fn spanning_message_is_copied_and_stamped_by_its_first_byte() {
    let wire = encode(fin(BINARY), None, &[5; 10]);
    let run = drive(&mut rx(Role::Client), &split(&wire, &[1, 6]));
    assert_eq!(run.msgs, vec![Got { kind: Kind::Binary, payload: vec![5; 10], direct: false, stamp: stamp(0) }]);
    // Header cut, payload whole in the next frame: still direct, stamped by the header's frame.
    let run = drive(&mut rx(Role::Client), &split(&wire, &[1, 2]));
    assert!(run.msgs[0].direct);
    assert_eq!(run.msgs[0].stamp, stamp(0));
}

#[test]
fn non_minimal_length_fails_at_the_header_however_split() {
    for role in [Role::Client, Role::Server] {
        let mbit = if role == Role::Server { 0x80 } else { 0 };
        let mut headers = Vec::new();
        for len in [0u16, 1, 125] {
            headers.push([&[fin(BINARY), mbit | 126][..], &len.to_be_bytes()].concat());
        }
        for len in [0u64, 125, 126, 65535] {
            headers.push([&[fin(BINARY), mbit | 127][..], &len.to_be_bytes()].concat());
        }
        for mut h in headers {
            if let Some(k) = mask_for(role) {
                h.extend_from_slice(&k);
            }
            for cut in 0..=h.len() {
                let chunks = split(&h, &[cut]);
                let run = drive(&mut rx(role), &chunks);
                assert_eq!(run.end, Some(WsError::Protocol(P::NonMinimalLength)), "{role:?} {h:x?} cut {cut}");
                assert_eq!(run.end_chunk, Some(chunks.len() - 1), "{role:?} {h:x?} cut {cut}");
            }
        }
    }
}

#[test]
fn pong_is_owed_once_per_ping() {
    let mut r = rx(Role::Client);
    assert_eq!(r.take_pong(), None);
    let ping = encode(fin(PING), None, b"hb");
    r.push(VecFrame { data: ping.clone(), stamp: 0 });
    assert!(r.recv(&mut ()).unwrap().is_none());
    assert_eq!(r.take_pong(), Some(&b"hb"[..]));
    assert_eq!(r.take_pong(), None);
    r.push(VecFrame { data: ping, stamp: 1 });
    assert!(r.recv(&mut ()).unwrap().is_none());
    assert_eq!(r.take_pong(), Some(&b"hb"[..]));
    assert_eq!(r.take_pong(), None);
}

struct CountedFrame {
    data: Vec<u8>,
    drops: Rc<Cell<usize>>,
}

impl Drop for CountedFrame {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

impl RxFrame for CountedFrame {
    fn payload(&self) -> &[u8] {
        &self.data
    }
    fn payload_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
    fn rx_timestamp(&self) -> u64 {
        0
    }
}

/// A fully consumed frame is released by the `recv` that returns `None`,
/// not kept until the next frame arrives.
#[test]
fn consumed_frames_are_dropped_before_recv_asks_for_more() {
    let drops = Rc::new(Cell::new(0));
    let frame = |data: Vec<u8>| CountedFrame { data, drops: drops.clone() };
    let mut r = WsRx::new(Raw::new(), Config::new(Role::Client, Limits::default()));

    // Direct: the message borrows the frame, so it lives until the message is gone.
    r.push(frame(encode(fin(TEXT), None, b"direct")));
    let m = r.recv(&mut ()).unwrap().unwrap();
    assert_eq!(m.bytes(), b"direct");
    assert_eq!(drops.get(), 0);
    assert!(r.recv(&mut ()).unwrap().is_none());
    assert_eq!(drops.get(), 1);

    // Scratch: the first frame is copied out and released before the rest arrives.
    let wire = encode(fin(BINARY), None, &[4; 20]);
    r.push(frame(wire[..10].to_vec()));
    assert!(r.recv(&mut ()).unwrap().is_none());
    assert_eq!(drops.get(), 2);
    r.push(frame(wire[10..].to_vec()));
    assert_eq!(r.recv(&mut ()).unwrap().unwrap().bytes(), [4; 20]);
    assert!(r.recv(&mut ()).unwrap().is_none());
    assert_eq!(drops.get(), 3);
}

/// What we may send in a close: the codes a peer may send, and a reason up to 123 B.
#[test]
fn close_sendable_checks_code_and_reason_length() {
    use crate::rx::close_sendable;
    let reason = |n| "r".repeat(n);
    assert!(close_sendable(1000, &reason(123)));
    assert!(!close_sendable(1000, &reason(124)));
    for code in [1000, 1003, 1007, 1014, 3000, 4999] {
        assert!(close_sendable(code, ""), "{code}");
    }
    for code in [0, 999, 1004, 1005, 1006, 1015, 2999, 5000] {
        assert!(!close_sendable(code, ""), "{code}");
    }
}

