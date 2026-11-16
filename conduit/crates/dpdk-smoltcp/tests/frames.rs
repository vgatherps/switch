//! Headers from two deterministic two-Stack scenarios, including dropped frames.
//! Each line has direction, Pair time in microseconds, frame length and header
//! bytes in lowercase hex. ARP frames are recorded whole; IPv4/TCP frames include
//! Ethernet, IPv4 and TCP headers with all options, without payload bytes.
//! The sixteen-connection trace omits these header bytes. These fixtures pin them
//! before the emitter changes. Run with SWITCH_FRAMES_BLESS=1 to write the fixtures.

use switch_test_kit::pair::drain_bytes;
use std::cell::RefCell;
use std::fmt::Write;
use std::rc::Rc;

use dpdk_smoltcp::TcpHandle;
use dpdk_smoltcp::smoltcp::socket::tcp::{RecvError, State, TcpFrame};
use dpdk_smoltcp::smoltcp::time::{Duration, Instant};
use dpdk_smoltcp::smoltcp::wire::{
    EthernetFrame, EthernetProtocol, IpEndpoint, Ipv4Packet, TcpControl, TcpPacket, TcpRepr,
    TCP_HEADER_LEN,
};
use dpdk_smoltcp::Stack;
use dpdk_smoltcp::{frame_reserve, rx_reserve, tx_reserve};

use switch_test_kit::pair::{
    ceil_ms, config, pattern, pool_count, segment, tcp_frame, tcp_socket_sized, windows, App, Dir, Pair,
    Round, Side, CLIENT_PORT, IDLE_ROUNDS, MAX_ROUNDS, SERVER_PORT, T0,
};
use switch_test_kit::wire::Verdict;
use switch_test_kit::with_eal;

const BULK: usize = 256 * 1024;
const ACK_DELAY: Duration = Duration::from_millis(10);
/// The measured RTO's least value: RTT samples here are 0 or 10 ms (delayed ACK), so
/// 4 · RTTVAR stays below smoltcp's 50 ms floor and the RTO is SRTT + 50 ms, from `RTO`
/// (no delayed sample yet) to `RTO` + `ACK_DELAY`.
const RTO: Duration = Duration::from_millis(50);
const TIME_WAIT: Duration = Duration::from_millis(10_000);
const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/frames");

struct Frame {
    dir: Dir,
    at: Instant,
    bytes: Vec<u8>,
    dropped: bool,
}

impl Frame {
    fn tcp(&self) -> Option<TcpRepr<'_>> {
        let eth = EthernetFrame::new_checked(&self.bytes[..]).unwrap();
        match eth.ethertype() {
            EthernetProtocol::Arp => None,
            EthernetProtocol::Ipv4 => Some(segment(
                &self.bytes,
                if self.dir == Dir::AToB {
                    Side::A
                } else {
                    Side::B
                },
            )),
            other => panic!("unexpected Ethernet protocol {other:?}"),
        }
    }
    fn line(&self) -> String {
        let eth = EthernetFrame::new_checked(&self.bytes[..]).unwrap();
        let end = match eth.ethertype() {
            EthernetProtocol::Arp => self.bytes.len(),
            EthernetProtocol::Ipv4 => {
                let ip = Ipv4Packet::new_checked(eth.payload()).unwrap();
                let tcp = TcpPacket::new_checked(ip.payload()).unwrap();
                EthernetFrame::<&[u8]>::header_len()
                    + usize::from(ip.header_len())
                    + usize::from(tcp.header_len())
            }
            other => panic!("unexpected Ethernet protocol {other:?}"),
        };
        let dir = if self.dir == Dir::AToB { "A>B" } else { "B>A" };
        let mut line = format!("{dir} {} {} ", self.at.total_micros(), self.bytes.len());
        for byte in &self.bytes[..end] {
            write!(line, "{byte:02x}").unwrap();
        }
        if self.dropped {
            line.push_str(" drop");
        }
        line
    }

}

fn tcp_frames(frames: &[Frame]) -> impl Iterator<Item = (&Frame, TcpRepr<'_>)> {
    frames
        .iter()
        .filter_map(|frame| frame.tcp().map(|tcp| (frame, tcp)))
}

fn pure_ack(tcp: &TcpRepr<'_>) -> bool {
    tcp.control == TcpControl::None && tcp.payload.is_empty() && tcp.ack_number.is_some()
}

#[derive(Default)]
struct Trace {
    now: Option<Instant>,
    frames: Vec<Frame>,
    drop_next_data: bool,
}

impl Trace {
    fn frame(&mut self, dir: Dir, bytes: &[u8]) -> Verdict {
        let mut frame = Frame {
            dir,
            at: self.now.expect("frame outside a recorded round"),
            bytes: bytes.to_vec(),
            dropped: false,
        };
        if self.drop_next_data
            && dir == Dir::AToB
            && frame.tcp().is_some_and(|tcp| !tcp.payload.is_empty())
        {
            self.drop_next_data = false;
            frame.dropped = true;
        }
        let verdict = if frame.dropped {
            Verdict::Drop
        } else {
            Verdict::Deliver
        };
        self.frames.push(frame);
        verdict
    }
}

fn record(pair: &mut Pair) -> Rc<RefCell<Trace>> {
    let trace = Rc::new(RefCell::new(Trace::default()));
    for (link, dir) in [(&mut pair.a_to_b, Dir::AToB), (&mut pair.b_to_a, Dir::BToA)] {
        let trace = Rc::clone(&trace);
        link.impair(move |frame| trace.borrow_mut().frame(dir, frame));
    }
    trace
}

fn round(pair: &mut Pair, trace: &RefCell<Trace>, at: Instant) -> Round {
    trace.borrow_mut().now = Some(at);
    pair.round_at(at, |_, _| {})
}

fn drive(
    pair: &mut Pair,
    trace: &RefCell<Trace>,
    mut app: impl FnMut(&mut Stack, &mut Stack) -> App,
) {
    let mut at = pair.now();
    let mut idle = 0;
    for count in 0..MAX_ROUNDS {
        let last = round(pair, trace, at);
        match app(&mut pair.a, &mut pair.b) {
            App::Done => return,
            App::Moved => idle = 0,
            App::Idle if last.moved() => idle = 0,
            App::Idle => {
                idle += 1;
                assert!(idle < IDLE_ROUNDS, "no progress for {idle} rounds at {at}");
                if let Some(deadline) = [pair.a.poll_at(), pair.b.poll_at()]
                    .into_iter()
                    .flatten()
                    .min()
                {
                    at = at.max(deadline);
                }
            }
        }
        assert!(
            count + 1 < MAX_ROUNDS,
            "not done after {MAX_ROUNDS} rounds at {at}"
        );
    }
}

fn open_sockets(
    pair: &mut Pair,
    trace: &RefCell<Trace>,
    size: usize,
) -> [TcpHandle; 2] {
    let b = pair.b.add_tcp(tcp_socket_sized(size)).unwrap();
    pair.b.listen(&b, SERVER_PORT).unwrap();
    let a = pair.a.add_tcp(tcp_socket_sized(size)).unwrap();
    pair.a
        .connect(
            &a,
            IpEndpoint::new(Side::B.ip().into(), SERVER_PORT),
            CLIENT_PORT,
        )
        .unwrap();
    drive(pair, trace, |left, right| {
        if left.tcp(&a).state() == State::Established && right.tcp(&b).state() == State::Established {
            App::Done
        } else {
            App::Idle
        }
    });
    assert_eq!(pair.now(), T0, "handshake advanced time");
    [a, b]
}

fn open(
    pair: &mut Pair,
    trace: &RefCell<Trace>,
    size: usize,
) -> [TcpHandle; 2] {
    let [a, b] = open_sockets(pair, trace, size);
    let recorded = trace.borrow();
    let syns: Vec<_> = tcp_frames(&recorded.frames)
        .filter(|(_, tcp)| tcp.control == TcpControl::Syn)
        .collect();
    assert_eq!(syns.len(), 2);
    for (index, (frame, tcp)) in syns.iter().enumerate() {
        assert_eq!(frame.dir, if index == 0 { Dir::AToB } else { Dir::BToA });
        assert_eq!(tcp.ack_number.is_some(), index == 1);
        assert_eq!(tcp.timestamp, None);
        let shift = tcp.window_scale.expect("SYN omitted window scale");
        if size == BULK {
            assert!(shift > 0, "large buffers negotiated zero shift");
        } else {
            assert_eq!(shift, 0, "small buffers negotiated a nonzero shift");
        }
        assert!(tcp.sack_permitted);
    }
    [a, b]
}

fn read_b(pair: &mut Pair, handles: &[TcpHandle; 2], expected: &[u8]) {
    let socket = pair.b.tcp_mut(&handles[1]);
    let bytes = drain_bytes(socket).unwrap();
    assert_eq!(bytes.len(), expected.len());
    assert_eq!(&bytes[..expected.len()], expected, "B read different bytes");
    assert!(socket.recv().unwrap().is_none());
}

fn receive_acked(pair: &mut Pair, trace: &RefCell<Trace>, handles: &[TcpHandle; 2], data: &[u8]) {
    let mut received = Vec::new();
    let mut pending = None;
    drive(pair, trace, |a, b| {
        let socket = b.tcp_mut(&handles[1]);
        let mut bytes = [0; 4096];
        let mut read = 0usize;
        while read < bytes.len() {
            if pending.is_none() {
                if !socket.can_recv() {
                    break;
                }
                pending = Some((socket.recv().unwrap().unwrap(), 0));
            }
            let (frame, offset) = pending.as_mut().unwrap();
            let count = (bytes.len() - read).min(frame.payload().len() - *offset);
            bytes[read..read + count].copy_from_slice(&frame.payload()[*offset..*offset + count]);
            *offset = offset.checked_add(count).expect("frame offset exceeds usize");
            read = read.checked_add(count).expect("read count exceeds usize");
            if *offset == frame.payload().len() {
                pending = None;
            }
        }
        received.extend_from_slice(&bytes[..read]);
        assert!(received.len() <= data.len(), "B received extra bytes");
        if received.len() == data.len() && a.tcp(&handles[0]).send_queue() == 0 {
            App::Done
        } else if read > 0 {
            App::Moved
        } else {
            App::Idle
        }
    });
    assert_eq!(received, data, "B read different bytes");
}

fn transfer(pair: &mut Pair, trace: &RefCell<Trace>, handles: &[TcpHandle; 2], data: &[u8]) {
    assert_eq!(pair.a.tcp_mut(&handles[0]).send_slice(data), Ok(data.len()));
    receive_acked(pair, trace, handles, data);
}

fn finish(pair: Pair, trace: &RefCell<Trace>, drops: u64) -> Vec<String> {
    assert!(
        !trace.borrow().drop_next_data,
        "scripted drop never happened"
    );
    assert_eq!((pair.a_to_b.dropped, pair.b_to_a.dropped), (drops, 0));
    for link in [&pair.a_to_b, &pair.b_to_a] {
        assert_eq!((link.ring_full, link.pool_empty, link.held), (0, 0, 0));
    }
    assert_eq!(pair.close(), 0);
    trace.borrow().frames.iter().map(Frame::line).collect()
}

fn compare(name: &str, lines: Vec<String>) {
    let mut text = lines.join("\n");
    text.push('\n');
    let path = format!("{FIXTURES}/{name}.txt");
    if std::env::var("SWITCH_FRAMES_BLESS").as_deref() == Ok("1") {
        std::fs::create_dir_all(FIXTURES).unwrap();
        std::fs::write(path, &text).unwrap();
        return;
    }
    let committed = std::fs::read_to_string(path).unwrap();
    for (index, (got, want)) in text.lines().zip(committed.lines()).enumerate() {
        assert_eq!(got, want, "{name} frame line {}", index + 1);
    }
    assert_eq!(
        text.lines().count(),
        committed.lines().count(),
        "{name} frame count"
    );
}


fn bulk_scenario(mut scenario: impl FnMut(&mut Pair, &RefCell<Trace>, &[TcpHandle; 2]) -> u64) {
    with_eal(|eal| {
        let mut pair = Pair::with_pools(
            eal,
            ["frsa", "frsb"],
            [config(Side::A), config(Side::B)],
            T0,
            512,
            pool_count(rx_reserve(16, 8, config(Side::A).rx_staging, 8), windows(1500, [BULK])),
            tx_reserve(512, config(Side::A).tx_pending) as u32,
            pool_count(frame_reserve(512, config(Side::A).tx_pending), windows(1500, [BULK])),
        );
        let trace = record(&mut pair);
        let handles = open_sockets(&mut pair, &trace, BULK);
        let drops = scenario(&mut pair, &trace, &handles);
        finish(pair, &trace, drops);
    });
}

#[switch_test_support::pinned_test]
fn bulk_handshake() {
    bulk_scenario(|pair, trace, handles| {
        let recorded = trace.borrow();
        let frames: Vec<_> = tcp_frames(&recorded.frames).collect();
        assert_eq!(frames.len(), 4);
        let [(syn_frame, syn), (syn_ack_frame, syn_ack), (ack_frame, ack), (update_frame, update)] =
            &frames[..]
        else {
            unreachable!();
        };
        assert_eq!(syn_frame.dir, Dir::AToB);
        assert_eq!(syn.control, TcpControl::Syn);
        assert_eq!(syn.ack_number, None);
        assert_eq!(syn_ack_frame.dir, Dir::BToA);
        assert_eq!(syn_ack.control, TcpControl::Syn);
        assert_eq!(syn_ack.ack_number, Some(syn.seq_number + 1));
        for tcp in [syn, syn_ack] {
            assert_eq!(tcp.max_seg_size, Some(1460));
            assert!(tcp.window_scale.expect("SYN window scale") > 0);
            assert!(tcp.sack_permitted);
            assert!(tcp.payload.is_empty());
        }
        assert_eq!(ack_frame.dir, Dir::AToB);
        assert!(pure_ack(ack));
        assert_eq!(ack.seq_number, syn.seq_number + 1);
        assert_eq!(ack.ack_number, Some(syn_ack.seq_number + 1));
        // B updates the unscaled SYN-ACK window to its full scaled allowance.
        assert_eq!(update_frame.dir, Dir::BToA);
        assert!(pure_ack(update));
        assert_eq!(update.seq_number, syn_ack.seq_number + 1);
        assert_eq!(update.ack_number, Some(syn.seq_number + 1));
        let shift = syn_ack.window_scale.unwrap();
        let socket = pair.b.tcp(&handles[1]);
        let allowance = socket.recv_capacity();
        let advertised = usize::from(update.window_len) << shift;
        assert_eq!(advertised, (allowance >> shift) << shift);
        assert!(advertised > usize::from(u16::MAX));
        assert!(frames.iter().all(|(frame, _)| frame.at == T0 && !frame.dropped));
        assert_eq!(pair.a.tcp(&handles[0]).state(), State::Established);
        assert_eq!(pair.b.tcp(&handles[1]).state(), State::Established);
        assert_eq!((pair.a.poll_at(), pair.b.poll_at()), (None, None));
        // Deliver B's window update to A so the link is empty at close.
        drop(frames);
        drop(recorded);
        round(pair, trace, T0);
        0
    });
}

#[switch_test_support::pinned_test]
fn bulk_handshake_ack_receive_window() {
    bulk_scenario(|pair, trace, handles| {
        let recorded = trace.borrow();
        let (_, syn) = tcp_frames(&recorded.frames)
            .find(|(frame, tcp)| frame.dir == Dir::AToB && tcp.control == TcpControl::Syn)
            .unwrap();
        let (_, syn_ack) = tcp_frames(&recorded.frames)
            .find(|(frame, tcp)| frame.dir == Dir::BToA && tcp.control == TcpControl::Syn)
            .unwrap();
        let (ack_frame, ack) = tcp_frames(&recorded.frames)
            .filter(|(frame, _)| frame.dir == Dir::AToB)
            .last()
            .unwrap();
        assert!(pure_ack(&ack));
        assert_eq!(ack.seq_number, syn.seq_number + 1);
        assert_eq!(ack.ack_number, Some(syn_ack.seq_number + 1));
        assert_eq!(ack_frame.at, T0);
        assert!(!ack_frame.dropped);
        let shift = syn.window_scale.unwrap();
        let socket = pair.a.tcp(&handles[0]);
        assert_eq!(usize::from(syn.window_len), socket.recv_capacity().min(usize::from(u16::MAX)));
        let allowance = socket.recv_capacity();
        let advertised = usize::from(ack.window_len) << shift;
        assert_eq!(advertised, (allowance >> shift) << shift);
        assert!(advertised > usize::from(syn.window_len));
        drop(recorded);
        round(pair, trace, T0);
        0
    });
}

#[switch_test_support::pinned_test]
fn bulk_small_writes() {
    bulk_scenario(|pair, trace, handles| {
        let (mut next, peer_seq) = {
            let recorded = trace.borrow();
            let (_, ack) = tcp_frames(&recorded.frames)
                .filter(|(frame, _)| frame.dir == Dir::AToB)
                .last()
                .unwrap();
            (ack.seq_number, ack.ack_number.unwrap())
        };
        for (seed, len) in [1, 2, 1447, 1448, 3000].into_iter().enumerate() {
            let data = pattern(len, u32::try_from(seed).unwrap());
            let start = trace.borrow().frames.len();
            transfer(pair, trace, handles, &data);
            let recorded = trace.borrow();
            let sent: Vec<_> = tcp_frames(&recorded.frames[start..])
                .filter(|(frame, tcp)| frame.dir == Dir::AToB && !tcp.payload.is_empty())
                .collect();
            let mut offset = 0usize;
            for (index, (frame, tcp)) in sent.iter().enumerate() {
                assert!(!frame.dropped);
                assert_eq!(tcp.seq_number, next + offset);
                assert_eq!(tcp.ack_number, Some(peer_seq));
                let end = offset.checked_add(tcp.payload.len()).expect("sent bytes");
                assert_eq!(tcp.payload, &data[offset..end]);
                assert_eq!(
                    tcp.control,
                    if index + 1 == sent.len() {
                        TcpControl::Psh
                    } else {
                        TcpControl::None
                    }
                );
                offset = end;
            }
            assert_eq!(offset, len);
            next += len;
            let (_, ack) = tcp_frames(&recorded.frames[start..])
                .filter(|(frame, tcp)| frame.dir == Dir::BToA && pure_ack(tcp))
                .last()
                .expect("B acknowledged the write");
            assert_eq!(ack.seq_number, peer_seq);
            assert_eq!(ack.ack_number, Some(next));
            assert_eq!(pair.a.tcp(&handles[0]).send_queue(), 0);
        }

        assert_eq!(pair.b.tcp(&handles[1]).ack_delay(), Some(ACK_DELAY));
        let data = pattern(7, 5);
        let start = trace.borrow().frames.len();
        let sent_at = pair.now();
        assert_eq!(pair.a.tcp_mut(&handles[0]).send_slice(&data), Ok(data.len()));
        let sent = round(pair, trace, sent_at);
        assert_eq!((sent.a_to_b, sent.b_to_a), (1, 0));
        assert_eq!(pair.a.tcp(&handles[0]).send_queue(), data.len());
        assert!(
            matches!(pair.b.poll_at(), Some(at) if at > sent_at && at <= ceil_ms(sent_at + ACK_DELAY))
        );
        receive_acked(pair, trace, handles, &data);
        assert_eq!(pair.now(), ceil_ms(sent_at + ACK_DELAY));
        let recorded = trace.borrow();
        let acks: Vec<_> = tcp_frames(&recorded.frames[start..])
            .filter(|(frame, tcp)| frame.dir == Dir::BToA && pure_ack(tcp))
            .collect();
        assert_eq!(acks.len(), 1);
        assert_eq!(acks[0].0.at, pair.now());
        assert_eq!(acks[0].1.ack_number, Some(next + data.len()));
        0
    });
}

/// Receive one window of data and return the held frames.
fn hold_every_frame(
    pair: &mut Pair,
    trace: &RefCell<Trace>,
    handles: &[TcpHandle; 2],
) -> Vec<TcpFrame> {
    // Both sides quiet first: A has B's post-handshake window update.
    drive(pair, trace, |a, b| {
        if a.poll_at().is_none() && b.poll_at().is_none() {
            App::Done
        } else {
            App::Idle
        }
    });
    let data = pattern(4096, 40);
    const SEGMENTS: usize = 8;
    for index in 0..SEGMENTS {
        let bytes = &data[index * data.len() / SEGMENTS..(index + 1) * data.len() / SEGMENTS];
        assert_eq!(pair.a.tcp_mut(&handles[0]).send_slice(bytes), Ok(bytes.len()));
        assert_eq!(pair.a.send().unwrap().tx_accepted, 1);
    }
    drive(pair, trace, |a, b| {
        if a.tcp(&handles[0]).send_queue() == 0 && b.tcp(&handles[1]).recv_queue() == data.len() {
            App::Done
        } else {
            App::Idle
        }
    });
    let mut held = Vec::new();
    while let Some(frame) = pair.b.tcp_mut(&handles[1]).recv().unwrap() {
        held.push(frame);
    }
    assert_eq!(held.len(), SEGMENTS);
    assert_eq!(held.iter().flat_map(|frame| frame.payload().iter().copied()).collect::<Vec<_>>(), data);
    held
}

/// Retaining frames does not limit delivery of the next receive window; held frames keep their
/// RX mbufs until dropped.
#[switch_test_support::pinned_test]
fn receiving_continues_while_frames_are_held() {
    with_eal(|eal| {
        let mut pair = Pair::new(eal, ["frza", "frzb"], [config(Side::A), config(Side::B)], T0);
        let trace = record(&mut pair);
        let handles = open(&mut pair, &trace, 4096);
        let idle = pair.in_use()[1].0;
        let first = hold_every_frame(&mut pair, &trace, &handles);
        let second = hold_every_frame(&mut pair, &trace, &handles);
        let held = u32::try_from(first.len() + second.len()).unwrap();
        assert_eq!(pair.in_use()[1].0, idle + held);
        drop((first, second));
        assert_eq!(pair.in_use()[1].0, idle);
        finish(pair, &trace, 0);
    });
}

/// Under a large window, dropping held frames cannot make a window update due:
/// `poll_at` stays quiet and no frame goes out.
#[switch_test_support::pinned_test]
fn bulk_held_frames_dropped_between_turns_stay_quiet() {
    bulk_scenario(|pair, trace, handles| {
        let data = pattern(3000, 41);
        assert_eq!(pair.a.tcp_mut(&handles[0]).send_slice(&data), Ok(data.len()));
        let mut held = Vec::new();
        let mut received = 0;
        drive(pair, trace, |a, b| {
            while let Some(frame) = b.tcp_mut(&handles[1]).recv().unwrap() {
                received += frame.payload().len();
                held.push(frame);
            }
            if a.tcp(&handles[0]).send_queue() == 0 && received == data.len() {
                App::Done
            } else {
                App::Idle
            }
        });
        let at = pair.now();
        round(pair, trace, at);
        assert_eq!(pair.b.poll_at(), None);
        assert!(!held.is_empty());
        let start = trace.borrow().frames.len();
        drop(held);
        assert_eq!(pair.b.poll_at(), None);
        round(pair, trace, at);
        assert_eq!(trace.borrow().frames.len(), start, "dropping frames sent a frame");
        0
    });
}

#[switch_test_support::pinned_test]
fn peer_zero_window_probe_then_full_delivery() {
    with_eal(|eal| {
        let mut pair = Pair::new(
            eal,
            ["frza", "frzb"],
            [config(Side::A), config(Side::B)],
            T0,
        );
        let trace = record(&mut pair);
        let handles = open(&mut pair, &trace, 4096);
        let (a_seq, b_seq) = {
            let recorded = trace.borrow();
            let (frame, ack) = tcp_frames(&recorded.frames).last().unwrap();
            assert_eq!(frame.dir, Dir::AToB);
            assert!(pure_ack(&ack));
            (ack.seq_number, ack.ack_number.unwrap())
        };
        let mut peer_ack = TcpRepr {
            src_port: SERVER_PORT,
            dst_port: CLIENT_PORT,
            control: TcpControl::None,
            seq_number: b_seq,
            ack_number: Some(a_seq),
            window_len: 0,
            window_scale: None,
            max_seg_size: None,
            sack_permitted: false,
            sack_ranges: [None; 3],
            timestamp: None,
            payload: &[],
        };
        // Do not let the real peer's window updates override the injected zero window.
        pair.b_to_a.paused = true;
        pair.inject(Side::A, &[tcp_frame(Side::B, &peer_ack)]);
        let at = pair.now();
        assert_eq!(round(&mut pair, &trace, at).a.rx_processed, 1);

        let data = pattern(1000, 37);
        let start = trace.borrow().frames.len();
        assert_eq!(pair.a.tcp_mut(&handles[0]).send_slice(&data), Ok(data.len()));
        round(&mut pair, &trace, at);
        let mut deadline = at;
        loop {
            let next = pair.a.poll_at().expect("zero-window probe deadline");
            assert!(next > deadline && next <= ceil_ms(at + RTO));
            round(&mut pair, &trace, next - Duration::from_millis(1));
            assert!(tcp_frames(&trace.borrow().frames[start..])
                .all(|(frame, tcp)| frame.dir != Dir::AToB || tcp.payload.is_empty()));
            round(&mut pair, &trace, next);
            deadline = next;
            if tcp_frames(&trace.borrow().frames[start..])
                .any(|(frame, tcp)| frame.dir == Dir::AToB && !tcp.payload.is_empty())
            {
                break;
            }
        }
        {
            let recorded = trace.borrow();
            let probes: Vec<_> = tcp_frames(&recorded.frames[start..])
                .filter(|(frame, tcp)| frame.dir == Dir::AToB && !tcp.payload.is_empty())
                .collect();
            assert_eq!(probes.len(), 1);
            assert_eq!(probes[0].0.at, deadline);
            assert_eq!(probes[0].1.seq_number, a_seq);
            assert_eq!(probes[0].1.ack_number, Some(b_seq));
            assert_eq!(probes[0].1.payload, &data[..1]);
        }
        assert_eq!(pair.b.tcp(&handles[1]).recv_queue(), 1);
        peer_ack.ack_number = Some(a_seq + 1);
        peer_ack.window_len = 4096;
        pair.inject(Side::A, &[tcp_frame(Side::B, &peer_ack)]);
        assert_eq!(pair.a.receive(deadline, 32).unwrap().rx_processed, 1);
        assert_eq!(pair.a.tcp(&handles[0]).send_queue(), data.len() - 1);
        pair.b_to_a.paused = false;
        receive_acked(&mut pair, &trace, &handles, &data);

        let after_delivery = pair.now() + RTO + RTO;
        round(&mut pair, &trace, after_delivery);
        let recorded = trace.borrow();
        let mut next = a_seq;
        for (_, tcp) in tcp_frames(&recorded.frames[start..])
            .filter(|(frame, tcp)| frame.dir == Dir::AToB && !tcp.payload.is_empty())
        {
            assert_eq!(tcp.seq_number, next, "data was retransmitted or skipped");
            next += tcp.payload.len();
        }
        assert_eq!(next, a_seq + data.len());
        assert_eq!((pair.a.tcp(&handles[0]).retransmits(), pair.a.tcp(&handles[0]).loss_events()), (0, 0));
        assert_eq!(pair.a.poll_at(), None);
        drop(recorded);
        finish(pair, &trace, 0);
    });
}

#[switch_test_support::pinned_test]
fn bulk_fast_retransmit() {
    bulk_scenario(|pair, trace, handles| {
        let mss = {
            let recorded = trace.borrow();
            let (_, syn_ack) = tcp_frames(&recorded.frames)
                .find(|(frame, tcp)| frame.dir == Dir::BToA && tcp.control == TcpControl::Syn)
                .unwrap();
            let (_, ack) = tcp_frames(&recorded.frames).last().unwrap();
            let options = ack
                .header_len()
                .checked_sub(TCP_HEADER_LEN)
                .expect("TCP header");
            usize::from(syn_ack.max_seg_size.unwrap())
                .checked_sub(options)
                .expect("MSS includes room for TCP options")
        };
        for seed in 8..24 {
            transfer(pair, trace, handles, &pattern(mss, seed));
        }
        let data = pattern(4 * mss, 24);
        let start = trace.borrow().frames.len();
        trace.borrow_mut().drop_next_data = true;
        let sent_at = pair.now();
        assert_eq!(pair.a.tcp_mut(&handles[0]).send_slice(&data), Ok(data.len()));
        round(pair, trace, sent_at);
        let rto_deadline = pair
            .a
            .poll_at()
            .expect("burst has no retransmission deadline");
        assert!(rto_deadline > sent_at);
        let lost_seq = {
            let recorded = trace.borrow();
            let flight: Vec<_> = tcp_frames(&recorded.frames[start..])
                .filter(|(frame, tcp)| frame.dir == Dir::AToB && !tcp.payload.is_empty())
                .collect();
            assert_eq!(flight.len(), 4, "burst did not put four segments in flight");
            let seq = flight[0].1.seq_number;
            for (index, (frame, tcp)) in flight.iter().enumerate() {
                assert_eq!(frame.at, sent_at);
                assert_eq!(frame.dropped, index == 0);
                assert_eq!(tcp.seq_number, seq + index * mss);
                assert_eq!(tcp.payload, &data[index * mss..(index + 1) * mss]);
                assert_eq!(
                    tcp.control,
                    if index == 3 {
                        TcpControl::Psh
                    } else {
                        TcpControl::None
                    }
                );
            }
            let sacks: Vec<_> = tcp_frames(&recorded.frames[start..])
                .filter(|(frame, tcp)| frame.dir == Dir::BToA && pure_ack(tcp))
                .collect();
            assert_eq!(
                sacks.len(),
                3,
                "later segments did not elicit three ingress ACKs"
            );
            for (index, (frame, tcp)) in sacks.iter().enumerate() {
                assert_eq!(frame.at, sent_at);
                assert_eq!(tcp.ack_number, Some(seq));
                assert_eq!(
                    tcp.sack_ranges,
                    [
                        Some(((seq + mss).0 as u32, (seq + (index + 2) * mss).0 as u32)),
                        None,
                        None,
                    ]
                );
            }
            seq
        };
        assert_eq!(pair.b.tcp(&handles[1]).recv_queue(), 0);
        receive_acked(pair, trace, handles, &data);
        assert_eq!(
            pair.now(),
            sent_at,
            "fast retransmit required a timer advance"
        );
        let recorded = trace.borrow();
        let retries: Vec<_> = tcp_frames(&recorded.frames[start..])
            .filter(|(frame, tcp)| {
                frame.dir == Dir::AToB
                    && !frame.dropped
                    && tcp.seq_number == lost_seq
                    && !tcp.payload.is_empty()
            })
            .collect();
        assert_eq!(retries.len(), 1, "missing fast retransmission");
        assert!(retries[0].0.at < rto_deadline);
        assert_eq!(retries[0].1.payload, &data[..mss]);
        let (_, ack) = tcp_frames(&recorded.frames[start..])
            .filter(|(frame, tcp)| frame.dir == Dir::BToA && pure_ack(tcp))
            .last()
            .unwrap();
        assert_eq!(ack.ack_number, Some(lost_seq + data.len()));
        assert_eq!(ack.sack_ranges, [None; 3]);
        assert_eq!(pair.a.poll_at(), None);
        1
    });
}

#[switch_test_support::pinned_test]
fn bulk_frames_match_committed() {
    let lines = with_eal(|eal| {
        let mut pair = Pair::with_pools(
            eal,
            ["frba", "frbb"],
            [config(Side::A), config(Side::B)],
            T0,
            512,
            pool_count(rx_reserve(16, 8, config(Side::A).rx_staging, 8), windows(1500, [BULK])),
            tx_reserve(512, config(Side::A).tx_pending) as u32,
            pool_count(frame_reserve(512, config(Side::A).tx_pending), windows(1500, [BULK])),
        );
        let trace = record(&mut pair);
        let handles = open(&mut pair, &trace, BULK);

        for (seed, len) in [1, 2, 1447, 1448, 3000].into_iter().enumerate() {
            let start = trace.borrow().frames.len();
            transfer(&mut pair, &trace, &handles, &pattern(len, seed as u32));
            let recorded = trace.borrow();
            let sent: Vec<_> = tcp_frames(&recorded.frames[start..])
                .filter(|(frame, tcp)| frame.dir == Dir::AToB && !tcp.payload.is_empty())
                .collect();
            assert_eq!(
                sent.iter().map(|(_, tcp)| tcp.payload.len()).sum::<usize>(),
                len
            );
            assert_eq!(sent.last().unwrap().1.control, TcpControl::Psh);
            assert!(sent.iter().all(|(frame, _)| !frame.dropped));
            assert_eq!(pair.a.tcp(&handles[0]).send_queue(), 0);
        }

        assert_eq!(pair.b.tcp(&handles[1]).ack_delay(), Some(ACK_DELAY));
        let data = pattern(7, 5);
        let start = trace.borrow().frames.len();
        let sent_at = pair.now();
        assert_eq!(pair.a.tcp_mut(&handles[0]).send_slice(&data), Ok(data.len()));
        let sent = round(&mut pair, &trace, sent_at);
        assert_eq!((sent.a_to_b, sent.b_to_a), (1, 0));
        assert_eq!(pair.a.tcp(&handles[0]).send_queue(), data.len());
        assert!(
            matches!(pair.b.poll_at(), Some(at) if at > sent_at && at <= ceil_ms(sent_at + ACK_DELAY))
        );
        receive_acked(&mut pair, &trace, &handles, &data);
        assert_eq!(pair.now(), ceil_ms(sent_at + ACK_DELAY));
        {
            let recorded = trace.borrow();
            let acks: Vec<_> = tcp_frames(&recorded.frames[start..])
                .filter(|(frame, tcp)| frame.dir == Dir::BToA && pure_ack(tcp))
                .collect();
            assert_eq!(acks.len(), 1);
            assert_eq!(acks[0].0.at, pair.now());
        }

        let data = pattern(3 * BULK / 4, 6);
        assert_eq!(pair.a.tcp_mut(&handles[0]).send_slice(&data), Ok(data.len()));
        drive(&mut pair, &trace, |a, b| {
            if a.tcp(&handles[0]).send_queue() == 0 && b.tcp(&handles[1]).recv_queue() == data.len() {
                App::Done
            } else {
                App::Idle
            }
        });
        read_b(&mut pair, &handles, &data);
        let at = pair.now();
        round(&mut pair, &trace, at);

        let data = pattern(1000, 7);
        let start = trace.borrow().frames.len();
        trace.borrow_mut().drop_next_data = true;
        let sent_at = pair.now();
        assert_eq!(pair.a.tcp_mut(&handles[0]).send_slice(&data), Ok(data.len()));
        round(&mut pair, &trace, sent_at);
        assert_eq!(pair.a_to_b.dropped, 1);
        assert_eq!(pair.b.tcp(&handles[1]).recv_queue(), 0);
        // The bulk transfer's delayed-ACK samples put SRTT above zero.
        assert!(
            matches!(pair.a.poll_at(), Some(at) if at > sent_at && at <= ceil_ms(sent_at + RTO + ACK_DELAY))
        );
        drive(&mut pair, &trace, |_, b| {
            if b.tcp(&handles[1]).recv_queue() == data.len() {
                App::Done
            } else {
                App::Idle
            }
        });
        {
            let recorded = trace.borrow();
            let attempts: Vec<_> = tcp_frames(&recorded.frames[start..])
                .filter(|(frame, tcp)| frame.dir == Dir::AToB && !tcp.payload.is_empty())
                .collect();
            assert_eq!(attempts.len(), 2);
            assert!(attempts[0].0.dropped);
            assert!(!attempts[1].0.dropped);
            let resent = attempts[1].0.at;
            assert!(resent > sent_at + RTO && resent <= ceil_ms(sent_at + RTO + ACK_DELAY), "{resent}");
            assert_eq!(attempts[0].1.seq_number, attempts[1].1.seq_number);
            assert_eq!(attempts[0].1.payload, &data[..]);
            assert_eq!(attempts[1].1.payload, &data[..]);
        }
        receive_acked(&mut pair, &trace, &handles, &data);

        // Cubic reduced its window at the RTO. Each acknowledged write grows it.
        for seed in 8..24 {
            transfer(&mut pair, &trace, &handles, &pattern(1448, seed));
        }
        let data = pattern(4 * 1448, 24);
        let start = trace.borrow().frames.len();
        trace.borrow_mut().drop_next_data = true;
        let sent_at = pair.now();
        assert_eq!(pair.a.tcp_mut(&handles[0]).send_slice(&data), Ok(data.len()));
        round(&mut pair, &trace, sent_at);
        let rto_deadline = pair
            .a
            .poll_at()
            .expect("burst has no retransmission deadline");
        assert!(rto_deadline > sent_at);
        let lost_seq = {
            let recorded = trace.borrow();
            let flight: Vec<_> = tcp_frames(&recorded.frames[start..])
                .filter(|(frame, tcp)| frame.dir == Dir::AToB && !tcp.payload.is_empty())
                .collect();
            assert_eq!(flight.len(), 4, "burst did not put four segments in flight");
            let seq = flight[0].1.seq_number;
            for (index, (frame, tcp)) in flight.iter().enumerate() {
                assert_eq!(frame.at, sent_at);
                assert_eq!(frame.dropped, index == 0);
                assert_eq!(tcp.seq_number, seq + index * 1460);
                let end = ((index + 1) * 1460).min(data.len());
                assert_eq!(tcp.payload, &data[index * 1460..end]);
            }
            let sacks: Vec<_> = tcp_frames(&recorded.frames[start..])
                .filter(|(frame, tcp)| frame.dir == Dir::BToA && pure_ack(tcp))
                .collect();
            assert_eq!(
                sacks.len(),
                3,
                "later segments did not elicit three ingress ACKs"
            );
            for (frame, tcp) in sacks {
                assert_eq!(frame.at, sent_at);
                assert_eq!(tcp.ack_number, Some(seq));
                assert!(
                    tcp.sack_ranges.iter().any(Option::is_some),
                    "ACK omitted SACK block"
                );
            }
            seq
        };
        assert_eq!(pair.b.tcp(&handles[1]).recv_queue(), 0);
        receive_acked(&mut pair, &trace, &handles, &data);
        assert_eq!(
            pair.now(),
            sent_at,
            "fast retransmit required a timer advance"
        );
        {
            let recorded = trace.borrow();
            let retries: Vec<_> = tcp_frames(&recorded.frames[start..])
                .filter(|(frame, tcp)| {
                    frame.dir == Dir::AToB
                        && !frame.dropped
                        && tcp.seq_number == lost_seq
                        && !tcp.payload.is_empty()
                })
                .collect();
            assert_eq!(retries.len(), 1, "missing fast retransmission");
            assert!(retries[0].0.at < rto_deadline);
            assert_eq!(retries[0].1.payload, &data[..1460]);
        }

        let start = trace.borrow().frames.len();
        pair.a.close(&handles[0]);
        drive(&mut pair, &trace, |a, b| {
            let server = b.tcp_mut(&handles[1]);
            if server.state() == State::CloseWait {
                assert!(matches!(server.recv(), Err(RecvError::Finished)));
                b.close(&handles[1]);
                App::Moved
            } else if a.tcp(&handles[0]).state() == State::TimeWait
                && server.state() == State::Closed
            {
                App::Done
            } else {
                App::Idle
            }
        });
        {
            let recorded = trace.borrow();
            let frames = &recorded.frames[start..];
            let fins: Vec<_> = tcp_frames(frames)
                .filter(|(_, tcp)| tcp.control == TcpControl::Fin)
                .collect();
            assert_eq!(fins.len(), 2);
            assert_eq!((fins[0].0.dir, fins[1].0.dir), (Dir::AToB, Dir::BToA));
            let (frame, ack) = tcp_frames(frames).last().unwrap();
            assert_eq!(frame.dir, Dir::AToB);
            assert!(pure_ack(&ack));
            assert_eq!(ack.seq_number, fins[0].1.seq_number + 1);
            assert_eq!(ack.ack_number, Some(fins[1].1.seq_number + 1));
        }
        let entered = pair.now();
        let frames = trace.borrow().frames.len();
        drive(&mut pair, &trace, |a, b| {
            if a.tcp(&handles[0]).state() == State::Closed
                && b.tcp(&handles[1]).state() == State::Closed
            {
                App::Done
            } else {
                App::Idle
            }
        });
        assert_eq!(pair.now(), ceil_ms(entered + TIME_WAIT));
        assert_eq!(
            trace.borrow().frames.len(),
            frames,
            "TIME-WAIT emitted an extra frame"
        );
        assert!(tcp_frames(&trace.borrow().frames).all(|(_, tcp)| tcp.timestamp.is_none()));
        finish(pair, &trace, 2)
    });
    compare("bulk", lines);
}

#[switch_test_support::pinned_test]
fn plain_frames_match_committed() {
    let lines = with_eal(|eal| {
        let mut pair = Pair::new(
            eal,
            ["frpa", "frpb"],
            [config(Side::A), config(Side::B)],
            T0,
        );
        let trace = record(&mut pair);
        let handles = open(&mut pair, &trace, 4096);
        let (a_seq, b_seq) = {
            let recorded = trace.borrow();
            let (frame, tcp) = tcp_frames(&recorded.frames).last().unwrap();
            assert_eq!(frame.dir, Dir::AToB);
            assert!(pure_ack(&tcp));
            (tcp.seq_number, tcp.ack_number.unwrap())
        };

        let interval = Duration::from_millis(100);
        let start = trace.borrow().frames.len();
        let idle_at = pair.now();
        assert_eq!(pair.a.poll_at(), None);
        let sent_at = idle_at + interval;
        let probe = tcp_frame(Side::A, &TcpRepr {
            src_port: CLIENT_PORT,
            dst_port: SERVER_PORT,
            control: TcpControl::None,
            seq_number: a_seq - 1,
            ack_number: Some(b_seq),
            window_len: 4096,
            window_scale: None,
            max_seg_size: None,
            sack_permitted: false,
            sack_ranges: [None; 3],
            timestamp: None,
            payload: &[],
        });
        trace.borrow_mut().now = Some(sent_at);
        assert!(matches!(trace.borrow_mut().frame(Dir::AToB, &probe), Verdict::Deliver));
        pair.inject(Side::B, &[probe]);
        round(&mut pair, &trace, sent_at);
        {
            let recorded = trace.borrow();
            let frames: Vec<_> = tcp_frames(&recorded.frames[start..]).collect();
            assert_eq!(frames.len(), 2, "keep-alive or its ACK is missing");
            let (frame, keep_alive) = &frames[0];
            assert_eq!((frame.dir, frame.at), (Dir::AToB, sent_at));
            assert_eq!(keep_alive.control, TcpControl::None);
            assert_eq!(keep_alive.seq_number, a_seq - 1);
            assert_eq!(keep_alive.ack_number, Some(b_seq));
            assert!(keep_alive.payload.is_empty());
            let (frame, ack) = &frames[1];
            assert_eq!((frame.dir, frame.at), (Dir::BToA, sent_at));
            assert!(pure_ack(ack));
            assert_eq!(ack.seq_number, b_seq);
            assert_eq!(ack.ack_number, Some(a_seq));
        }
        round(&mut pair, &trace, sent_at);
        assert_eq!(
            pair.b.tcp(&handles[1]).recv_queue(),
            0,
            "keep-alive reached the application"
        );


        let start = trace.borrow().frames.len();
        pair.a.abort(&handles[0]);
        drive(&mut pair, &trace, |a, b| {
            if a.tcp(&handles[0]).state() == State::Closed
                && b.tcp(&handles[1]).state() == State::Closed
            {
                App::Done
            } else {
                App::Idle
            }
        });
        {
            let recorded = trace.borrow();
            let resets: Vec<_> = tcp_frames(&recorded.frames[start..]).collect();
            assert_eq!(resets.len(), 1);
            assert_eq!(resets[0].0.dir, Dir::AToB);
            assert_eq!(resets[0].1.control, TcpControl::Rst);
            assert_eq!(resets[0].1.seq_number, a_seq);
            assert_eq!(resets[0].1.ack_number, Some(b_seq));
            assert!(resets[0].1.payload.is_empty());
        }
        assert!(tcp_frames(&trace.borrow().frames).all(|(_, tcp)| tcp.timestamp.is_none()));
        assert_eq!((pair.a.poll_at(), pair.b.poll_at()), (None, None));
        finish(pair, &trace, 0)
    });
    compare("plain", lines);
}
