//! Send core: frames read back by relay's receive core and by tungstenite,
//! exact header bytes, masking, the unsent slot, and control frames.

mod common;

use std::cell::RefCell;
use std::convert::Infallible;
use std::io::Cursor;
use std::rc::Rc;

use common::*;
use relay::{Kind, PlainTx, Role, TxError, TxFrame, WsError, WsTx};
use tungstenite::Message as TMessage;
use tungstenite::protocol::{Role as TRole, WebSocket};

const HEAD: usize = 5;
const TAIL: usize = 17;
const FILL: u8 = 0xee;

#[derive(Default)]
struct Log {
    /// Plaintext of each frame sent, in order.
    sent: Vec<Vec<u8>>,
    /// Number of upcoming `try_send` calls to refuse.
    refuse: usize,
    refused: usize,
    allocs: usize,
    seals: u64,
    /// Records the lower layer must send on its own, oldest first.
    pending: Vec<Vec<u8>>,
    take_pending_calls: usize,
}

/// A TLS-shaped lower layer: plaintext at `HEAD`, `TAIL` bytes after it.
/// Sealing numbers the frame; sending checks the numbers arrive in order with
/// none missing, which is what a TLS peer requires.
struct MockTx {
    room: usize,
    log: Rc<RefCell<Log>>,
}

struct MockFrame {
    buf: Vec<u8>,
}

impl TxFrame for MockFrame {
    fn room_mut(&mut self) -> &mut [u8] {
        &mut self.buf
    }
}

struct MockSealed {
    buf: Vec<u8>,
    len: usize,
    seq: u64,
}

impl MockTx {
    fn sealed(&mut self, f: MockFrame, len: usize) -> MockSealed {
        assert!(len <= self.room);
        let mut log = self.log.borrow_mut();
        log.seals += 1;
        MockSealed { buf: f.buf, len, seq: log.seals - 1 }
    }
}

impl PlainTx for MockTx {
    type Frame = MockFrame;
    type Sealed = MockSealed;
    type Error = Infallible;

    fn alloc(&mut self) -> Result<MockFrame, Infallible> {
        self.log.borrow_mut().allocs += 1;
        Ok(MockFrame { buf: vec![FILL; HEAD + self.room + TAIL] })
    }

    fn plain(&self) -> (usize, usize) {
        (HEAD, self.room)
    }

    fn seal(&mut self, f: MockFrame, len: usize) -> Result<MockSealed, Infallible> {
        Ok(self.sealed(f, len))
    }

    fn has_pending(&self) -> bool {
        !self.log.borrow().pending.is_empty()
    }

    fn take_pending(&mut self, mut f: MockFrame) -> Result<Option<MockSealed>, Infallible> {
        let record = {
            let mut log = self.log.borrow_mut();
            log.take_pending_calls += 1;
            if log.pending.is_empty() {
                return Ok(None);
            }
            log.pending.remove(0)
        };
        f.buf[HEAD..HEAD + record.len()].copy_from_slice(&record);
        Ok(Some(self.sealed(f, record.len())))
    }

    fn try_send(&mut self, s: MockSealed) -> Result<(), MockSealed> {
        let mut log = self.log.borrow_mut();
        if log.refuse > 0 {
            log.refuse -= 1;
            log.refused += 1;
            return Err(s);
        }
        assert_eq!(s.seq, log.sent.len() as u64, "sent out of seal order or a frame was dropped");
        assert!(s.buf[..HEAD].iter().all(|&b| b == FILL), "wrote before the plaintext head");
        log.sent.push(s.buf[HEAD..HEAD + s.len].to_vec());
        Ok(())
    }
}

fn tx(role: Role, room: usize) -> (WsTx<MockTx>, Rc<RefCell<Log>>) {
    let log = Rc::new(RefCell::new(Log::default()));
    (WsTx::new(MockTx { room, log: log.clone() }, role), log)
}

fn peer(role: Role) -> Role {
    match role {
        Role::Client => Role::Server,
        Role::Server => Role::Client,
    }
}

fn sent(log: &Rc<RefCell<Log>>) -> Vec<Vec<u8>> {
    log.borrow().sent.clone()
}

/// The frames as relay's receive core of the peer reads them, one TCP frame each.
fn relay_reads(sender: Role, frames: &[Vec<u8>]) -> Run {
    let chunks: Vec<&[u8]> = frames.iter().map(Vec::as_slice).collect();
    drive(&mut rx(peer(sender)), &chunks)
}

/// The frames as tungstenite in the peer's role reads them.
fn tungstenite_reads(sender: Role, frames: &[Vec<u8>]) -> Vec<TMessage> {
    let role = if sender == Role::Client { TRole::Server } else { TRole::Client };
    let mut ws = WebSocket::from_raw_socket(Duplex(Cursor::new(frames.concat())), role, None);
    let mut out = Vec::new();
    loop {
        match ws.read() {
            Ok(m) => out.push(m),
            Err(tungstenite::Error::Protocol(tungstenite::error::ProtocolError::ResetWithoutClosingHandshake)) => {
                return out;
            }
            Err(tungstenite::Error::ConnectionClosed) => return out,
            Err(e) => panic!("tungstenite: {e}"),
        }
    }
}

fn send_all(t: &mut WsTx<MockTx>, kind: Kind, payload: &[u8]) {
    let mut rest = payload;
    assert_eq!(t.send(kind, &mut rest), Ok(true));
    assert!(rest.is_empty());
}

#[test]
#[cfg_attr(miri, ignore)]
fn round_trips_through_relay_and_tungstenite() {
    let mut rng = Rng(11);
    for role in [Role::Client, Role::Server] {
        // One frame holds 65536 B; then MSS-sized frames, so larger messages fragment.
        for room in [70_000, 1430] {
            for len in [0, 1, 125, 126, 65535, 65536, 200_000] {
                let (mut t, log) = tx(role, room);
                let payload = rng.bytes(len);
                send_all(&mut t, Kind::Binary, &payload);
                let text = "€uro".repeat(len / 6 + 1);
                send_all(&mut t, Kind::Text, text.as_bytes());
                let frames = sent(&log);
                assert!(frames.iter().all(|f| f.len() <= room));
                let run = relay_reads(role, &frames);
                assert_eq!(run.end, None, "{role:?} room {room} len {len}");
                let got: Vec<_> = run.msgs.into_iter().map(|m| (m.kind, m.payload)).collect();
                assert_eq!(got, [(Kind::Binary, payload.clone()), (Kind::Text, text.clone().into_bytes())]);
                let t = tungstenite_reads(role, &frames);
                assert_eq!(t, [TMessage::Binary(payload.into()), TMessage::Text(text.into())]);
            }
        }
    }
}

/// The exact bytes: minimal length encoding, payload right behind the header.
#[test]
fn header_bytes_and_length_classes() {
    let cases: [(usize, &[u8]); 5] = [
        (0, &[0x82, 0]),
        (125, &[0x82, 125]),
        (126, &[0x82, 126, 0, 126]),
        (65535, &[0x82, 126, 0xff, 0xff]),
        (65536, &[0x82, 127, 0, 0, 0, 0, 0, 1, 0, 0]),
    ];
    for (len, header) in cases {
        let (mut t, log) = tx(Role::Server, 70_000);
        let payload = vec![7; len];
        send_all(&mut t, Kind::Binary, &payload);
        assert_eq!(sent(&log), [[header, &payload[..]].concat()], "len {len}");
    }
    // Room reserves the 4-byte class; a short payload moves down behind a 2-byte header.
    for (room, len, header) in [(1430, 10, &[0x82u8, 10][..]), (300, 200, &[0x82, 126, 0, 200]), (300, 125, &[0x82, 125])] {
        let (mut t, log) = tx(Role::Server, room);
        let mut f = t.frame(Kind::Binary).unwrap().unwrap();
        f.room()[..len].fill(3);
        f.commit(len, true).unwrap();
        assert_eq!(sent(&log), [[header, &vec![3; len][..]].concat()], "room {room} len {len}");
    }
}

#[test]
fn room_is_the_frame_room_minus_the_header() {
    for (role, room, cap) in [(Role::Server, 1430, 1426), (Role::Client, 1430, 1422), (Role::Server, 127, 125), (Role::Server, 128, 124)] {
        let (mut t, _) = tx(role, room);
        assert_eq!(t.frame(Kind::Text).unwrap().unwrap().room().len(), cap, "{role:?} {room}");
    }
}

/// Client frames carry a fresh key per frame; the payload is XORed with
/// key[i % 4] (RFC 6455 §5.3), which is what tungstenite and relay undo.
#[test]
fn client_masks_every_frame_with_a_fresh_key() {
    let (mut t, log) = tx(Role::Client, 1430);
    let payload: Vec<u8> = (0..40).collect();
    for _ in 0..8 {
        send_all(&mut t, Kind::Binary, &payload);
    }
    let frames = sent(&log);
    let mut keys = Vec::new();
    for f in &frames {
        assert_eq!(f[..2], [0x82, 0x80 | 40]);
        let key: [u8; 4] = f[2..6].try_into().unwrap();
        let unmasked: Vec<u8> = f[6..].iter().enumerate().map(|(i, b)| b ^ key[i % 4]).collect();
        assert_eq!(unmasked, payload);
        keys.push(key);
    }
    keys.sort();
    keys.dedup();
    assert!(keys.len() > 1, "same key on every frame");
    assert_eq!(relay_reads(Role::Client, &frames).msgs.len(), 8);
    assert_eq!(tungstenite_reads(Role::Client, &frames).len(), 8);
    // The server role does not mask.
    let (mut t, log) = tx(Role::Server, 1430);
    send_all(&mut t, Kind::Binary, &payload);
    assert_eq!(sent(&log), [[&[0x82, 40][..], &payload].concat()]);
}

#[test]
fn refused_frame_is_kept_and_sent_first() {
    for role in [Role::Client, Role::Server] {
        let (mut t, log) = tx(role, 1430);
        log.borrow_mut().refuse = 1;
        let mut f = t.frame(Kind::Text).unwrap().unwrap();
        f.room()[..1].copy_from_slice(b"a");
        f.commit(1, true).unwrap();
        assert!(sent(&log).is_empty());
        assert_eq!(log.borrow().refused, 1);

        // The next frame goes out only after the kept one.
        send_all(&mut t, Kind::Text, b"b");
        assert_eq!(t.flush(), Ok(true));
        let frames = sent(&log);
        assert_eq!(frames.len(), 2);
        assert_eq!(log.borrow().seals, 2);
        let got: Vec<_> = relay_reads(role, &frames).msgs.into_iter().map(|m| m.payload).collect();
        assert_eq!(got, [b"a".to_vec(), b"b".to_vec()]);
    }
}

#[test]
fn no_new_frame_while_the_kept_one_is_refused() {
    let (mut t, log) = tx(Role::Server, 1430);
    log.borrow_mut().refuse = 4;
    send_all(&mut t, Kind::Binary, b"a");
    assert!(t.frame(Kind::Binary).unwrap().is_none());
    assert_eq!(t.flush(), Ok(false));
    assert_eq!(log.borrow().allocs, 1, "allocated while a frame waited");
    let mut rest: &[u8] = b"b";
    assert_eq!(t.send(Kind::Binary, &mut rest), Ok(false));
    assert_eq!(rest, b"b");
    assert_eq!(t.send(Kind::Binary, &mut rest), Ok(true));
    let got: Vec<_> = relay_reads(Role::Server, &sent(&log)).msgs.into_iter().map(|m| m.payload).collect();
    assert_eq!(got, [b"a".to_vec(), b"b".to_vec()]);
}

/// A refusal mid-message leaves it open; the next call continues it.
#[test]
fn send_resumes_after_a_refusal() {
    for role in [Role::Client, Role::Server] {
        let (mut t, log) = tx(role, 100);
        let payload: Vec<u8> = (0..1000).map(|i| i as u8).collect();
        let mut rest = &payload[..];
        log.borrow_mut().refuse = 2;
        assert_eq!(t.send(Kind::Binary, &mut rest), Ok(false));
        let left = rest.len();
        assert!(0 < left && left < payload.len());
        assert_eq!(t.send(Kind::Binary, &mut rest), Ok(true));
        let frames = sent(&log);
        assert!(frames.len() > 10);
        assert_eq!(frames[0][0], 0x02, "first fragment: binary, not fin");
        assert!(frames[1..frames.len() - 1].iter().all(|f| f[0] == 0x00), "middle fragments: continuation");
        assert_eq!(frames[frames.len() - 1][0], 0x80, "last fragment: fin continuation");
        let run = relay_reads(role, &frames);
        assert_eq!(run.msgs.len(), 1);
        assert_eq!(run.msgs[0].payload, payload);
        assert_eq!(tungstenite_reads(role, &frames), [TMessage::Binary(payload.into())]);
    }
}

#[test]
fn pong_goes_between_fragments() {
    for role in [Role::Client, Role::Server] {
        let (mut t, log) = tx(role, 1430);
        let mut f = t.frame(Kind::Text).unwrap().unwrap();
        f.room()[..3].copy_from_slice(b"abc");
        f.commit(3, false).unwrap();
        t.pong(b"old");
        t.pong(b"p");
        let mut f = t.frame(Kind::Text).unwrap().unwrap();
        f.room()[..3].copy_from_slice(b"def");
        f.commit(3, true).unwrap();
        let frames = sent(&log);
        assert_eq!(frames.iter().map(|f| f[0]).collect::<Vec<_>>(), [0x01, 0x8a, 0x80]);
        let run = relay_reads(role, &frames);
        assert_eq!(run.end, None);
        assert_eq!(run.msgs.len(), 1);
        assert_eq!(run.msgs[0].payload, b"abcdef");
        assert_eq!(
            tungstenite_reads(role, &frames),
            [TMessage::Pong(b"p".to_vec().into()), TMessage::Text("abcdef".into())]
        );
    }
}

#[test]
fn refused_pong_is_kept() {
    let (mut t, log) = tx(Role::Server, 1430);
    t.pong(b"p");
    log.borrow_mut().refuse = 1;
    assert_eq!(t.flush(), Ok(false));
    assert!(t.frame(Kind::Binary).unwrap().is_some());
    assert_eq!(tungstenite_reads(Role::Server, &sent(&log)), [TMessage::Pong(b"p".to_vec().into())]);
}

/// The receive half's ping becomes the send half's pong.
#[test]
fn ping_is_answered_with_its_payload() {
    let (mut t, log) = tx(Role::Client, 1430);
    let mut r = rx(Role::Client);
    r.push(VecFrame { data: encode(fin(PING), None, b"hb"), stamp: 0 }, &mut ()).unwrap();
    assert!(r.recv(&mut ()).unwrap().is_none());
    if let Some(p) = r.take_pong() {
        t.pong(p);
    }
    assert_eq!(t.flush(), Ok(true));
    assert_eq!(tungstenite_reads(Role::Client, &sent(&log)), [TMessage::Pong(b"hb".to_vec().into())]);
}

/// The peer's close is echoed; after that no data frame and no pong goes out.
#[test]
fn close_is_echoed_and_ends_sending() {
    let (mut t, log) = tx(Role::Server, 1430);
    let mut r = rx(Role::Server);
    let close = [&1001u16.to_be_bytes()[..], b"bye"].concat();
    r.push(VecFrame { data: encode(fin(CLOSE), Some([1, 2, 3, 4]), &close), stamp: 0 }, &mut ()).unwrap();
    assert!(matches!(r.recv(&mut ()), Err(WsError::Closed)));
    t.pong(b"first");
    t.close(r.peer_close().unwrap());
    t.close(b"ignored");
    t.pong(b"after");
    assert!(matches!(t.frame(Kind::Binary), Err(TxError::Closed)));
    assert_eq!(t.flush(), Ok(true));
    let frames = sent(&log);
    assert_eq!(frames, [[&[0x8a, 5][..], b"first"].concat(), [&[0x88, 5][..], &close].concat()]);
    let msgs = tungstenite_reads(Role::Server, &frames);
    assert_eq!(msgs.len(), 2);
    assert!(matches!(&msgs[1], TMessage::Close(Some(c)) if u16::from(c.code) == 1001 && c.reason == "bye"));
    assert_eq!(t.flush(), Ok(true));
    assert_eq!(sent(&log).len(), 2);
}

#[test]
#[should_panic(expected = "message is still open")]
fn other_kind_while_a_message_is_open_panics() {
    let (mut t, _) = tx(Role::Server, 1430);
    t.frame(Kind::Text).unwrap().unwrap().commit(0, false).unwrap();
    let _ = t.frame(Kind::Binary);
}

/// Flush order: the refused frame, then the lower layer's own records, then
/// the pong, then the close; a refusal stops it and the rest waits in order.
#[test]
fn flush_order_unsent_pending_pong_close() {
    let (mut t, log) = tx(Role::Server, 1430);
    log.borrow_mut().refuse = 1;
    send_all(&mut t, Kind::Binary, b"data");
    log.borrow_mut().pending = vec![b"alert1".to_vec(), b"alert2".to_vec()];
    t.pong(b"p");
    t.close(&1000u16.to_be_bytes());
    // The pending records come after the refused frame and are not lost when it is refused again.
    log.borrow_mut().refuse = 1;
    assert_eq!(t.flush(), Ok(false));
    assert!(sent(&log).is_empty());
    assert_eq!(t.flush(), Ok(true));
    assert_eq!(
        sent(&log),
        [
            [&[0x82, 4][..], b"data"].concat(),
            b"alert1".to_vec(),
            b"alert2".to_vec(),
            [&[0x8a, 1][..], b"p"].concat(),
            [&[0x88, 2][..], &1000u16.to_be_bytes()].concat(),
        ]
    );
    assert_eq!(log.borrow().take_pending_calls, 2);
}

/// A refused pending record is kept, sent first, and stops the pong behind it.
#[test]
fn refused_pending_record_is_kept() {
    let (mut t, log) = tx(Role::Server, 1430);
    log.borrow_mut().pending = vec![b"alert".to_vec()];
    t.pong(b"p");
    log.borrow_mut().refuse = 1;
    assert_eq!(t.flush(), Ok(false));
    assert!(sent(&log).is_empty());
    assert_eq!(t.flush(), Ok(true));
    assert_eq!(sent(&log), [b"alert".to_vec(), [&[0x8a, 1][..], b"p"].concat()]);
}

/// Without pending records, sending allocates exactly one frame per WebSocket
/// frame and never asks for pending records.
#[test]
fn no_pending_no_extra_frames() {
    let (mut t, log) = tx(Role::Client, 1430);
    for _ in 0..5 {
        send_all(&mut t, Kind::Text, b"hi");
    }
    assert_eq!(t.flush(), Ok(true));
    assert_eq!(log.borrow().allocs, 5);
    assert_eq!(log.borrow().take_pending_calls, 0);
}
