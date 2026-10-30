//! The send core, runtime-free: its sync transitions, and its async methods polled by hand
//! with a no-op waker over a mock lower layer whose waits are gated. Frames are read back by
//! relay's receive core and by tungstenite.

use std::cell::RefCell;
use std::io::Cursor;
use std::pin::pin;
use std::rc::Rc;
use std::task::Poll;

use super::*;
use crate::tx::{End, Failure, Job, MAX_SEND, TxCore, TxError, Work};
use tungstenite::Message as TMessage;
use tungstenite::protocol::{Role as TRole, WebSocket};

const HEAD: usize = 5;
const TAIL: usize = 17;
const FILL: u8 = 0xee;
pub(super) const ROOM: usize = 1430;

pub(super) const CLOSE_RECORD: &[u8] = b"close_notify";

/// The lower layer reporting a dead socket.
#[derive(Debug, PartialEq)]
pub(super) struct Dead;

#[derive(Default)]
pub(super) struct Log {
    /// Plaintext of each frame TCP took, in order.
    pub(super) sent: Vec<Vec<u8>>,
    /// Socket bytes free; `None`: unlimited.
    pub(super) room: Option<usize>,
    /// Frames the pool can still give; `None`: unlimited.
    pub(super) frames: Option<usize>,
    /// Every wait pends once on its first poll (so each poll of a driver passes one wait).
    pub(super) stall: bool,
    /// Each wait that pended, in order.
    pub(super) waited: Vec<&'static str>,
    /// Upcoming `try_send` calls to refuse; a refusal also takes all room away, as a full
    /// socket would.
    pub(super) refuse: usize,
    /// The socket is dead: waits fail, sends are refused.
    pub(super) dead: bool,
    /// A sealed frame is held by the core, not yet taken (sheath allows one).
    pub(super) in_flight: bool,
    pub(super) seals: u64,
    /// Sealed frames TCP took.
    pub(super) taken: u64,
    /// Records the lower layer sends on its own, oldest first.
    pub(super) pending: Vec<Vec<u8>>,
    pub(super) shutdowns: usize,
    /// `delivered` resolves.
    pub(super) delivered: bool,
    /// One per frame the mock gave out and nobody dropped or TCP took.
    pub(super) live: Rc<()>,
}

impl Log {
    /// Frames given out and not yet dropped or taken by TCP.
    pub(super) fn live_frames(&self) -> usize {
        Rc::strong_count(&self.live) - 1
    }
}

/// A TLS-shaped lower layer: plaintext at `HEAD`, `TAIL` bytes after it. Every seal entry
/// point asserts the previous sealed frame was taken; TCP takes frames in seal order.
pub(super) struct MockTx {
    room: usize,
    log: Rc<RefCell<Log>>,
}

pub(super) struct MockFrame {
    buf: Vec<u8>,
    _live: Rc<()>,
}

impl TxFrame for MockFrame {
    fn room_mut(&mut self) -> &mut [u8] {
        &mut self.buf
    }
}

pub(super) struct MockSealed {
    buf: Vec<u8>,
    len: usize,
    seq: u64,
    _live: Rc<()>,
}

impl MockTx {
    fn sealed(&mut self, f: MockFrame, len: usize) -> MockSealed {
        assert!(len <= self.room);
        let mut log = self.log.borrow_mut();
        assert!(!log.in_flight, "sealed while a sealed frame was not taken");
        log.in_flight = true;
        log.seals += 1;
        MockSealed { buf: f.buf, len, seq: log.seals - 1, _live: f._live }
    }

    /// A gated wait: pends once if stalling (recording `name`), then while `blocked`.
    async fn wait(&self, name: &'static str, blocked: impl Fn(&Log) -> bool) -> Result<(), Dead> {
        let mut first = true;
        std::future::poll_fn(|_| {
            let mut log = self.log.borrow_mut();
            if log.dead {
                return Poll::Ready(Err(Dead));
            }
            if (first && log.stall) || blocked(&log) {
                first = false;
                log.waited.push(name);
                return Poll::Pending;
            }
            Poll::Ready(Ok(()))
        })
        .await
    }
}

impl FrameTx for MockTx {
    type Frame = MockFrame;
    type Sealed = MockSealed;
    type Error = Dead;

    async fn alloc(&mut self) -> Result<MockFrame, Dead> {
        self.wait("alloc", |l| l.frames == Some(0)).await?;
        if let Some(n) = &mut self.log.borrow_mut().frames {
            *n -= 1;
        }
        let live = self.log.borrow().live.clone();
        Ok(MockFrame { buf: vec![FILL; HEAD + self.room + TAIL], _live: live })
    }

    async fn room(&mut self, len: usize) -> Result<(), Dead> {
        self.wait("room", |l| l.room.is_some_and(|r| r < len)).await
    }

    fn plain(&self) -> (usize, usize) {
        (HEAD, self.room)
    }

    fn seal(&mut self, f: MockFrame, len: usize) -> Result<MockSealed, Dead> {
        Ok(self.sealed(f, len))
    }

    fn try_send(&mut self, s: MockSealed) -> Result<(), MockSealed> {
        let mut log = self.log.borrow_mut();
        if log.dead || log.refuse > 0 {
            log.refuse = log.refuse.saturating_sub(1);
            log.room = Some(0);
            return Err(s);
        }
        assert_eq!(log.shutdowns, 0, "sent after FIN");
        assert_eq!(s.seq, log.taken, "sent out of seal order");
        log.taken += 1;
        assert!(s.buf[..HEAD].iter().all(|&b| b == FILL), "wrote before the plaintext head");
        if let Some(room) = &mut log.room {
            *room -= s.len;
        }
        log.in_flight = false;
        log.sent.push(s.buf[HEAD..HEAD + s.len].to_vec());
        Ok(())
    }

    fn shutdown(&mut self) {
        self.log.borrow_mut().shutdowns += 1;
    }

    async fn delivered(&mut self) {
        let _ = self.wait("delivered", |l| !l.delivered).await;
    }
}

impl PlainTx for MockTx {
    const HAS_CLOSE_RECORD: bool = true;

    fn has_pending(&self) -> bool {
        !self.log.borrow().pending.is_empty()
    }

    fn take_pending(&mut self, mut f: MockFrame) -> Result<Option<MockSealed>, Dead> {
        let record = self.log.borrow_mut().pending.remove(0);
        f.buf[HEAD..HEAD + record.len()].copy_from_slice(&record);
        Ok(Some(self.sealed(f, record.len())))
    }

    fn seal_close(&mut self, mut f: MockFrame) -> Result<Option<MockSealed>, Dead> {
        f.buf[HEAD..HEAD + CLOSE_RECORD.len()].copy_from_slice(CLOSE_RECORD);
        Ok(Some(self.sealed(f, CLOSE_RECORD.len())))
    }
}

pub(super) type Core = TxCore<MockTx>;

pub(super) fn core(role: Role, room: usize) -> (Core, Rc<RefCell<Log>>) {
    let log = Rc::new(RefCell::new(Log { delivered: true, ..Log::default() }));
    (TxCore::new(MockTx { room, log: log.clone() }, role), log)
}

fn peer(role: Role) -> Role {
    match role {
        Role::Client => Role::Server,
        Role::Server => Role::Client,
    }
}

pub(super) fn sent(log: &Rc<RefCell<Log>>) -> Vec<Vec<u8>> {
    log.borrow().sent.clone()
}

/// The frames as relay's receive core of the peer reads them, one TCP frame each.
fn relay_reads(sender: Role, frames: &[Vec<u8>]) -> Run {
    let chunks: Vec<&[u8]> = frames.iter().map(Vec::as_slice).collect();
    drive(&mut rx(peer(sender)), &chunks)
}

/// The frames as tungstenite in the peer's role reads them; the close it saw, if any.
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

/// The data and control frames as tungstenite reads them, without the close record.
fn messages(sender: Role, log: &Rc<RefCell<Log>>) -> Vec<TMessage> {
    let frames: Vec<_> = sent(log).into_iter().filter(|f| f != CLOSE_RECORD).collect();
    tungstenite_reads(sender, &frames)
}

fn send_all(c: &mut Core, kind: Kind, payload: &[u8]) {
    assert_eq!(ready(c.send(kind, payload)), Ok(()));
}

fn tmsg(kind: Kind, payload: &[u8]) -> TMessage {
    match kind {
        Kind::Text => TMessage::Text(String::from_utf8(payload.to_vec()).unwrap().into()),
        Kind::Binary => TMessage::Binary(payload.to_vec().into()),
    }
}

/// Polls `f` at most `n` times; its output if it finished.
fn poll_n<F: Future>(f: F, n: usize) -> Option<F::Output> {
    let mut f = pin!(f);
    for _ in 0..n {
        if let Poll::Ready(out) = poll(f.as_mut()) {
            return Some(out);
        }
    }
    None
}

/// The core's state that a refused call must leave as it was.
pub(super) fn state(c: &Core) -> (Job, bool, Option<Failure>, End) {
    (c.next_job(), c.sent(), c.failed().cloned(), c.end())
}

#[test]
#[cfg_attr(miri, ignore)]
fn round_trips_through_relay_and_tungstenite() {
    let mut rng = Rng(11);
    for role in [Role::Client, Role::Server] {
        // One frame holds 65536 B; then MSS-sized frames, so larger messages fragment.
        for room in [70_000, ROOM] {
            for len in [0, 1, 125, 126, 65535, 65536, 200_000] {
                let (mut c, log) = core(role, room);
                let payload = rng.bytes(len);
                send_all(&mut c, Kind::Binary, &payload);
                let text = "€uro".repeat(len / 6 + 1);
                send_all(&mut c, Kind::Text, text.as_bytes());
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

/// The exact bytes: minimal length encoding, payload right behind the header, for a message in
/// one frame and the fragments of a longer one.
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
        let (mut c, log) = core(Role::Server, 70_000);
        let payload = vec![7; len];
        send_all(&mut c, Kind::Binary, &payload);
        assert_eq!(sent(&log), [[header, &payload[..]].concat()], "len {len}");
    }
    // Frame room 1430: 1426 B per fragment (4 B header), the last one shorter.
    let (mut c, log) = core(Role::Server, ROOM);
    let payload = vec![3; 1426 + 10];
    send_all(&mut c, Kind::Text, &payload);
    assert_eq!(
        sent(&log),
        [[&[0x01, 126, 0x05, 0x92][..], &payload[..1426]].concat(), [&[0x80, 10][..], &payload[1426..]].concat()]
    );
    // A payload that fits one frame exactly is one frame.
    let (mut c, log) = core(Role::Server, ROOM);
    send_all(&mut c, Kind::Binary, &[1; 1426]);
    assert_eq!(sent(&log).len(), 1);
}

/// Client frames carry a fresh key per frame; the payload is XORed with key[i % 4]
/// (RFC 6455 §5.3), which is what tungstenite and relay undo.
#[test]
fn client_masks_every_frame_with_a_fresh_key() {
    let (mut c, log) = core(Role::Client, ROOM);
    let payload: Vec<u8> = (0..40).collect();
    for _ in 0..8 {
        send_all(&mut c, Kind::Binary, &payload);
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
}

/// Waits come before the seal: room, then the frame. With no room nothing is sealed (no
/// sequence number used); the send resumes once room comes, a fragment per frame of room.
#[test]
fn send_waits_for_room_then_a_frame_before_sealing() {
    let (mut c, log) = core(Role::Server, ROOM);
    log.borrow_mut().room = Some(0);
    {
        let mut f = pin!(c.send(Kind::Binary, &[9; 4000]));
        assert!(poll(f.as_mut()).is_pending());
        assert_eq!((log.borrow().seals, log.borrow().waited.clone()), (0, vec!["room"]));
        log.borrow_mut().room = Some(ROOM);
        assert!(poll(f.as_mut()).is_pending());
        assert_eq!(sent(&log).len(), 1);
        log.borrow_mut().room = None;
        log.borrow_mut().frames = Some(0);
        assert!(poll(f.as_mut()).is_pending());
        assert_eq!(log.borrow().waited.last(), Some(&"alloc"));
        log.borrow_mut().frames = None;
        assert_eq!(poll(f.as_mut()), Poll::Ready(Ok(())));
    }
    assert_eq!(messages(Role::Server, &log), [TMessage::Binary(vec![9; 4000].into())]);
}

/// A send dropped at each of its awaits, then a second send (same kind, and in another run the
/// other kind): tungstenite reads either just the second message (the first was dropped before
/// acceptance) or both, whole and apart. Never a merged message, never half of one. (The old
/// send core left the message open: the next same-kind send continued it, the other kind
/// panicked.)
#[test]
#[cfg_attr(miri, ignore)]
fn a_dropped_send_never_merges_into_the_next_message() {
    for first_len in [10, 4000] {
        for second in [Kind::Binary, Kind::Text] {
            let first = vec![1u8; first_len];
            for cut in 0.. {
                let (mut c, log) = core(Role::Client, ROOM);
                log.borrow_mut().stall = true;
                if poll_n(c.send(Kind::Binary, &first), cut).is_some() {
                    break;
                }
                log.borrow_mut().stall = false;
                send_all(&mut c, second, b"second");
                let got = messages(Role::Client, &log);
                let whole = [TMessage::Binary(first.clone().into()), tmsg(second, b"second")];
                assert!(got == whole[1..] || got == whole, "len {first_len} cut {cut}: {got:?}");
            }
        }
    }
}

/// A second send dropped while it drains the first (a dropped large send's rest) sends
/// nothing of its own message; the first still goes out whole.
#[test]
fn second_send_dropped_while_draining_the_first_sends_nothing_of_its_own() {
    let (mut c, log) = core(Role::Server, ROOM);
    log.borrow_mut().room = Some(ROOM);
    assert!(poll_n(c.send(Kind::Binary, &[1; 4000]), 1).is_none());
    assert_eq!(c.next_job(), Job::Frame(ROOM, Work::Fragment));
    assert!(poll_n(c.send(Kind::Text, b"second"), 1).is_none());
    log.borrow_mut().room = None;
    send_all(&mut c, Kind::Text, b"third");
    assert_eq!(messages(Role::Server, &log), [TMessage::Binary(vec![1; 4000].into()), tmsg(Kind::Text, b"third")]);
}

/// A sealed frame TCP refuses waits in the slot and goes out before anything else; the send
/// completes only once TCP took it (a small message, and a large one's final fragment).
#[test]
fn completion_waits_for_tcp_to_take_the_last_frame() {
    for len in [10, 1426 + 10] {
        let (mut c, log) = core(Role::Server, ROOM);
        let payload = vec![5; len];
        {
            let mut f = pin!(c.send(Kind::Binary, &payload));
            if len > 1426 {
                // The first fragment takes all the room; the second waits.
                log.borrow_mut().room = Some(ROOM);
                assert!(poll(f.as_mut()).is_pending());
                assert_eq!(sent(&log).len(), 1);
                log.borrow_mut().room = None;
            }
            log.borrow_mut().refuse = 1;
            for _ in 0..4 {
                assert!(poll(f.as_mut()).is_pending(), "len {len}: done though TCP refused the last frame");
            }
            assert_eq!((log.borrow().refuse, log.borrow().seals), (0, if len > 1426 { 2 } else { 1 }));
            log.borrow_mut().room = None;
            assert_eq!(poll(f.as_mut()), Poll::Ready(Ok(())), "len {len}");
        }
        assert_eq!(messages(Role::Server, &log), [TMessage::Binary(vec![5; len].into())], "len {len}");
    }
}

/// A refused frame kept through a dropped future goes out first on the next call.
#[test]
fn refused_frame_survives_a_dropped_future() {
    let (mut c, log) = core(Role::Server, ROOM);
    log.borrow_mut().refuse = 1;
    log.borrow_mut().room = Some(0);
    {
        let mut f = pin!(c.send(Kind::Text, b"first"));
        // No room: nothing sealed. Then room: sealed, refused, and the resend waits.
        assert!(poll(f.as_mut()).is_pending());
        assert_eq!(log.borrow().seals, 0);
        log.borrow_mut().room = Some(ROOM);
        assert!(poll(f.as_mut()).is_pending());
    }
    assert_eq!(c.next_job(), Job::Resend(7));
    log.borrow_mut().room = None;
    send_all(&mut c, Kind::Text, b"second");
    assert_eq!(messages(Role::Server, &log), [tmsg(Kind::Text, b"first"), tmsg(Kind::Text, b"second")]);
}

/// Work order: the refused frame, lower records, pong, fragments, our close, the end.
#[test]
fn next_job_order() {
    let (mut c, log) = core(Role::Server, ROOM);
    log.borrow_mut().refuse = 1;
    assert!(poll_n(c.send(Kind::Binary, &[1; 3000]), 1).is_none());
    log.borrow_mut().pending.push(b"alert".to_vec());
    c.ping(b"p");
    assert_eq!(c.queue_close(1000, ""), Ok(()));
    log.borrow_mut().room = None;
    let mut order = Vec::new();
    while let job @ (Job::Resend(_) | Job::Frame(..)) = c.next_job() {
        order.push(job);
        assert!(ready(c.step()));
    }
    let works: Vec<_> = order.iter().map(|j| if let Job::Frame(_, w) = j { Some(*w) } else { None }).collect();
    assert_eq!(
        works,
        [None, Some(Work::Pending), Some(Work::Pong), Some(Work::Fragment), Some(Work::Fragment), Some(Work::Close)]
    );
}

/// A ping answered while a large message is going out: the pong goes between its fragments.
#[test]
fn pong_goes_between_fragments() {
    let (mut c, log) = core(Role::Server, ROOM);
    log.borrow_mut().room = Some(ROOM);
    assert!(poll_n(c.send(Kind::Binary, &[2; 3000]), 2).is_none());
    c.ping(b"hb");
    log.borrow_mut().room = None;
    ready(c.drive(|c| !c.sent()));
    let frames = sent(&log);
    assert_eq!(frames[1], [&[0x8a, 2][..], b"hb"].concat());
    assert_eq!(
        tungstenite_reads(Role::Server, &frames),
        [TMessage::Pong(b"hb".to_vec().into()), TMessage::Binary(vec![2; 3000].into())]
    );
}

/// Our close queued (and the send dropped) before the peer's arrives: ours goes out, after the
/// message in progress; the peer's payload is kept by the receive side, not echoed.
#[test]
fn our_queued_close_goes_out_after_the_message_and_is_not_replaced() {
    let (mut c, log) = core(Role::Server, ROOM);
    log.borrow_mut().room = Some(ROOM);
    assert!(poll_n(c.send(Kind::Binary, &[4; 3000]), 1).is_none());
    assert_eq!(c.queue_close(1000, "mine"), Ok(()));
    c.peer_close(&[0x03, 0xe9, b'x']);
    log.borrow_mut().room = None;
    ready(c.finish());
    let frames = sent(&log);
    let msgs = tungstenite_reads(Role::Server, &frames[..frames.len() - 1]);
    assert_eq!(msgs[0], TMessage::Binary(vec![4; 3000].into()));
    assert!(matches!(&msgs[1], TMessage::Close(Some(f)) if u16::from(f.code) == 1000 && f.reason == "mine"));
    assert_eq!(frames.last().unwrap(), CLOSE_RECORD);
    assert_eq!((c.end(), log.borrow().shutdowns), (End::Delivered, 1));
}

/// The peer's close while our message goes out: the rest of it, then the echo, then
/// close_notify, FIN and its delivery.
#[test]
fn peer_close_during_a_large_message() {
    let (mut c, log) = core(Role::Server, ROOM);
    log.borrow_mut().room = Some(ROOM);
    assert!(poll_n(c.send(Kind::Binary, &[4; 3000]), 1).is_none());
    c.peer_close(&[0x03, 0xe9]);
    log.borrow_mut().room = None;
    log.borrow_mut().delivered = false;
    assert!(poll_n(c.finish(), 3).is_none());
    assert_eq!(c.end(), End::Fin);
    log.borrow_mut().delivered = true;
    ready(c.finish());
    assert_eq!(c.end(), End::Delivered);
    let frames = sent(&log);
    assert_eq!(frames.last().unwrap(), CLOSE_RECORD);
    let msgs = tungstenite_reads(Role::Server, &frames[..frames.len() - 1]);
    assert_eq!(msgs[0], TMessage::Binary(vec![4; 3000].into()));
    assert!(matches!(&msgs[1], TMessage::Close(Some(f)) if u16::from(f.code) == 1001));
}

/// close_notify refused: it waits in the slot (End Notify), and FIN comes only after TCP took
/// it. A refused close likewise completes `close` only once taken.
#[test]
fn refused_close_and_close_notify_complete_only_when_taken() {
    let (mut c, log) = core(Role::Server, ROOM);
    log.borrow_mut().refuse = 1;
    log.borrow_mut().room = Some(0);
    {
        let mut f = pin!(c.close(1000, ""));
        assert!(poll(f.as_mut()).is_pending());
        log.borrow_mut().room = Some(ROOM);
        assert!(poll(f.as_mut()).is_pending(), "close done though TCP refused it");
        log.borrow_mut().room = None;
        assert_eq!(poll(f.as_mut()), Poll::Ready(Ok(())));
    }
    c.peer_close(&[0x03, 0xe8]);
    log.borrow_mut().refuse = 1;
    log.borrow_mut().room = Some(ROOM);
    assert!(ready(c.step()));
    assert_eq!((c.end(), c.next_job()), (End::Notify, Job::Resend(ROOM)));
    assert_eq!(log.borrow().shutdowns, 0);
    log.borrow_mut().room = None;
    ready(c.finish());
    assert_eq!(sent(&log).last().unwrap(), CLOSE_RECORD);
    assert_eq!((c.end(), log.borrow().shutdowns), (End::Delivered, 1));
}

/// A failure abandons the unsealed work (the rest of a message, the pong, a queued close) and
/// drains what is sealed or owed below: the refused frame, the lower records (TLS alert), then
/// close_notify unless the failure is TLS, then FIN. Our Close already sealed still goes out;
/// no further Close is sent.
#[test]
fn failure_drains_only_what_is_sealed_or_owed_below() {
    for failure in [Failure::Protocol(crate::ProtocolError::ReservedBits), Failure::PeerEof, Failure::Tls(sheath::Error::Truncated)] {
        let (mut c, log) = core(Role::Server, ROOM);
        log.borrow_mut().room = Some(ROOM);
        assert!(poll_n(c.send(Kind::Binary, &[4; 4000]), 2).is_none());
        log.borrow_mut().refuse = 1;
        log.borrow_mut().room = Some(ROOM);
        assert!(ready(c.step()));
        assert_eq!(c.next_job(), Job::Resend(ROOM));
        c.ping(b"p");
        log.borrow_mut().pending.push(b"alert".to_vec());
        let before = sent(&log).len();
        c.fail(failure.clone());
        assert_eq!(c.failed(), Some(&failure));
        log.borrow_mut().room = None;
        ready(c.finish());
        let after: Vec<_> = sent(&log).split_off(before);
        let tls = matches!(failure, Failure::Tls(_));
        assert_eq!(after.len(), if tls { 2 } else { 3 }, "{failure:?}: {after:?}");
        assert_eq!(after[0][..2], [0x00, 126], "the refused fragment first");
        assert_eq!(after[1], b"alert");
        if !tls {
            assert_eq!(after[2], CLOSE_RECORD);
        }
        assert_eq!((c.end(), log.borrow().shutdowns), (End::Delivered, 1), "{failure:?}");
        assert_eq!(c.result(), Err(TxError::Failed(failure)));
    }
}

/// The socket dies after a failure, with a sealed frame retained and records owed: everything
/// is discarded, nothing more is sealed or sent, and the first failure is the one reported.
#[test]
fn death_after_a_failure_discards_everything_and_keeps_the_first_failure() {
    for first in [Failure::Protocol(crate::ProtocolError::ReservedBits), Failure::Tls(sheath::Error::Truncated)] {
        let (mut c, log) = core(Role::Server, ROOM);
        log.borrow_mut().refuse = 1;
        assert!(poll_n(c.send(Kind::Binary, b"x"), 1).is_none());
        log.borrow_mut().pending.push(b"alert".to_vec());
        c.fail(first.clone());
        log.borrow_mut().dead = true;
        ready(c.finish());
        assert_eq!((c.end(), c.next_job()), (End::Dead, Job::Idle));
        assert_eq!((sent(&log).len(), log.borrow().seals), (0, 1));
        assert_eq!(log.borrow().live_frames(), 0, "the refused frame is kept after death");
        assert_eq!(c.result(), Err(TxError::Failed(first)));
    }
}

/// After a failure: send and close refuse with the first failure and change nothing; a ping is
/// not answered; a later failure does not replace it.
#[test]
fn every_op_after_a_failure() {
    let (mut c, log) = core(Role::Server, ROOM);
    c.fail(Failure::PeerEof);
    ready(c.finish());
    let before = (state(&c), sent(&log));
    assert_eq!(ready(c.send(Kind::Text, b"x")), Err(TxError::Failed(Failure::PeerEof)));
    assert_eq!(ready(c.close(1000, "")), Err(TxError::Failed(Failure::PeerEof)));
    c.ping(b"p");
    c.fail(Failure::Protocol(crate::ProtocolError::ReservedBits));
    assert_eq!((state(&c), sent(&log)), before);
    c.die();
    assert_eq!(c.failed(), Some(&Failure::PeerEof));
}

/// Our close sealed and the peer's not yet received, then a failure or `abandon`: no further
/// Close is sent, the sealed one still goes out, and a later `close` is `Closed`.
#[test]
fn abandon_after_our_close_was_sent() {
    for failure in [Some(Failure::Protocol(crate::ProtocolError::ReservedBits)), Some(Failure::PeerEof), Some(Failure::Tls(sheath::Error::Truncated)), None] {
        let (mut c, log) = core(Role::Server, ROOM);
        log.borrow_mut().refuse = 1;
        assert!(poll_n(c.close(1000, "bye"), 2).is_none());
        assert!(matches!(c.next_job(), Job::Resend(_)));
        match &failure {
            Some(f) => c.fail(f.clone()),
            None => c.abandon(),
        }
        log.borrow_mut().room = None;
        ready(c.finish());
        let msgs = messages(Role::Server, &log);
        assert!(matches!(&msgs[..], [TMessage::Close(Some(f))] if f.reason == "bye"), "{failure:?}: {msgs:?}");
        assert_eq!(log.borrow().shutdowns, 1);
        if failure.is_none() {
            assert_eq!(ready(c.close(1000, "")), Err(TxError::Closed));
        }
    }
}

/// `close` after a dropped linger (abandon): `Closed`, nothing sent.
#[test]
fn close_after_a_cancelled_linger_is_closed() {
    let (mut c, log) = core(Role::Server, ROOM);
    log.borrow_mut().delivered = false;
    c.abandon();
    assert!(poll_n(c.finish(), 4).is_none());
    assert_eq!(ready(c.close(1000, "")), Err(TxError::Closed));
    assert_eq!(sent(&log), [CLOSE_RECORD.to_vec()]);
}

/// A send whose drain of earlier work finds the socket dead accepts nothing: it returns the
/// death without copying its message in or sealing it.
#[test]
fn send_whose_drain_dies_accepts_nothing() {
    let (mut c, log) = core(Role::Server, ROOM);
    log.borrow_mut().refuse = 1;
    assert!(poll_n(c.send(Kind::Binary, b"x"), 1).is_none());
    log.borrow_mut().dead = true;
    assert_eq!(ready(c.send(Kind::Binary, &[1; 4000])), Err(TxError::Failed(Failure::Dead)));
    assert_eq!((c.buf_capacity(), log.borrow().seals), (0, 1));
}

/// After death, records the lower layer still owes are never sealed.
#[test]
fn dead_seals_nothing_more() {
    let (mut c, log) = core(Role::Server, ROOM);
    log.borrow_mut().pending.push(b"alert".to_vec());
    c.die();
    assert_eq!(c.next_job(), Job::Idle);
    ready(c.finish());
    assert_eq!(log.borrow().seals, 0);
}

/// Caller input refused before anything changes: Text not UTF-8, a message over MAX_SEND, a
/// close code we may not send or a reason over 123 B, a send after our close.
#[test]
#[cfg_attr(miri, ignore)]
fn invalid_input_changes_nothing() {
    let (mut c, log) = core(Role::Server, ROOM);
    let before = state(&c);
    assert_eq!(ready(c.send(Kind::Text, &[b'a', 0xff])), Err(TxError::InvalidText));
    assert_eq!(ready(c.send(Kind::Binary, &vec![0; MAX_SEND + 1])), Err(TxError::TooBig));
    assert_eq!(ready(c.close(1000, &"r".repeat(124))), Err(TxError::InvalidClose));
    for code in [999, 1005, 1006, 1015] {
        assert_eq!(ready(c.close(code, "")), Err(TxError::InvalidClose), "{code}");
    }
    assert_eq!((state(&c), log.borrow().seals), (before, 0));
    send_all(&mut c, Kind::Binary, &vec![0; MAX_SEND]);
    assert_eq!(ready(c.close(1000, &"r".repeat(123))), Ok(()));
    assert_eq!(ready(c.send(Kind::Text, b"late")), Err(TxError::Closed));
}

/// The send buffer grows by clamped doubling: across increasing message sizes (40, 41, 63 MiB)
/// its capacity never passes MAX_SEND, where plain `Vec` growth would reach 80 MiB.
#[test]
#[cfg_attr(miri, ignore)]
fn send_buffer_never_grows_past_max_send() {
    let (mut c, log) = core(Role::Server, 70_000);
    for mib in [40, 41, 63] {
        send_all(&mut c, Kind::Binary, &vec![0; mib << 20]);
        assert!(c.buf_capacity() <= MAX_SEND, "{mib} MiB: {}", c.buf_capacity());
        log.borrow_mut().sent.clear();
    }
}

/// A queued close waits for the message in progress (data accepted before the close goes
/// first).
#[test]
fn queued_close_waits_for_the_message() {
    let (mut c, log) = core(Role::Server, ROOM);
    log.borrow_mut().room = Some(ROOM);
    assert!(poll_n(c.send(Kind::Binary, &[4; 3000]), 1).is_none());
    assert_eq!(c.queue_close(1000, ""), Ok(()));
    assert!(matches!(c.next_job(), Job::Frame(_, Work::Fragment)));
}
