//! The connection: `recv`'s wait pass over input and the send core's next job, the end of
//! receiving, EOF, and `linger`. The input is a gated queue whose waits store their wakers;
//! the send side is the send core's mock (tests/tx.rs). Wake tests run in a switch runtime,
//! whose poll report shows whether the task ran: a wait that lost its waker never finishes,
//! and a spinning one is polled with nothing changed.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::io::Write;
use std::pin::pin;
use std::rc::Rc;
use std::task::{Poll, Waker};

use super::peer::{Peer, TestTx, Wire, connect};
use super::tx::{Log, MockTx, ROOM, messages, mock, poll_n, sent, set, tmsg};
use super::*;
use crate::lower::PlainRx;
use crate::session::{Input, InputEnd, RxIn, Session};
use crate::tx::{Failure, TxError};
use rustls::ServerConnection;
use sheath::{TlsRx, TlsTx};
use tungstenite::Message as TMessage;

/// A received frame that counts itself live while held.
pub struct LiveFrame {
    data: Vec<u8>,
    stamp: u64,
    _live: Rc<()>,
}

impl RxFrame for LiveFrame {
    fn payload(&self) -> &[u8] {
        &self.data
    }
    fn payload_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
    fn rx_timestamp(&self) -> u64 {
        self.stamp
    }
}

#[derive(Default)]
struct In {
    frames: VecDeque<Vec<u8>>,
    end: Option<InputEnd>,
    wakers: Wakers,
    /// Frames handed out so far (each is stamped with its index).
    taken: u64,
    live: Rc<()>,
}

impl In {
    fn live_frames(&self) -> usize {
        Rc::strong_count(&self.live) - 1
    }
}

type Inputs = Rc<RefCell<In>>;

/// Queues `bytes` as one frame; wakes the waiting input if `wake`.
fn feed(input: &Inputs, bytes: &[u8], wake: bool) {
    let mut i = input.borrow_mut();
    i.frames.push_back(bytes.to_vec());
    if wake {
        let wakers = i.wakers.take();
        drop(i);
        wakers.into_iter().for_each(Waker::wake);
    }
}

/// Ends the input after its queued frames, waking it.
fn end(input: &Inputs, how: InputEnd) {
    let wakers = {
        let mut i = input.borrow_mut();
        i.end = Some(how);
        i.wakers.take()
    };
    wakers.into_iter().for_each(Waker::wake);
}

pub struct MockInput(Inputs);

impl Input for MockInput {
    type Frame = LiveFrame;

    async fn recv(&mut self) -> Result<LiveFrame, InputEnd> {
        let id = self.0.borrow_mut().wakers.id();
        let _unregister = OnDrop(|| self.0.borrow_mut().wakers.remove(id));
        std::future::poll_fn(|cx| {
            let mut i = self.0.borrow_mut();
            if let Some(data) = i.frames.pop_front() {
                i.taken += 1;
                return Poll::Ready(Ok(LiveFrame { data, stamp: i.taken - 1, _live: i.live.clone() }));
            }
            if let Some(end) = i.end {
                return Poll::Ready(Err(end));
            }
            i.wakers.register(id, cx.waker());
            Poll::Pending
        })
        .await
    }
}

type RawSession = Session<Raw<LiveFrame, MockTx<false>>, MockTx<false>, MockInput>;
type TlsSession = Session<TlsRx<LiveFrame, TestTx>, TlsTx<TestTx>, MockInput>;

/// A raw (ws://) server-role connection over the send mock and a gated input.
fn raw() -> (RawSession, Rc<RefCell<Log>>, Inputs) {
    let config = Config::new(Role::Server);
    let (tx, log) = mock::<false>(&config, ROOM);
    let input = Inputs::default();
    (Session::new(WsRx::new(Raw::new(), config), tx, MockInput(input.clone())), log, input)
}

/// A wss:// client connection after the handshake; the rustls server, and what the client
/// sent.
fn tls() -> (TlsSession, ServerConnection, Wire, Inputs) {
    let config = Config::new(Role::Client);
    let Peer { tx, wire, server } = connect();
    let input = Inputs::default();
    let session = Session::new(WsRx::new(TlsRx::new(), config), TxCore::new(tx, &config), MockInput(input.clone()));
    (session, server, wire, input)
}

/// The TLS records the server has queued (its session tickets first).
fn server_bytes(server: &mut ServerConnection) -> Vec<u8> {
    let mut out = Vec::new();
    while server.wants_write() {
        server.write_tls(&mut out).unwrap();
    }
    out
}

/// A client frame (masked), as the raw server-role connection receives it.
fn client_frame(op: u8, payload: &[u8]) -> Vec<u8> {
    encode(fin(op), Some([1, 2, 3, 4]), payload)
}

type Recvd = Result<Option<(Kind, Vec<u8>)>, TxError>;

/// `recv` once, polled with a no-op waker at most `n` times; the message copied out.
fn recv_n<R, T, I>(s: &mut Session<R, T, I>, n: usize) -> Option<Recvd>
where
    R: PlainRx<Tx = T, Frame = LiveFrame>,
    T: PlainTx,
    I: Input<Frame = LiveFrame>,
    Failure: From<R::Error>,
{
    let mut f = pin!(s.recv());
    for _ in 0..n {
        if let Poll::Ready(r) = poll(f.as_mut()) {
            return Some(r.map(|m| m.map(|m| (m.kind(), m.bytes().to_vec()))));
        }
    }
    None
}

type Got = Rc<RefCell<Vec<Recvd>>>;

/// Receives until the end (`None` or an error), recording every result; then lingers if
/// asked, recording that too.
#[switch::task]
async fn receiving(mut s: RawSession, got: Got, lingered: Rc<RefCell<Option<Result<(), TxError>>>>, linger: bool) {
    loop {
        let r = s.recv().await.map(|m| m.map(|m| (m.kind(), m.bytes().to_vec())));
        let done = !matches!(r, Ok(Some(_)));
        got.borrow_mut().push(r);
        if done {
            break;
        }
    }
    if linger {
        *lingered.borrow_mut() = Some(s.linger().await);
    }
}

struct Task {
    runtime: switch::Runtime,
    got: Got,
    lingered: Rc<RefCell<Option<Result<(), TxError>>>>,
}

impl Task {
    fn spawn(s: RawSession, linger: bool) -> Task {
        let runtime = switch::Runtime::new(0);
        let got = Got::default();
        let lingered = Rc::default();
        assert!(runtime.spawner().spawn(receiving(s, got.clone(), Rc::clone(&lingered), linger)).is_ok());
        Task { runtime, got, lingered }
    }

    /// One runtime turn; how many times the task ran.
    fn turn(&mut self) -> usize {
        self.runtime.poll(0).unwrap().tasks_polled
    }

    /// Turns with nothing changed run nothing: the task waits, it does not spin.
    fn assert_parked(&mut self) {
        for _ in 0..3 {
            assert_eq!(self.turn(), 0, "the task ran with nothing changed");
        }
    }

    fn got(&self) -> Vec<Recvd> {
        self.got.borrow().clone()
    }
}

fn msg(kind: Kind, payload: &[u8]) -> Recvd {
    Ok(Some((kind, payload.to_vec())))
}

/// A send of `len` dropped after its first fragment, with the room left at `room`: its rest
/// is owed.
fn drop_large_send(s: &mut RawSession, log: &Rc<RefCell<Log>>, len: usize, room: Option<usize>) {
    log.borrow_mut().room = Some(ROOM);
    assert!(poll_n(s.tx.send(Kind::Binary, &vec![7; len]), 1).is_none());
    assert_eq!(sent(log).len(), 1);
    log.borrow_mut().room = room;
}

#[test]
fn recv_returns_messages_split_across_frames_and_answers_a_ping() {
    let (mut s, log, input) = raw();
    let mut wire = client_frame(TEXT, b"one");
    wire.extend(client_frame(PING, b"p"));
    wire.extend(client_frame(BINARY, &[2; 300]));
    for chunk in wire.chunks(7) {
        feed(&input, chunk, false);
    }
    assert_eq!(recv_n(&mut s, 1), Some(msg(Kind::Text, b"one")));
    assert_eq!(recv_n(&mut s, 1), Some(msg(Kind::Binary, &[2; 300])));
    assert_eq!(messages(Role::Server, &log), [TMessage::Pong(b"p".to_vec().into())]);
    assert_eq!(recv_n(&mut s, 3), None);
    assert_eq!(input.borrow().live_frames(), 0);
}

/// An idle `recv` waits on input alone without spinning; a frame wakes it.
#[test]
fn idle_recv_is_woken_by_input_alone() {
    let (s, _, input) = raw();
    let mut task = Task::spawn(s, false);
    task.turn();
    task.assert_parked();
    feed(&input, &client_frame(TEXT, b"hi"), true);
    assert_eq!(task.turn(), 1);
    assert_eq!(task.got(), [msg(Kind::Text, b"hi")]);
}

/// A dropped large send's rest is owed while `recv` waits for input: room alone wakes it,
/// and the rest goes out without any input. (Slice 3's recv waited on input alone here.)
#[test]
fn recv_is_woken_by_room_alone_and_sends_an_accepted_messages_rest() {
    let (mut s, log, _input) = raw();
    drop_large_send(&mut s, &log, 4000, Some(0));
    let mut task = Task::spawn(s, false);
    task.turn();
    task.assert_parked();
    set(&log, |l| l.room = None);
    assert_eq!(task.turn(), 1);
    assert_eq!(messages(Role::Server, &log), [TMessage::Binary(vec![7; 4000].into())]);
    task.assert_parked();
    assert!(task.got().is_empty());
}

/// After the peer's close, recv sends the echo and FIN and waits for FIN's delivery, which
/// alone wakes it; then `None`.
#[test]
fn recv_is_woken_by_delivery_alone() {
    let (s, log, input) = raw();
    log.borrow_mut().delivered = false;
    feed(&input, &client_frame(CLOSE, &1000u16.to_be_bytes()), false);
    let mut task = Task::spawn(s, false);
    task.turn();
    assert_eq!((log.borrow().shutdowns, log.borrow().waited.last().copied()), (1, Some("delivered")));
    task.assert_parked();
    set(&log, |l| l.delivered = true);
    assert_eq!(task.turn(), 1);
    assert_eq!(task.got(), [Ok(None)]);
}

/// A reset reported by the input alone, or by the send side alone (while it waits for room),
/// wakes recv: `Failed(Dead)`, nothing more sent.
#[test]
fn recv_is_woken_by_a_reset_alone() {
    for on_input in [true, false] {
        let (mut s, log, input) = raw();
        drop_large_send(&mut s, &log, 4000, Some(0));
        let mut task = Task::spawn(s, false);
        task.turn();
        task.assert_parked();
        if on_input {
            end(&input, InputEnd::Reset);
        } else {
            set(&log, |l| l.dead = true);
        }
        assert_eq!(task.turn(), 1, "on input {on_input}");
        assert_eq!(task.got(), [Err(TxError::Failed(Failure::Dead))], "on input {on_input}");
        assert_eq!((sent(&log).len(), log.borrow().live_frames()), (1, 0));
    }
}

/// The peer resets the socket once the close handshake is done: switchnet reports our FIN
/// delivered (the socket closed) and the input reset in the same poll. The peer's close came
/// first, so the reset only ends the input: `None`, and `linger` after it is `Ok`.
/// (Autobahn's fuzzingserver aborts its control connections this way.)
#[test]
fn a_reset_after_the_peer_close_is_a_clean_end() {
    let (s, log, input) = raw();
    log.borrow_mut().delivered = false;
    feed(&input, &client_frame(CLOSE, &1000u16.to_be_bytes()), false);
    let mut task = Task::spawn(s, true);
    task.turn();
    task.assert_parked();
    log.borrow_mut().delivered = true;
    end(&input, InputEnd::Reset);
    assert_eq!(task.turn(), 1);
    assert_eq!((task.got(), task.lingered.borrow().clone()), (vec![Ok(None)], Some(Ok(()))));
}

/// Our close sent, then `linger` with the peer's close not in: our FIN goes out and is
/// delivered, then the peer resets. Its close never came, so the reset is the socket's death,
/// though nothing of ours was lost.
#[test]
fn a_reset_before_the_peer_close_is_dead_even_after_our_fin() {
    let (mut s, log, input) = raw();
    assert_eq!(ready(s.close(1000, "")), Ok(()));
    assert!(poll_n(s.linger(), 2).is_none());
    assert_eq!((s.tx.end(), log.borrow().shutdowns), (crate::tx::End::Delivered, 1));
    end(&input, InputEnd::Reset);
    assert_eq!(poll_n(s.linger(), 1), Some(Err(TxError::Failed(Failure::Dead))));
}

/// The send job and a frame of input ready in the same poll: the job's frame goes out, and
/// the input frame is parsed and its message delivered once (not lost with the wait).
#[test]
fn send_job_and_input_ready_in_one_poll_are_both_handled() {
    let (mut s, log, input) = raw();
    drop_large_send(&mut s, &log, 2000, Some(0));
    let mut task = Task::spawn(s, false);
    task.turn();
    task.assert_parked();
    feed(&input, &client_frame(TEXT, b"both"), false);
    set(&log, |l| l.room = None);
    assert_eq!(task.turn(), 1);
    assert_eq!(task.got(), [msg(Kind::Text, b"both")]);
    assert_eq!(messages(Role::Server, &log), [TMessage::Binary(vec![7; 2000].into())]);
}

/// With a message in hand, recv gives queued output one pass of at most one fragment and
/// returns without waiting on it, even with no room at all. (The first recv's wait also
/// sent one: input and the send job were ready in the same poll.)
#[test]
fn a_ready_message_gets_at_most_one_fragment_of_service() {
    let (mut s, log, input) = raw();
    drop_large_send(&mut s, &log, 8000, None);
    let mut wire = client_frame(TEXT, b"a");
    wire.extend(client_frame(TEXT, b"b"));
    wire.extend(client_frame(TEXT, b"c"));
    feed(&input, &wire, false);
    assert_eq!(recv_n(&mut s, 1), Some(msg(Kind::Text, b"a")));
    assert_eq!(sent(&log).len(), 3);
    assert_eq!(recv_n(&mut s, 1), Some(msg(Kind::Text, b"b")));
    assert_eq!(sent(&log).len(), 4);
    log.borrow_mut().room = Some(0);
    assert_eq!(recv_n(&mut s, 1), Some(msg(Kind::Text, b"c")));
    assert_eq!(sent(&log).len(), 4);
}

/// Input ready on every pass (a message arriving a byte per frame) does not starve sending:
/// the send job is polled on every pass, so the owed fragments are all out before the message
/// completes.
#[test]
fn input_ready_on_every_pass_does_not_starve_sending() {
    let (mut s, log, input) = raw();
    drop_large_send(&mut s, &log, 5000, None);
    for byte in client_frame(TEXT, b"slow message") {
        feed(&input, &[byte], false);
    }
    assert_eq!(recv_n(&mut s, 1), Some(msg(Kind::Text, b"slow message")));
    assert_eq!(messages(Role::Server, &log), [TMessage::Binary(vec![7; 5000].into())]);
}

/// The peer's close, then more frames (bytes that would not even parse), then its FIN, while
/// our echo waits for room: the later frames are dropped unread, and recv ends with `None`
/// once the echo and FIN are out.
#[test]
fn frames_after_the_peer_close_are_dropped_unread() {
    let (mut s, log, input) = raw();
    log.borrow_mut().room = Some(0);
    feed(&input, &client_frame(CLOSE, &1000u16.to_be_bytes()), false);
    feed(&input, &[0xff; 40], false);
    feed(&input, &client_frame(TEXT, b"late"), false);
    end(&input, InputEnd::Fin);
    assert_eq!(recv_n(&mut s, 3), None);
    assert_eq!((input.borrow().frames.len(), input.borrow().live_frames(), s.rx_in()), (0, 0, RxIn::Eof));
    log.borrow_mut().room = None;
    assert_eq!(recv_n(&mut s, 1), Some(Ok(None)));
    assert_eq!(sent(&log), [encode(fin(CLOSE), None, &1000u16.to_be_bytes())]);
    assert_eq!(log.borrow().shutdowns, 1);
}

/// The peer's FIN while reading (no close from it) fails the connection: `PeerEof` after our
/// FIN; after its close, FIN is the normal end.
#[test]
fn raw_fin_while_reading_is_peer_eof() {
    let (mut s, log, input) = raw();
    feed(&input, &client_frame(TEXT, b"x"), false);
    end(&input, InputEnd::Fin);
    assert_eq!(recv_n(&mut s, 1), Some(msg(Kind::Text, b"x")));
    assert_eq!(recv_n(&mut s, 1), Some(Err(TxError::Failed(Failure::PeerEof))));
    assert_eq!((sent(&log).len(), log.borrow().shutdowns), (0, 1));
    let (mut s, log, input) = raw();
    feed(&input, &client_frame(CLOSE, &[]), false);
    end(&input, InputEnd::Fin);
    assert_eq!(recv_n(&mut s, 1), Some(Ok(None)));
    assert_eq!(sent(&log), [vec![0x88, 0]]);
}

/// TLS close_notify with no WebSocket close and TCP still open: `PeerEof` (with our
/// close_notify and FIN), not a wait for a FIN that may never come.
#[test]
#[cfg_attr(miri, ignore)]
fn tls_close_notify_without_a_close_is_peer_eof() {
    let (mut s, mut server, wire, input) = tls();
    server.writer().write_all(&encode(fin(TEXT), None, b"last")).unwrap();
    server.send_close_notify();
    feed(&input, &server_bytes(&mut server), false);
    assert_eq!(recv_n(&mut s, 1), Some(msg(Kind::Text, b"last")));
    assert_eq!(recv_n(&mut s, 1), Some(Err(TxError::Failed(Failure::PeerEof))));
    assert!(input.borrow().end.is_none());
    assert!(wire.shut.get(), "no FIN");
    let mut tls = &wire.sent.borrow()[..];
    while !tls.is_empty() {
        server.read_tls(&mut tls).unwrap();
    }
    assert!(server.process_new_packets().unwrap().peer_has_closed(), "no close_notify");
}

/// TCP FIN with no close_notify: the TLS stream was truncated.
#[test]
#[cfg_attr(miri, ignore)]
fn tls_fin_without_close_notify_is_truncation() {
    let (mut s, mut server, wire, input) = tls();
    feed(&input, &server_bytes(&mut server), false);
    end(&input, InputEnd::Fin);
    assert_eq!(recv_n(&mut s, 1), Some(Err(TxError::Failed(Failure::Tls(sheath::Error::Truncated)))));
    assert!(wire.shut.get(), "no FIN");
}

/// The server's close followed by more records in the same frame: receiving stops and the
/// frame is released at once (not held to the connection's drop); recv ends with `None`.
#[test]
#[cfg_attr(miri, ignore)]
fn tls_close_with_records_behind_it_releases_the_frame() {
    let (mut s, mut server, _, input) = tls();
    server.writer().write_all(&encode(fin(CLOSE), None, &1000u16.to_be_bytes())).unwrap();
    server.writer().write_all(&encode(fin(TEXT), None, b"after the close")).unwrap();
    feed(&input, &server_bytes(&mut server), false);
    assert_eq!(recv_n(&mut s, 1), Some(Ok(None)));
    assert_eq!((input.borrow().live_frames(), s.rx_in()), (0, RxIn::Stopped));
}

/// Our close, the peer's echo, our FIN delivered, while the peer's FIN is late and frames
/// still arrive: recv gives `None`; linger then reads and drops to the peer's FIN, and only
/// that ends it.
#[test]
fn linger_ends_at_the_peer_fin_not_at_our_fin_delivery() {
    let (mut s, log, input) = raw();
    assert_eq!(ready(s.close(1000, "")), Ok(()));
    feed(&input, &client_frame(CLOSE, &1000u16.to_be_bytes()), false);
    let mut task = Task::spawn(s, true);
    task.turn();
    assert_eq!(task.got(), [Ok(None)]);
    assert_eq!((log.borrow().shutdowns, task.lingered.borrow().clone()), (1, None));
    task.assert_parked();
    feed(&input, &client_frame(TEXT, b"late"), true);
    task.turn();
    assert_eq!((task.lingered.borrow().clone(), input.borrow().live_frames()), (None, 0));
    end(&input, InputEnd::Fin);
    assert_eq!(task.turn(), 1);
    assert_eq!(task.lingered.borrow().clone(), Some(Ok(())));
    assert_eq!(sent(&log).len(), 1);
}

/// linger after a protocol error: no close frame, FIN, then the peer's bytes dropped to its
/// FIN. A reset while lingering is `Failed(Dead)`.
#[test]
fn linger_after_a_failure_drains_to_the_peer_fin_and_reports_a_reset() {
    for reset in [false, true] {
        let (mut s, log, input) = raw();
        feed(&input, &encode(fin(TEXT), None, b"unmasked"), false);
        assert_eq!(recv_n(&mut s, 1), Some(Err(TxError::Failed(Failure::Protocol(crate::ProtocolError::UnmaskedFrame)))));
        feed(&input, &[0xff; 10], false);
        end(&input, if reset { InputEnd::Reset } else { InputEnd::Fin });
        let want = if reset { Err(TxError::Failed(Failure::Dead)) } else { Ok(()) };
        assert_eq!(poll_n(s.linger(), 1), Some(want), "reset {reset}");
        assert_eq!((sent(&log).len(), log.borrow().shutdowns, input.borrow().live_frames()), (0, 1, 0));
    }
}

/// A reset while linger still has to send (our FIN waits for room behind a refused frame):
/// `Failed(Dead)`, the frame discarded, no FIN.
#[test]
fn linger_reports_a_reset_before_our_fin() {
    let (mut s, log, input) = raw();
    log.borrow_mut().refuse = 1;
    assert!(poll_n(s.tx.send(Kind::Text, b"stuck"), 1).is_none());
    assert!(poll_n(s.linger(), 2).is_none());
    end(&input, InputEnd::Reset);
    assert_eq!(poll_n(s.linger(), 1), Some(Err(TxError::Failed(Failure::Dead))));
    assert_eq!((sent(&log).len(), log.borrow().shutdowns, log.borrow().live_frames()), (0, 0, 0));
}

/// recv dropped while it waits (on input, on room, on a frame, on delivery) loses nothing:
/// the next recv picks up where it stopped.
#[test]
fn recv_dropped_at_any_wait_loses_nothing() {
    for gate in ["input", "room", "alloc", "delivered"] {
        let (mut s, log, input) = raw();
        match gate {
            "room" => drop_large_send(&mut s, &log, 3000, Some(0)),
            "alloc" => {
                drop_large_send(&mut s, &log, 3000, None);
                log.borrow_mut().frames = Some(0);
            }
            "delivered" => {
                log.borrow_mut().delivered = false;
                feed(&input, &client_frame(CLOSE, &[]), false);
            }
            _ => {}
        }
        assert_eq!(recv_n(&mut s, 2), None, "{gate}");
        assert_eq!(log.borrow().waited.last().copied(), (gate != "input").then_some(gate), "{gate}");
        {
            let mut l = log.borrow_mut();
            (l.room, l.frames, l.delivered) = (None, None, true);
        }
        let want = if gate == "delivered" {
            Ok(None)
        } else {
            feed(&input, &client_frame(TEXT, b"next"), false);
            msg(Kind::Text, b"next")
        };
        assert_eq!(recv_n(&mut s, 1), Some(want), "{gate}");
        if gate == "room" || gate == "alloc" {
            assert_eq!(recv_n(&mut s, 2), None);
            assert_eq!(messages(Role::Server, &log), [tmsg(Kind::Binary, &[7; 3000])], "{gate}");
        }
    }
}
