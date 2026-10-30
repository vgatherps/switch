//! The send core's event sequences, over both lower-layer shapes (TLS and raw TCP): every
//! sequence of up to 4 events, and seeded random walks of 12. Sends go through `send` itself,
//! polled once and dropped if it pends. After every event the state invariants hold
//! (`TxCore::check_invariants`), the first failure never changes, nothing is sealed once dead,
//! and a call refused by its checks changes nothing; the mock asserts one sealed frame at a
//! time, seal order and nothing sent after FIN. At every state three probes run on a replay,
//! every gate open, within a bounded number of steps (so a core that spins fails instead of
//! hanging): the plain drain sends exactly the messages accepted, whole, in order (decoded by
//! relay's receive core); `abandon` drains to the delivered FIN in the order refused frame,
//! lower records, close_notify; a dead socket seals nothing more and ends.

use std::cell::RefCell;
use std::pin::pin;
use std::rc::Rc;

use super::tx::{CLOSE_RECORD, Log, MockTx, ROOM, mock, relay_reads, sent, state};
use super::*;
use crate::tx::{End, Failure, Job, Local, TxCore};

#[derive(Clone, Copy, Debug)]
enum Ev {
    Ping,
    PeerClose,
    QueueClose,
    FailProtocol,
    FailPeerEof,
    FailTls,
    Abandon,
    /// `send`'s acceptance of a one-frame message, once its drain left nothing.
    SendSmall,
    /// `send`'s acceptance of a three-frame message, once its drain left nothing.
    SendLarge,
    /// One poll of `step`; dropped if it pends.
    Step,
    RefuseNext,
    PendingAlert,
    KillSocket,
    RoomOpen,
}

const EVENTS: [Ev; 14] = [
    Ev::Ping,
    Ev::PeerClose,
    Ev::QueueClose,
    Ev::FailProtocol,
    Ev::FailPeerEof,
    Ev::FailTls,
    Ev::Abandon,
    Ev::SendSmall,
    Ev::SendLarge,
    Ev::Step,
    Ev::RefuseNext,
    Ev::PendingAlert,
    Ev::KillSocket,
    Ev::RoomOpen,
];

/// Steps a probe may take; every drain here needs fewer than 10.
const STEP_BOUND: usize = 64;

struct Walk<const TLS: bool> {
    c: TxCore<MockTx<TLS>>,
    log: Rc<RefCell<Log>>,
    first: Option<Failure>,
    /// Seals counted when the core died.
    seals_at_death: Option<u64>,
    /// Sends attempted so far (each payload is unique).
    attempts: u8,
    /// The messages accepted, in order.
    accepted: Vec<(Kind, Vec<u8>)>,
}

impl<const TLS: bool> Walk<TLS> {
    fn new() -> Self {
        let (c, log) = mock::<TLS>(&Config::new(Role::Server), ROOM);
        Walk { c, log, first: None, seals_at_death: None, attempts: 0, accepted: Vec::new() }
    }

    fn replay(seq: &[Ev]) -> Self {
        let mut w = Self::new();
        for (i, &ev) in seq.iter().enumerate() {
            w.apply(ev);
            w.check(&seq[..=i]);
        }
        w
    }

    /// One poll of `send`. A one-frame message is accepted once sealed (the last frame sealed
    /// is it), a longer one once copied in (the buffer holds it).
    fn send(&mut self, kind: Kind, payload: Vec<u8>) {
        let refused = self.c.check_send(kind, &payload).is_err();
        let before = state(&self.c);
        let done = poll(pin!(self.c.send(kind, &payload)));
        if refused {
            assert_eq!(state(&self.c), before, "a refused send changed the core");
            return;
        }
        let accepted = match self.c.small(payload.len()) {
            Some(_) => {
                let last = &self.log.borrow().last_sealed;
                matches!(done, Poll::Ready(Ok(()))) || last.len() > 2 && last[2..] == payload[..]
            }
            None => self.c.snapshot().buf == payload,
        };
        if accepted {
            self.accepted.push((kind, payload));
        }
    }

    fn apply(&mut self, ev: Ev) {
        match ev {
            Ev::Ping => self.c.ping(b"p"),
            Ev::PeerClose => self.c.peer_close(&[0x03, 0xe8]),
            Ev::QueueClose => {
                let before = state(&self.c);
                if self.c.queue_close(1000, "").is_err() {
                    assert_eq!(state(&self.c), before, "a refused close changed the core");
                }
            }
            Ev::FailProtocol => self.c.fail(Failure::Protocol(crate::ProtocolError::ReservedBits)),
            Ev::FailPeerEof => self.c.fail(Failure::PeerEof),
            Ev::FailTls => self.c.fail(Failure::Tls(sheath::Error::Truncated)),
            Ev::Abandon => self.c.abandon(),
            Ev::SendSmall => {
                self.attempts += 1;
                self.send(Kind::Text, format!("s{}", self.attempts).into_bytes());
            }
            Ev::SendLarge => {
                self.attempts += 1;
                self.send(Kind::Binary, vec![self.attempts; 3000]);
            }
            Ev::Step => {
                let _ = poll(pin!(self.c.step()));
            }
            Ev::RefuseNext => self.log.borrow_mut().refuse += 1,
            Ev::PendingAlert => self.log.borrow_mut().pending.push(b"alert".to_vec()),
            Ev::KillSocket => self.log.borrow_mut().dead = true,
            Ev::RoomOpen => self.log.borrow_mut().room = None,
        }
    }

    fn check(&mut self, seq: &[Ev]) {
        self.c.check_invariants();
        match &self.first {
            Some(f) => assert_eq!(self.c.failed(), Some(f), "{seq:?}: the first failure changed"),
            None => self.first = self.c.failed().cloned(),
        }
        let seals = self.log.borrow().seals;
        match self.seals_at_death {
            Some(n) => assert_eq!(seals, n, "{seq:?}: sealed after death"),
            None if self.c.end() == End::Dead => self.seals_at_death = Some(seals),
            None => {}
        }
    }

    /// Steps until no job is left; panics past the bound or if a step pends.
    fn finish_bounded(&mut self, seq: &[Ev]) {
        for _ in 0..STEP_BOUND {
            let stepped = poll(pin!(self.c.step()));
            match stepped {
                Poll::Ready(true) => self.c.check_invariants(),
                Poll::Ready(false) => return,
                Poll::Pending => panic!("{seq:?}: a step pended with every gate open"),
            }
        }
        panic!("{seq:?}: no end within {STEP_BOUND} steps");
    }
}

/// Opens every gate the socket controls.
fn open_gates(log: &Rc<RefCell<Log>>) {
    let mut log = log.borrow_mut();
    log.room = None;
    log.frames = None;
    log.refuse = 0;
}

/// The plain drain, every gate open: the messages relay's receive core decodes from what went
/// out are exactly those accepted, whole and in order, unless a failure or `abandon` (both leave
/// our close Abandoned) cut the last one short (then that one is missing). Never a protocol error, a merge or a mixed kind.
fn probe_drain<const TLS: bool>(seq: &[Ev]) {
    let mut w = Walk::<TLS>::replay(seq);
    if w.log.borrow().dead {
        return;
    }
    open_gates(&w.log);
    w.finish_bounded(seq);
    let frames: Vec<_> = sent(&w.log).into_iter().filter(|f| f != b"alert" && f != CLOSE_RECORD).collect();
    let run = relay_reads(Role::Server, &frames);
    assert!(matches!(run.end, None | Some(WsError::Closed)), "{seq:?}: {:?}", run.end);
    let got: Vec<_> = run.msgs.into_iter().map(|m| (m.kind, m.payload)).collect();
    let cut = w.c.snapshot().local == Local::Abandoned;
    let all = got == w.accepted;
    let short = cut && !w.accepted.is_empty() && got[..] == w.accepted[..w.accepted.len() - 1];
    assert!(all || short, "{seq:?}: got {:?}, accepted {:?}", kinds(&got), kinds(&w.accepted));
}

fn kinds(msgs: &[(Kind, Vec<u8>)]) -> Vec<(Kind, usize, u8)> {
    msgs.iter().map(|(k, p)| (*k, p.len(), p.last().copied().unwrap_or(0))).collect()
}

/// `abandon`, every gate open: the delivered FIN after exactly the refused frame, the lower
/// records (TLS, only while the transport is open) and close_notify (TLS, unless the
/// transport's end started or a TLS failure), in that order.
fn probe_abandon<const TLS: bool>(seq: &[Ev]) {
    let mut w = Walk::<TLS>::replay(seq);
    if w.log.borrow().dead {
        return;
    }
    open_gates(&w.log);
    let open = w.c.end() == End::Open;
    let resend = usize::from(matches!(w.c.next_job(), Job::Resend(_)));
    let alerts = if TLS && open { w.log.borrow().pending.len() } else { 0 };
    let notify = TLS && open && !matches!(w.c.failed(), Some(Failure::Tls(_)));
    let before = sent(&w.log).len();
    w.c.abandon();
    w.finish_bounded(seq);
    assert_eq!((w.c.end(), w.log.borrow().shutdowns), (End::Delivered, 1), "{seq:?}");
    let after = sent(&w.log).split_off(before);
    assert_eq!(after.len(), resend + alerts + usize::from(notify), "{seq:?}: {after:?}");
    assert!(after[resend..resend + alerts].iter().all(|r| r == b"alert"), "{seq:?}: {after:?}");
    if notify {
        assert_eq!(after.last().map(Vec::as_slice), Some(CLOSE_RECORD), "{seq:?}");
    }
}

/// The socket dies: nothing more is sealed or sent; work that needs the socket ends in Dead,
/// the transport's end without it (FIN, its delivery) completes.
fn probe_death<const TLS: bool>(seq: &[Ev]) {
    let mut w = Walk::<TLS>::replay(seq);
    w.log.borrow_mut().dead = true;
    let job = w.c.next_job();
    let end = w.c.end();
    let (seals, sent_n) = (w.log.borrow().seals, sent(&w.log).len());
    w.finish_bounded(seq);
    assert_eq!((w.log.borrow().seals, sent(&w.log).len()), (seals, sent_n), "{seq:?}: sent after death");
    let want = match job {
        Job::Resend(_) | Job::Frame(..) => End::Dead,
        Job::Fin | Job::Delivered => End::Delivered,
        Job::Idle => end,
    };
    assert_eq!(w.c.end(), want, "{seq:?} from {job:?}");
}

fn probes<const TLS: bool>(seq: &[Ev]) {
    probe_drain::<TLS>(seq);
    probe_abandon::<TLS>(seq);
    probe_death::<TLS>(seq);
}

fn visit<const TLS: bool>(seq: &mut Vec<Ev>, depth: usize) {
    probes::<TLS>(seq);
    if depth == 0 {
        return;
    }
    for ev in EVENTS {
        seq.push(ev);
        visit::<TLS>(seq, depth - 1);
        seq.pop();
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn every_sequence_of_four_events() {
    visit::<true>(&mut Vec::new(), 4);
    visit::<false>(&mut Vec::new(), 4);
}

#[test]
#[cfg_attr(miri, ignore)]
fn random_walks_of_twelve_events() {
    let mut rng = Rng(0x5eed);
    for _ in 0..20_000 {
        let seq: Vec<Ev> = (0..12).map(|_| EVENTS[rng.next() as usize % EVENTS.len()]).collect();
        probes::<true>(&seq);
        probes::<false>(&seq);
    }
}

/// The depth-3 sequence that once never ended: a failure, a refused frame, the socket's death.
#[test]
fn fail_refuse_die_ends() {
    let seq = [Ev::SendSmall, Ev::RefuseNext, Ev::SendSmall, Ev::FailProtocol, Ev::KillSocket, Ev::Step];
    let w = Walk::<true>::replay(&seq);
    assert_eq!(w.c.end(), End::Dead);
    probe_death::<true>(&seq[..4]);
}
