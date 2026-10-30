//! The send core's event sequences: every sequence of up to 4 events, and seeded random walks
//! of 12, over the mock lower layer. After every event the state invariants hold
//! (`TxCore::check_invariants`), the first failure never changes, nothing is sealed once dead,
//! and a refused call changes nothing; the mock asserts one sealed frame at a time, seal order
//! and nothing sent after FIN. At every state two liveness probes run on a replay: `abandon`
//! drains to the delivered FIN in the order refused frame, lower records, close_notify; a
//! dead socket seals nothing more and ends. Both within a bounded number of steps, so a core
//! that spins fails instead of hanging.

use std::cell::RefCell;
use std::pin::pin;
use std::rc::Rc;

use super::tx::{CLOSE_RECORD, Core, Log, ROOM, core, sent, state};
use super::*;
use crate::tx::{End, Failure, Job};

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

struct Walk {
    c: Core,
    log: Rc<RefCell<Log>>,
    first: Option<Failure>,
    /// Seals counted when the core died.
    seals_at_death: Option<u64>,
}

impl Walk {
    fn new() -> Walk {
        let (c, log) = core(Role::Server, ROOM);
        Walk { c, log, first: None, seals_at_death: None }
    }

    fn replay(seq: &[Ev]) -> Walk {
        let mut w = Walk::new();
        for (i, &ev) in seq.iter().enumerate() {
            w.apply(ev);
            w.check(&seq[..=i]);
        }
        w
    }

    fn send(&mut self, payload: &[u8]) {
        let before = state(&self.c);
        if self.c.check_send(Kind::Binary, payload).is_err() || self.c.next_job() != Job::Idle {
            assert_eq!(state(&self.c), before);
            return;
        }
        match self.c.small(payload.len()) {
            Some(_) if self.log.borrow().dead => self.c.die(),
            Some(_) => {
                let f = ready(self.c.lower_mut().alloc()).expect("alive");
                self.c.accept_small(Kind::Binary, payload, f);
            }
            None => self.c.accept_large(Kind::Binary, payload),
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
            Ev::SendSmall => self.send(b"small"),
            Ev::SendLarge => self.send(&[9; 3000]),
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

/// `abandon`, every gate open: the delivered FIN after exactly the refused frame, the lower
/// records (only while the transport is open) and close_notify (unless the transport's end
/// started or a TLS failure), in that order.
fn probe_abandon(seq: &[Ev]) {
    let mut w = Walk::replay(seq);
    if w.log.borrow().dead {
        return;
    }
    {
        let mut log = w.log.borrow_mut();
        log.room = None;
        log.refuse = 0;
    }
    let open = w.c.end() == End::Open;
    let resend = usize::from(matches!(w.c.next_job(), Job::Resend(_)));
    let alerts = if open { w.log.borrow().pending.len() } else { 0 };
    let notify = open && !matches!(w.c.failed(), Some(Failure::Tls(_)));
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
fn probe_death(seq: &[Ev]) {
    let mut w = Walk::replay(seq);
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

fn visit(seq: &mut Vec<Ev>, depth: usize) {
    Walk::replay(seq);
    probe_abandon(seq);
    probe_death(seq);
    if depth == 0 {
        return;
    }
    for ev in EVENTS {
        seq.push(ev);
        visit(seq, depth - 1);
        seq.pop();
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn every_sequence_of_four_events() {
    visit(&mut Vec::new(), 4);
}

#[test]
#[cfg_attr(miri, ignore)]
fn random_walks_of_twelve_events() {
    let mut rng = Rng(0x5eed);
    for _ in 0..20_000 {
        let seq: Vec<Ev> = (0..12).map(|_| EVENTS[rng.next() as usize % EVENTS.len()]).collect();
        Walk::replay(&seq);
        probe_abandon(&seq);
        probe_death(&seq);
    }
}

/// The depth-3 sequence that once never ended: a failure, a refused frame, the socket's death.
#[test]
fn fail_refuse_die_ends() {
    let seq = [Ev::SendSmall, Ev::RefuseNext, Ev::FailProtocol, Ev::KillSocket, Ev::Step];
    let w = Walk::replay(&seq);
    assert_eq!(w.c.end(), End::Dead);
    probe_death(&seq[..3]);
}
