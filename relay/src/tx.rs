//! Send core. All protocol state lives in [`TxCore`] and changes only in its synchronous
//! methods; its async methods only await the lower layer's waits (`room`, `alloc`,
//! `delivered`) and then call those.
//!
//! A message is **accepted** at one synchronous step, and from then on it goes out whole and
//! alone: a message that fits one frame when its frame is sealed, a longer one when it is
//! copied into the core's buffer, whose fragments then go out from an offset. Dropping a send
//! before acceptance sends nothing of its message; after, the next send, close or receive
//! finishes it first. No other message's bytes ever enter it.
//!
//! Each frame is emitted as: wait for room, wait for a frame, then (sync) header, payload,
//! mask, seal, `try_send`. A sealed frame TCP refuses waits in a one-frame slot and goes out
//! before anything else, so a sealed frame (and its TLS sequence number) is never lost.

// A new variant must be decided at every match.
#![warn(clippy::wildcard_enum_match_arm)]

use rand::Rng;
use rand::rngs::StdRng;

use sheath::TxFrame;

use crate::lower::PlainTx;
use crate::rx::{Config, Control, Kind, ProtocolError, Role, close_sendable, grow};

const CONT: u8 = 0;
const TEXT: u8 = 1;
const BINARY: u8 = 2;
const CLOSE: u8 = 8;
const PONG: u8 = 10;
const FIN: u8 = 0x80;

/// Why a connection failed. The first failure is kept and reported by every later call.
#[derive(Clone, Debug, PartialEq)]
pub enum Failure {
    /// The peer broke the WebSocket protocol; no close frame is sent.
    Protocol(ProtocolError),
    /// The peer ended the stream before its WebSocket close.
    PeerEof,
    /// A TLS record from the peer failed; its alert is sent, no close_notify.
    Tls(sheath::Error),
    /// The socket closed or reset: nothing more is sent.
    Dead,
}

/// Why `send` or `close` refused; nothing changed.
#[derive(Debug, PartialEq)]
pub(crate) enum TxError {
    /// A close is queued or sent (RFC 6455 §5.5.1).
    Closed,
    Failed(Failure),
    TooBig,
    InvalidText,
    InvalidClose,
}

/// Header bytes for a `len`-byte payload, minimal length encoding (RFC 6455 §5.2).
fn header_len(len: usize, masked: bool) -> usize {
    let ext = if len <= 125 {
        0
    } else if len <= 0xffff {
        2
    } else {
        8
    };
    2 + ext + if masked { 4 } else { 0 }
}

/// Bytes of the largest control frame `role` sends: 125 B of payload and its header (masked as
/// a client). Every frame room must carry it.
pub(crate) fn max_control_frame(role: Role) -> usize {
    header_len(125, role == Role::Client) + 125
}

/// The most payload one frame of `room` bytes carries.
fn frame_cap(room: usize, masked: bool) -> usize {
    let fit = |max| room.saturating_sub(header_len(max, masked)).min(max);
    fit(125).max(fit(0xffff)).max(fit(usize::MAX))
}

/// Writes one frame (first byte `b0`, `payload`) at `room[..]`, masked with `mask` if a client.
/// `room` is exactly `header_len(payload.len(), masked) + payload.len()` bytes.
fn write_frame(room: &mut [u8], b0: u8, payload: &[u8], mask: Option<[u8; 4]>) {
    let len = payload.len();
    let h = header_len(len, mask.is_some());
    let m = if mask.is_some() { 0x80 } else { 0 };
    room[0] = b0;
    let key_at = if len <= 125 {
        room[1] = m | len as u8;
        2
    } else if len <= 0xffff {
        room[1] = m | 126;
        room[2..4].copy_from_slice(&(len as u16).to_be_bytes());
        4
    } else {
        room[1] = m | 127;
        room[2..10].copy_from_slice(&(len as u64).to_be_bytes());
        10
    };
    let body = &mut room[h..h + len];
    body.copy_from_slice(payload);
    if let Some(k) = mask {
        room[key_at..key_at + 4].copy_from_slice(&k);
        for (i, b) in room[h..h + len].iter_mut().enumerate() {
            *b ^= k[i & 3];
        }
    }
}

/// The data message being sent from `buf`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Out {
    Idle,
    /// `buf[offset..]` is still to go; offset > 0 means the next fragment is a continuation.
    Large { kind: Kind, offset: usize },
}

/// Our close frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Local {
    Open,
    /// To send once the message in `buf` is out.
    Queued(Control),
    /// Sealed (it may still wait in `unsent`).
    Sent,
    /// No further close frame will be sent (a failure or `abandon`); one already sealed may
    /// still wait in `unsent`.
    Abandoned,
}

/// The peer's close frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Peer {
    Open,
    Closed,
}

/// The transport's end: close_notify (TLS), FIN, its delivery.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum End {
    Open,
    /// close_notify sealed (it may still wait in `unsent`).
    Notify,
    /// FIN sent; waiting for its delivery.
    Fin,
    Delivered,
    /// The socket died: nothing is sent any more.
    Dead,
}

/// What the shell waits for next, in this order: the refused frame, the lower layer's own
/// records, a pong, the next fragment, our close, the end of the transport.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Job {
    Idle,
    /// `room(len)`, then [`TxCore::resend`].
    Resend(usize),
    /// `room(len)`, then a frame, then [`TxCore::emit`] it.
    Frame(usize, Work),
    /// [`TxCore::fin`] (no wait).
    Fin,
    /// `delivered`, then [`TxCore::delivered`].
    Delivered,
}

/// What a `Job::Frame` carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Work {
    Pending,
    Pong,
    Fragment,
    Close,
    Notify,
}

/// [`TxCore::snapshot`].
#[cfg(test)]
#[derive(Debug, PartialEq)]
pub(crate) struct Snapshot {
    job: Job,
    unsent: Option<usize>,
    out: Out,
    pub(crate) buf: Vec<u8>,
    pong: Option<Control>,
    pub(crate) local: Local,
    peer: Peer,
    end: End,
    failed: Option<Failure>,
}

/// The send core over lower layer `L`.
pub(crate) struct TxCore<L: PlainTx> {
    lower: L,
    /// Mask source; `Some` iff client role.
    rng: Option<StdRng>,
    /// The largest message `send` accepts.
    max_send: usize,
    /// A sealed frame TCP refused, and the room its job waited for (lower records and
    /// close_notify wait for a whole frame's room; their length is not known before sealing).
    unsent: Option<(L::Sealed, usize)>,
    out: Out,
    buf: Vec<u8>,
    /// The newest ping's payload, owed as a pong.
    pong: Option<Control>,
    local: Local,
    peer: Peer,
    end: End,
    failed: Option<Failure>,
}

impl<L: PlainTx> TxCore<L> {
    /// Panics if one frame cannot carry the largest control frame (setup checks this).
    pub(crate) fn new(lower: L, config: &Config) -> TxCore<L> {
        let role = config.role;
        assert!(lower.plain().1 >= max_control_frame(role), "frame room below a control frame");
        TxCore {
            lower,
            rng: (role == Role::Client).then(rand::make_rng),
            max_send: config.max_send,
            unsent: None,
            out: Out::Idle,
            buf: Vec::new(),
            pong: None,
            local: Local::Open,
            peer: Peer::Open,
            end: End::Open,
            failed: None,
        }
    }

    /// The receive path decrypts through the TLS connection this owns.
    pub(crate) fn lower_mut(&mut self) -> &mut L {
        &mut self.lower
    }

    pub(crate) fn failed(&self) -> Option<&Failure> {
        self.failed.as_ref()
    }

    pub(crate) fn end(&self) -> End {
        self.end
    }

    fn masked(&self) -> bool {
        self.rng.is_some()
    }

    fn mask(&mut self) -> Option<[u8; 4]> {
        self.rng.as_mut().map(|r| r.next_u32().to_ne_bytes())
    }

    fn room(&self) -> usize {
        self.lower.plain().1
    }

    fn control_len(&self, p: &Control) -> usize {
        header_len(p.bytes().len(), self.masked()) + p.bytes().len()
    }

    /// The next fragment's payload length.
    fn fragment_len(&self, offset: usize) -> usize {
        (self.buf.len() - offset).min(frame_cap(self.room(), self.masked()))
    }

    /// Our close is out, or we will send none: the transport may end once the peer's close
    /// came too (or at once when abandoned).
    fn ending(&self) -> bool {
        match self.local {
            Local::Abandoned => true,
            Local::Sent => self.peer == Peer::Closed,
            Local::Open | Local::Queued(_) => false,
        }
    }

    /// The transport ends with close_notify: TLS, unless a TLS failure (its alert ends it).
    fn notify(&self) -> bool {
        L::HAS_CLOSE_RECORD
            && match &self.failed {
                Some(Failure::Tls(_)) => false,
                None | Some(Failure::Protocol(_) | Failure::PeerEof | Failure::Dead) => true,
            }
    }

    pub(crate) fn next_job(&self) -> Job {
        // Job 0: once dead nothing is sent, whatever is still stored.
        if self.end == End::Dead {
            return Job::Idle;
        }
        if let Some((_, len)) = &self.unsent {
            return Job::Resend(*len);
        }
        // Records the lower layer queues once close_notify is sealed are never sent.
        if self.end == End::Open && self.lower.has_pending() {
            return Job::Frame(self.room(), Work::Pending);
        }
        if let Some(p) = &self.pong {
            return Job::Frame(self.control_len(p), Work::Pong);
        }
        if let Out::Large { offset, .. } = self.out {
            let n = self.fragment_len(offset);
            return Job::Frame(header_len(n, self.masked()) + n, Work::Fragment);
        }
        if let Local::Queued(c) = &self.local {
            return Job::Frame(self.control_len(c), Work::Close);
        }
        match self.end {
            End::Open if self.ending() => {
                if self.notify() {
                    Job::Frame(self.room(), Work::Notify)
                } else {
                    Job::Fin
                }
            }
            End::Open | End::Delivered | End::Dead => Job::Idle,
            End::Notify => Job::Fin,
            End::Fin => Job::Delivered,
        }
    }

    /// Hands `s` (carrying `len` plaintext bytes) to TCP, or keeps it for `resend`.
    fn put(&mut self, s: L::Sealed, len: usize) {
        if let Err(s) = self.lower.try_send(s) {
            self.unsent = Some((s, len));
        }
    }

    /// After `room` for the refused frame: send it. Refused again only if the socket died,
    /// which the next `room` reports.
    pub(crate) fn resend(&mut self) {
        if let Some((s, len)) = self.unsent.take() {
            self.put(s, len);
        }
    }

    /// Seals `len` bytes of `f` and sends them; a seal error ends the connection (a TLS
    /// sender refusing after a failure, or a record lost below).
    fn seal_put(&mut self, f: L::Frame, len: usize) {
        match self.lower.seal(f, len) {
            Ok(s) => self.put(s, len),
            Err(_) => self.die(),
        }
    }

    /// Writes one frame into `f` at the lower layer's head and sends it.
    fn frame(&mut self, mut f: L::Frame, b0: u8, payload: &[u8]) {
        let head = self.lower.plain().0;
        let len = header_len(payload.len(), self.masked()) + payload.len();
        let mask = self.mask();
        write_frame(&mut f.room_mut()[head..head + len], b0, payload, mask);
        self.seal_put(f, len);
    }

    /// After `room` and a frame for the `Job::Frame` that `next_job` returned (nothing changed
    /// since): fills, seals and sends it.
    pub(crate) fn emit(&mut self, mut f: L::Frame) {
        let job = self.next_job();
        let Job::Frame(len, work) = job else {
            debug_assert!(false, "emit for {job:?}: the shell raced the core");
            return;
        };
        match work {
            Work::Pending | Work::Notify => {
                let sealed = if work == Work::Pending { self.lower.take_pending(f) } else { self.lower.seal_close(f) };
                match sealed {
                    Ok(Some(s)) => self.put(s, len),
                    Ok(None) => {}
                    Err(_) => return self.die(),
                }
                if work == Work::Notify {
                    self.end = End::Notify;
                }
            }
            Work::Pong => {
                let p = self.pong.take().expect("a pong job");
                self.frame(f, FIN | PONG, p.bytes());
            }
            Work::Close => {
                let Local::Queued(c) = self.local else { unreachable!("a close job") };
                self.local = Local::Sent;
                self.frame(f, FIN | CLOSE, c.bytes());
            }
            Work::Fragment => {
                let Out::Large { kind, offset } = self.out else { unreachable!("a fragment job") };
                let n = self.fragment_len(offset);
                let end = offset + n;
                let op = if offset > 0 {
                    CONT
                } else {
                    match kind {
                        Kind::Text => TEXT,
                        Kind::Binary => BINARY,
                    }
                };
                let b0 = if end == self.buf.len() { FIN | op } else { op };
                self.out = if end == self.buf.len() { Out::Idle } else { Out::Large { kind, offset: end } };
                let head = self.lower.plain().0;
                let h = header_len(n, self.masked());
                let mask = self.mask();
                write_frame(&mut f.room_mut()[head..head + h + n], b0, &self.buf[offset..end], mask);
                self.seal_put(f, h + n);
            }
        }
    }

    /// FIN, with nothing left to send before it.
    pub(crate) fn fin(&mut self) {
        self.lower.shutdown();
        self.end = End::Fin;
    }

    /// Our FIN (or the connection's end) reached the peer.
    pub(crate) fn delivered(&mut self) {
        if self.end == End::Fin {
            self.end = End::Delivered;
        }
    }

    /// The newest ping's payload, owed as a pong while we may still send.
    pub(crate) fn ping(&mut self, p: &[u8]) {
        if self.local == Local::Open {
            self.pong = Some(Control::new(p));
        }
    }

    /// The peer's close arrived: echo `echo` unless our own close is queued or out.
    pub(crate) fn peer_close(&mut self, echo: &[u8]) {
        self.peer = Peer::Closed;
        if self.local == Local::Open {
            self.local = Local::Queued(Control::new(echo));
        }
    }

    /// Send no further close frame and drop what is not sealed yet; then only the frame
    /// already sealed, the lower layer's records and the transport's end go out.
    pub(crate) fn abandon(&mut self) {
        self.out = Out::Idle;
        self.pong = None;
        self.local = Local::Abandoned;
    }

    /// The connection failed (the first failure is kept): abandon, then the drain runs.
    pub(crate) fn fail(&mut self, f: Failure) {
        self.failed.get_or_insert(f);
        self.abandon();
    }

    /// The socket died: whatever failed first, nothing more is sent.
    pub(crate) fn die(&mut self) {
        self.failed.get_or_insert(Failure::Dead);
        self.unsent = None;
        self.abandon();
        self.end = End::Dead;
    }

    fn refuse(&self) -> Result<(), TxError> {
        if let Some(f) = &self.failed {
            return Err(TxError::Failed(f.clone()));
        }
        if self.local != Local::Open {
            return Err(TxError::Closed);
        }
        Ok(())
    }

    /// `send`'s checks, before anything changes.
    pub(crate) fn check_send(&self, kind: Kind, payload: &[u8]) -> Result<(), TxError> {
        self.refuse()?;
        if payload.len() > self.max_send {
            return Err(TxError::TooBig);
        }
        if kind == Kind::Text && core::str::from_utf8(payload).is_err() {
            return Err(TxError::InvalidText);
        }
        Ok(())
    }

    /// The room a message of `len` bytes needs if it fits one frame.
    pub(crate) fn small(&self, len: usize) -> Option<usize> {
        (len <= frame_cap(self.room(), self.masked())).then(|| header_len(len, self.masked()) + len)
    }

    /// Accepts a message that fits one frame, into `f` (after its room): sealed and sent now.
    pub(crate) fn accept_small(&mut self, kind: Kind, payload: &[u8], f: L::Frame) {
        debug_assert!(self.next_job() == Job::Idle && self.small(payload.len()).is_some());
        let op = if kind == Kind::Text { TEXT } else { BINARY };
        self.frame(f, FIN | op, payload);
    }

    /// Accepts a message longer than one frame: copied, then sent a fragment per frame.
    pub(crate) fn accept_large(&mut self, kind: Kind, payload: &[u8]) {
        debug_assert!(self.next_job() == Job::Idle && self.small(payload.len()).is_none());
        self.buf.clear();
        grow(&mut self.buf, payload.len(), self.max_send);
        self.buf.extend_from_slice(payload);
        self.out = Out::Large { kind, offset: 0 };
    }

    #[cfg(test)]
    pub(crate) fn buf_capacity(&self) -> usize {
        self.buf.capacity()
    }

    /// The accepted message is out: all of it sealed and taken by TCP.
    pub(crate) fn sent(&self) -> bool {
        self.out == Out::Idle && self.unsent.is_none()
    }

    /// Queues our close (`code`, `reason`); a second call while ours is queued or out does
    /// nothing.
    pub(crate) fn queue_close(&mut self, code: u16, reason: &str) -> Result<(), TxError> {
        if let Some(f) = &self.failed {
            return Err(TxError::Failed(f.clone()));
        }
        if !close_sendable(code, reason) {
            return Err(TxError::InvalidClose);
        }
        match self.local {
            Local::Open => {
                let mut p = [0; 125];
                p[..2].copy_from_slice(&code.to_be_bytes());
                p[2..2 + reason.len()].copy_from_slice(reason.as_bytes());
                self.local = Local::Queued(Control::new(&p[..2 + reason.len()]));
                Ok(())
            }
            Local::Abandoned => Err(TxError::Closed),
            Local::Queued(_) | Local::Sent => Ok(()),
        }
    }

    /// Our close is sealed and taken by TCP.
    pub(crate) fn close_sent(&self) -> bool {
        self.local == Local::Sent && self.unsent.is_none()
    }

    /// Jobs that go out before `recv` returns a message it has: the refused frame, lower
    /// records, a pong, our close. (The rest of a large message goes out before `recv` waits
    /// for input, see `backlog`.)
    pub(crate) fn urgent(&self) -> bool {
        match self.next_job() {
            Job::Resend(_) | Job::Frame(_, Work::Pending | Work::Pong | Work::Close) => true,
            Job::Idle | Job::Frame(_, Work::Fragment | Work::Notify) | Job::Fin | Job::Delivered => false,
        }
    }

    /// Work that goes out before `recv` waits for input: every job but waiting for our FIN's
    /// delivery (which needs the peer). Without it an accepted message's rest, and a Close
    /// queued behind it, would wait for input the peer sends only after them.
    pub(crate) fn backlog(&self) -> bool {
        match self.next_job() {
            Job::Resend(_) | Job::Frame(..) | Job::Fin => true,
            Job::Idle | Job::Delivered => false,
        }
    }

    /// The first failure, if any.
    pub(crate) fn result(&self) -> Result<(), TxError> {
        match &self.failed {
            Some(f) => Err(TxError::Failed(f.clone())),
            None => Ok(()),
        }
    }

    /// All the state a refused call must leave as it was.
    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> Snapshot {
        Snapshot {
            job: self.next_job(),
            unsent: self.unsent.as_ref().map(|(_, len)| *len),
            out: self.out,
            buf: self.buf.clone(),
            pong: self.pong,
            local: self.local,
            peer: self.peer,
            end: self.end,
            failed: self.failed.clone(),
        }
    }

    /// The state invariants; the sequence walk checks them after every event.
    #[cfg(test)]
    pub(crate) fn check_invariants(&self) {
        let job = self.next_job();
        // A refused frame goes out before anything else.
        if self.unsent.is_some() {
            assert!(matches!(job, Job::Resend(_)), "unsent but {job:?}");
        }
        if let Out::Large { offset, .. } = self.out {
            assert!(offset < self.buf.len(), "offset {offset} of {}", self.buf.len());
        }
        assert!(self.buf.capacity() <= self.max_send, "buffer {} over {}", self.buf.capacity(), self.max_send);
        if self.peer == Peer::Closed {
            assert_ne!(self.local, Local::Open, "the peer's close neither echoed nor answered");
        }
        // Open: nothing ended, so `send`'s first drive never reaches a Close or the end.
        if self.local == Local::Open {
            assert_eq!((self.end, &self.failed), (End::Open, &None));
            assert!(
                matches!(job, Job::Idle | Job::Resend(_) | Job::Frame(_, Work::Pending | Work::Pong | Work::Fragment)),
                "open but {job:?}"
            );
        }
        if self.failed.is_some() {
            assert_eq!(self.local, Local::Abandoned);
        }
        if self.local == Local::Abandoned {
            assert_eq!((self.out, &self.pong), (Out::Idle, &None));
        }
        match self.end {
            End::Open => {}
            End::Notify | End::Fin | End::Delivered => {
                let closed = self.local == Local::Sent && self.peer == Peer::Closed && self.out == Out::Idle;
                assert!(closed && self.pong.is_none() || self.local == Local::Abandoned, "ended at {:?} with {:?}", self.end, (self.local, self.peer, self.out, self.pong.is_some()));
                if self.end != End::Notify {
                    assert!(self.unsent.is_none(), "{:?} with a frame unsent", self.end);
                }
            }
            End::Dead => {
                assert!(self.unsent.is_none() && job == Job::Idle && self.failed.is_some());
            }
        }
    }
}

// The shell: each method awaits the lower layer and calls the core; none changes state itself.
impl<L: PlainTx> TxCore<L> {
    /// One job of `next_job`; `false` when there is none.
    pub(crate) async fn step(&mut self) -> bool {
        match self.next_job() {
            Job::Idle => return false,
            Job::Resend(len) => match self.lower.room(len).await {
                Ok(()) => self.resend(),
                Err(_) => self.die(),
            },
            Job::Frame(len, _) => match self.frame_after_room(len).await {
                Some(f) => self.emit(f),
                None => self.die(),
            },
            Job::Fin => self.fin(),
            Job::Delivered => {
                self.lower.delivered().await;
                self.delivered();
            }
        }
        true
    }

    /// Room for `len` bytes, then a frame; `None` if the socket died.
    async fn frame_after_room(&mut self, len: usize) -> Option<L::Frame> {
        self.lower.room(len).await.ok()?;
        self.lower.alloc().await.ok()
    }

    /// Steps while `more` holds and there is work.
    pub(crate) async fn drive(&mut self, more: impl Fn(&Self) -> bool) {
        while more(self) && self.step().await {}
    }

    /// Sends `payload` as one message: first what earlier calls left (their message's rest
    /// included), then this one, accepted whole, then out. Dropped before acceptance, nothing
    /// of it was sent; after, the rest goes out with the next call.
    pub(crate) async fn send(&mut self, kind: Kind, payload: &[u8]) -> Result<(), TxError> {
        self.check_send(kind, payload)?;
        self.drive(|c| c.next_job() != Job::Idle).await;
        // The drain changes only the failure state (Local stays Open while open).
        self.result()?;
        match self.small(payload.len()) {
            Some(len) => match self.frame_after_room(len).await {
                Some(f) => self.accept_small(kind, payload, f),
                None => self.die(),
            },
            None => self.accept_large(kind, payload),
        }
        self.drive(|c| !c.sent()).await;
        self.result()
    }

    /// Queues our close and sends it once the message in progress is out.
    pub(crate) async fn close(&mut self, code: u16, reason: &str) -> Result<(), TxError> {
        self.queue_close(code, reason)?;
        self.drive(|c| !c.close_sent()).await;
        self.result()
    }

    /// Sends the jobs that cannot wait (see `urgent`).
    pub(crate) async fn flush(&mut self) {
        self.drive(Self::urgent).await;
    }

    /// Sends everything that does not need the peer (see `backlog`).
    pub(crate) async fn drain(&mut self) {
        self.drive(Self::backlog).await;
    }

    /// Everything left, through the transport's end, as far as it goes without the peer.
    pub(crate) async fn finish(&mut self) {
        self.drive(|_| true).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every frame room a core takes (from the largest control frame to 70000 B), both roles:
    /// `frame_cap` is the largest payload whose minimally encoded frame fits the room.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn frame_cap_is_the_largest_payload_that_fits() {
        for role in [Role::Server, Role::Client] {
            let masked = role == Role::Client;
            for room in max_control_frame(role)..=70_000 {
                let cap = frame_cap(room, masked);
                assert!(header_len(cap, masked) + cap <= room, "{role:?} room {room} cap {cap}");
                assert!(header_len(cap + 1, masked) + cap + 1 > room, "{role:?} room {room}: {cap} is not the most");
            }
        }
    }

    /// `write_frame` stays inside a buffer of exactly the frame's length: each length class's
    /// edges, and the cap of the rooms around each class edge.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn write_frame_fills_exactly_the_frame() {
        let rooms = (127..=140).chain(65_530..=65_550);
        for masked in [false, true] {
            let mask = masked.then_some([1, 2, 3, 4]);
            let caps = rooms.clone().filter(|&r| r >= max_control_frame(Role::Client) || !masked).map(|r| frame_cap(r, masked));
            for len in [0, 1, 124, 125, 126, 127, 0xffff, 0x1_0000].into_iter().chain(caps) {
                let payload = vec![0x5a; len];
                let mut frame = vec![0; header_len(len, masked) + len];
                write_frame(&mut frame, FIN | BINARY, &payload, mask);
                let body = &frame[header_len(len, masked)..];
                let unmasked: Vec<u8> = body.iter().enumerate().map(|(i, b)| b ^ mask.map_or(0, |k| k[i & 3])).collect();
                assert_eq!(unmasked, payload, "masked {masked} len {len}");
            }
        }
    }
}
