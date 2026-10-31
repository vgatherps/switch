use std::convert::Infallible;
use std::ops::Range;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

pub(crate) use sheath::{FrameTx, RxFrame, TxFrame};

pub(crate) use crate::lower::PlainTx;
pub(crate) use crate::raw::Raw;
pub(crate) use crate::rx::{Config, Kind, Limits, Role, WsError, WsRx};
pub(crate) use crate::tx::TxCore;

mod differential;
mod peer;
mod rules;
mod session;
mod split;
mod tx;
mod tx_walk;
mod wss;

/// The wakers of a mock's pending waits. Like switchnet's futures, a wait unregisters when it
/// is dropped (an [`OnDrop`] calling `remove`), so a caller that drops and rebuilds its waits
/// across pending polls is never woken.
#[derive(Default)]
pub struct Wakers {
    next: u64,
    waiting: Vec<(u64, Waker)>,
}

impl Wakers {
    /// A fresh wait's id.
    pub fn id(&mut self) -> u64 {
        self.next += 1;
        self.next
    }

    pub fn register(&mut self, id: u64, waker: &Waker) {
        self.remove(id);
        self.waiting.push((id, waker.clone()));
    }

    pub fn remove(&mut self, id: u64) {
        self.waiting.retain(|(i, _)| *i != id);
    }

    /// Every registered waker, unregistered.
    pub fn take(&mut self) -> Vec<Waker> {
        std::mem::take(&mut self.waiting).into_iter().map(|(_, w)| w).collect()
    }
}

/// Runs its closure when dropped.
pub struct OnDrop<F: FnMut()>(pub F);

impl<F: FnMut()> Drop for OnDrop<F> {
    fn drop(&mut self) {
        (self.0)()
    }
}

pub struct VecFrame {
    pub data: Vec<u8>,
    pub stamp: u64,
}

impl RxFrame for VecFrame {
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

pub type Rx = WsRx<Raw<VecFrame>>;
pub type Error = WsError<Infallible>;

pub fn rx(role: Role) -> Rx {
    WsRx::new(Raw::new(), Config::new(role, Limits::default()))
}

pub const TEXT: u8 = 1;
pub const BINARY: u8 = 2;
pub const CONT: u8 = 0;
pub const CLOSE: u8 = 8;
pub const PING: u8 = 9;
pub const PONG: u8 = 10;

/// One frame on the wire: `b0` is fin | rsv | opcode, minimal length encoding.
pub fn encode(b0: u8, mask: Option<[u8; 4]>, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![b0];
    let m = if mask.is_some() { 0x80 } else { 0 };
    match payload.len() {
        n @ 0..=125 => out.push(m | n as u8),
        n @ 126..=0xffff => {
            out.push(m | 126);
            out.extend_from_slice(&(n as u16).to_be_bytes());
        }
        n => {
            out.push(m | 127);
            out.extend_from_slice(&(n as u64).to_be_bytes());
        }
    }
    match mask {
        Some(k) => {
            out.extend_from_slice(&k);
            out.extend(payload.iter().enumerate().map(|(i, b)| b ^ k[i % 4]));
        }
        None => out.extend_from_slice(payload),
    }
    out
}

pub fn fin(op: u8) -> u8 {
    0x80 | op
}

pub fn kind_op(kind: Kind) -> u8 {
    match kind {
        Kind::Text => TEXT,
        Kind::Binary => BINARY,
    }
}

/// A message the stream should yield.
#[derive(Clone, Debug)]
pub struct Expect {
    pub kind: Kind,
    pub payload: Vec<u8>,
    /// Stream offset of the first header byte.
    pub first: usize,
    /// Payload range in the stream for an unfragmented message.
    pub single: Option<Range<usize>>,
}

/// A wire stream plus the messages it carries. Masked iff `role` is the
/// receiving server; mask keys vary per frame.
pub struct Stream {
    pub role: Role,
    pub bytes: Vec<u8>,
    pub msgs: Vec<Expect>,
    pub last_ping: Option<Vec<u8>>,
    open: Option<Expect>,
    key: u32,
}

impl Stream {
    pub fn new(role: Role) -> Stream {
        Stream { role, bytes: Vec::new(), msgs: Vec::new(), last_ping: None, open: None, key: 0x9e37_79b9 }
    }

    fn put(&mut self, b0: u8, payload: &[u8]) -> Range<usize> {
        let mask = (self.role == Role::Server).then(|| {
            self.key = self.key.wrapping_mul(0x0101_0101).wrapping_add(0x1357_9bdf);
            self.key.to_be_bytes()
        });
        let f = encode(b0, mask, payload);
        let end = self.bytes.len() + f.len();
        self.bytes.extend_from_slice(&f);
        end - payload.len()..end
    }

    pub fn msg(&mut self, kind: Kind, payload: &[u8]) -> &mut Self {
        let first = self.bytes.len();
        let r = self.put(fin(kind_op(kind)), payload);
        self.msgs.push(Expect { kind, payload: payload.to_vec(), first, single: Some(r) });
        self
    }

    pub fn begin(&mut self, kind: Kind, part: &[u8]) -> &mut Self {
        let first = self.bytes.len();
        self.put(kind_op(kind), part);
        self.open = Some(Expect { kind, payload: part.to_vec(), first, single: None });
        self
    }

    pub fn cont(&mut self, part: &[u8], last: bool) -> &mut Self {
        self.put(if last { fin(CONT) } else { CONT }, part);
        let mut m = self.open.take().expect("cont without begin");
        m.payload.extend_from_slice(part);
        if last {
            self.msgs.push(m);
        } else {
            self.open = Some(m);
        }
        self
    }

    pub fn ping(&mut self, payload: &[u8]) -> &mut Self {
        self.put(fin(PING), payload);
        self.last_ping = Some(payload.to_vec());
        self
    }

    pub fn pong(&mut self, payload: &[u8]) -> &mut Self {
        self.put(fin(PONG), payload);
        self
    }
}

/// A message as received.
#[derive(Debug, PartialEq, Eq)]
pub struct Got {
    pub kind: Kind,
    pub payload: Vec<u8>,
    /// The payload slice pointed into a pushed frame's buffer.
    pub direct: bool,
    pub stamp: u64,
}

pub struct Run {
    pub msgs: Vec<Got>,
    pub end: Option<Error>,
    pub pong: Option<Vec<u8>>,
    /// Chunk index after whose push `end` was returned.
    pub end_chunk: Option<usize>,
}

/// Stamp of chunk `i`.
pub fn stamp(i: usize) -> u64 {
    1000 + i as u64
}

/// Feed `chunks` as frames stamped `stamp(i)`, draining messages after each push.
pub fn drive(rx: &mut Rx, chunks: &[&[u8]]) -> Run {
    let mut msgs = Vec::new();
    for (i, c) in chunks.iter().enumerate() {
        let f = VecFrame { data: c.to_vec(), stamp: stamp(i) };
        let p = f.data.as_ptr() as usize;
        // Raw holds only the frame pushed last, so a direct message is in it (an earlier
        // frame's freed memory may hold the scratch since)
        let frame: Range<usize> = p..p + f.data.len();
        rx.push(f);
        loop {
            match rx.recv(&mut ()) {
                Ok(Some(m)) => {
                    let b = m.bytes();
                    let at = b.as_ptr() as usize;
                    let direct = !b.is_empty() && frame.contains(&at);
                    if m.kind() == Kind::Text {
                        assert_eq!(m.text().unwrap().as_bytes(), b);
                    } else {
                        assert!(m.text().is_none());
                    }
                    msgs.push(Got { kind: m.kind(), payload: b.to_vec(), direct, stamp: m.rx_timestamp() });
                }
                Ok(None) => break,
                Err(e) => {
                    let pong = rx.take_pong().map(<[u8]>::to_vec);
                    return Run { msgs, end: Some(e), pong, end_chunk: Some(i) };
                }
            }
        }
    }
    Run { msgs, end: None, pong: rx.take_pong().map(<[u8]>::to_vec), end_chunk: None }
}

/// Split `bytes` at sorted `cuts`.
pub fn split<'a>(bytes: &'a [u8], cuts: &[usize]) -> Vec<&'a [u8]> {
    let mut out = Vec::new();
    let mut at = 0;
    for &c in cuts.iter().chain(std::iter::once(&bytes.len())) {
        out.push(&bytes[at..c]);
        at = c;
    }
    out.retain(|c| !c.is_empty());
    out
}

pub struct Rng(pub u64);

impl Rng {
    pub fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    pub fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    pub fn bytes(&mut self, n: usize) -> Vec<u8> {
        (0..n).map(|_| self.next() as u8).collect()
    }
}

/// Random sorted cuts of `len` bytes into chunks of 1..=max bytes.
pub fn random_cuts(rng: &mut Rng, len: usize, max: usize) -> Vec<usize> {
    let mut cuts = Vec::new();
    let mut at = 0;
    loop {
        at += 1 + rng.below(max);
        if at >= len {
            return cuts;
        }
        cuts.push(at);
    }
}

/// A byte stream for tungstenite: reads `wire`, discards writes.
pub struct Duplex(pub std::io::Cursor<Vec<u8>>);

impl std::io::Read for Duplex {
    fn read(&mut self, b: &mut [u8]) -> std::io::Result<usize> {
        self.0.read(b)
    }
}

impl std::io::Write for Duplex {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Polls `f` once.
pub fn poll<F: Future>(f: std::pin::Pin<&mut F>) -> Poll<F::Output> {
    f.poll(&mut Context::from_waker(Waker::noop()))
}

/// `f`'s output; panics if it is not ready on the first poll (the lower layer did not wait).
pub fn ready<F: Future>(f: F) -> F::Output {
    match poll(pin!(f)) {
        Poll::Ready(out) => out,
        Poll::Pending => panic!("future pending though the lower layer has room"),
    }
}
