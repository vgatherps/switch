//! Receive core: parses WebSocket frames (RFC 6455) out of the lower layer's
//! runs and hands out messages that borrow the received bytes in place.

use crate::lower::PlainRx;
use crate::utf8::Utf8;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// Receives unmasked frames.
    Client,
    /// Receives masked frames.
    Server,
}

/// A connection's size limits. A received frame or message over its limit fails the connection
/// (`MessageTooBig`/`FrameTooBig`, close code 1009); a send over `max_send` is refused. The
/// receive buffer grows only with bytes received, up to `max_message`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Largest frame payload accepted.
    pub max_frame: usize,
    /// Largest message accepted.
    pub max_message: usize,
    /// Largest message sent.
    pub max_send: usize,
}

impl Default for Limits {
    /// tungstenite's limits: frame 16 MiB, message 64 MiB (either way).
    fn default() -> Limits {
        Limits { max_frame: 16 << 20, max_message: 64 << 20, max_send: 64 << 20 }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub role: Role,
    pub limits: Limits,
}

impl Config {
    pub fn new(role: Role, limits: Limits) -> Config {
        Config { role, limits }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Text,
    Binary,
}

/// Why the connection failed. Each maps to the close code to send.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProtocolError {
    ReservedBits,
    UnknownOpcode(u8),
    FragmentedControl,
    ControlTooBig,
    /// A continuation frame with no message in progress.
    UnexpectedContinuation,
    /// A new data frame while a fragmented message is in progress.
    ExpectedContinuation,
    /// Server role: a frame without mask.
    UnmaskedFrame,
    /// Client role: a masked frame.
    MaskedFrame,
    /// The 64-bit length has its top bit set.
    LengthTopBit,
    /// A 16-bit length below 126 or a 64-bit length below 65536 (RFC 6455 §5.2).
    NonMinimalLength,
    /// A close payload of 1 byte.
    InvalidCloseLength,
    InvalidUtf8,
    FrameTooBig,
    MessageTooBig,
}

impl ProtocolError {
    pub fn close_code(self) -> u16 {
        match self {
            ProtocolError::InvalidUtf8 => 1007,
            ProtocolError::FrameTooBig | ProtocolError::MessageTooBig => 1009,
            _ => 1002,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum WsError<E> {
    Protocol(ProtocolError),
    /// The peer sent a valid close frame; see [`WsRx::peer_close`].
    Closed,
    Lower(E),
}

/// A connection failure; once set every later `recv` returns it again.
#[derive(Clone, Copy, Debug)]
enum Stop {
    Protocol(ProtocolError),
    Closed,
}

impl From<ProtocolError> for Stop {
    fn from(e: ProtocolError) -> Stop {
        Stop::Protocol(e)
    }
}

impl<E> From<Stop> for WsError<E> {
    fn from(s: Stop) -> WsError<E> {
        match s {
            Stop::Protocol(e) => WsError::Protocol(e),
            Stop::Closed => WsError::Closed,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Op {
    Cont,
    Text,
    Binary,
    Close,
    Ping,
    Pong,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Body {
    /// First frame of an unfragmented-so-far message: direct or scratch is
    /// decided once its payload starts arriving.
    Undecided,
    Scratch,
    Control,
}

#[derive(Clone, Copy)]
struct Frame {
    op: Op,
    fin: bool,
    mask: Option<[u8; 4]>,
    phase: usize,
    left: usize,
    body: Body,
}

#[derive(Clone, Copy)]
struct Msg {
    kind: Kind,
    stamp: u64,
}

/// Where a delivered message lives, without borrowing it.
#[derive(Clone, Copy)]
struct Located {
    /// In the lower layer's current run; otherwise in scratch.
    direct: bool,
    len: usize,
    kind: Kind,
    stamp: u64,
}

/// A control frame payload (at most 125 B, RFC 6455 §5.5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Control {
    buf: [u8; 125],
    len: usize,
}

impl Control {
    pub(crate) const EMPTY: Control = Control { buf: [0; 125], len: 0 };

    /// Panics above 125 B.
    pub(crate) fn new(p: &[u8]) -> Control {
        let mut c = Control::EMPTY;
        c.buf[..p.len()].copy_from_slice(p);
        c.len = p.len();
        c
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}

struct Header {
    /// The length used the shortest encoding.
    minimal: bool,
    fin: bool,
    rsv: u8,
    opcode: u8,
    mask: Option<[u8; 4]>,
    len: u64,
}

fn header_len(b1: u8) -> usize {
    let ext = match b1 & 0x7f {
        126 => 2,
        127 => 8,
        _ => 0,
    };
    2 + ext + if b1 & 0x80 != 0 { 4 } else { 0 }
}

/// `h` is exactly `header_len(h[1])` bytes.
fn parse_header(h: &[u8]) -> Header {
    let (len, rest) = match h[1] & 0x7f {
        126 => (u16::from_be_bytes([h[2], h[3]]) as u64, &h[4..]),
        127 => (u64::from_be_bytes(h[2..10].try_into().unwrap()), &h[10..]),
        n => (n as u64, &h[2..]),
    };
    let minimal = match h[1] & 0x7f {
        126 => len >= 126,
        127 => len >= 1 << 16,
        _ => true,
    };
    Header {
        minimal,
        fin: h[0] & 0x80 != 0,
        rsv: h[0] & 0x70,
        opcode: h[0] & 0x0f,
        mask: (h[1] & 0x80 != 0).then(|| rest[..4].try_into().unwrap()),
        len,
    }
}

fn unmask(b: &mut [u8], mask: Option<[u8; 4]>, phase: &mut usize) {
    if let Some(m) = mask {
        for x in b {
            *x ^= m[*phase & 3];
            *phase += 1;
        }
    }
}

/// RFC 6455 §7.4 plus the IANA registry (1014 Bad Gateway).
fn close_code_allowed(c: u16) -> bool {
    matches!(c, 1000..=1003 | 1007..=1014 | 3000..=4999)
}

/// Whether we may send a close with `code` and `reason`: a code a peer may send, and a
/// payload (2 B code + reason) within a control frame's 125 B.
pub(crate) fn close_sendable(code: u16, reason: &str) -> bool {
    close_code_allowed(code) && reason.len() <= 123
}

/// Make room for `additional` more bytes: at least doubling, never past `max`.
/// The caller has checked `len + additional <= max`.
pub(crate) fn grow(v: &mut Vec<u8>, additional: usize, max: usize) {
    let need = v.len() + additional;
    if need > v.capacity() {
        let target = need.max(2 * v.capacity()).min(max);
        v.reserve_exact(target - v.len());
    }
}

struct Core {
    role: Role,
    max_frame: usize,
    max_message: usize,
    stop: Option<Stop>,
    hdr: [u8; 14],
    hdr_len: usize,
    /// Stamp of the run holding the current header's first byte.
    hdr_stamp: u64,
    frame: Option<Frame>,
    ctrl: Control,
    /// Message being assembled in scratch.
    msg: Option<Msg>,
    utf8: Utf8,
    scratch: Vec<u8>,
    /// The newest ping's payload; owed as a pong while `pong_due`.
    pong: Control,
    pong_due: bool,
    close: Option<Control>,
}

impl Core {
    /// Header bytes from `b`: bytes used, and the header once complete.
    /// Parsed from `b` directly when whole, else gathered in `hdr`.
    fn gather(&mut self, b: &[u8], stamp: u64) -> (usize, Option<Header>) {
        if self.hdr_len == 0 {
            self.hdr_stamp = stamp;
            if b.len() >= 2 {
                let n = header_len(b[1]);
                if b.len() >= n {
                    return (n, Some(parse_header(&b[..n])));
                }
            }
        }
        let mut used = 0;
        loop {
            let need = if self.hdr_len < 2 { 2 } else { header_len(self.hdr[1]) };
            if self.hdr_len == need {
                self.hdr_len = 0;
                return (used, Some(parse_header(&self.hdr[..need])));
            }
            if used == b.len() {
                return (used, None);
            }
            let k = (need - self.hdr_len).min(b.len() - used);
            self.hdr[self.hdr_len..self.hdr_len + k].copy_from_slice(&b[used..used + k]);
            self.hdr_len += k;
            used += k;
        }
    }

    /// Checks in tungstenite's order, so both report the same failure class.
    fn validate(&mut self, h: Header) -> Result<Frame, ProtocolError> {
        let op = match h.opcode {
            0 => Op::Cont,
            1 => Op::Text,
            2 => Op::Binary,
            8 => Op::Close,
            9 => Op::Ping,
            10 => Op::Pong,
            o => return Err(ProtocolError::UnknownOpcode(o)),
        };
        if h.len >> 63 != 0 {
            return Err(ProtocolError::LengthTopBit);
        }
        if !h.minimal {
            return Err(ProtocolError::NonMinimalLength);
        }
        if h.len > self.max_frame as u64 {
            return Err(ProtocolError::FrameTooBig);
        }
        let len = h.len as usize;
        if self.role == Role::Server && h.mask.is_none() {
            return Err(ProtocolError::UnmaskedFrame);
        }
        if h.rsv != 0 {
            return Err(ProtocolError::ReservedBits);
        }
        if self.role == Role::Client && h.mask.is_some() {
            return Err(ProtocolError::MaskedFrame);
        }
        let body = match op {
            Op::Close | Op::Ping | Op::Pong => {
                if !h.fin {
                    return Err(ProtocolError::FragmentedControl);
                }
                if len > 125 {
                    return Err(ProtocolError::ControlTooBig);
                }
                self.ctrl.len = 0;
                Body::Control
            }
            Op::Cont => {
                if self.msg.is_none() {
                    return Err(ProtocolError::UnexpectedContinuation);
                }
                if len > self.max_message - self.scratch.len() {
                    return Err(ProtocolError::MessageTooBig);
                }
                Body::Scratch
            }
            Op::Text | Op::Binary => {
                if self.msg.is_some() {
                    return Err(ProtocolError::ExpectedContinuation);
                }
                if len > self.max_message {
                    return Err(ProtocolError::MessageTooBig);
                }
                Body::Undecided
            }
        };
        Ok(Frame { op, fin: h.fin, mask: h.mask, phase: 0, left: len, body })
    }

    fn control(&mut self, op: Op) -> Result<(), Stop> {
        match op {
            Op::Ping => {
                self.pong = self.ctrl;
                self.pong_due = true;
            }
            Op::Close => {
                let p = self.ctrl.bytes();
                if p.len() == 1 {
                    return Err(ProtocolError::InvalidCloseLength.into());
                }
                if p.len() >= 2 {
                    let code = u16::from_be_bytes([p[0], p[1]]);
                    if !close_code_allowed(code) {
                        // RFC 6455 §7.4.1: the close handshake still completes, with 1002
                        self.close = Some(Control::new(&1002u16.to_be_bytes()));
                        return Err(Stop::Closed);
                    }
                    if core::str::from_utf8(&p[2..]).is_err() {
                        return Err(ProtocolError::InvalidUtf8.into());
                    }
                }
                self.close = Some(self.ctrl);
                return Err(Stop::Closed);
            }
            _ => {}
        }
        Ok(())
    }

    /// Parse `buf` (one run, first byte stamped `stamp`) until a message is
    /// complete or the run ends. Returns the bytes used: on a direct message
    /// this stops before its payload, which stays in the run.
    fn step(&mut self, buf: &mut [u8], stamp: u64) -> Result<(usize, Option<Located>), Stop> {
        let mut pos = 0;
        loop {
            let mut f = match self.frame {
                Some(f) => f,
                None => {
                    if pos == buf.len() {
                        return Ok((pos, None));
                    }
                    let (used, h) = self.gather(&buf[pos..], stamp);
                    pos += used;
                    match h {
                        Some(h) => self.validate(h)?,
                        None => return Ok((pos, None)),
                    }
                }
            };
            let avail = buf.len() - pos;
            match f.body {
                Body::Control => {
                    let n = f.left.min(avail);
                    let dst = &mut self.ctrl.buf[self.ctrl.len..self.ctrl.len + n];
                    dst.copy_from_slice(&buf[pos..pos + n]);
                    unmask(dst, f.mask, &mut f.phase);
                    self.ctrl.len += n;
                    pos += n;
                    f.left -= n;
                    if f.left > 0 {
                        self.frame = Some(f);
                        return Ok((pos, None));
                    }
                    self.frame = None;
                    self.control(f.op)?;
                }
                Body::Undecided => {
                    let kind = if f.op == Op::Text { Kind::Text } else { Kind::Binary };
                    if f.fin && f.left <= avail {
                        let payload = &mut buf[pos..pos + f.left];
                        unmask(payload, f.mask, &mut f.phase);
                        if kind == Kind::Text && core::str::from_utf8(payload).is_err() {
                            return Err(ProtocolError::InvalidUtf8.into());
                        }
                        self.frame = None;
                        let at = Located { direct: true, len: f.left, kind, stamp: self.hdr_stamp };
                        return Ok((pos, Some(at)));
                    }
                    if avail == 0 {
                        self.frame = Some(f);
                        return Ok((pos, None));
                    }
                    self.scratch.clear();
                    self.msg = Some(Msg { kind, stamp: self.hdr_stamp });
                    f.body = Body::Scratch;
                    self.frame = Some(f);
                }
                Body::Scratch => {
                    let n = f.left.min(avail);
                    let piece = &mut buf[pos..pos + n];
                    unmask(piece, f.mask, &mut f.phase);
                    let msg = self.msg.expect("scratch frame without message");
                    if msg.kind == Kind::Text && self.utf8.feed(piece).is_err() {
                        return Err(ProtocolError::InvalidUtf8.into());
                    }
                    // grown by the bytes received, not the length a header claims (the header
                    // checks kept the message within `max_message`)
                    grow(&mut self.scratch, n, self.max_message);
                    self.scratch.extend_from_slice(piece);
                    pos += n;
                    f.left -= n;
                    if f.left > 0 {
                        self.frame = Some(f);
                        return Ok((pos, None));
                    }
                    self.frame = None;
                    if f.fin {
                        self.msg = None;
                        if msg.kind == Kind::Text && self.utf8.finish().is_err() {
                            return Err(ProtocolError::InvalidUtf8.into());
                        }
                        let at = Located {
                            direct: false,
                            len: self.scratch.len(),
                            kind: msg.kind,
                            stamp: msg.stamp,
                        };
                        return Ok((pos, Some(at)));
                    }
                }
            }
        }
    }
}

/// Receive half of a connection over lower layer `R`.
pub struct WsRx<R: PlainRx> {
    lower: R,
    core: Core,
    /// Payload of the last direct message, consumed at the next `recv`.
    pending: usize,
    /// A complete message found by `has_message`, returned by the next `take_message`.
    ready: Option<Located>,
}

impl<R: PlainRx> WsRx<R> {
    pub fn new(lower: R, config: Config) -> WsRx<R> {
        WsRx {
            lower,
            core: Core {
                role: config.role,
                max_frame: config.limits.max_frame,
                max_message: config.limits.max_message,
                stop: None,
                hdr: [0; 14],
                hdr_len: 0,
                hdr_stamp: 0,
                frame: None,
                ctrl: Control::EMPTY,
                msg: None,
                utf8: Utf8::default(),
                // allocated on the first message spanning runs, then kept
                scratch: Vec::new(),
                pong: Control::EMPTY,
                pong_due: false,
                close: None,
            },
            pending: 0,
            ready: None,
        }
    }

    /// Hand the next frame to the lower layer. Call after `has_message` returned `false`.
    pub fn push(&mut self, f: R::Frame) {
        self.lower.push(f)
    }

    /// The next complete message, or `None` when a `push` is needed. Control
    /// frames are handled here: a ping's payload is owed as a pong ([`take_pong`](Self::take_pong)),
    /// a valid close ends the connection with [`WsError::Closed`].
    ///
    /// The message borrows `self`, so no further `recv` runs while it is held. (The async
    /// receive loop uses `has_message` and `take_message`, which a loop can hold across awaits.)
    #[cfg(test)]
    pub fn recv(&mut self, tx: &mut R::Tx) -> Result<Option<Message<'_>>, WsError<R::Error>> {
        self.has_message(tx)?;
        Ok(self.take_message())
    }

    /// The message the last `has_message` found, once; `None` if it found none. Parses
    /// nothing.
    pub(crate) fn take_message(&mut self) -> Option<Message<'_>> {
        let at = self.ready.take()?;
        Some(Message {
            kind: at.kind,
            bytes: if at.direct { &self.lower.current()[..at.len] } else { &self.core.scratch[..at.len] },
            rx_timestamp: at.stamp,
        })
    }

    /// Parse until a message is complete: `Ok(true)` and `take_message` returns it, or
    /// `Ok(false)` when a frame must be pushed. An async receive loop awaits frames on
    /// `false`; it cannot keep a message across such a loop.
    pub fn has_message(&mut self, tx: &mut R::Tx) -> Result<bool, WsError<R::Error>> {
        if self.ready.is_none() {
            self.ready = self.next(tx)?;
        }
        Ok(self.ready.is_some())
    }

    /// The pong owed for the newest ping, once: a later call returns `None`
    /// until another ping arrives.
    pub fn take_pong(&mut self) -> Option<&[u8]> {
        let due = core::mem::take(&mut self.core.pong_due);
        due.then(|| self.core.pong.bytes())
    }

    /// The close payload to echo once `recv` returned `Closed`: the peer's, validated, or
    /// 1002 (protocol error) when its status code is not one a peer may send.
    pub fn peer_close(&self) -> Option<&[u8]> {
        self.core.close.as_ref().map(Control::bytes)
    }

    /// Stops receiving: drops what the lower layer holds and any found message. Nothing is
    /// pushed or parsed after.
    pub(crate) fn discard(&mut self) {
        self.lower.discard();
        self.pending = 0;
        self.ready = None;
    }

    /// The lower layer ended its stream (TLS close_notify) and `has_message` found no message.
    pub(crate) fn eof(&self) -> bool {
        self.lower.eof()
    }

    /// TCP FIN while reading; see [`PlainRx::fin`].
    pub(crate) fn fin(&mut self, tx: &mut R::Tx) -> Result<(), R::Error> {
        self.lower.fin(tx)
    }

    fn next(&mut self, tx: &mut R::Tx) -> Result<Option<Located>, WsError<R::Error>> {
        if let Some(s) = self.core.stop {
            return Err(s.into());
        }
        if self.pending > 0 {
            self.lower.consume(self.pending);
            self.pending = 0;
        }
        loop {
            let Some(c) = self.lower.chunk(tx).map_err(WsError::Lower)? else {
                return Ok(None);
            };
            match self.core.step(c.bytes, c.rx_timestamp) {
                Err(s) => {
                    self.core.stop = Some(s);
                    return Err(s.into());
                }
                Ok((used, at)) => {
                    self.lower.consume(used);
                    if let Some(at) = at {
                        if at.direct {
                            self.pending = at.len;
                        }
                        return Ok(Some(at));
                    }
                }
            }
        }
    }
}

/// A received message, borrowing either the lower layer's run (direct) or the receive scratch.
/// It borrows the receive half, so no further `recv` runs while it is held.
pub struct Message<'r> {
    kind: Kind,
    bytes: &'r [u8],
    rx_timestamp: u64,
}

impl Message<'_> {
    pub fn kind(&self) -> Kind {
        self.kind
    }

    pub fn bytes(&self) -> &[u8] {
        self.bytes
    }

    /// `Some` iff text; no second pass over the bytes.
    pub fn text(&self) -> Option<&str> {
        match self.kind {
            Kind::Text => {
                debug_assert!(core::str::from_utf8(self.bytes).is_ok(), "validated text changed");
                // SAFETY: a Text message's bytes were validated as UTF-8 on receipt
                // (`Core::step`: `from_utf8` over a direct payload, `Utf8::feed` and `finish`
                // over scratch), and these are those bytes: scratch is written only by a later
                // `recv`, which this message's borrow of the receive half rules out, and a
                // direct payload is read through `PlainRx::current`, which returns exactly the
                // bytes it handed out, unchanged, until the next `chunk` or `push` (the same
                // borrow rules those out; `TlsRx::current` documents it for sheath).
                Some(unsafe { core::str::from_utf8_unchecked(self.bytes) })
            }
            Kind::Binary => None,
        }
    }

    /// Stamp of the frame holding the message's first byte.
    pub fn rx_timestamp(&self) -> u64 {
        self.rx_timestamp
    }
}


#[cfg(test)]
mod tests {
    use sheath::RxFrame;

    use super::*;
    use crate::raw::Raw;

    struct F(Vec<u8>);

    impl RxFrame for F {
        fn payload(&self) -> &[u8] {
            &self.0
        }
        fn payload_mut(&mut self) -> &mut [u8] {
            &mut self.0
        }
        fn rx_timestamp(&self) -> u64 {
            0
        }
    }

    fn frame(b0: u8, payload: &[u8]) -> Vec<u8> {
        assert!(payload.len() < 126);
        [&[b0, payload.len() as u8][..], payload].concat()
    }

    /// `text()` gives back the validated payload without checking it again: a direct message
    /// (read through the lower layer) and one assembled in scratch, both with multibyte text.
    /// The check is `text()`'s debug_assert, which re-validates in every test build. relay is
    /// not in `make miri`; by hand, `cargo +nightly miri test -p relay --lib` runs this too.
    #[test]
    fn text_is_the_payload_validated_on_receipt() {
        let t = "a€𝄞b";
        let mut rx = WsRx::new(Raw::new(), Config::new(Role::Client, Limits::default()));
        rx.push(F(frame(0x81, t.as_bytes())));
        assert_eq!(rx.recv(&mut ()).unwrap().unwrap().text(), Some(t));
        assert!(rx.recv(&mut ()).unwrap().is_none());
        let wire = frame(0x81, t.as_bytes());
        rx.push(F(wire[..5].to_vec()));
        assert!(rx.recv(&mut ()).unwrap().is_none());
        rx.push(F(wire[5..].to_vec()));
        let m = rx.recv(&mut ()).unwrap().unwrap();
        assert_eq!(m.text(), Some(t));
        assert!(!m.bytes().is_empty());
        assert!(rx.recv(&mut ()).unwrap().is_none());
        rx.push(F(frame(0x82, &[0xff])));
        assert_eq!(rx.recv(&mut ()).unwrap().unwrap().text(), None);
    }

    /// Doubling would pass `max_message`; capacity stops at it.
    #[test]
    fn scratch_capacity_never_exceeds_max_message() {
        let cfg = Config::new(Role::Client, Limits { max_frame: 100, max_message: 1000, ..Limits::default() });
        let mut rx = WsRx::new(Raw::new(), cfg);
        // 10 fragments of 100 B: capacity 0 -> 100 -> 200 -> 400 -> 800 -> 1000 (doubling alone: 1600).
        for i in 0..10 {
            let b0 = match i {
                0 => 2,
                9 => 0x80,
                _ => 0,
            };
            rx.push(F(frame(b0, &[i; 100])));
            let got = rx.recv(&mut ()).unwrap().map(|m| m.bytes().len());
            assert_eq!(got, (i == 9).then_some(1000));
            assert!(rx.core.scratch.capacity() <= 1000, "{}", rx.core.scratch.capacity());
        }

        // One frame spanning two pushes: clamped too.
        let cfg = Config::new(Role::Client, Limits { max_frame: 1000, max_message: 1000, ..Limits::default() });
        let mut rx = WsRx::new(Raw::new(), cfg);
        let mut wire = vec![0x82, 126];
        wire.extend_from_slice(&1000u16.to_be_bytes());
        wire.extend_from_slice(&[7; 1000]);
        rx.push(F(wire[..10].to_vec()));
        assert!(rx.recv(&mut ()).unwrap().is_none());
        rx.push(F(wire[10..].to_vec()));
        assert_eq!(rx.recv(&mut ()).unwrap().unwrap().bytes(), &[7; 1000][..]);
        assert!(rx.core.scratch.capacity() <= 1000, "{}", rx.core.scratch.capacity());
    }
}
