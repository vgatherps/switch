//! Send core: the app writes the payload straight into a frame buffer, the
//! WebSocket header goes in front of it, and client-role masking is done in
//! place.

use rand::Rng;
use rand::rngs::StdRng;

use crate::lower::{PlainTx, TxFrame};
use crate::rx::{Control, Kind, Role};

const CONT: u8 = 0;
const TEXT: u8 = 1;
const BINARY: u8 = 2;
const CLOSE: u8 = 8;
const PONG: u8 = 10;
const FIN: u8 = 0x80;

#[derive(Debug, PartialEq, Eq)]
pub enum TxError<E> {
    /// A close frame is queued or sent, so no data frame may follow (RFC 6455 §5.5.1).
    Closed,
    Lower(E),
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

/// `h` is exactly `header_len(len, mask.is_some())` bytes.
fn write_header(h: &mut [u8], b0: u8, len: usize, mask: Option<[u8; 4]>) {
    let m = if mask.is_some() { 0x80 } else { 0 };
    h[0] = b0;
    let key = if len <= 125 {
        h[1] = m | len as u8;
        &mut h[2..]
    } else if len <= 0xffff {
        h[1] = m | 126;
        h[2..4].copy_from_slice(&(len as u16).to_be_bytes());
        &mut h[4..]
    } else {
        h[1] = m | 127;
        h[2..10].copy_from_slice(&(len as u64).to_be_bytes());
        &mut h[10..]
    };
    if let Some(k) = mask {
        key.copy_from_slice(&k);
    }
}

/// Header bytes to reserve and the payload capacity, for `room` plaintext
/// bytes per frame. The header class is fixed before the payload is written.
fn layout(room: usize, masked: bool) -> (usize, usize) {
    let m = if masked { 4 } else { 0 };
    for (ext, max) in [(0, 125), (2, 0xffff), (8, usize::MAX)] {
        let h = 2 + ext + m;
        let cap = room.saturating_sub(h);
        if cap <= max {
            return (h, cap);
        }
    }
    unreachable!("the last class takes any length")
}

enum Close {
    None,
    Queued(Control),
    Sent,
}

/// Send half of a connection over lower layer `L`.
///
/// At most one frame waits unsent: one the lower layer refused. Every call
/// that sends sends it first, and a new frame is handed out only once it is
/// gone, so no sealed frame is ever dropped.
pub struct WsTx<L: PlainTx> {
    lower: L,
    /// Mask source; `Some` iff client role.
    rng: Option<StdRng>,
    unsent: Option<L::Frame>,
    /// Kind of the fragmented message being sent.
    open: Option<Kind>,
    pong: Option<Control>,
    close: Close,
}

impl<L: PlainTx> WsTx<L> {
    /// Panics if one frame cannot carry a maximal header plus one payload byte.
    pub fn new(lower: L, role: Role) -> WsTx<L> {
        assert!(lower.plain().1 > 14, "frame room below one WebSocket frame");
        WsTx {
            lower,
            rng: (role == Role::Client).then(rand::make_rng),
            unsent: None,
            open: None,
            pong: None,
            close: Close::None,
        }
    }

    /// Queue a pong; a newer one replaces one not yet sent (RFC 6455 §5.5.3).
    /// Ignored once a close is queued. Panics above 125 B.
    pub fn pong(&mut self, payload: &[u8]) {
        if matches!(self.close, Close::None) {
            self.pong = Some(Control::new(payload));
        }
    }

    /// Queue a close frame: a status code (2 B, big-endian) and UTF-8
    /// reason, or empty; to echo the peer's, pass [`WsRx::peer_close`](crate::WsRx::peer_close).
    /// Only the first call counts. Panics above 125 B.
    pub fn close(&mut self, payload: &[u8]) {
        if matches!(self.close, Close::None) {
            self.close = Close::Queued(Control::new(payload));
        }
    }

    /// Send what waits: the refused frame, then a queued pong, then a queued
    /// close. `Ok(false)`: the lower layer refused; call again once it has room.
    pub fn flush(&mut self) -> Result<bool, L::Error> {
        if let Some(f) = self.unsent.take()
            && let Err(f) = self.lower.try_send(f)
        {
            self.unsent = Some(f);
            return Ok(false);
        }
        if let Some(p) = self.pong.take() {
            self.control(PONG, p)?;
            if self.unsent.is_some() {
                return Ok(false);
            }
        }
        if let Close::Queued(c) = self.close {
            self.close = Close::Sent;
            self.control(CLOSE, c)?;
            if self.unsent.is_some() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// One frame to write a payload into, after [`flush`](Self::flush);
    /// `None` when that could not finish. Panics if a message of another
    /// kind is still open.
    pub fn frame(&mut self, kind: Kind) -> Result<Option<OutFrame<'_, L>>, TxError<L::Error>> {
        if !matches!(self.close, Close::None) {
            return Err(TxError::Closed);
        }
        if let Some(open) = self.open {
            assert_eq!(open, kind, "a {open:?} message is still open");
        }
        if !self.flush().map_err(TxError::Lower)? {
            return Ok(None);
        }
        let frame = self.lower.alloc().map_err(TxError::Lower)?;
        let (head, room) = self.lower.plain();
        let (reserved, cap) = layout(room, self.rng.is_some());
        Ok(Some(OutFrame { tx: self, frame, kind, head, reserved, cap }))
    }

    /// Copy `rest` out as the (rest of the) message, one frame per lower-layer
    /// frame. `Ok(true)`: all of it is committed and the message ended.
    /// `Ok(false)`: the lower layer refused; `rest` holds what is left, and the
    /// frames sent so far form an open fragmented message. Call again with the
    /// same kind once the lower layer has room.
    pub fn send(&mut self, kind: Kind, rest: &mut &[u8]) -> Result<bool, TxError<L::Error>> {
        loop {
            let Some(mut f) = self.frame(kind)? else {
                return Ok(false);
            };
            let room = f.room();
            let n = rest.len().min(room.len());
            room[..n].copy_from_slice(&rest[..n]);
            let fin = n == rest.len();
            f.commit(n, fin)?;
            *rest = &rest[n..];
            if fin {
                return Ok(true);
            }
        }
    }

    fn control(&mut self, op: u8, c: Control) -> Result<(), L::Error> {
        let mut f = self.lower.alloc()?;
        let (head, _) = self.lower.plain();
        let p = c.bytes();
        let h = header_len(p.len(), self.rng.is_some());
        f.room_mut()[head + h..head + h + p.len()].copy_from_slice(p);
        self.finish(f, FIN | op, head, h, p.len())
    }

    /// The payload is at `head + reserved`. Moves it down if its header is
    /// shorter than reserved, writes the header at `head`, masks, seals, sends.
    fn finish(&mut self, mut f: L::Frame, b0: u8, head: usize, reserved: usize, len: usize) -> Result<(), L::Error> {
        debug_assert!(self.unsent.is_none());
        let mask = self.rng.as_mut().map(|r| r.next_u32().to_ne_bytes());
        let h = header_len(len, mask.is_some());
        let room = f.room_mut();
        if h < reserved {
            room.copy_within(head + reserved..head + reserved + len, head + h);
        }
        write_header(&mut room[head..head + h], b0, len, mask);
        if let Some(k) = mask {
            for (i, b) in room[head + h..head + h + len].iter_mut().enumerate() {
                *b ^= k[i & 3];
            }
        }
        self.lower.seal(&mut f, h + len)?;
        if let Err(f) = self.lower.try_send(f) {
            self.unsent = Some(f);
        }
        Ok(())
    }
}

/// A frame being filled by the app. Dropping it without `commit` sends nothing.
pub struct OutFrame<'a, L: PlainTx> {
    tx: &'a mut WsTx<L>,
    frame: L::Frame,
    kind: Kind,
    head: usize,
    reserved: usize,
    cap: usize,
}

impl<L: PlainTx> OutFrame<'_, L> {
    /// Where the payload goes.
    pub fn room(&mut self) -> &mut [u8] {
        let start = self.head + self.reserved;
        &mut self.frame.room_mut()[start..start + self.cap]
    }

    /// Send the first `len` bytes of `room()` as one WebSocket frame; `fin`
    /// ends the message, otherwise later frames continue it. If the lower
    /// layer refuses the frame it is kept and sent first by the next call.
    pub fn commit(self, len: usize, fin: bool) -> Result<(), TxError<L::Error>> {
        assert!(len <= self.cap, "commit past the frame room");
        let op = match (self.tx.open, self.kind) {
            (Some(_), _) => CONT,
            (None, Kind::Text) => TEXT,
            (None, Kind::Binary) => BINARY,
        };
        self.tx.open = (!fin).then_some(self.kind);
        let b0 = if fin { FIN | op } else { op };
        self.tx.finish(self.frame, b0, self.head, self.reserved, len).map_err(TxError::Lower)
    }
}
