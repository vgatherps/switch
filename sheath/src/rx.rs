use core::marker::PhantomData;
use core::ops::Range;

use rustls::unbuffered::{ConnectionState, UnbufferedStatus};

use crate::{Chunk, Error, FrameTx, RxFrame, TlsTx, encode_into};

const HEADER_LEN: usize = 5;

/// Largest TLS 1.3 record body on the wire: 2^14 B of plaintext plus 256 B of expansion
/// (RFC 8446 §5.2). rustls's own header check allows up to 18431 B.
const MAX_CIPHERTEXT_LEN: usize = (1 << 14) + 256;

/// Largest TLS 1.3 record on the wire. The scratch buffer is allocated once at this size.
const MAX_RECORD_LEN: usize = HEADER_LEN + MAX_CIPHERTEXT_LEN;

/// Most bytes rustls may need kept: the records of a handshake message still being joined plus
/// the next record. rustls's buffered API stops reading at the same bound while joining
/// (`MAX_HANDSHAKE_SIZE` in msgs/deframer/buffers.rs); without it, a peer that always leaves a
/// handshake message open grows the scratch buffer without limit.
const MAX_RETAINED_LEN: usize = 0xffff;

#[derive(Debug)]
enum State {
    Open,
    /// The peer sent close_notify; later bytes are ignored (RFC 8446 §6.1).
    Closed,
    Failed(Error),
}

/// Where the current run's bytes are.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Src {
    Frame,
    Scratch,
    Carry,
}

/// The unconsumed plaintext of the record the last `chunk` returned.
#[derive(Debug)]
struct Run {
    src: Src,
    at: Range<usize>,
    stamp: u64,
}

impl Run {
    const NONE: Run = Run {
        src: Src::Carry,
        at: 0..0,
        stamp: 0,
    };
}

/// Receive half of a TLS 1.3 client connection, fed with frames in stream order; each run it
/// hands out is one record's plaintext.
///
/// The rustls connection lives in [`TlsTx`], which each call borrows. Records rustls wants
/// sent while receiving go to the send half's `take_pending`, and a receive error stops the
/// send half too.
///
/// The bytes being worked on (the source) are `scratch` if it is non-empty, else the current
/// frame from `pos`. The source starts with `seen` bytes that rustls has processed but must
/// see again (records of a handshake message that is still incomplete, which rustls joins in
/// place, so their headers are no longer readable); the next record starts after them.
pub struct TlsRx<F, L> {
    frame: Option<F>,
    /// Bytes at the front of `frame` that rustls has consumed.
    pos: usize,
    /// Records straddling frames or left by `take_frame`, after the `seen` bytes.
    scratch: Vec<u8>,
    /// Stamp of the frame holding the first byte of the record after the `seen` bytes in
    /// `scratch`.
    scratch_stamp: u64,
    seen: usize,
    run: Run,
    /// Bytes at the front of the source taken by the run's record; dropped once the run is
    /// used up.
    returned: usize,
    /// Plaintext of a record whose frame `take_frame` moved out.
    carry: Vec<u8>,
    state: State,
    _tx: PhantomData<fn(&mut TlsTx<L>)>,
}

impl<F: RxFrame, L: FrameTx> Default for TlsRx<F, L> {
    fn default() -> Self {
        Self::new()
    }
}

impl<F: RxFrame, L: FrameTx> TlsRx<F, L> {
    pub fn new() -> Self {
        Self {
            frame: None,
            pos: 0,
            scratch: Vec::with_capacity(MAX_RECORD_LEN),
            scratch_stamp: 0,
            seen: 0,
            run: Run::NONE,
            returned: 0,
            carry: Vec::new(),
            state: State::Open,
            _tx: PhantomData,
        }
    }

    /// Starts with `leftover`, bytes after the handshake that rustls has not processed (the
    /// start of a record), from a frame stamped `stamp`.
    #[cfg(feature = "switchnet")]
    pub(crate) fn with_leftover(leftover: &[u8], stamp: u64) -> Self {
        let mut rx = Self::new();
        rx.scratch.extend_from_slice(leftover);
        rx.scratch_stamp = stamp;
        rx
    }

    /// The peer closed the stream with close_notify.
    pub fn eof(&self) -> bool {
        matches!(self.state, State::Closed)
    }

    /// The transport ended (TCP FIN); call once `chunk` returns `Ok(None)`. `Ok` iff the peer
    /// sent close_notify first; otherwise the stream was truncated, which stops sending too.
    pub fn fin(&mut self, tx: &mut TlsTx<L>) -> Result<(), Error> {
        match &self.state {
            State::Open => {
                self.end(tx, State::Failed(Error::Truncated));
                Err(Error::Truncated)
            }
            State::Closed => Ok(()),
            State::Failed(e) => Err(e.clone()),
        }
    }

    /// Decrypts records until one has plaintext; `false` if more frames are needed or the
    /// stream ended.
    fn next_run(&mut self, tx: &mut TlsTx<L>) -> Result<bool, Error> {
        loop {
            match &self.state {
                State::Open => {}
                State::Closed => return Ok(false),
                State::Failed(e) => return Err(e.clone()),
            }

            if !self.scratch.is_empty() {
                if self.scratch.len() >= self.seen + HEADER_LEN
                    && let Some(state) = check_bounds(&self.scratch, self.seen)
                {
                    self.end(tx, state);
                    continue;
                }
                let (gathered, target) = self.gather();
                match gathered {
                    Gathered::NeedFrame => return Ok(false),
                    Gathered::Header => {
                        let state = check_bounds(&self.scratch, self.seen)
                            .or_else(|| check_header(tx, &mut self.scratch[..target], self.seen));
                        if let Some(state) = state {
                            self.end(tx, state);
                        }
                    }
                    Gathered::Record => match process(tx, &mut self.scratch[..target], self.seen) {
                        Step::Record {
                            consumed,
                            start,
                            len,
                        } => {
                            self.start_run(Src::Scratch, target, consumed, start..start + len);
                            self.run.stamp = self.scratch_stamp;
                            return Ok(true);
                        }
                        Step::NeedMore { consumed } => {
                            self.scratch.drain(..consumed);
                            self.seen = target - consumed;
                        }
                        Step::End(state) => self.end(tx, state),
                    },
                }
                continue;
            }

            let Some(frame) = &mut self.frame else {
                return Ok(false);
            };
            let pos = self.pos;
            let available = frame.payload().len() - pos;
            if available == 0 {
                self.frame = None;
                return Ok(false);
            }
            if available >= self.seen + HEADER_LEN
                && let Some(state) = check_bounds(&frame.payload()[pos..], self.seen)
            {
                self.end(tx, state);
                continue;
            }
            let Some(end) = record_end(&frame.payload()[pos..], self.seen) else {
                // the record straddles frames: let rustls check its header, if here, before
                // gathering it in `scratch`
                let header_end = self.seen + HEADER_LEN;
                if available >= header_end {
                    let header = &mut frame.payload_mut()[pos..pos + header_end];
                    if let Some(state) = check_header(tx, header, self.seen) {
                        self.end(tx, state);
                        continue;
                    }
                }
                self.scratch.extend_from_slice(&frame.payload()[pos..]);
                self.scratch_stamp = frame.rx_timestamp();
                self.frame = None;
                return Ok(false);
            };
            match process(tx, &mut frame.payload_mut()[pos..pos + end], self.seen) {
                Step::Record {
                    consumed,
                    start,
                    len,
                } => {
                    let stamp = frame.rx_timestamp();
                    self.start_run(Src::Frame, end, consumed, pos + start..pos + start + len);
                    self.run.stamp = stamp;
                    return Ok(true);
                }
                Step::NeedMore { consumed } => {
                    self.pos += consumed;
                    self.seen = end - consumed;
                }
                Step::End(state) => self.end(tx, state),
            }
        }
    }

    /// Makes the record rustls returned from the first `end` bytes of the source the run.
    fn start_run(&mut self, src: Src, end: usize, consumed: usize, at: Range<usize>) {
        // rustls rejects application data between the records of a handshake message, so
        // nothing before the record is still held
        assert_eq!(consumed, end, "rustls returned a record behind held bytes");
        self.seen = 0;
        self.returned = consumed;
        self.run = Run { src, at, stamp: 0 };
    }

    /// Drops the used-up run: its record's bytes in the source, or the carry.
    fn drop_run(&mut self) {
        let returned = core::mem::take(&mut self.returned);
        match self.run.src {
            Src::Frame => self.pos += returned,
            Src::Scratch => {
                self.scratch.drain(..returned);
            }
            Src::Carry => self.carry.clear(),
        }
        self.run = Run::NONE;
    }

    fn run_chunk(&mut self) -> Chunk<'_> {
        let at = self.run.at.clone();
        let bytes = match self.run.src {
            Src::Frame => &mut self.frame.as_mut().expect("the run's frame").payload_mut()[at],
            Src::Scratch => &mut self.scratch[at],
            Src::Carry => &mut self.carry[at],
        };
        Chunk {
            bytes,
            rx_timestamp: self.run.stamp,
        }
    }

    /// Appends bytes of the current frame to `scratch` up to the end of the next record's
    /// header, or once that is there, up to the end of the record. Returns what `scratch`
    /// holds up to the returned length.
    fn gather(&mut self) -> (Gathered, usize) {
        let header_end = self.seen + HEADER_LEN;
        let (target, gathered) = if self.scratch.len() < header_end {
            (header_end, Gathered::Header)
        } else {
            (body_end(&self.scratch, self.seen), Gathered::Record)
        };
        if self.scratch.len() < target {
            let Some(frame) = &self.frame else {
                return (Gathered::NeedFrame, target);
            };
            let available = &frame.payload()[self.pos..];
            let n = available.len().min(target - self.scratch.len());
            if self.scratch.len() == self.seen && n > 0 {
                self.scratch_stamp = frame.rx_timestamp();
            }
            self.scratch.extend_from_slice(&available[..n]);
            self.pos += n;
            if self.pos == frame.payload().len() {
                self.frame = None;
            }
            if self.scratch.len() < target {
                return (Gathered::NeedFrame, target);
            }
        }
        (gathered, target)
    }

    /// Latches `state`; an error also stops the send half.
    fn end(&mut self, tx: &mut TlsTx<L>, state: State) {
        if let State::Failed(e) = &state {
            tx.fail(e.clone());
        }
        self.state = state;
        self.frame = None;
        self.scratch.clear();
        self.seen = 0;
        self.carry.clear();
        self.run = Run::NONE;
        self.returned = 0;
    }
}

impl<F: RxFrame, L: FrameTx> TlsRx<F, L> {
    /// Hands over the next frame. Allowed only after `chunk` returned `Ok(None)`. Frames
    /// pushed after close_notify or an error are dropped.
    pub fn push(&mut self, frame: F) {
        assert!(
            self.frame.is_none(),
            "push before chunk returned None: the current frame still holds unprocessed bytes"
        );
        if matches!(self.state, State::Open) {
            self.frame = Some(frame);
            self.pos = 0;
        }
    }

    /// The unconsumed plaintext of the current record, else the next record's, decrypted now;
    /// `Ok(None)` if more frames are needed or the peer has closed (see [`TlsRx::eof`]). A run
    /// borrows `self`, so the next `chunk` cannot run while it is held:
    ///
    /// ```compile_fail,E0499
    /// # use sheath::{FrameTx, RxFrame};
    /// # fn f<F: RxFrame, L: FrameTx>(rx: &mut sheath::TlsRx<F, L>, tx: &mut sheath::TlsTx<L>) {
    /// let first = rx.chunk(tx).unwrap().unwrap();
    /// let second = rx.chunk(tx);
    /// first.bytes[0] = 0;
    /// # }
    /// ```
    pub fn chunk(&mut self, tx: &mut TlsTx<L>) -> Result<Option<Chunk<'_>>, Error> {
        if self.run.at.is_empty() {
            self.drop_run();
            let found = self.next_run(tx);
            // what rustls holds and wants sent must be visible to the send half now
            tx.hold(&self.scratch[..self.seen]);
            tx.collect();
            if !found? {
                return Ok(None);
            }
        }
        Ok(Some(self.run_chunk()))
    }

    /// The unconsumed plaintext of the run the last `chunk` returned (empty if none): exactly
    /// those bytes, unchanged, until the next `chunk`, `push` or `take_frame`. relay relies on
    /// this to read text it validated at receipt without validating again.
    pub fn current(&self) -> &[u8] {
        let at = self.run.at.clone();
        match self.run.src {
            Src::Frame => &self.frame.as_ref().expect("the run's frame").payload()[at],
            Src::Scratch => &self.scratch[at],
            Src::Carry => &self.carry[at],
        }
    }

    /// Marks the first `n` bytes of `current()` consumed.
    pub fn consume(&mut self, n: usize) {
        assert!(n <= self.run.at.len(), "consume past the current run");
        self.run.at.start += n;
    }

    /// Moves out the frame holding the current record: its plaintext's first `len` bytes are
    /// consumed, the rest of the record's plaintext is copied to a carry buffer and the bytes
    /// after the record to `scratch`, so later chunks are unaffected.
    pub fn take_frame(&mut self, len: usize) -> Option<(F, usize)> {
        if self.run.src != Src::Frame || self.run.at.is_empty() {
            return None;
        }
        assert!(len <= self.run.at.len(), "take_frame past the current run");
        let frame = self.frame.take().expect("the run's frame");
        let start = self.run.at.start;
        let payload = frame.payload();
        self.carry.clear();
        self.carry
            .extend_from_slice(&payload[start + len..self.run.at.end]);
        debug_assert!(self.scratch.is_empty() && self.seen == 0);
        self.scratch
            .extend_from_slice(&payload[self.pos + self.returned..]);
        self.scratch_stamp = frame.rx_timestamp();
        self.pos = 0;
        self.returned = 0;
        self.run = Run {
            src: Src::Carry,
            at: 0..self.carry.len(),
            stamp: self.run.stamp,
        };
        Some((frame, start))
    }
}

enum Gathered {
    NeedFrame,
    /// `scratch` holds, up to the target, the header of a record whose body is still to come.
    Header,
    /// `scratch` holds, up to the target, a whole record.
    Record,
}

/// End of the record starting at `start` in `buf`, given its header is there.
fn body_end(buf: &[u8], start: usize) -> usize {
    start + HEADER_LEN + usize::from(u16::from_be_bytes([buf[start + 3], buf[start + 4]]))
}

/// End of the record starting at `start` in `buf`, if the whole record is there.
fn record_end(buf: &[u8], start: usize) -> Option<usize> {
    if buf.len() < start + HEADER_LEN {
        return None;
    }
    let end = body_end(buf, start);
    (end <= buf.len()).then_some(end)
}

enum Step {
    /// A record's plaintext lies at `start..start + len`; `consumed` bytes from the front are
    /// done with once it has been read.
    Record {
        consumed: usize,
        start: usize,
        len: usize,
    },
    /// rustls processed `consumed` bytes from the front and needs more after the rest.
    NeedMore {
        consumed: usize,
    },
    End(State),
}

/// Runs rustls over `buf`, which starts with the `held` bytes rustls holds, until it returns a
/// record, needs more bytes, or ends the stream. Records rustls wants sent on the way go to the
/// send half.
fn process<L: FrameTx>(tx: &mut TlsTx<L>, buf: &mut [u8], held: usize) -> Step {
    let base = buf.as_ptr() as usize;
    let mut done = 0;
    loop {
        let UnbufferedStatus { discard, state } = tx.conn.process_tls_records(&mut buf[done..]);
        match state {
            Ok(ConnectionState::ReadTraffic(mut traffic)) => {
                return match traffic.next_record().expect("ReadTraffic holds a record") {
                    Ok(record) => Step::Record {
                        consumed: done + discard + record.discard,
                        start: record.payload.as_ptr() as usize - base,
                        len: record.payload.len(),
                    },
                    Err(e) => Step::End(State::Failed(Error::Tls(e))),
                };
            }
            Ok(ConnectionState::WriteTraffic(_)) => {
                return Step::NeedMore {
                    consumed: done + discard,
                };
            }
            Ok(ConnectionState::PeerClosed) => return Step::End(State::Closed),
            Ok(ConnectionState::EncodeTlsData(mut data)) => encode_into(&mut tx.pending, &mut data),
            Ok(ConnectionState::TransmitTlsData(data)) => data.done(),
            Ok(state) => panic!("rustls produced {state:?} while receiving after the handshake"),
            Err(e) => {
                // collect the alert rustls queued, giving it only the bytes it holds, so it does not
                // process the rest of `buf` again
                tx.collect_from(&mut buf[..held]);
                return Step::End(State::Failed(Error::Tls(e)));
            }
        }
        done += discard;
    }
}

/// Checks the length in the complete header of the record at `start` in `buf`, before its body
/// is gathered or decrypted. Returns the end state on rejection.
fn check_bounds(buf: &[u8], start: usize) -> Option<State> {
    let end = body_end(buf, start);
    let error = if end - start - HEADER_LEN > MAX_CIPHERTEXT_LEN {
        rustls::Error::PeerSentOversizedRecord
    } else if end > MAX_RETAINED_LEN {
        rustls::Error::InvalidMessage(rustls::InvalidMessage::MessageTooLarge)
    } else {
        return None;
    };
    Some(State::Failed(Error::Tls(error)))
}

/// Gives rustls `buf`, which ends with the header of a record whose body has not arrived, so
/// it rejects a bad header before the body is gathered. Returns the end state on rejection.
fn check_header<L: FrameTx>(tx: &mut TlsTx<L>, buf: &mut [u8], held: usize) -> Option<State> {
    match process(tx, buf, held) {
        Step::NeedMore { consumed } => {
            assert_eq!(consumed, 0, "rustls consumed part of an incomplete record");
            None
        }
        Step::End(state) => Some(state),
        Step::Record { .. } => unreachable!("rustls returned an incomplete record"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_end_needs_header_and_body() {
        let buf = [9, 9, 0x17, 0x03, 0x03, 0x00, 0x02, 1, 2, 0x17];
        assert_eq!(record_end(&buf[..6], 2), None);
        assert_eq!(body_end(&buf, 2), 9);
        assert_eq!(record_end(&buf[..8], 2), None);
        assert_eq!(record_end(&buf[..9], 2), Some(9));
        assert_eq!(record_end(&buf, 2), Some(9));
        assert_eq!(record_end(&buf, 9), None);
    }
}
