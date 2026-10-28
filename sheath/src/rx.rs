use rustls::unbuffered::{ConnectionState, UnbufferedStatus};

use crate::{Chunk, Error, PlainTx, RxFrame, TlsTx, encode_into};

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

/// Receive half of a TLS 1.3 client connection, fed with frames in stream order.
///
/// The rustls connection lives in [`TlsTx`], which each [`TlsRx::chunk`] call borrows. Records
/// rustls wants sent while receiving go to the send half's [`TlsTx::take_pending`], and a
/// receive error stops the send half too.
///
/// The bytes being worked on (the source) are `scratch` if it is non-empty, else the current
/// frame from `pos`. The source starts with `seen` bytes that rustls has processed but must
/// see again (records of a handshake message that is still incomplete, which rustls joins in
/// place, so their headers are no longer readable); the next record starts after them.
pub struct TlsRx<F> {
    frame: Option<F>,
    /// Bytes at the front of `frame` that rustls has consumed.
    pos: usize,
    /// A record straddling frames, after the `seen` bytes that came with it.
    scratch: Vec<u8>,
    /// Stamp of the frame holding the first byte of the record after the `seen` bytes in
    /// `scratch`.
    scratch_stamp: u64,
    seen: usize,
    /// Bytes at the front of the source taken by the record returned by the last `chunk`;
    /// consumed at the next call.
    returned: usize,
    state: State,
}

impl<F: RxFrame> Default for TlsRx<F> {
    fn default() -> Self {
        Self::new()
    }
}

impl<F: RxFrame> TlsRx<F> {
    pub fn new() -> Self {
        Self {
            frame: None,
            pos: 0,
            scratch: Vec::with_capacity(MAX_RECORD_LEN),
            scratch_stamp: 0,
            seen: 0,
            returned: 0,
            state: State::Open,
        }
    }

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

    /// Decrypts and returns the next record's plaintext; `Ok(None)` if more frames are needed
    /// or the peer has closed (see [`TlsRx::eof`]). The plaintext stays valid until the next
    /// `chunk` or `push`, which the borrow enforces:
    ///
    /// ```compile_fail,E0499
    /// # fn f<F: sheath::RxFrame, L: sheath::PlainTx>(rx: &mut sheath::TlsRx<F>, tx: &mut sheath::TlsTx<L>) {
    /// let first = rx.chunk(tx).unwrap().unwrap();
    /// let second = rx.chunk(tx);
    /// first.bytes[0] = 0;
    /// # }
    /// ```
    pub fn chunk<L: PlainTx>(&mut self, tx: &mut TlsTx<L>) -> Result<Option<Chunk<'_>>, Error> {
        self.consume_returned();
        loop {
            match &self.state {
                State::Open => {}
                State::Closed => return Ok(None),
                State::Failed(e) => return Err(e.clone()),
            }

            if !self.scratch.is_empty() {
                match self.gather() {
                    Gathered::NeedFrame => return Ok(None),
                    Gathered::Header => {
                        let state = check_bounds(&self.scratch, self.seen)
                            .or_else(|| check_header(tx, &mut self.scratch));
                        if let Some(state) = state {
                            self.end(tx, state);
                        }
                    }
                    Gathered::Record => {
                        let end = self.scratch.len();
                        match process(tx, &mut self.scratch) {
                            Step::Record {
                                consumed,
                                start,
                                len,
                            } => {
                                self.returned = consumed;
                                self.seen = end - consumed;
                                return Ok(Some(Chunk {
                                    bytes: &mut self.scratch[start..start + len],
                                    rx_timestamp: self.scratch_stamp,
                                }));
                            }
                            Step::NeedMore { consumed } => {
                                self.scratch.drain(..consumed);
                                self.seen = end - consumed;
                            }
                            Step::End(state) => self.end(tx, state),
                        }
                    }
                }
                continue;
            }

            let Some(frame) = &mut self.frame else {
                return Ok(None);
            };
            let pos = self.pos;
            let available = frame.payload().len() - pos;
            if available == 0 {
                self.frame = None;
                return Ok(None);
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
                    if let Some(state) = check_header(tx, header) {
                        self.end(tx, state);
                        continue;
                    }
                }
                self.scratch.extend_from_slice(&frame.payload()[pos..]);
                self.scratch_stamp = frame.rx_timestamp();
                self.frame = None;
                return Ok(None);
            };
            match process(tx, &mut frame.payload_mut()[pos..pos + end]) {
                Step::Record {
                    consumed,
                    start,
                    len,
                } => {
                    self.returned = consumed;
                    self.seen = end - consumed;
                    let frame = self.frame.as_mut().expect("frame checked above");
                    let rx_timestamp = frame.rx_timestamp();
                    return Ok(Some(Chunk {
                        bytes: &mut frame.payload_mut()[pos + start..pos + start + len],
                        rx_timestamp,
                    }));
                }
                Step::NeedMore { consumed } => {
                    self.pos += consumed;
                    self.seen = end - consumed;
                }
                Step::End(state) => self.end(tx, state),
            }
        }
    }

    /// The peer closed the stream with close_notify.
    pub fn eof(&self) -> bool {
        matches!(self.state, State::Closed)
    }

    /// The transport ended (TCP FIN); call once `chunk` returns `Ok(None)`. `Ok` iff the peer
    /// sent close_notify first; otherwise the stream was truncated, which stops sending too.
    pub fn fin<L: PlainTx>(&mut self, tx: &mut TlsTx<L>) -> Result<(), Error> {
        match &self.state {
            State::Open => {
                self.end(tx, State::Failed(Error::Truncated));
                Err(Error::Truncated)
            }
            State::Closed => Ok(()),
            State::Failed(e) => Err(e.clone()),
        }
    }

    fn consume_returned(&mut self) {
        let returned = core::mem::take(&mut self.returned);
        if self.scratch.is_empty() {
            self.pos += returned;
        } else {
            self.scratch.drain(..returned);
        }
    }

    /// Appends bytes of the current frame to `scratch` up to the end of the next record's
    /// header, or once that is there, up to the end of the record.
    fn gather(&mut self) -> Gathered {
        let Some(frame) = &self.frame else {
            return Gathered::NeedFrame;
        };
        let header_end = self.seen + HEADER_LEN;
        let (target, gathered) = if self.scratch.len() < header_end {
            (header_end, Gathered::Header)
        } else {
            (body_end(&self.scratch, self.seen), Gathered::Record)
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
            Gathered::NeedFrame
        } else {
            gathered
        }
    }

    /// Latches `state`; an error also stops the send half.
    fn end<L: PlainTx>(&mut self, tx: &mut TlsTx<L>, state: State) {
        if let State::Failed(e) = &state {
            tx.fail(e.clone());
        }
        self.state = state;
        self.frame = None;
        self.scratch.clear();
    }
}

enum Gathered {
    NeedFrame,
    /// `scratch` ends with the header of a record whose body is still to come.
    Header,
    /// `scratch` ends with a whole record.
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

/// Runs rustls over `buf` until it returns a record, needs more bytes, or ends the stream.
/// Records rustls wants sent on the way go to the send half.
fn process<L: PlainTx>(tx: &mut TlsTx<L>, buf: &mut [u8]) -> Step {
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
            Err(e) => return Step::End(State::Failed(Error::Tls(e))),
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
fn check_header<L: PlainTx>(tx: &mut TlsTx<L>, buf: &mut [u8]) -> Option<State> {
    match process(tx, buf) {
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
