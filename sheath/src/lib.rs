//! TLS 1.3 client receive path over received TCP frames.
//!
//! [`TlsRx`] hands each frame's bytes to rustls's unbuffered client, which decrypts records in
//! place: a record lying wholly inside one frame is returned as a slice of that frame. Only a
//! record straddling frames is copied, into one reusable scratch buffer, and decrypted there.
//! Decryption is lazy: rustls is given one record per step, and [`TlsRx::chunk`] returns at
//! most one record.

use std::fmt;

use rustls::ProtocolVersion;
use rustls::client::UnbufferedClientConnection;
use rustls::unbuffered::{ConnectionState, UnbufferedStatus};

const HEADER_LEN: usize = 5;

/// Largest TLS 1.3 record on the wire: header, 2^14 B of plaintext, 256 B of expansion
/// (RFC 8446 §5.2). The scratch buffer is allocated once at this size.
const MAX_RECORD_LEN: usize = HEADER_LEN + (1 << 14) + 256;

/// A received TCP segment's payload with its hardware RX timestamp.
pub trait RxFrame {
    fn payload(&self) -> &[u8];
    fn payload_mut(&mut self) -> &mut [u8];
    fn rx_timestamp(&self) -> u64;
}

/// One record's authenticated plaintext.
#[derive(Debug)]
pub struct Chunk<'a> {
    pub bytes: &'a mut [u8],
    /// Stamp of the frame holding the record's first byte.
    pub rx_timestamp: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Error {
    Tls(rustls::Error),
    /// The transport ended without the peer's close_notify.
    Truncated,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tls(e) => write!(f, "tls: {e}"),
            Self::Truncated => {
                f.write_str("tls stream truncated: transport closed without close_notify")
            }
        }
    }
}

impl std::error::Error for Error {}

#[derive(Debug)]
enum State {
    Open,
    /// The peer sent close_notify; later bytes are ignored (RFC 8446 §6.1).
    Closed,
    Failed(Error),
}

/// Receive half of a TLS 1.3 client connection, fed with frames in stream order.
///
/// The rustls connection is passed to each [`TlsRx::chunk`] call, because the send half
/// drives the same connection.
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

impl<F: RxFrame> TlsRx<F> {
    /// `conn` must have completed a TLS 1.3 handshake.
    pub fn new(conn: &UnbufferedClientConnection) -> Self {
        assert!(
            !conn.is_handshaking(),
            "TlsRx needs a connection whose handshake is complete"
        );
        assert_eq!(
            conn.protocol_version(),
            Some(ProtocolVersion::TLSv1_3),
            "TlsRx supports TLS 1.3 only"
        );
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
    /// # fn f<F: sheath::RxFrame>(rx: &mut sheath::TlsRx<F>, conn: &mut rustls::client::UnbufferedClientConnection) {
    /// let first = rx.chunk(conn).unwrap().unwrap();
    /// let second = rx.chunk(conn);
    /// first.bytes[0] = 0;
    /// # }
    /// ```
    pub fn chunk(
        &mut self,
        conn: &mut UnbufferedClientConnection,
    ) -> Result<Option<Chunk<'_>>, Error> {
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
                        if let Some(state) = check_header(conn, &mut self.scratch) {
                            self.end(state);
                        }
                    }
                    Gathered::Record => {
                        let end = self.scratch.len();
                        match process(conn, &mut self.scratch) {
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
                            Step::End(state) => self.end(state),
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
            let Some(end) = record_end(&frame.payload()[pos..], self.seen) else {
                // the record straddles frames: let rustls check its header, if here, before
                // gathering it in `scratch`
                let header_end = self.seen + HEADER_LEN;
                if available >= header_end {
                    let header = &mut frame.payload_mut()[pos..pos + header_end];
                    if let Some(state) = check_header(conn, header) {
                        self.end(state);
                        continue;
                    }
                }
                self.scratch.extend_from_slice(&frame.payload()[pos..]);
                self.scratch_stamp = frame.rx_timestamp();
                self.frame = None;
                return Ok(None);
            };
            match process(conn, &mut frame.payload_mut()[pos..pos + end]) {
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
                Step::End(state) => self.end(state),
            }
        }
    }

    /// The peer closed the stream with close_notify.
    pub fn eof(&self) -> bool {
        matches!(self.state, State::Closed)
    }

    /// The transport ended (TCP FIN); call once `chunk` returns `Ok(None)`. `Ok` iff the peer
    /// sent close_notify first; otherwise the stream was truncated.
    pub fn fin(&mut self) -> Result<(), Error> {
        match &self.state {
            State::Open => {
                self.end(State::Failed(Error::Truncated));
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

    fn end(&mut self, state: State) {
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

fn process(conn: &mut UnbufferedClientConnection, buf: &mut [u8]) -> Step {
    let base = buf.as_ptr() as usize;
    let UnbufferedStatus { discard, state } = conn.process_tls_records(buf);
    match state {
        Ok(ConnectionState::ReadTraffic(mut traffic)) => {
            match traffic.next_record().expect("ReadTraffic holds a record") {
                Ok(record) => Step::Record {
                    consumed: discard + record.discard,
                    start: record.payload.as_ptr() as usize - base,
                    len: record.payload.len(),
                },
                Err(e) => Step::End(State::Failed(Error::Tls(e))),
            }
        }
        Ok(ConnectionState::WriteTraffic(_)) => Step::NeedMore { consumed: discard },
        Ok(ConnectionState::PeerClosed) => Step::End(State::Closed),
        Ok(state) => panic!(
            "rustls produced {state:?} while receiving after a TLS 1.3 handshake; \
             sending is not supported yet"
        ),
        Err(e) => Step::End(State::Failed(Error::Tls(e))),
    }
}

/// Gives rustls `buf`, which ends with the header of a record whose body has not arrived, so
/// it rejects a bad header before the body is gathered. Returns the end state on rejection.
fn check_header(conn: &mut UnbufferedClientConnection, buf: &mut [u8]) -> Option<State> {
    match process(conn, buf) {
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
