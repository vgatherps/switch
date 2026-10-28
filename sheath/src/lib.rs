//! TLS 1.3 client over TCP frames, on rustls's unbuffered client.
//!
//! [`TlsRx`] hands each received frame's bytes to rustls, which decrypts records in place: a
//! record lying wholly inside one frame is returned as a slice of that frame. Only a record
//! straddling frames is copied, into one reusable scratch buffer, and decrypted there.
//! Decryption is lazy: rustls is given one record per step, and [`TlsRx::chunk`] returns at
//! most one record.
//!
//! [`TlsTx`] owns the rustls connection and seals each frame's plaintext into one record in
//! that frame, on top of a lower [`PlainTx`] that sends frames as they are.

use std::fmt;

use rustls::unbuffered::{EncodeError, EncodeTlsData};

mod rx;
mod tx;

pub use rx::TlsRx;
pub use tx::{Sealed, TlsTx};

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

/// A frame buffer to fill and send.
pub trait TxFrame {
    /// The whole writable room of the frame.
    fn room_mut(&mut self) -> &mut [u8];
}

/// A sender of frames: raw TCP below [`TlsTx`], and [`TlsTx`] itself.
pub trait PlainTx {
    type Frame: TxFrame;
    /// A frame whose bytes are final. Only `seal` makes one and only `try_send` takes one, so
    /// an unsealed frame cannot be sent.
    type Sealed;
    type Error;

    /// A new, empty frame.
    fn alloc(&mut self) -> Result<Self::Frame, Self::Error>;
    /// `(head, room)`: plaintext starts at `room_mut()[head]`, and one frame carries at most
    /// `room` plaintext bytes.
    fn plain(&self) -> (usize, usize);
    /// Turns the plaintext `room_mut()[head..head + len]` into the bytes to send (TLS: one
    /// record; raw: sets the length). Under TLS this consumes a sequence number, so the sealed
    /// frame must then be sent, never dropped.
    fn seal(&mut self, f: Self::Frame, len: usize) -> Result<Self::Sealed, Self::Error>;
    /// Sends now, or hands the frame back unchanged when there is no room.
    fn try_send(&mut self, f: Self::Sealed) -> Result<(), Self::Sealed>;
}

/// A receive error.
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

/// A send error.
#[derive(Clone, Debug, PartialEq)]
pub enum TxError<E> {
    /// Receiving failed (a TLS error or truncation), which ends the connection.
    Received(Error),
    /// We sent close_notify.
    Closed,
    /// The frame sender below failed.
    Lower(E),
    /// A record was encrypted but the frame sender below failed to take it: its sequence
    /// number is gone, so the peer cannot decrypt any later record.
    Broken,
}

impl<E: fmt::Display> fmt::Display for TxError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Received(e) => write!(f, "receiving failed: {e}"),
            Self::Closed => f.write_str("tls: close_notify already sent"),
            Self::Lower(e) => write!(f, "transport: {e}"),
            Self::Broken => f.write_str("tls: an encrypted record was lost below"),
        }
    }
}

impl<E: fmt::Debug + fmt::Display> std::error::Error for TxError<E> {}

/// Appends a record rustls queued itself (an alert, close_notify) to `pending`, for
/// [`TlsTx::take_pending`] to send.
fn encode_into<Data>(pending: &mut Vec<u8>, data: &mut EncodeTlsData<'_, Data>) {
    let len = match data.encode(&mut []) {
        Err(EncodeError::InsufficientSize(e)) => e.required_size,
        other => unreachable!("encoding into an empty buffer: {other:?}"),
    };
    let at = pending.len();
    pending.resize(at + len, 0);
    data.encode(&mut pending[at..])
        .expect("buffer sized as rustls asked");
}
