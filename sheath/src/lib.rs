//! TLS 1.3 client over TCP frames, on rustls's unbuffered client.
//!
//! [`TlsRx`] hands each received frame's bytes to rustls, which decrypts records in place: a
//! record lying wholly inside one frame is returned as a slice of that frame. Only a record
//! straddling frames is copied, into one reusable scratch buffer, and decrypted there.
//! Decryption is lazy: one record per run.
//!
//! [`TlsTx`] owns the rustls connection and seals each frame's plaintext into one record in
//! that frame, on top of a lower [`FrameTx`] that sends frames as they are; it is a
//! [`FrameTx`] itself.
//!
//! Feature `switchnet`: module `switchnet`, TLS over switchnet TCP (handshake, then the two
//! halves), with switchnet's frames as [`RxFrame`]/[`TxFrame`].

use std::fmt;

use rustls::unbuffered::{EncodeError, EncodeTlsData};

mod rx;
mod tx;

#[cfg(feature = "switchnet")]
pub mod switchnet;

pub use rx::TlsRx;
pub use tx::{Sealed, TlsTx};

/// A received TCP segment's payload with its hardware RX timestamp.
pub trait RxFrame {
    fn payload(&self) -> &[u8];
    fn payload_mut(&mut self) -> &mut [u8];
    fn rx_timestamp(&self) -> u64;
}

/// One run of received plaintext: a record's, from where the last run was consumed.
pub struct Chunk<'a> {
    /// Never empty.
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
pub trait FrameTx {
    type Frame: TxFrame;
    /// A frame whose bytes are final. Only `seal` makes one and only `try_send` takes one, so
    /// an unsealed frame cannot be sent.
    type Sealed;
    type Error;

    /// A new, empty frame, once one is free.
    fn alloc(&mut self) -> impl Future<Output = Result<Self::Frame, Self::Error>>;
    /// Resolves once a sealed frame carrying `len` plaintext bytes can be sent: `try_send` of
    /// it then succeeds until the transport runs again. `Err` once it can no longer send.
    fn room(&mut self, len: usize) -> impl Future<Output = Result<(), Self::Error>>;
    /// `(head, room)`: plaintext starts at `room_mut()[head]`, and one frame carries at most
    /// `room` plaintext bytes.
    fn plain(&self) -> (usize, usize);
    /// Turns the plaintext `room_mut()[head..head + len]` into the bytes to send (TLS: one
    /// record; raw: sets the length). Under TLS this consumes a sequence number, so the sealed
    /// frame must then be sent, never dropped.
    fn seal(&mut self, f: Self::Frame, len: usize) -> Result<Self::Sealed, Self::Error>;
    /// Sends now, or hands the sealed frame back unchanged when there is no room.
    fn try_send(&mut self, s: Self::Sealed) -> Result<(), Self::Sealed>;
    /// Ends the transport after the frames sent (TCP FIN). Receiving goes on.
    fn shutdown(&mut self);
    /// After `shutdown`: resolves once the peer has everything sent, the end included (TCP:
    /// the FIN acked), or the transport can deliver nothing more (reset). What was sent may
    /// otherwise still sit in the transport, lost if it is torn down now.
    fn delivered(&mut self) -> impl Future<Output = ()>;
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
/// `take_pending` to send.
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
