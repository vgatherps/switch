//! TLS 1.3 client over TCP frames, on rustls's unbuffered client, as relay's lower layer.
//!
//! [`TlsRx`] ([`relay::PlainRx`]) hands each received frame's bytes to rustls, which decrypts
//! records in place: a record lying wholly inside one frame is returned as a slice of that
//! frame. Only a record straddling frames is copied, into one reusable scratch buffer, and
//! decrypted there. Decryption is lazy: one record per run.
//!
//! [`TlsTx`] ([`relay::PlainTx`]) owns the rustls connection and seals each frame's plaintext
//! into one record in that frame, on top of a lower `PlainTx` that sends frames as they are.
//!
//! Feature `switchnet`: module `switchnet`, a wss client (TLS handshake, WebSocket upgrade,
//! relay messages) over switchnet TCP.

use std::fmt;

use rustls::unbuffered::{EncodeError, EncodeTlsData};

mod rx;
mod tx;

#[cfg(feature = "switchnet")]
pub mod switchnet;

pub use rx::TlsRx;
pub use tx::{Sealed, TlsTx};

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
