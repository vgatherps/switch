//! TLS 1.3 client over TCP, on rustls's unbuffered client. Applications reach it as
//! `switch::tls` (feature `tls`): [`net::tls_connect`] runs the handshake on a connected TCP
//! stream and returns a [`net::Tls`] that reads and writes plaintext.
//!
//! The items at the root are the TLS layer itself, over any frame source that implements
//! [`RxFrame`], [`TxFrame`] and [`FrameTx`]. `switch::ws` runs its `wss://` connections on them.
//! They build without DPDK.

use std::fmt;

use rustls::unbuffered::{EncodeError, EncodeTlsData};

mod rx;
mod tx;

#[cfg(feature = "switch-net")]
pub mod net;

pub use rx::TlsRx;
pub use tx::{CLOSE_NOTIFY_LEN, Sealed, TlsTx};

#[cfg(doctest)]
mod api_guards {
    //! A run borrows the receive half, so the next `chunk` cannot run while it is held:
    //!
    //! ```compile_fail,E0499
    //! # use switch_tls::{FrameTx, RxFrame};
    //! # fn f<F: RxFrame, L: FrameTx>(rx: &mut switch_tls::TlsRx<F, L>, tx: &mut switch_tls::TlsTx<L>) {
    //! let first = rx.chunk(tx).unwrap().unwrap();
    //! let second = rx.chunk(tx);
    //! first.bytes[0] = 0;
    //! # }
    //! ```
    //!
    //! Only `TlsTx::try_send` takes a `Sealed` frame, so a frame that was not sealed cannot be
    //! sent in place of a sealed one:
    //!
    //! ```compile_fail,E0308
    //! # use switch_tls::FrameTx;
    //! async fn send_unsealed<L: FrameTx>(tx: &mut switch_tls::TlsTx<L>) {
    //!     let f = tx.alloc().await.ok().unwrap();
    //!     let _ = tx.try_send(f);
    //! }
    //! ```
}

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
    /// Waits until retrying a sealed frame carrying `len` plaintext bytes may succeed.
    /// `try_send` can still return the frame when full; wait for room and retry it.
    /// `Err` once the transport can no longer send.
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

/// Why receiving failed. Receiving and sending are both over: later receives return the same
/// error, and sends fail with [`TxError::Received`].
#[derive(Clone, Debug, PartialEq)]
pub enum Error {
    /// rustls rejected the server's handshake, a received record, or (from
    /// [`net::tls_connect`]) the config.
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
    /// Receiving failed earlier with this error, which ends the connection.
    Received(Error),
    /// close_notify was already sent ([`TlsTx::close_notify`], [`TlsTx::send_close_notify`] or
    /// [`net::Tls::close`]).
    Closed,
    /// The frame sender below failed with this error.
    Lower(E),
    /// An encrypted record never reached the frame sender: a [`Sealed`] frame was dropped
    /// before `try_send` took it, or an earlier seal failed below (reported then as `Lower`).
    /// Its sequence number is gone, so the peer cannot decrypt any later record; every later
    /// send fails with `Broken`.
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
