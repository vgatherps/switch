use rustls::ProtocolVersion;
use rustls::client::{ClientConnectionData, UnbufferedClientConnection};
use rustls::unbuffered::{ConnectionState, WriteTraffic};

use crate::{Error, PlainTx, TxError, TxFrame, encode_into};

/// A TLS 1.3 record's header, in front of the plaintext.
const RECORD_HEAD: usize = 5;
/// After the plaintext: the inner content type and the AEAD tag.
const RECORD_TAIL: usize = 1 + 16;
/// A KeyUpdate record: header, 4 B handshake header and 1 B request flag, content type, tag.
const KEY_UPDATE_LEN: usize = RECORD_HEAD + 4 + 1 + RECORD_TAIL;
/// rustls writes up to two KeyUpdate records in front of the next data record: its reply to a
/// peer's request, and its own update at the cipher suite's confidentiality limit.
const KEY_UPDATE_ROOM: usize = 2 * KEY_UPDATE_LEN;
/// Room per frame that is not plaintext.
const OVERHEAD: usize = RECORD_HEAD + RECORD_TAIL + KEY_UPDATE_ROOM;

/// A frame holding records that used sequence numbers. Only [`TlsTx`] makes one, and only
/// [`TlsTx::try_send`](PlainTx::try_send) takes one, so a frame that was not sealed cannot be
/// sent in place of the sealed one:
///
/// ```compile_fail,E0308
/// # use sheath::{PlainTx, TlsTx};
/// fn send_unsealed<L: PlainTx>(tx: &mut TlsTx<L>) {
///     let f = tx.alloc().ok().unwrap();
///     let _ = tx.try_send(f);
/// }
/// ```
#[derive(Debug)]
pub struct Sealed<S>(S);

#[derive(Debug)]
enum State {
    Open,
    Closed,
    Received(Error),
    Broken,
}

/// Send half of a TLS 1.3 client connection; owns the rustls connection.
///
/// One [`Sealed`] frame at a time: the one from [`PlainTx::seal`], [`TlsTx::close_notify`] or
/// [`TlsTx::take_pending`] must be accepted by [`PlainTx::try_send`] (offered again while it is
/// handed back) before the next is made. Its records used sequence numbers, so dropping it
/// would break every later record; the next of those three calls panics if it was dropped.
pub struct TlsTx<L> {
    pub(crate) conn: UnbufferedClientConnection,
    lower: L,
    /// The plaintext of the frame being sealed: rustls encrypts from one buffer into another.
    plaintext: Vec<u8>,
    /// Records rustls queued itself (the alert after an error), for `take_pending`.
    pub(crate) pending: Vec<u8>,
    state: State,
    /// A `Sealed` frame exists that the lower layer has not accepted.
    in_flight: bool,
}

impl<L: PlainTx> TlsTx<L> {
    /// `conn` must have completed a TLS 1.3 handshake. Panics if `lower`'s frames cannot carry
    /// a record with one byte of plaintext.
    pub fn new(conn: UnbufferedClientConnection, lower: L) -> Self {
        assert!(
            !conn.is_handshaking(),
            "TlsTx needs a connection whose handshake is complete"
        );
        assert_eq!(
            conn.protocol_version(),
            Some(ProtocolVersion::TLSv1_3),
            "TlsTx supports TLS 1.3 only"
        );
        let (_, room) = lower.plain();
        assert!(room > OVERHEAD, "frame room {room} B below one TLS record");
        Self {
            conn,
            lower,
            plaintext: Vec::with_capacity(room - OVERHEAD),
            pending: Vec::new(),
            state: State::Open,
            in_flight: false,
        }
    }

    /// Writes records rustls queued itself, the alert after a receive error, into `f`;
    /// `Ok(None)` if there are none. Call it before sealing.
    pub fn take_pending(
        &mut self,
        mut f: L::Frame,
    ) -> Result<Option<Sealed<L::Sealed>>, TxError<L::Error>> {
        self.check_sent();
        if matches!(self.state, State::Broken) {
            return Err(TxError::Broken);
        }
        collect(&mut self.conn, &mut self.pending);
        if self.pending.is_empty() {
            return Ok(None);
        }
        let (head, room) = self.lower.plain();
        let n = self.pending.len().min(room);
        f.room_mut()[head..head + n].copy_from_slice(&self.pending[..n]);
        self.pending.drain(..n);
        self.finish(f, n).map(Some)
    }

    /// Seals close_notify into `f`. Later seals fail with [`TxError::Closed`]; receiving goes on.
    pub fn close_notify(
        &mut self,
        mut f: L::Frame,
    ) -> Result<Sealed<L::Sealed>, TxError<L::Error>> {
        self.check_sent();
        let (head, room) = self.lower.plain();
        let mut traffic = self.write_traffic()?;
        let n = traffic
            .queue_close_notify(&mut f.room_mut()[head..head + room])
            .expect("a frame fits a close_notify record");
        self.state = State::Closed;
        self.finish(f, n)
    }

    /// Stops sending after a receive error.
    pub(crate) fn fail(&mut self, e: Error) {
        if matches!(self.state, State::Open | State::Closed) {
            self.state = State::Received(e);
        }
    }

    fn check_sent(&self) {
        assert!(
            !self.in_flight,
            "the last sealed frame was not sent: it is still held or was dropped"
        );
    }

    fn write_traffic(
        &mut self,
    ) -> Result<WriteTraffic<'_, ClientConnectionData>, TxError<L::Error>> {
        match &self.state {
            State::Open => {}
            State::Closed => return Err(TxError::Closed),
            State::Received(e) => return Err(TxError::Received(e.clone())),
            State::Broken => return Err(TxError::Broken),
        }
        collect(&mut self.conn, &mut self.pending);
        assert!(
            self.pending.is_empty(),
            "rustls queued a record while the connection is healthy; send it with take_pending first"
        );
        match self.conn.process_tls_records(&mut []).state {
            Ok(ConnectionState::WriteTraffic(traffic)) => Ok(traffic),
            state => {
                panic!("rustls produced {state:?} instead of WriteTraffic after the handshake")
            }
        }
    }

    /// Hands `f`, whose first `n` bytes from `head` are encrypted records, to the lower layer.
    /// Their sequence numbers are used, so a failure here breaks the connection for good.
    fn finish(&mut self, f: L::Frame, n: usize) -> Result<Sealed<L::Sealed>, TxError<L::Error>> {
        self.in_flight = true;
        match self.lower.seal(f, n) {
            Ok(sealed) => Ok(Sealed(sealed)),
            Err(e) => {
                self.state = State::Broken;
                self.in_flight = false;
                Err(TxError::Lower(e))
            }
        }
    }
}

/// Moves records rustls queued itself into `pending`.
fn collect(conn: &mut UnbufferedClientConnection, pending: &mut Vec<u8>) {
    loop {
        match conn.process_tls_records(&mut []).state {
            Ok(ConnectionState::EncodeTlsData(mut data)) => encode_into(pending, &mut data),
            Ok(ConnectionState::TransmitTlsData(data)) => data.done(),
            _ => return,
        }
    }
}

impl<L: PlainTx> PlainTx for TlsTx<L> {
    type Frame = L::Frame;
    type Sealed = Sealed<L::Sealed>;
    type Error = TxError<L::Error>;

    fn alloc(&mut self) -> Result<L::Frame, Self::Error> {
        self.lower.alloc().map_err(TxError::Lower)
    }

    fn plain(&self) -> (usize, usize) {
        let (head, room) = self.lower.plain();
        (head + RECORD_HEAD, room - OVERHEAD)
    }

    /// Encrypts the plaintext into one record in place of it, behind any KeyUpdate records
    /// rustls has to send first. Panics if `len` is 0 (rustls makes no record of nothing) or
    /// exceeds the room.
    fn seal(&mut self, mut f: L::Frame, len: usize) -> Result<Self::Sealed, Self::Error> {
        self.check_sent();
        let (head, room) = self.lower.plain();
        assert!(len > 0, "sealing an empty record");
        assert!(
            len <= room - OVERHEAD,
            "{len} B of plaintext exceed the frame room"
        );
        let start = head + RECORD_HEAD;
        self.plaintext.clear();
        self.plaintext
            .extend_from_slice(&f.room_mut()[start..start + len]);
        let plaintext = core::mem::take(&mut self.plaintext);
        let encrypted = self.write_traffic().map(|mut traffic| {
            traffic
                .encrypt(&plaintext, &mut f.room_mut()[head..head + room])
                .expect("frame room reserved for the record and KeyUpdates")
        });
        self.plaintext = plaintext;
        self.finish(f, encrypted?)
    }

    fn try_send(&mut self, f: Self::Sealed) -> Result<(), Self::Sealed> {
        self.lower.try_send(f.0).map_err(Sealed)?;
        self.in_flight = false;
        Ok(())
    }
}
