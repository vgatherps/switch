use rustls::ProtocolVersion;
use rustls::client::{ClientConnectionData, UnbufferedClientConnection};
use rustls::unbuffered::{ConnectionState, WriteTraffic};

use crate::{PlainTx, TxError, TxFrame, encode_into};

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

/// Send half of a TLS 1.3 client connection; owns the rustls connection.
///
/// One frame at a time: a frame filled by [`PlainTx::seal`], [`TlsTx::close_notify`] or
/// [`TlsTx::take_pending`] must go out through [`PlainTx::try_send`] (retried with the same
/// frame while it is handed back) before the next. A sealed frame has used a sequence number,
/// so dropping it would break every later record; the next of those three calls panics if the
/// last frame was not sent.
pub struct TlsTx<L> {
    pub(crate) conn: UnbufferedClientConnection,
    lower: L,
    /// The plaintext of the frame being sealed: rustls encrypts from one buffer into another.
    plaintext: Vec<u8>,
    /// Records rustls queued itself (the alert after an error), for `take_pending`.
    pub(crate) pending: Vec<u8>,
    failed: Option<rustls::Error>,
    closed: bool,
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
            failed: None,
            closed: false,
            in_flight: false,
        }
    }

    /// Writes records rustls queued itself, the alert after a receive error, into `f`; `false`
    /// if there are none. Call it before sealing, and send `f` like a sealed frame.
    pub fn take_pending(&mut self, f: &mut L::Frame) -> Result<bool, TxError<L::Error>> {
        self.check_sent();
        collect(&mut self.conn, &mut self.pending);
        if self.pending.is_empty() {
            return Ok(false);
        }
        let (head, room) = self.lower.plain();
        let n = self.pending.len().min(room);
        f.room_mut()[head..head + n].copy_from_slice(&self.pending[..n]);
        self.lower.seal(f, n).map_err(TxError::Lower)?;
        self.pending.drain(..n);
        self.in_flight = true;
        Ok(true)
    }

    /// Seals close_notify into `f`. Later seals fail with [`TxError::Closed`]; receiving goes on.
    pub fn close_notify(&mut self, f: &mut L::Frame) -> Result<(), TxError<L::Error>> {
        self.check_sent();
        let (head, room) = self.lower.plain();
        let mut traffic = self.write_traffic()?;
        let n = traffic
            .queue_close_notify(&mut f.room_mut()[head..head + room])
            .expect("a frame fits a close_notify record");
        self.closed = true;
        self.lower.seal(f, n).map_err(TxError::Lower)?;
        self.in_flight = true;
        Ok(())
    }

    /// Stops sending after a receive error.
    pub(crate) fn fail(&mut self, e: rustls::Error) {
        self.failed.get_or_insert(e);
    }

    fn check_sent(&self) {
        assert!(
            !self.in_flight,
            "the last frame from seal, close_notify or take_pending was not sent"
        );
    }

    fn write_traffic(
        &mut self,
    ) -> Result<WriteTraffic<'_, ClientConnectionData>, TxError<L::Error>> {
        if let Some(e) = &self.failed {
            return Err(TxError::Tls(e.clone()));
        }
        if self.closed {
            return Err(TxError::Closed);
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
    type Error = TxError<L::Error>;

    fn alloc(&mut self) -> Result<L::Frame, Self::Error> {
        self.lower.alloc().map_err(TxError::Lower)
    }

    fn plain(&self) -> (usize, usize) {
        let (head, room) = self.lower.plain();
        (head + RECORD_HEAD, room - OVERHEAD)
    }

    /// Encrypts the plaintext into one record in place of it, behind any KeyUpdate records
    /// rustls has to send first.
    fn seal(&mut self, f: &mut L::Frame, len: usize) -> Result<(), Self::Error> {
        self.check_sent();
        let (head, room) = self.lower.plain();
        assert!(
            len <= room - OVERHEAD,
            "{len} B of plaintext exceed the frame room"
        );
        let start = head + RECORD_HEAD;
        self.plaintext.clear();
        self.plaintext
            .extend_from_slice(&f.room_mut()[start..start + len]);
        let plaintext = core::mem::take(&mut self.plaintext);
        let sealed = self.write_traffic().map(|mut traffic| {
            traffic
                .encrypt(&plaintext, &mut f.room_mut()[head..head + room])
                .expect("frame room reserved for the record and KeyUpdates")
        });
        self.plaintext = plaintext;
        self.lower.seal(f, sealed?).map_err(TxError::Lower)?;
        self.in_flight = true;
        Ok(())
    }

    fn try_send(&mut self, f: L::Frame) -> Result<(), L::Frame> {
        self.lower.try_send(f)?;
        self.in_flight = false;
        Ok(())
    }
}
