use rustls::ProtocolVersion;
use rustls::client::{ClientConnectionData, UnbufferedClientConnection};
use rustls::unbuffered::{ConnectionState, UnbufferedStatus, WriteTraffic};

use crate::{Error, FrameTx, TxError, TxFrame, encode_into};

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
pub(crate) const OVERHEAD: usize = RECORD_HEAD + RECORD_TAIL + KEY_UPDATE_ROOM;

/// Plaintext one frame of `room` bytes carries as one record: the room less the record
/// overhead, and at most one record's plaintext under the client's `max_fragment_size`
/// (rustls's limit counts the record header and is at least 32 B).
pub(crate) fn plain_room(room: usize, max_fragment_size: Option<usize>) -> usize {
    let plain = room - OVERHEAD;
    max_fragment_size.map_or(plain, |max| plain.min(max - RECORD_HEAD))
}

/// A frame holding records that used sequence numbers. Only [`TlsTx`] makes one, and only
/// [`TlsTx::try_send`](FrameTx::try_send) takes one, so a frame that was not sealed cannot be
/// sent in place of the sealed one:
///
/// ```compile_fail,E0308
/// # use sheath::FrameTx;
/// async fn send_unsealed<L: FrameTx>(tx: &mut sheath::TlsTx<L>) {
///     let f = tx.alloc().await.ok().unwrap();
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

/// Send half of a TLS 1.3 client connection, a [`FrameTx`] over the frame sender `L`; owns the
/// rustls connection.
///
/// One [`Sealed`] frame at a time: the one from `seal`, `take_pending` or
/// [`TlsTx::close_notify`] must be accepted by `try_send` (offered again while it is handed
/// back) before the next is made. Its records used sequence numbers, so dropping it breaks
/// every later record: the next of those three calls (and every one after) fails with
/// [`TxError::Broken`].
pub struct TlsTx<L> {
    pub(crate) conn: UnbufferedClientConnection,
    lower: L,
    /// A copy of the bytes rustls holds between receive steps: the records of a handshake
    /// message that is still incomplete. Every rustls call must be given a buffer starting
    /// with them, also the ones made here to send. Empty unless such a message is split.
    held: Vec<u8>,
    /// The plaintext of the frame being sealed: rustls encrypts from one buffer into another.
    plaintext: Vec<u8>,
    /// Records rustls queued itself (the alert after an error), for `take_pending`.
    pub(crate) pending: Vec<u8>,
    state: State,
    /// A `Sealed` frame exists that the lower layer has not accepted.
    in_flight: bool,
    /// Plaintext per frame ([`plain_room`]): rustls would split a larger seal into several
    /// records, past the frame.
    room: usize,
}

impl<L: FrameTx> TlsTx<L> {
    /// `conn` must have completed a TLS 1.3 handshake; `max_fragment_size` is the
    /// `ClientConfig::max_fragment_size` it was made with. Panics if `lower`'s frames cannot
    /// carry a record with one byte of plaintext.
    pub fn new(conn: UnbufferedClientConnection, lower: L, max_fragment_size: Option<usize>) -> Self {
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
        let room = plain_room(room, max_fragment_size);
        Self {
            conn,
            lower,
            held: Vec::new(),
            plaintext: Vec::with_capacity(room),
            pending: Vec::new(),
            state: State::Open,
            in_flight: false,
            room,
        }
    }

    /// Seals close_notify into `f`. Later seals fail with [`TxError::Closed`]; receiving goes on.
    pub fn close_notify(
        &mut self,
        mut f: L::Frame,
    ) -> Result<Sealed<L::Sealed>, TxError<L::Error>> {
        self.check_sent()?;
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

    /// Records the bytes rustls holds after a receive step.
    pub(crate) fn hold(&mut self, held: &[u8]) {
        self.held.clear();
        self.held.extend_from_slice(held);
    }

    /// Moves records rustls queued itself into `pending`. Once receiving has failed, rustls is
    /// not called again: the alert it queued then was collected by the receive step.
    pub(crate) fn collect(&mut self) {
        if matches!(self.state, State::Open | State::Closed) {
            let mut held = core::mem::take(&mut self.held);
            self.collect_from(&mut held);
            self.held = held;
        }
    }

    /// Moves records rustls queued itself into `pending`, giving rustls `buf`, which starts
    /// with the bytes it holds.
    pub(crate) fn collect_from(&mut self, buf: &mut [u8]) {
        loop {
            match self.conn.process_tls_records(buf).state {
                Ok(ConnectionState::EncodeTlsData(mut data)) => {
                    encode_into(&mut self.pending, &mut data)
                }
                Ok(ConnectionState::TransmitTlsData(data)) => data.done(),
                _ => return,
            }
        }
    }

    /// The last sealed frame was taken by the lower layer. If not (dropped, or still held and
    /// never sent), its sequence numbers are lost to every later record: the sender is broken.
    fn check_sent(&mut self) -> Result<(), TxError<L::Error>> {
        if self.in_flight {
            self.state = State::Broken;
            return Err(TxError::Broken);
        }
        Ok(())
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
        // what rustls queued is already in `pending`: each later call into rustls (the receive
        // step, `finish`, `take_pending`) ends with a collect, and rustls hands out queued
        // records before WriteTraffic, so a connection past its handshake starts with none
        assert!(
            self.pending.is_empty(),
            "rustls queued a record while the connection is healthy; send it with take_pending first"
        );
        let UnbufferedStatus { discard, state } = self.conn.process_tls_records(&mut self.held);
        assert_eq!(discard, 0, "rustls discarded bytes it holds");
        match state {
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
        self.collect();
        match self.lower.seal(f, n) {
            Ok(sealed) => Ok(Sealed(sealed)),
            Err(e) => {
                self.state = State::Broken;
                self.in_flight = false;
                Err(TxError::Lower(e))
            }
        }
    }

    /// Records rustls queued itself, the alert after a receive error. Gathered at the end of
    /// every receive step and every seal, so this never misses one.
    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Writes the records rustls queued itself into `f`; `Ok(None)`, dropping `f`, if there
    /// are none.
    pub fn take_pending(
        &mut self,
        mut f: L::Frame,
    ) -> Result<Option<Sealed<L::Sealed>>, TxError<L::Error>> {
        self.check_sent()?;
        if matches!(self.state, State::Broken) {
            return Err(TxError::Broken);
        }
        self.collect();
        if self.pending.is_empty() {
            return Ok(None);
        }
        let (head, room) = self.lower.plain();
        let n = self.pending.len().min(room);
        f.room_mut()[head..head + n].copy_from_slice(&self.pending[..n]);
        self.pending.drain(..n);
        self.finish(f, n).map(Some)
    }
}

impl<L: FrameTx> FrameTx for TlsTx<L> {
    type Frame = L::Frame;
    type Sealed = Sealed<L::Sealed>;
    type Error = TxError<L::Error>;

    async fn alloc(&mut self) -> Result<L::Frame, Self::Error> {
        self.lower.alloc().await.map_err(TxError::Lower)
    }

    /// Room below for the record carrying `len` B and the KeyUpdates that may precede it.
    async fn room(&mut self, len: usize) -> Result<(), Self::Error> {
        self.lower.room(len + OVERHEAD).await.map_err(TxError::Lower)
    }

    fn plain(&self) -> (usize, usize) {
        (self.lower.plain().0 + RECORD_HEAD, self.room)
    }

    /// Encrypts the plaintext into one record in place of it, behind any KeyUpdate records
    /// rustls has to send first. Panics if `len` is 0 (rustls makes no record of nothing) or
    /// exceeds the room.
    fn seal(&mut self, mut f: L::Frame, len: usize) -> Result<Self::Sealed, Self::Error> {
        self.check_sent()?;
        let (head, room) = self.lower.plain();
        assert!(len > 0, "sealing an empty record");
        assert!(
            len <= self.room,
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

    fn shutdown(&mut self) {
        self.lower.shutdown();
    }

    async fn delivered(&mut self) {
        self.lower.delivered().await
    }
}
