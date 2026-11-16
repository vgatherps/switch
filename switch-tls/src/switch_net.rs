//! TLS 1.3 over switch-net TCP: [`tls_connect`] runs the handshake (a rare path, which
//! allocates), then [`Tls`] holds the two halves and the TCP receive half feeding them. Its
//! futures run as switch tasks. switch-net's frames are this crate's [`RxFrame`]/[`TxFrame`].
//!
//! TCP receive and send failures carry [`switch_net::Error`]; [`Error`] retains their TLS
//! context. Frame sizing uses the stream's cached negotiated MSS.

use std::fmt;
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::Arc;
use std::task::Poll;

use rustls::ClientConfig;
use rustls::client::UnbufferedClientConnection;
use rustls::pki_types::ServerName;
use rustls::unbuffered::{ConnectionState, UnbufferedStatus};
use switch_net::{Error as NetError, NetHandle, TcpFrame, TcpRx, TcpStream, TcpTx};

use crate::tx::{OVERHEAD, plain_room};
use crate::{Error as TlsError, FrameTx, RxFrame, TlsRx, TlsTx, TxError, TxFrame, encode_into};

impl RxFrame for TcpFrame {
    fn payload(&self) -> &[u8] {
        TcpFrame::payload(self)
    }

    fn payload_mut(&mut self) -> &mut [u8] {
        TcpFrame::payload_mut(self)
    }

    fn rx_timestamp(&self) -> u64 {
        TcpFrame::rx_timestamp(self)
    }
}

impl TxFrame for switch_net::OwnedFrame {
    fn room_mut(&mut self) -> &mut [u8] {
        self
    }
}

/// Why a [`SwitchTx`] (and so a connection) could not be set up. Nothing was sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetupError {
    /// The stream is not on `handle`'s Net: its frames would be refused as foreign.
    ForeignHandle,
    /// One frame carries `room` bytes (the frame room, clamped to the negotiated MSS and send
    /// buffer), fewer than the `need` bytes the caller's largest frame takes.
    RoomTooSmall { room: usize, need: usize },
    /// The socket closed or reset before a frame could be taken.
    Dead,
}

impl fmt::Display for SetupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ForeignHandle => f.write_str("the stream is not on the handle's Net"),
            Self::RoomTooSmall { room, need } => write!(f, "frame room {room} B, below the {need} B needed"),
            Self::Dead => f.write_str("the socket closed before setup"),
        }
    }
}

/// Raw TCP: frames go out as they are. Below [`TlsTx`], or alone for a plain stream.
pub struct SwitchTx {
    handle: NetHandle,
    tcp: TcpTx,
    /// Bytes per frame: the Stack's frame capacity, the connection's MSS and the socket's send
    /// buffer, whichever is least.
    room: usize,
}

impl SwitchTx {
    /// Sends on `tcp`, in frames from `handle`, sized by the Stack's frame capacity (one frame
    /// is taken to read it), the connection's MSS (so each frame is one segment, sent without
    /// a copy) and the socket's send buffer (so every `room` request can resolve). `min_room`
    /// is the largest frame the caller will send; a smaller room is refused here, before any
    /// byte goes out.
    pub async fn new(handle: &NetHandle, tcp: TcpTx, min_room: usize) -> Result<SwitchTx, SetupError> {
        if !tcp.is_on(handle) {
            return Err(SetupError::ForeignHandle);
        }
        let mut tx = SwitchTx { handle: handle.clone(), tcp, room: 0 };
        let capacity = tx.alloc().await.map_err(|_| SetupError::Dead)?.len();
        let mss = tx.tcp.max_segment_payload().ok_or(SetupError::Dead)?;
        tx.room = capacity.min(mss).min(tx.tcp.send_capacity());
        if tx.room < min_room {
            return Err(SetupError::RoomTooSmall { room: tx.room, need: min_room });
        }
        Ok(tx)
    }
}

impl FrameTx for SwitchTx {
    type Frame = switch_net::OwnedFrame;
    type Sealed = switch_net::ImmutableFrame;
    type Error = NetError;

    /// A frame from the Net, or `InvalidState` if the socket has closed or reset (checked
    /// first on every poll, so a dead socket never gets a frame). Called only before
    /// `shutdown`, where the connection ending is the only way `delivered` resolves.
    async fn alloc(&mut self) -> Result<switch_net::OwnedFrame, NetError> {
        let mut dead = pin!(self.tcp.delivered());
        let mut frame = pin!(self.handle.tx_frame());
        poll_fn(|cx| {
            if dead.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Err(NetError::InvalidState));
            }
            frame.as_mut().poll(cx).map(Ok)
        })
        .await
    }

    async fn room(&mut self, len: usize) -> Result<(), NetError> {
        self.tcp.room(len).await
    }

    fn plain(&self) -> (usize, usize) {
        (0, self.room)
    }

    fn seal(&mut self, mut f: switch_net::OwnedFrame, len: usize) -> Result<switch_net::ImmutableFrame, NetError> {
        f.truncate(len);
        Ok(f.freeze())
    }

    fn try_send(&mut self, f: switch_net::ImmutableFrame) -> Result<(), switch_net::ImmutableFrame> {
        self.tcp.try_send(f).map_err(|(f, _)| f)
    }

    fn shutdown(&mut self) {
        self.tcp.shutdown();
    }

    async fn delivered(&mut self) {
        self.tcp.delivered().await
    }
}

#[derive(Debug)]
pub enum Error {
    /// TCP receive failed.
    Recv(NetError),
    /// The TLS handshake or a received record failed.
    Tls(TlsError),
    /// Sending failed (TLS or TCP).
    Send(TxError<NetError>),
    /// The handshake could not start (a bad server name) or ended in a state other than
    /// application traffic.
    Handshake(String),
    /// The connection could not be set up; nothing was sent.
    Setup(SetupError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Recv(e) => write!(f, "tcp receive: {e}"),
            Self::Tls(e) => write!(f, "{e}"),
            Self::Send(e) => write!(f, "send: {e}"),
            Self::Handshake(e) => write!(f, "tls handshake: {e}"),
            Self::Setup(e) => write!(f, "setup: {e}"),
        }
    }
}

impl std::error::Error for Error {}

/// A TLS 1.3 connection over switch-net TCP, after the handshake: the receive half, the send
/// half, and the TCP receive half that feeds `rx`.
pub struct Tls {
    pub rx: TlsRx<TcpFrame, SwitchTx>,
    pub tx: TlsTx<SwitchTx>,
    pub tcp: TcpRx,
}

impl Tls {
    /// Waits until `rx` holds plaintext to read through `rx.current()`/`rx.consume` (`Ok(true)`),
    /// or the server closed with close_notify (`Ok(false)`). A TLS error first sends the alert
    /// rustls queued for it, then FIN, and returns once the server acked them (or the connection
    /// ended): until then an erroring call stays pending, so the caller bounds it. The TCP stream
    /// ending without close_notify is truncation.
    pub async fn ready(&mut self) -> Result<bool, Error> {
        loop {
            let failed = match self.rx.chunk(&mut self.tx).map(|run| run.is_some()) {
                Ok(true) => return Ok(true),
                Ok(false) if self.rx.eof() => return Ok(false),
                Ok(false) => match self.tcp.recv().await {
                    Ok(frame) => {
                        self.rx.push(frame);
                        None
                    }
                    Err(NetError::Finished) => self.rx.fin(&mut self.tx).err(),
                    Err(e) => return Err(Error::Recv(e)),
                },
                Err(e) => Some(e),
            };
            if let Some(e) = failed {
                abort(&mut self.tx).await;
                return Err(Error::Tls(e));
            }
        }
    }

    /// Sends close_notify, then FIN, and returns once the server acked them (or the connection
    /// ended), so the connection may be dropped then; until then it stays pending, so the caller
    /// bounds it. Receiving goes on.
    pub async fn close(&mut self) -> Result<(), Error> {
        let f = self.tx.alloc().await.map_err(Error::Send)?;
        let (_, room) = self.tx.plain();
        self.tx.room(room).await.map_err(Error::Send)?;
        let sealed = self.tx.close_notify(f).map_err(Error::Send)?;
        send_sealed(&mut self.tx, sealed, room).await.map_err(Error::Send)?;
        self.tx.shutdown();
        self.tx.delivered().await;
        Ok(())
    }
}

/// After a TLS error: sends the alert rustls queued for it, if any, then FIN, and waits until the
/// server acked them or the connection ended. A drop before that would lose them: the Stack sends
/// them only on later turns, and a drop with server data unread aborts with RST at once. Best
/// effort: the caller reports the TLS error, which matters more than a failed send.
async fn abort(tx: &mut TlsTx<SwitchTx>) {
    if tx.has_pending() {
        let sent = async {
            let f = tx.alloc().await?;
            let (_, room) = tx.plain();
            tx.room(room).await?;
            if let Some(sealed) = tx.take_pending(f)? {
                send_sealed(tx, sealed, room).await?;
            }
            Ok::<(), TxError<NetError>>(())
        };
        let _ = sent.await;
    }
    tx.shutdown();
    tx.delivered().await;
}

/// Sends `sealed`, waiting for room of `len` bytes each time it is refused.
async fn send_sealed<L: FrameTx>(lower: &mut L, mut sealed: L::Sealed, len: usize) -> Result<(), L::Error> {
    while let Err(s) = lower.try_send(sealed) {
        sealed = s;
        lower.room(len).await?;
    }
    Ok(())
}

/// The TLS handshake for `host` on `stream`. If rustls rejects the server's handshake, the alert
/// it queued goes out, then FIN, and the error returns once the server acked them (or the
/// connection ended): until then a failing handshake stays pending, so the caller bounds it.
/// `min_plain` is the largest plaintext record the caller will send (0 counts as 1); a frame
/// that cannot carry it as one record (its room, or the client's `max_fragment_size`) is
/// `Setup(RoomTooSmall)` before the ClientHello.
pub async fn tls_connect(
    handle: &NetHandle,
    stream: TcpStream,
    config: Arc<ClientConfig>,
    host: &str,
    min_plain: usize,
) -> Result<Tls, Error> {
    let (mut tcp_rx, tcp) = stream.split();
    // a record carries at least one byte (TlsTx's floor); saturating on purpose: a minimum past
    // usize::MAX is impossible, and the room check refuses it as RoomTooSmall
    let need = min_plain.max(1).saturating_add(OVERHEAD);
    let mut raw = SwitchTx::new(handle, tcp, need).await.map_err(Error::Setup)?;
    let name = ServerName::try_from(host.to_owned()).map_err(|e| Error::Handshake(format!("server name {host}: {e}")))?;
    let max_fragment_size = config.max_fragment_size;
    let mut conn = UnbufferedClientConnection::new(config, name).map_err(|e| Error::Tls(TlsError::Tls(e)))?;
    // rustls has checked the fragment limit; it caps what one frame carries as one record
    let plain = plain_room(raw.room, max_fragment_size);
    if plain < min_plain.max(1) {
        return Err(Error::Setup(SetupError::RoomTooSmall { room: plain + OVERHEAD, need }));
    }

    // Received bytes are joined in `received`, sent records gathered in `out`.
    let mut received = Vec::new();
    let mut stamp = 0;
    let mut out = Vec::new();
    loop {
        enum Next {
            Again,
            Send,
            Read,
            Done,
            Failed(rustls::Error),
        }
        // Once the handshake is over (the server's Finished processed), what follows is for
        // `TlsRx`, unseen: given it, rustls would decrypt the next record (half-RTT data) and
        // offer ReadTraffic before WriteTraffic. It holds nothing then (Finished ends a flight).
        let fresh = if conn.is_handshaking() { received.len() } else { 0 };
        let UnbufferedStatus { discard, state } = conn.process_tls_records(&mut received[..fresh]);
        let next = match state {
            Ok(ConnectionState::EncodeTlsData(mut data)) => {
                encode_into(&mut out, &mut data);
                Next::Again
            }
            Ok(ConnectionState::TransmitTlsData(data)) => {
                data.done();
                Next::Send
            }
            Ok(ConnectionState::BlockedHandshake) => Next::Read,
            Ok(ConnectionState::WriteTraffic(_)) => Next::Done,
            Ok(state) => return Err(Error::Handshake(format!("ended in {state:?}"))),
            Err(e) => Next::Failed(e),
        };
        received.drain(..discard);
        if let Next::Failed(e) = next {
            // after what was already encoded, the alert rustls queued for the error (none if
            // the server sent one). rustls hands queued records out before deframing; once
            // they are gone it would deframe the rejected record again, so stop there.
            while conn.wants_write() {
                match conn.process_tls_records(&mut received).state {
                    Ok(ConnectionState::EncodeTlsData(mut data)) => encode_into(&mut out, &mut data),
                    other => unreachable!("rustls holds records to send but produced {other:?}"),
                }
            }
            let _ = send_bytes(&mut raw, &out).await;
            raw.shutdown();
            // dropping the socket before the server has them would lose them (see `abort`)
            raw.delivered().await;
            return Err(Error::Tls(TlsError::Tls(e)));
        }
        if !out.is_empty() && !matches!(next, Next::Again) {
            send_bytes(&mut raw, &out).await.map_err(|e| Error::Send(TxError::Lower(e)))?;
            out.clear();
        }
        match next {
            Next::Again | Next::Send | Next::Failed(_) => {}
            Next::Read => {
                let frame = tcp_rx.recv().await.map_err(Error::Recv)?;
                stamp = frame.rx_timestamp();
                received.extend_from_slice(frame.payload());
            }
            Next::Done => break,
        }
    }
    Ok(Tls {
        tx: TlsTx::new(conn, raw, max_fragment_size),
        rx: TlsRx::with_leftover(&received, stamp),
        tcp: tcp_rx,
    })
}

/// Sends `bytes` through `lower`, a frame at a time, each sent once there is room for it.
pub async fn send_bytes<L: FrameTx>(lower: &mut L, bytes: &[u8]) -> Result<(), L::Error> {
    let (head, room) = lower.plain();
    for piece in bytes.chunks(room) {
        let mut f = lower.alloc().await?;
        f.room_mut()[head..head + piece.len()].copy_from_slice(piece);
        lower.room(piece.len()).await?;
        let sealed = lower.seal(f, piece.len())?;
        send_sealed(lower, sealed, piece.len()).await?;
    }
    Ok(())
}
