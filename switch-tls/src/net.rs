//! The TLS client on a TCP stream. [`tls_connect`] runs the handshake on a connected
//! [`TcpStream`] and returns a [`Tls`]: read with [`Tls::ready`] and [`TlsRx::current`], write
//! with [`send_bytes`], end with [`Tls::close`].
//!
//! Each of these waits on the server, and a silent server keeps it waiting. Bound them with
//! [`within`](switch_net::within).

use std::fmt;
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::Arc;
use std::task::Poll;

use rustls::{ClientConfig, ProtocolVersion};
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

/// Why a connection could not be set up. Nothing was sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetupError {
    /// The stream does not belong to the Net of the handle passed with it.
    ForeignHandle,
    /// A frame can carry `room` bytes, fewer than the `need` bytes the connection's largest
    /// frame takes. `room` is bounded by the backend's frame size, the socket's send buffer
    /// and, over TLS, the config's `max_fragment_size`.
    RoomTooSmall { room: usize, need: usize },
    /// The socket had already closed or reset.
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
    /// Bytes per frame: the connection's MSS, raised to the caller's largest frame, within the
    /// Stack's frame capacity and the socket's send buffer.
    room: usize,
}

impl SwitchTx {
    /// Sends on `tcp`, in frames from `handle`. A frame carries the connection's MSS (one
    /// segment), or `min_room`, the largest frame the caller will send, if that is more: TCP
    /// cuts such a frame into segments. Its storage, the Stack's frame capacity (one frame is
    /// taken to read it) and the socket's send buffer (so every `room` request can resolve),
    /// bounds it; storage below `min_room` is refused here, before any byte goes out.
    pub async fn new(handle: &NetHandle, tcp: TcpTx, min_room: usize) -> Result<SwitchTx, SetupError> {
        if !tcp.is_on(handle) {
            return Err(SetupError::ForeignHandle);
        }
        let mut tx = SwitchTx { handle: handle.clone(), tcp, room: 0 };
        let capacity = tx.alloc().await.map_err(|_| SetupError::Dead)?.len();
        let dead = {
            let mut delivered = pin!(tx.tcp.delivered());
            poll_fn(|cx| Poll::Ready(delivered.as_mut().poll(cx).is_ready())).await
        };
        if dead {
            return Err(SetupError::Dead);
        }
        let mss = tx.tcp.max_segment_payload().ok_or(SetupError::Dead)?;
        let storage = capacity.min(tx.tcp.send_capacity());
        if storage < min_room {
            return Err(SetupError::RoomTooSmall { room: storage, need: min_room });
        }
        tx.room = mss.max(min_room).min(storage);
        Ok(tx)
    }
}

impl FrameTx for SwitchTx {
    type Frame = switch_net::OwnedFrame;
    type Sealed = switch_net::ImmutableFrame;
    type Error = NetError;

    /// A frame from the Net; while none is free, `InvalidState` if the socket has closed or
    /// reset. Called only before `shutdown`, where the connection ending is the only way
    /// `delivered` resolves.
    async fn alloc(&mut self) -> Result<switch_net::OwnedFrame, NetError> {
        let mut frame = pin!(self.handle.tx_frame());
        let mut dead = pin!(self.tcp.delivered());
        poll_fn(|cx| {
            if let Poll::Ready(f) = frame.as_mut().poll(cx) {
                return Poll::Ready(Ok(f));
            }
            dead.as_mut().poll(cx).map(|()| Err(NetError::InvalidState))
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

/// Why [`tls_connect`], [`Tls::ready`] or [`Tls::close`] failed.
///
/// | Error | From | When |
/// |---|---|---|
/// | `Setup(e)` | `tls_connect` | The connection could not be set up; nothing was sent. |
/// | `Handshake(msg)` | `tls_connect` | `host` is not a valid server name (nothing was sent), the server negotiated a version other than TLS 1.3, or the handshake ended in a state other than application traffic. |
/// | `Tls(Tls(e))` | `tls_connect`, `ready` | rustls refused `config` (nothing was sent), rejected the server's handshake, or rejected a received record. In the last two cases the alert rustls queued and a FIN went out, and the call returned once the server acknowledged them or the connection ended. |
/// | `Tls(Truncated)` | `ready` | TCP ended without the server's close_notify. A FIN went out, as above. |
/// | `Recv(e)` | `tls_connect`, `ready` | TCP receive failed: a reset, or the server's FIN during the handshake. |
/// | `Send(e)` | `tls_connect`, `close` | Sending failed; see [`TxError`]. |
#[derive(Debug)]
pub enum Error {
    /// TCP receive failed.
    Recv(NetError),
    /// The TLS handshake or a received record failed, or the stream was truncated.
    Tls(TlsError),
    /// Sending failed (TLS or TCP).
    Send(TxError<NetError>),
    /// The handshake could not start (a bad server name), negotiated a version other than
    /// TLS 1.3, or ended in a state other than application traffic.
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

/// A TLS 1.3 connection after the handshake, from [`tls_connect`]: the receive half `rx`, the
/// send half `tx` (write with [`send_bytes`]), and the TCP receive half that feeds `rx`. Use it
/// from one task.
pub struct Tls {
    pub rx: TlsRx<TcpFrame, SwitchTx>,
    pub tx: TlsTx<SwitchTx>,
    pub tcp: TcpRx,
}

impl Tls {
    /// Waits until `rx` holds plaintext (`Ok(true)`): read it with [`TlsRx::current`] and mark
    /// what was used with [`TlsRx::consume`]. `Ok(false)` once the server sent close_notify, and
    /// on every call after.
    ///
    /// On a bad record, or TCP ending without close_notify, it sends the alert rustls queued (if
    /// any) and FIN, and returns the error once the server acknowledged them or the connection
    /// ended; a silent server keeps it waiting, so bound it. Later calls return the same error.
    /// A TCP reset returns `Recv` at once. Dropping the future while it waits for data loses
    /// nothing.
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

    /// Sends close_notify, then FIN, and returns once the server acknowledged them or the
    /// connection ended; the `Tls` may be dropped then. A silent server keeps it waiting, so
    /// bound it. It does not read: data the server sends meanwhile stays unread.
    ///
    /// Fails with `Send(TxError::Closed)` if close_notify was already sent,
    /// `Send(TxError::Received(e))` after a receive error, and `Send(TxError::Lower(e))` if the
    /// socket closed or reset.
    pub async fn close(&mut self) -> Result<(), Error> {
        self.tx.send_close_notify().await.map_err(Error::Send)?;
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

/// Runs the TLS 1.3 handshake for `host` on `stream`, a connected stream from `handle`'s Net,
/// and returns the connection. Errors are listed on [`Error`].
///
/// `host` is the server name sent in the handshake and checked against the certificate.
/// `config` decides certificate checking and the crypto provider; this function adds neither.
/// Only TLS 1.3 is accepted: a handshake that negotiates another version, which `config` may
/// allow, fails with `Handshake`.
///
/// `min_plain` is the largest plaintext the caller will send as one record (0 counts as 1);
/// [`send_bytes`] splits larger writes, so 1 suits a caller that sends only through it. One
/// frame must hold such a record: `min_plain` plus 76 B of record overhead must fit the
/// backend's frame size and the socket's send buffer, `min_plain` must be at most 16384, and
/// `config.max_fragment_size`, if set, at least `min_plain + 5`. Otherwise the result is
/// `Setup(RoomTooSmall)`, before anything is sent.
///
/// The handshake waits on the server. A rejected handshake also waits, until the alert and FIN
/// it sends are acknowledged or the connection ends. Bound the call with
/// [`within`](switch_net::within); dropping the future drops the stream.
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
    if conn.protocol_version() != Some(ProtocolVersion::TLSv1_3) {
        return Err(Error::Handshake(format!("negotiated {:?}; TLS 1.3 required", conn.protocol_version())));
    }
    Ok(Tls {
        tx: TlsTx::new(conn, raw, max_fragment_size),
        rx: TlsRx::with_leftover(&received, stamp),
        tcp: tcp_rx,
    })
}

/// Sends `bytes` through `lower` (for TLS, `&mut tls.tx`), a frame and one record at a time,
/// waiting for TCP room for each. The error is `lower`'s, [`TxError`] for TLS.
///
/// Dropped before it returns, it may have sent a prefix of `bytes` in whole records. Dropped
/// while a sealed record waits for room, it breaks the connection: later sends fail with
/// [`TxError::Broken`].
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
