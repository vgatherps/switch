//! Connecting, accepting, and the connection types. [`connect`] opens a `wss://` connection
//! ([`Wss`]), [`connect_ws`] a `ws://` one, and [`accept_ws`] serves the `ws://` upgrade on an
//! accepted stream (both [`Ws`]). Each takes a TCP stream that is already connected or
//! accepted, with the handle of the Net it came from.
//!
//! Calls that wait on the peer wait as long as it is silent; bound them with
//! [`within`](switch_net::within).

use std::convert::Infallible;
use std::fmt;
use std::sync::Arc;

use rustls::ClientConfig;
pub use switch_tls::net::SetupError;
use switch_tls::net::{SwitchTx, send_bytes, tls_connect};
use switch_tls::{TlsRx, TlsTx, TxError as TlsTxError};
use switch_net::{Error as NetError, NetHandle, TcpFrame, TcpRx, TcpStream};

use crate::lower::{PlainRx, PlainTx};
use crate::raw::Raw;
use crate::rx::{Config, Kind, Limits, Message, Role, WsRx};
use crate::session::{Input, InputEnd, Session};
pub use crate::tx::Failure;
use crate::tx::{TxCore, TxError, max_control_frame};
use crate::upgrade::{self, UpgradeError};

/// Raw TCP sends nothing of its own and ends with FIN alone.
impl PlainTx for SwitchTx {
    const CLOSE_RECORD_LEN: Option<usize> = None;

    fn has_pending(&self) -> bool {
        false
    }

    fn take_pending(&mut self, _: switch_net::OwnedFrame) -> Result<Option<switch_net::ImmutableFrame>, NetError> {
        Ok(None)
    }

    fn seal_close(&mut self, _: switch_net::OwnedFrame) -> Result<Option<switch_net::ImmutableFrame>, NetError> {
        Ok(None)
    }
}

/// Why a call failed.
///
/// [`connect`], [`connect_ws`] and [`accept_ws`] fail with:
///
/// | Error | When |
/// |---|---|
/// | `Config(SetupError::ForeignHandle)` | The stream does not belong to the handle's Net. Nothing was sent. |
/// | `Config(SetupError::RoomTooSmall { room, need })` | A frame cannot carry the largest control frame: 131 B for a client, 127 B for the server, and over TLS 76 B more of record overhead. The socket's send buffer or the backend's frame size is too small, or the TLS config's `max_fragment_size` is set below 136. Nothing was sent. |
/// | `Failed(Failure::Dead)` | The socket had already closed or reset. |
/// | `Handshake(msg)` | `connect` only: `host` is not a valid server name, the server negotiated a version other than TLS 1.3, or the handshake ended in an unexpected state. |
/// | `Tls(e)` | `connect` only: the TLS handshake failed, or during the upgrade a record failed or TCP ended without close_notify. The alert (if any) and FIN went out, and the error returned once they were acknowledged or the connection ended. |
/// | `Recv(e)` | TCP receive failed: a reset, or the peer's FIN during the TLS handshake or a `ws://` upgrade. |
/// | `Send(e)` | Sending the TLS handshake, the upgrade request or the 101 response failed. |
/// | `Upgrade(e)` | The upgrade was refused; see [`UpgradeError`]. |
///
/// `send`, `close` and [`WsSender::send`] fail with the errors below. Each refusal sends
/// nothing, except a `Failed` that happens during the call.
///
/// | Error | When |
/// |---|---|
/// | `Failed(f)` | The connection failed, earlier or during the call; `f` is its first failure. |
/// | `Closed` | `send` after our close, after the peer's close arrived, or after `linger`; `close` after `linger`. |
/// | `TooBig { max_send }` | `send` of a message over [`Limits::max_send`]. |
/// | `InvalidText` | `send` of a Text message that is not UTF-8. |
/// | `InvalidClose` | `close` with a code a peer may not send (allowed: 1000 to 1003, 1007 to 1014, 3000 to 4999) or a reason over 123 B. |
///
/// `recv`, `recv_with_sender` and `linger` fail only with `Failed`.
#[derive(Debug)]
pub enum Error {
    /// TCP receive failed during setup (the upgrade).
    Recv(NetError),
    /// The TLS handshake failed, or a record during the upgrade.
    Tls(switch_tls::Error),
    /// Sending the upgrade failed below WebSocket (TLS or TCP).
    Send(TlsTxError<NetError>),
    /// The handshake could not start, negotiated a version other than TLS 1.3, or ended in a
    /// state other than application traffic.
    Handshake(String),
    /// The server refused or botched the WebSocket upgrade.
    Upgrade(UpgradeError),
    /// A send after a close, or a send or close after `linger`.
    Closed,
    /// The connection could not be set up (a handle from another Net, or a frame room below
    /// the largest control frame); nothing was sent.
    Config(SetupError),
    /// The connection failed; every later `send`, `close` and `recv` reports this first
    /// failure.
    Failed(Failure),
    /// A Text message that is not UTF-8; nothing was sent.
    InvalidText,
    /// A close code we may not send, or a reason over 123 B; nothing was sent.
    InvalidClose,
    /// A message over the connection's `Limits::max_send`; nothing was sent.
    TooBig { max_send: usize },
}

impl From<TxError> for Error {
    fn from(e: TxError) -> Self {
        match e {
            TxError::Closed => Self::Closed,
            TxError::Failed(f) => Self::Failed(f),
            TxError::TooBig { max_send } => Self::TooBig { max_send },
            TxError::InvalidText => Self::InvalidText,
            TxError::InvalidClose => Self::InvalidClose,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Recv(e) => write!(f, "tcp receive: {e}"),
            Self::Tls(e) => write!(f, "{e}"),
            Self::Send(e) => write!(f, "send: {e}"),
            Self::Handshake(e) => write!(f, "tls handshake: {e}"),
            Self::Upgrade(e) => write!(f, "websocket upgrade: {e}"),
            Self::Closed => f.write_str("websocket closed for sending"),
            Self::Config(e) => write!(f, "setup: {e}"),
            Self::Failed(e) => write!(f, "connection failed: {e:?}"),
            Self::InvalidText => f.write_str("text message is not UTF-8"),
            Self::InvalidClose => f.write_str("close code not sendable or reason over 123 B"),
            Self::TooBig { max_send } => write!(f, "message over {max_send} B"),
        }
    }
}

impl std::error::Error for Error {}

impl From<switch_tls::net::Error> for Error {
    fn from(e: switch_tls::net::Error) -> Self {
        match e {
            switch_tls::net::Error::Recv(e) => Self::Recv(e),
            switch_tls::net::Error::Tls(e) => Self::Tls(e),
            switch_tls::net::Error::Send(e) => Self::Send(e),
            switch_tls::net::Error::Handshake(e) => Self::Handshake(e),
            switch_tls::net::Error::Setup(e) => e.into(),
        }
    }
}

/// A socket that died before setup is the same failure as one that dies later.
impl From<SetupError> for Error {
    fn from(e: SetupError) -> Self {
        match e {
            SetupError::Dead => Self::Failed(Failure::Dead),
            e => Self::Config(e),
        }
    }
}

/// The upgrade's sends.
impl From<TlsTxError<NetError>> for Error {
    fn from(e: TlsTxError<NetError>) -> Self {
        Self::Send(e)
    }
}

impl From<NetError> for Error {
    fn from(e: NetError) -> Self {
        Self::Send(TlsTxError::Lower(e))
    }
}

impl From<Infallible> for Error {
    fn from(e: Infallible) -> Self {
        match e {}
    }
}

/// Opens a `wss://` connection on `stream`, a TCP stream connected from `handle`'s Net: the
/// TLS 1.3 handshake, then the WebSocket upgrade for `path`. The connection keeps to `limits`.
/// Errors are listed on [`Error`].
///
/// `host` is the TLS server name (checked against the certificate) and the upgrade's `Host`
/// header; `port` is the server's port, named in `Host` only when it is not 443. `config`
/// decides certificate checking and the crypto provider. Only TLS 1.3 is accepted (see
/// [`tls_connect`]), and `config.max_fragment_size` must be unset or at least 136.
///
/// The call waits on the server for the handshake and the upgrade response, and a failing
/// handshake waits until its alert and FIN are acknowledged: bound it with
/// [`within`](switch_net::within). Dropping the future drops the stream.
pub async fn connect(
    handle: &NetHandle,
    stream: TcpStream,
    config: Arc<ClientConfig>,
    host: &str,
    port: u16,
    path: &str,
    limits: Limits,
) -> Result<Wss, Error> {
    let mut tls = tls_connect(handle, stream, config, host, max_control_frame(Role::Client)).await?;
    let (request, key) = upgrade::request(host, port, 443, path, rand::random());
    send_bytes(&mut tls.tx, request.as_bytes()).await?;
    let mut head = Vec::new();
    loop {
        if !tls.ready().await? {
            return Err(Error::Upgrade(UpgradeError::Closed));
        }
        let (taken, done) = upgrade::read_head(&mut head, tls.rx.current()).map_err(Error::Upgrade)?;
        tls.rx.consume(taken);
        if done {
            break;
        }
    }
    upgrade::check(&head, &key).map_err(Error::Upgrade)?;
    Ok(Wss(session(tls.rx, tls.tx, tls.tcp, Config::new(Role::Client, limits))))
}

/// Opens a `ws://` connection on `stream`, a TCP stream connected from `handle`'s Net: the
/// WebSocket upgrade for `path`. The connection keeps to `limits`. Errors are listed on
/// [`Error`].
///
/// `host` is the upgrade's `Host` header; `port` is the server's port, named in `Host` only
/// when it is not 80. The call waits on the server for the upgrade response: bound it with
/// [`within`](switch_net::within).
pub async fn connect_ws(
    handle: &NetHandle,
    stream: TcpStream,
    host: &str,
    port: u16,
    path: &str,
    limits: Limits,
) -> Result<Ws, Error> {
    let (mut tcp, tcp_tx) = stream.split();
    let mut tx = SwitchTx::new(handle, tcp_tx, max_control_frame(Role::Client)).await?;
    let (request, key) = upgrade::request(host, port, 80, path, rand::random());
    send_bytes(&mut tx, request.as_bytes()).await?;
    let mut rx = Raw::new();
    let head = read_head(&mut rx, &mut tx, &mut tcp).await?;
    upgrade::check(&head, &key).map_err(Error::Upgrade)?;
    Ok(Ws(session(rx, tx, tcp, Config::new(Role::Client, limits))))
}

/// Serves the `ws://` upgrade on `stream`, a TCP stream accepted from `handle`'s Net: reads
/// the client's request and answers `101 Switching Protocols`. The connection keeps to
/// `limits`. Errors are listed on [`Error`].
///
/// Any path is accepted. Offers of extensions or subprotocols are ignored, and none is
/// selected. The request must be an HTTP/1.1 GET with `Host`, `Upgrade: websocket`,
/// `Connection: upgrade`, `Sec-WebSocket-Version: 13` and one `Sec-WebSocket-Key` that is
/// base64 of 16 bytes, in a head of at most 16 KiB; otherwise the result is `Upgrade(e)` and
/// nothing is sent back. A client that never sends its request keeps the call waiting: bound
/// it with [`within`](switch_net::within).
pub async fn accept_ws(handle: &NetHandle, stream: TcpStream, limits: Limits) -> Result<Ws, Error> {
    let (mut tcp, tcp_tx) = stream.split();
    let mut tx = SwitchTx::new(handle, tcp_tx, max_control_frame(Role::Server)).await?;
    let mut rx = Raw::new();
    let head = read_head(&mut rx, &mut tx, &mut tcp).await?;
    let response = upgrade::accept(&head).map_err(Error::Upgrade)?;
    send_bytes(&mut tx, response.as_bytes()).await?;
    Ok(Ws(session(rx, tx, tcp, Config::new(Role::Server, limits))))
}

/// Reads an HTTP head off plain TCP, through its blank line; what follows stays in `rx`.
async fn read_head(rx: &mut Raw<TcpFrame, SwitchTx>, tx: &mut SwitchTx, tcp: &mut TcpRx) -> Result<Vec<u8>, Error> {
    let mut head = Vec::new();
    loop {
        match rx.chunk(tx)? {
            Some(run) => {
                let (taken, done) = upgrade::read_head(&mut head, run.bytes).map_err(Error::Upgrade)?;
                rx.consume(taken);
                if done {
                    return Ok(head);
                }
            }
            None => rx.push(tcp.recv().await.map_err(Error::Recv)?),
        }
    }
}

/// An open `wss://` connection, from [`connect`]. Use it from one task.
///
/// There is no background task: the connection sends and receives only while a call to
/// `recv`, `send`, `close` or `linger` runs. What one call leaves goes out during the next: the
/// pong for the newest ping (older ones go unanswered), the echo of the server's close, and the
/// rest of a message whose `send` was dropped. A program that only sends must still call
/// `recv`.
///
/// `send` and `recv` take `&mut self`, so they cannot run at once. On the local backend, if
/// both ends send more than the other's receive window before reading, both wait for good;
/// keep sends within the window or alternate them with reads. On the junction backend, `send`
/// waits only while the session's send ring is full.
///
/// To close, call [`close`](Wss::close), then `recv` until it returns `None`, both bounded with
/// [`within`](switch_net::within). The ways a connection ends, as `recv` reports them:
///
/// | Event while receiving | Sent first | `recv` returns |
/// |---|---|---|
/// | The server's close frame | The echo (unless our close went first), close_notify, FIN | `Ok(None)`, once the FIN is acknowledged |
/// | The server breaks the protocol | close_notify, FIN | `Err(Failed(Protocol(e)))` |
/// | A bad TLS record | The alert, FIN | `Err(Failed(Tls(Tls(e))))` |
/// | FIN without close_notify | FIN | `Err(Failed(Tls(Truncated)))` |
/// | close_notify or FIN without a close frame | close_notify, FIN | `Err(Failed(PeerEof))` |
/// | A reset before the server's close | Nothing | `Err(Failed(Dead))` |
/// | A reset after the server's close | Nothing | `Ok(None)` |
///
/// After `Ok(None)` the connection, and its Net, may be dropped: nothing is left in flight.
pub struct Wss(Session<TlsRx<TcpFrame, SwitchTx>, TlsTx<SwitchTx>, TcpRx>);

/// An open `ws://` connection, client (from [`connect_ws`]) or server (from [`accept_ws`]). Use
/// it from one task. It behaves as [`Wss`] without TLS: no close_notify is sent, the TLS rows of
/// that table do not apply, and a peer's FIN before its close frame is `PeerEof`.
pub struct Ws(Session<Raw<TcpFrame, SwitchTx>, SwitchTx, TcpRx>);

/// A send handle held alongside a message returned by [`Ws::recv_with_sender`].
/// It borrows the connection until both the handle and message are no longer used.
pub struct WsSender<'a>(&'a mut TxCore<SwitchTx>);

impl WsSender<'_> {
    /// Sends a message with the same ordering, errors and cancellation behavior as [`Ws::send`].
    /// The payload may borrow the message returned with this handle.
    pub async fn send(&mut self, kind: Kind, payload: &[u8]) -> Result<(), Error> {
        Ok(self.0.send(kind, payload).await?)
    }
}

impl Wss {
    /// The next message. `Ok(None)` once the close handshake is done (ours or the server's) and
    /// our FIN is acknowledged; after a failure, the first failure, once what is owed is
    /// delivered (see [`Failure`]). Messages that arrive after our close, before the server's,
    /// are returned; bytes after the server's close are read and dropped. Dropping the future
    /// loses nothing: the next call goes on where it stopped.
    pub async fn recv(&mut self) -> Result<Option<Message<'_>>, Error> {
        self.0.recv().await.map_err(Error::Failed)
    }

    /// Sends `payload` as one message, after what earlier calls left, and returns once all of
    /// it is handed to TCP. A message that fits one frame goes out as one WebSocket frame; a
    /// longer one is copied once into a buffer and sent a frame at a time. It is never
    /// interleaved with another message. Errors are listed on [`Error`].
    ///
    /// Dropped before the message is accepted, nothing of it is sent. A message that fits one
    /// frame is accepted once a frame is free, a longer one once it is copied. After that it
    /// goes out whole, finished by the next `send`, `close` or `recv`.
    pub async fn send(&mut self, kind: Kind, payload: &[u8]) -> Result<(), Error> {
        Ok(self.0.send(kind, payload).await?)
    }

    /// The server's close status and reason once its close frame arrived (at the latest when
    /// `recv` returned `None`); `None` if it sent none. A close without a status reads as 1005.
    /// A status a server may not send reads as 1002: the close handshake still completes, and
    /// 1002 is what was echoed.
    pub fn peer_close(&self) -> Option<(u16, &str)> {
        peer_close(self.0.rx.peer_close()?)
    }

    /// Sends a close frame (`code`, `reason`), after any message in progress, and returns once
    /// it is handed to TCP. Receiving goes on: `recv` returns messages that crossed it on the
    /// wire, then `None` once the server's echo arrived and close_notify and FIN are
    /// acknowledged. A server that never echoes keeps `recv` waiting: bound it.
    ///
    /// `code` must be one a peer may send (1000 to 1003, 1007 to 1014, 3000 to 4999) and
    /// `reason` at most 123 B; otherwise `InvalidClose`, with nothing sent. A second `close`, or
    /// one after the server's close arrived, returns `Ok` and sends no close frame of its own.
    /// If the future is dropped once started, the close frame stays queued and the next `recv`
    /// sends it.
    pub async fn close(&mut self, code: u16, reason: &str) -> Result<(), Error> {
        Ok(self.0.close(code, reason).await?)
    }

    /// Ends without a close frame of our own: one already sealed still goes out, but one only
    /// queued, and the unsent rest of a dropped `send`, are dropped. Sends close_notify (unless
    /// TLS failed) and FIN, then reads and drops what the server sends until its FIN, so
    /// dropping the `Wss` afterwards sends no reset. Use it after a failure while the server may
    /// still be sending.
    ///
    /// `Err(Failed(Dead))` if the socket reset before the server's close; other failures are not
    /// reported. Waits for the server's FIN: bound it. Afterwards `send` and `close` fail.
    pub async fn linger(&mut self) -> Result<(), Error> {
        self.0.linger().await.map_err(Error::Failed)
    }
}

impl Ws {
    /// The next message, as [`Wss::recv`]; `Ok(None)` once the close handshake is done and our
    /// FIN is acknowledged.
    pub async fn recv(&mut self) -> Result<Option<Message<'_>>, Error> {
        self.0.recv().await.map_err(Error::Failed)
    }

    /// Receives as [`Ws::recv`], and also lends a sender, so the message's bytes can be sent
    /// (for an echo) without copying them into a buffer of the caller's. Sending still copies
    /// them into the outgoing frame, or into the send buffer for a message longer than a frame.
    pub async fn recv_with_sender(&mut self) -> Result<Option<(Message<'_>, WsSender<'_>)>, Error> {
        Ok(self.0.recv_with_sender().await.map_err(Error::Failed)?.map(|(message, tx)| (message, WsSender(tx))))
    }

    /// Sends `payload` as one message, as [`Wss::send`].
    pub async fn send(&mut self, kind: Kind, payload: &[u8]) -> Result<(), Error> {
        Ok(self.0.send(kind, payload).await?)
    }

    /// The peer's close status and reason, as [`Wss::peer_close`].
    pub fn peer_close(&self) -> Option<(u16, &str)> {
        peer_close(self.0.rx.peer_close()?)
    }

    /// Sends a close frame (`code`, `reason`), as [`Wss::close`]; `recv` returns `None` once the
    /// peer's echo arrived and our FIN is acknowledged.
    pub async fn close(&mut self, code: u16, reason: &str) -> Result<(), Error> {
        Ok(self.0.close(code, reason).await?)
    }

    /// Ends without a close frame of our own, as [`Wss::linger`], with FIN alone.
    pub async fn linger(&mut self) -> Result<(), Error> {
        self.0.linger().await.map_err(Error::Failed)
    }
}

/// A received close payload as status and reason (the receive core validated it).
fn peer_close(p: &[u8]) -> Option<(u16, &str)> {
    Some(match p {
        [] => (1005, ""),
        [a, b, reason @ ..] => {
            let reason = core::str::from_utf8(reason).expect("the receive core validated the reason");
            (u16::from_be_bytes([*a, *b]), reason)
        }
        [_] => unreachable!("the receive core refuses a 1-byte close"),
    })
}

/// A connection's halves and TCP input, with its role and limits.
fn session<R, T>(rx: R, tx: T, tcp: TcpRx, config: Config) -> Session<R, T, TcpRx>
where
    R: PlainRx<Frame = TcpFrame, Tx = T>,
    T: PlainTx,
    Failure: From<R::Error>,
{
    Session::new(WsRx::new(rx, config), TxCore::new(tx, &config), tcp)
}

impl Input for TcpRx {
    type Frame = TcpFrame;

    async fn recv(&mut self) -> Result<TcpFrame, InputEnd> {
        // switch-net reports a reset after the peer's FIN as the reset, not `Finished`
        TcpRx::recv(self).await.map_err(|e| match e {
            NetError::Finished => InputEnd::Fin,
            _ => InputEnd::Reset,
        })
    }
}
