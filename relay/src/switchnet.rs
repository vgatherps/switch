//! WebSocket over switchnet TCP: [`connect`] for a wss:// client (TLS 1.3 through sheath),
//! [`connect_ws`] for a plain ws:// client and [`accept_ws`] for a plain ws:// server. Each runs
//! the WebSocket upgrade (a rare path, which allocates), then [`Wss`]/[`Ws`] receive and send
//! messages. Their futures run as switch tasks.

use std::convert::Infallible;
use std::fmt;
use std::sync::Arc;

use rustls::ClientConfig;
pub use sheath::switchnet::SetupError;
use sheath::switchnet::{SwitchTx, send_bytes, tls_connect};
use sheath::{TlsRx, TlsTx, TxError as TlsTxError};
use switchnet::{NetHandle, RecvError, SendError, TcpFrame, TcpRx, TcpStream};

use crate::lower::{PlainRx, PlainTx};
use crate::raw::Raw;
use crate::rx::{Config, Kind, Limits, Message, Role, WsRx};
use crate::session::{Input, InputEnd, Session};
pub use crate::tx::Failure;
use crate::tx::{TxCore, TxError, max_control_frame};
use crate::upgrade::{self, UpgradeError};

/// Raw TCP sends nothing of its own and ends with FIN alone.
impl PlainTx for SwitchTx {
    const HAS_CLOSE_RECORD: bool = false;

    fn has_pending(&self) -> bool {
        false
    }

    fn take_pending(&mut self, _: switchnet::TxFrame) -> Result<Option<switchnet::TxFrame>, SendError> {
        Ok(None)
    }

    fn seal_close(&mut self, _: switchnet::TxFrame) -> Result<Option<switchnet::TxFrame>, SendError> {
        Ok(None)
    }
}

#[derive(Debug)]
pub enum Error {
    /// TCP receive failed during setup (the upgrade).
    Recv(RecvError),
    /// The TLS handshake failed.
    Tls(sheath::Error),
    /// Sending the upgrade failed below WebSocket (TLS or TCP).
    Send(TlsTxError<SendError>),
    /// The TLS handshake could not start or ended in a state other than application traffic.
    Handshake(String),
    /// The server refused or botched the WebSocket upgrade.
    Upgrade(UpgradeError),
    /// A send or close after our close (or after `linger`).
    Closed,
    /// The connection could not be set up (a handle from another Net, or a frame room below
    /// the largest control frame); nothing was sent.
    Config(SetupError),
    /// The connection failed; every later `send` and `close` reports this first failure.
    Failed(Failure),
    /// A Text message that is not UTF-8; nothing was sent.
    InvalidText,
    /// A close code we may not send, or a reason over 123 B; nothing was sent.
    InvalidClose,
    /// A message over the connection's `Limits::max_send`; nothing was sent.
    TooBig,
}

impl From<TxError> for Error {
    fn from(e: TxError) -> Self {
        match e {
            TxError::Closed => Self::Closed,
            TxError::Failed(f) => Self::Failed(f),
            TxError::TooBig => Self::TooBig,
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
            Self::TooBig => f.write_str("message over 64 MiB"),
        }
    }
}

impl std::error::Error for Error {}

impl From<sheath::switchnet::Error> for Error {
    fn from(e: sheath::switchnet::Error) -> Self {
        match e {
            sheath::switchnet::Error::Recv(e) => Self::Recv(e),
            sheath::switchnet::Error::Tls(e) => Self::Tls(e),
            sheath::switchnet::Error::Send(e) => Self::Send(e),
            sheath::switchnet::Error::Handshake(e) => Self::Handshake(e),
            sheath::switchnet::Error::Setup(e) => e.into(),
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
impl From<TlsTxError<SendError>> for Error {
    fn from(e: TlsTxError<SendError>) -> Self {
        Self::Send(e)
    }
}

impl From<SendError> for Error {
    fn from(e: SendError) -> Self {
        Self::Send(TlsTxError::Lower(e))
    }
}

impl From<Infallible> for Error {
    fn from(e: Infallible) -> Self {
        match e {}
    }
}

/// The wss:// handshake for `host` on `stream`, connected to the server's `port`, then the
/// WebSocket upgrade for `path`; the connection keeps to `limits`.
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

/// The ws:// upgrade for `path` on `stream`, connected to `host`'s `port`; the connection keeps
/// to `limits`.
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

/// The server side of the ws:// upgrade on an accepted `stream`: the client's request is
/// answered with 101, selecting no extension or subprotocol. A request that is not a
/// WebSocket upgrade is `Error::Upgrade`, with nothing sent. The connection keeps to `limits`.
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

/// An open wss:// connection, used from one task. A server ping is answered within `recv`.
/// The close handshake ends there too, either way: the server's close is echoed, or the echo
/// of ours arrives; then close_notify and FIN go out, and `recv` returns `None` once the server
/// acked them, so the connection (and the Net) may be dropped at once. A server that never
/// acks keeps `recv` waiting: bound it.
///
/// Progress needs a call: there is no background task. Output queued by one call (the rest of
/// a message, a pong, a close echo) goes out during the next `send`, `close`, `recv` or
/// `linger`. `send` and `recv` cannot run at once: if both peers send past each other's
/// windows before reading, both wait; keep sends within the window or alternate them with
/// reads.
pub struct Wss(Session<TlsRx<TcpFrame, SwitchTx>, TlsTx<SwitchTx>, TcpRx>);

/// An open ws:// connection, client or server, used from one task. A ping is answered within
/// `recv`. The close handshake ends there too, either way: the peer's close is echoed, or the
/// echo of ours arrives; then FIN goes out, and `recv` returns `None` once the peer acked it,
/// so the connection (and the Net) may be dropped at once. A peer that never acks keeps
/// `recv` waiting: bound it. Progress and concurrency as on [`Wss`].
pub struct Ws(Session<Raw<TcpFrame, SwitchTx>, SwitchTx, TcpRx>);

impl Wss {
    /// The next message; `None` once the close handshake is done (ours or the server's) and the
    /// transport's end delivered. After a failure, the first failure, once what is owed (the
    /// TLS alert, close_notify, FIN) is delivered. Bytes the server sends after its close are read
    /// and dropped.
    pub async fn recv(&mut self) -> Result<Option<Message<'_>>, Error> {
        Ok(self.0.recv().await?)
    }

    /// Sends `payload` as one message, a TCP frame at a time, after what earlier calls left.
    /// Dropped before the message is accepted (sealed whole, or copied in if it spans
    /// frames), nothing of it is sent; once accepted it goes out whole and alone, the next
    /// `send`, `close` or `recv` finishing it. Refused with nothing sent: `Closed`, `Failed`,
    /// `InvalidText`, `TooBig`.
    pub async fn send(&mut self, kind: Kind, payload: &[u8]) -> Result<(), Error> {
        Ok(self.0.send(kind, payload).await?)
    }

    /// The server's close status and reason once `recv` returned `None`: 1005 (none) for a close
    /// without a status, 1002 (protocol error, what we echoed) for one with a status code a
    /// server may not send.
    pub fn peer_close(&self) -> Option<(u16, &str)> {
        peer_close(self.0.rx.peer_close()?)
    }

    /// Sends a close frame (`code`, `reason`) and nothing more (RFC 6455 §7.1.1: TCP closes after
    /// the handshake). Receiving goes on until the server's echo, when `recv` sends close_notify
    /// and FIN and returns `None`; a server that never echoes keeps `recv` waiting: bound it.
    pub async fn close(&mut self, code: u16, reason: &str) -> Result<(), Error> {
        Ok(self.0.close(code, reason).await?)
    }

    /// Ends without a close frame (one already sent still goes out): close_notify (unless a TLS
    /// failure) and FIN, then reads and drops what the server still sends until its FIN, so
    /// dropping the `Wss` afterwards sends no RST (a socket dropped with data unread aborts).
    /// `Err(Failed(Dead))` if the socket reset before the peer's close. Waits for the server: bound it.
    pub async fn linger(&mut self) -> Result<(), Error> {
        Ok(self.0.linger().await?)
    }
}

impl Ws {
    /// The next message; `None` once the close handshake is done (ours or the peer's) and the
    /// FIN delivered. After a failure, the first failure, once our FIN is delivered. Bytes the peer
    /// sends after its close are read and dropped.
    pub async fn recv(&mut self) -> Result<Option<Message<'_>>, Error> {
        Ok(self.0.recv().await?)
    }

    /// Sends `payload` as one message, a TCP frame at a time, after what earlier calls left.
    /// Dropped before the message is accepted (sealed whole, or copied in if it spans
    /// frames), nothing of it is sent; once accepted it goes out whole and alone, the next
    /// `send`, `close` or `recv` finishing it. Refused with nothing sent: `Closed`, `Failed`,
    /// `InvalidText`, `TooBig`.
    pub async fn send(&mut self, kind: Kind, payload: &[u8]) -> Result<(), Error> {
        Ok(self.0.send(kind, payload).await?)
    }

    /// The peer's close status and reason once `recv` returned `None`, as on [`Wss`].
    pub fn peer_close(&self) -> Option<(u16, &str)> {
        peer_close(self.0.rx.peer_close()?)
    }

    /// Sends a close frame (`code`, `reason`) and nothing more (RFC 6455 §7.1.1: TCP closes after
    /// the handshake). Receiving goes on until the peer's echo, when `recv` sends FIN and returns
    /// `None`; a peer that never echoes keeps `recv` waiting: bound it.
    pub async fn close(&mut self, code: u16, reason: &str) -> Result<(), Error> {
        Ok(self.0.close(code, reason).await?)
    }

    /// Ends without a close frame (one already sent still goes out), as after `recv` failed
    /// the connection: FIN, then reads and drops what the peer still sends until its FIN, so
    /// dropping the `Ws` afterwards sends no RST (a socket dropped with data unread aborts).
    /// `Err(Failed(Dead))` if the socket reset before the peer's close. Waits for the peer: bound it.
    pub async fn linger(&mut self) -> Result<(), Error> {
        Ok(self.0.linger().await?)
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
        // switchnet reports a reset after the peer's FIN as the reset, not `Finished`
        TcpRx::recv(self).await.map_err(|e| match e {
            RecvError::Finished => InputEnd::Fin,
            _ => InputEnd::Reset,
        })
    }
}
