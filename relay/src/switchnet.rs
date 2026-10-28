//! WebSocket clients over switchnet TCP: [`connect`] for wss:// (TLS 1.3 through sheath) and
//! [`connect_ws`] for plain ws://. Each runs the WebSocket upgrade (a rare path, which
//! allocates), then [`Wss`]/[`Ws`] receive and send messages. Their futures run as switch tasks.

use std::convert::Infallible;
use std::fmt;
use std::sync::Arc;

use rustls::ClientConfig;
use sheath::switchnet::{SwitchTx, send_bytes, tls_connect};
use sheath::{TlsRx, TlsTx, TxError as TlsTxError};
use switchnet::{NetHandle, RecvError, SendError, TcpFrame, TcpRx, TcpStream};

use crate::lower::{PlainRx, PlainTx};
use crate::raw::Raw;
use crate::rx::{Config, Kind, Message, ProtocolError, Role, WsError, WsRx};
use crate::tx::{TxError, WsTx};
use crate::upgrade;

/// Raw TCP sends nothing of its own and ends with FIN alone.
impl PlainTx for SwitchTx {
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
    /// TCP receive failed, or the stream ended (FIN) before the WebSocket close.
    Recv(RecvError),
    /// The TLS handshake or a received record failed.
    Tls(sheath::Error),
    /// Sending failed below WebSocket (TLS or TCP).
    Send(TlsTxError<SendError>),
    /// The TLS handshake could not start or ended in a state other than application traffic.
    Handshake(String),
    /// The server broke the WebSocket protocol.
    Protocol(ProtocolError),
    /// The server refused or botched the WebSocket upgrade.
    Upgrade(String),
    /// A send after close.
    Closed,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Recv(e) => write!(f, "tcp receive: {e}"),
            Self::Tls(e) => write!(f, "{e}"),
            Self::Send(e) => write!(f, "send: {e}"),
            Self::Handshake(e) => write!(f, "tls handshake: {e}"),
            Self::Protocol(e) => write!(f, "websocket protocol: {e:?}"),
            Self::Upgrade(e) => write!(f, "websocket upgrade: {e}"),
            Self::Closed => f.write_str("websocket closed for sending"),
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
        }
    }
}

impl From<sheath::Error> for Error {
    fn from(e: sheath::Error) -> Self {
        Self::Tls(e)
    }
}

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
/// WebSocket upgrade for `path`.
pub async fn connect(
    handle: &NetHandle,
    stream: TcpStream,
    config: Arc<ClientConfig>,
    host: &str,
    port: u16,
    path: &str,
) -> Result<Wss, Error> {
    let mut tls = tls_connect(handle, stream, config, host).await?;
    let (request, key) = upgrade::request(host, port, 443, path, rand::random());
    send_bytes(&mut tls.tx, request.as_bytes()).await?;
    let mut head = Vec::new();
    loop {
        if !tls.ready().await? {
            return Err(Error::Upgrade("server closed before the response".into()));
        }
        let (taken, done) = upgrade::read_head(&mut head, tls.rx.current()).map_err(Error::Upgrade)?;
        tls.rx.consume(taken);
        if done {
            break;
        }
    }
    upgrade::check(&head, &key).map_err(Error::Upgrade)?;
    Ok(Wss(Session::new(tls.rx, tls.tx, tls.tcp)))
}

/// The ws:// upgrade for `path` on `stream`, connected to `host`'s `port`.
pub async fn connect_ws(handle: &NetHandle, stream: TcpStream, host: &str, port: u16, path: &str) -> Result<Ws, Error> {
    let (mut tcp, tcp_tx) = stream.split();
    let mut tx = SwitchTx::new(handle, tcp_tx).await;
    let (request, key) = upgrade::request(host, port, 80, path, rand::random());
    send_bytes(&mut tx, request.as_bytes()).await?;
    let mut rx = Raw::new();
    let mut head = Vec::new();
    loop {
        match rx.chunk(&mut tx)? {
            Some(run) => {
                let (taken, done) = upgrade::read_head(&mut head, run.bytes).map_err(Error::Upgrade)?;
                rx.consume(taken);
                if done {
                    break;
                }
            }
            None => rx.push(tcp.recv().await.map_err(Error::Recv)?, &mut tx)?,
        }
    }
    upgrade::check(&head, &key).map_err(Error::Upgrade)?;
    Ok(Ws(Session::new(rx, tx, tcp)))
}

/// An open wss:// connection. A server ping is answered within `recv`; the server's close is
/// echoed there, followed by close_notify and FIN.
pub struct Wss(Session<TlsRx<TcpFrame, SwitchTx>, TlsTx<SwitchTx>>);

/// An open ws:// connection. A server ping is answered within `recv`; the server's close is
/// echoed there, followed by FIN.
pub struct Ws(Session<Raw<TcpFrame, SwitchTx>, SwitchTx>);

impl Wss {
    /// The next message; `None` once the close handshake is done (ours or the server's).
    pub async fn recv(&mut self) -> Result<Option<Message<'_>>, Error> {
        self.0.recv().await
    }

    /// Sends `payload` as one message, a TCP frame at a time.
    pub async fn send(&mut self, kind: Kind, payload: &[u8]) -> Result<(), Error> {
        self.0.send(kind, payload).await
    }

    /// Sends a close frame (`code`, `reason`), then close_notify and FIN. Receiving goes on until
    /// `recv` returns `None`, after the server's echo.
    pub async fn close(&mut self, code: u16, reason: &str) -> Result<(), Error> {
        self.0.close(code, reason).await
    }
}

impl Ws {
    /// The next message; `None` once the close handshake is done (ours or the server's).
    pub async fn recv(&mut self) -> Result<Option<Message<'_>>, Error> {
        self.0.recv().await
    }

    /// Sends `payload` as one message, a TCP frame at a time.
    pub async fn send(&mut self, kind: Kind, payload: &[u8]) -> Result<(), Error> {
        self.0.send(kind, payload).await
    }

    /// Sends a close frame (`code`, `reason`), then FIN. Receiving goes on until
    /// `recv` returns `None`, after the server's echo.
    pub async fn close(&mut self, code: u16, reason: &str) -> Result<(), Error> {
        self.0.close(code, reason).await
    }
}

/// The WebSocket halves over one lower layer, and the TCP receive half that feeds it.
struct Session<R: PlainRx, T: PlainTx> {
    rx: WsRx<R>,
    tx: WsTx<T>,
    tcp: TcpRx,
}

impl<R, T> Session<R, T>
where
    R: PlainRx<Frame = TcpFrame, Tx = T>,
    T: PlainTx,
    Error: From<R::Error> + From<T::Error>,
{
    fn new(rx: R, tx: T, tcp: TcpRx) -> Self {
        Session { rx: WsRx::new(rx, Config::new(Role::Client)), tx: WsTx::new(tx, Role::Client), tcp }
    }

    async fn recv(&mut self) -> Result<Option<Message<'_>>, Error> {
        loop {
            let ready = match self.rx.has_message_via(&mut self.tx) {
                Ok(ready) => ready,
                Err(WsError::Closed) => {
                    self.tx.close(self.rx.peer_close().expect("a close was received"));
                    self.tx.shutdown().await?;
                    return Ok(None);
                }
                Err(WsError::Lower(e)) => return Err(self.lower_failed(e).await),
                Err(WsError::Protocol(e)) => return Err(Error::Protocol(e)),
            };
            if let Some(p) = self.rx.take_pong() {
                self.tx.pong(p);
            }
            self.tx.flush().await?;
            if ready {
                return match self.rx.recv_via(&mut self.tx) {
                    Ok(m) => Ok(Some(m.expect("has_message_via found one"))),
                    Err(_) => unreachable!("has_message_via found a message"),
                };
            }
            let frame = self.tcp.recv().await.map_err(Error::Recv)?;
            if let Err(e) = self.rx.push_via(frame, &mut self.tx) {
                return Err(self.lower_failed(e).await);
            }
        }
    }

    /// A receive error below WebSocket (TLS): sends the alert the lower layer queued for it,
    /// best effort, and yields once so the Net's next turn sends it before the caller can drop
    /// the connection (a drop with server data unread aborts with RST and discards it). FIN
    /// follows when the connection is dropped.
    async fn lower_failed(&mut self, e: R::Error) -> Error {
        let _ = self.tx.flush().await;
        switch::yield_now().await;
        e.into()
    }

    async fn send(&mut self, kind: Kind, payload: &[u8]) -> Result<(), Error> {
        self.tx.send(kind, payload).await.map_err(|e| match e {
            TxError::Closed => Error::Closed,
            TxError::Lower(e) => e.into(),
        })
    }

    async fn close(&mut self, code: u16, reason: &str) -> Result<(), Error> {
        let mut payload = code.to_be_bytes().to_vec();
        payload.extend_from_slice(reason.as_bytes());
        self.tx.close(&payload);
        Ok(self.tx.shutdown().await?)
    }
}
