//! A wss client over switchnet TCP: [`connect`] runs the TLS 1.3 handshake and the WebSocket
//! upgrade (rare paths, which allocate), then [`Wss`] receives and sends messages through
//! relay over [`TlsRx`]/[`TlsTx`]. Its futures run as switch tasks.

use std::fmt;
use std::sync::Arc;

use relay::{Config, Kind, PlainTx, ProtocolError, Role, RxFrame, TxError as WsTxError, WsError, WsRx, WsTx};
use rustls::ClientConfig;
use rustls::client::UnbufferedClientConnection;
use rustls::pki_types::ServerName;
use rustls::unbuffered::{ConnectionState, UnbufferedStatus};
use sha1::{Digest, Sha1};
use switchnet::{NetHandle, RecvError, SendError, TcpRx, TcpStream, TcpTx};

use crate::{Error as TlsError, TlsRx, TlsTx, TxError, encode_into};

/// TCP payload per sent frame: the Ethernet MSS. A lower effective MSS makes conduit cut and
/// copy each frame, which stays correct.
const FRAME_PAYLOAD: usize = 1460;

/// Most bytes of the server's upgrade response accepted.
const MAX_RESPONSE: usize = 16 * 1024;

/// A received TCP frame as relay and sheath see it.
#[derive(Debug)]
pub struct Frame(pub switchnet::TcpFrame);

impl RxFrame for Frame {
    fn payload(&self) -> &[u8] {
        self.0.payload()
    }

    fn payload_mut(&mut self) -> &mut [u8] {
        self.0.payload_mut()
    }

    fn rx_timestamp(&self) -> u64 {
        self.0.rx_timestamp()
    }
}

/// A frame to send, as relay and sheath fill it.
pub struct SendFrame(switchnet::TxFrame);

impl relay::TxFrame for SendFrame {
    fn room_mut(&mut self) -> &mut [u8] {
        self.0.room_mut()
    }
}

/// Raw TCP below [`TlsTx`]: frames go out as they are.
pub struct SwitchTx {
    handle: NetHandle,
    tcp: TcpTx,
}

impl PlainTx for SwitchTx {
    type Frame = SendFrame;
    type Sealed = SendFrame;
    type Error = SendError;

    async fn alloc(&mut self) -> Result<SendFrame, SendError> {
        Ok(SendFrame(self.handle.tx_frame().await))
    }

    async fn room(&mut self, len: usize) -> Result<(), SendError> {
        self.tcp.room(len).await
    }

    fn plain(&self) -> (usize, usize) {
        (0, FRAME_PAYLOAD)
    }

    fn seal(&mut self, mut f: SendFrame, len: usize) -> Result<SendFrame, SendError> {
        f.0.set_len(len);
        Ok(f)
    }

    fn has_pending(&self) -> bool {
        false
    }

    fn take_pending(&mut self, _: SendFrame) -> Result<Option<SendFrame>, SendError> {
        Ok(None)
    }

    fn seal_close(&mut self, _: SendFrame) -> Result<Option<SendFrame>, SendError> {
        Ok(None)
    }

    fn try_send(&mut self, s: SendFrame) -> Result<(), SendFrame> {
        self.tcp.try_send(s.0).map_err(|(f, _)| SendFrame(f))
    }

    fn shutdown(&mut self) {
        self.tcp.shutdown();
    }
}

/// A received message.
pub type Message<'a> = relay::Message<'a, TlsRx<Frame, SwitchTx>>;

#[derive(Debug)]
pub enum Error {
    /// TCP receive failed, or the stream ended (FIN) before the WebSocket close.
    Recv(RecvError),
    /// The TLS handshake or a received record failed.
    Tls(TlsError),
    /// Sending failed below WebSocket (TLS or TCP).
    Send(TxError<SendError>),
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
            Self::Protocol(e) => write!(f, "websocket protocol: {e:?}"),
            Self::Upgrade(e) => write!(f, "websocket upgrade: {e}"),
            Self::Closed => f.write_str("websocket closed for sending"),
        }
    }
}

impl std::error::Error for Error {}

impl From<WsError<TlsError>> for Error {
    fn from(e: WsError<TlsError>) -> Self {
        match e {
            WsError::Protocol(e) => Self::Protocol(e),
            WsError::Closed => unreachable!("the peer's close is handled by Wss::recv"),
            WsError::Lower(e) => Self::Tls(e),
        }
    }
}

impl From<WsTxError<TxError<SendError>>> for Error {
    fn from(e: WsTxError<TxError<SendError>>) -> Self {
        match e {
            WsTxError::Closed => Self::Closed,
            WsTxError::Lower(e) => Self::Send(e),
        }
    }
}

/// The TLS handshake for `host` on `stream`, then the WebSocket upgrade for `path`.
pub async fn connect(
    handle: &NetHandle,
    stream: TcpStream,
    config: Arc<ClientConfig>,
    host: &str,
    path: &str,
) -> Result<Wss, Error> {
    let (mut tcp_rx, tcp) = stream.split();
    let mut raw = SwitchTx { handle: handle.clone(), tcp };
    let name = ServerName::try_from(host.to_owned()).map_err(|e| Error::Upgrade(format!("server name {host}: {e}")))?;
    let mut key = [0; 16];
    config
        .crypto_provider()
        .secure_random
        .fill(&mut key)
        .map_err(|_| Error::Upgrade("no randomness for Sec-WebSocket-Key".into()))?;
    let mut conn = UnbufferedClientConnection::new(config, name).map_err(|e| Error::Tls(TlsError::Tls(e)))?;

    // TLS handshake: received bytes are joined in `received`, sent records gathered in `out`.
    let mut received = Vec::new();
    let mut stamp = 0;
    let mut out = Vec::new();
    loop {
        enum Next {
            Again,
            Send,
            Read,
            Done,
        }
        let UnbufferedStatus { discard, state } = conn.process_tls_records(&mut received);
        let next = match state.map_err(|e| Error::Tls(TlsError::Tls(e)))? {
            ConnectionState::EncodeTlsData(mut data) => {
                encode_into(&mut out, &mut data);
                Next::Again
            }
            ConnectionState::TransmitTlsData(data) => {
                data.done();
                Next::Send
            }
            ConnectionState::BlockedHandshake => Next::Read,
            ConnectionState::WriteTraffic(_) => Next::Done,
            state => return Err(Error::Upgrade(format!("TLS handshake ended in {state:?}"))),
        };
        received.drain(..discard);
        if !out.is_empty() && !matches!(next, Next::Again) {
            send_bytes(&mut raw, &out).await.map_err(|e| Error::Send(TxError::Lower(e)))?;
            out.clear();
        }
        match next {
            Next::Again | Next::Send => {}
            Next::Read => {
                let frame = tcp_rx.recv().await.map_err(Error::Recv)?;
                stamp = frame.rx_timestamp();
                received.extend_from_slice(frame.payload());
            }
            Next::Done => break,
        }
    }
    let mut tls = TlsTx::new(conn, raw);
    let mut rx = TlsRx::with_leftover(&received, stamp);

    // WebSocket upgrade (RFC 6455 §4.1).
    let key = data_encoding::BASE64.encode(&key);
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n"
    );
    send_bytes(&mut tls, request.as_bytes()).await.map_err(Error::Send)?;
    let response = read_response(&mut rx, &mut tls, &mut tcp_rx).await?;
    check_response(&response, &key)?;

    Ok(Wss {
        rx: WsRx::new(rx, Config::new(Role::Client)),
        tx: WsTx::new(tls, Role::Client),
        tcp: tcp_rx,
    })
}

/// Sends `bytes` through `lower`, a frame at a time, each sent once there is room for it.
async fn send_bytes<L: PlainTx>(lower: &mut L, bytes: &[u8]) -> Result<(), L::Error> {
    let (head, room) = lower.plain();
    for piece in bytes.chunks(room) {
        let mut f = lower.alloc().await?;
        relay::TxFrame::room_mut(&mut f)[head..head + piece.len()].copy_from_slice(piece);
        lower.room(piece.len()).await?;
        let mut sealed = lower.seal(f, piece.len())?;
        while let Err(s) = lower.try_send(sealed) {
            sealed = s;
            lower.room(piece.len()).await?;
        }
    }
    Ok(())
}

/// Reads the upgrade response's head, through the blank line; bytes after it stay unread for
/// the WebSocket receive.
async fn read_response(
    rx: &mut TlsRx<Frame, SwitchTx>,
    tls: &mut TlsTx<SwitchTx>,
    tcp: &mut TcpRx,
) -> Result<Vec<u8>, Error> {
    use relay::PlainRx;

    let mut head = Vec::new();
    loop {
        match rx.chunk(tls).map_err(Error::Tls)? {
            Some(chunk) => {
                let before = head.len();
                head.extend_from_slice(chunk.bytes);
                let end = head.windows(4).position(|w| w == b"\r\n\r\n").map(|at| at + 4);
                let taken = end.unwrap_or(head.len());
                head.truncate(taken);
                rx.consume(taken - before);
                if end.is_some() {
                    return Ok(head);
                }
                if head.len() > MAX_RESPONSE {
                    return Err(Error::Upgrade(format!("response head over {MAX_RESPONSE} B")));
                }
            }
            None => {
                let frame = tcp.recv().await.map_err(Error::Recv)?;
                rx.push(Frame(frame), tls).map_err(Error::Tls)?;
            }
        }
    }
}

/// RFC 6455 §4.2.2: status 101, `Upgrade: websocket`, `Connection: upgrade`, and the accept key
/// derived from `key`.
fn check_response(response: &[u8], key: &str) -> Result<(), Error> {
    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut parsed = httparse::Response::new(&mut headers);
    match parsed.parse(response) {
        Ok(httparse::Status::Complete(_)) => {}
        Ok(httparse::Status::Partial) => unreachable!("the head ends with a blank line"),
        Err(e) => return Err(Error::Upgrade(format!("response: {e}"))),
    }
    if parsed.code != Some(101) {
        return Err(Error::Upgrade(format!("status {:?} {:?}", parsed.code, parsed.reason)));
    }
    let header = |name: &str| {
        parsed.headers.iter().find(|h| h.name.eq_ignore_ascii_case(name)).map(|h| h.value)
    };
    if !header("upgrade").is_some_and(|v| v.eq_ignore_ascii_case(b"websocket")) {
        return Err(Error::Upgrade("no Upgrade: websocket".into()));
    }
    let connection = header("connection").unwrap_or_default();
    if !connection.split(|&b| b == b',').any(|t| t.trim_ascii().eq_ignore_ascii_case(b"upgrade")) {
        return Err(Error::Upgrade("no Connection: upgrade".into()));
    }
    let mut sha1 = Sha1::new();
    sha1.update(key.as_bytes());
    sha1.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    let accept = data_encoding::BASE64.encode(&sha1.finalize());
    if header("sec-websocket-accept") != Some(accept.as_bytes()) {
        return Err(Error::Upgrade("wrong Sec-WebSocket-Accept".into()));
    }
    Ok(())
}

/// An open wss connection. A server ping is answered within `recv`; the server's close is
/// echoed there, followed by close_notify and FIN.
pub struct Wss {
    rx: WsRx<TlsRx<Frame, SwitchTx>>,
    tx: WsTx<TlsTx<SwitchTx>>,
    tcp: TcpRx,
}

impl Wss {
    /// The next message; `None` once the close handshake is done (ours or the server's).
    pub async fn recv(&mut self) -> Result<Option<Message<'_>>, Error> {
        loop {
            let ready = match self.rx.has_message_via(&mut self.tx) {
                Ok(ready) => ready,
                Err(WsError::Closed) => {
                    self.tx.close(self.rx.peer_close().expect("a close was received"));
                    self.tx.shutdown().await.map_err(Error::Send)?;
                    return Ok(None);
                }
                Err(e) => return Err(e.into()),
            };
            if let Some(p) = self.rx.take_pong() {
                self.tx.pong(p);
            }
            self.tx.flush().await.map_err(Error::Send)?;
            if ready {
                let m = self.rx.recv_via(&mut self.tx)?;
                return Ok(Some(m.expect("has_message_via found one")));
            }
            let frame = self.tcp.recv().await.map_err(Error::Recv)?;
            self.rx.push_via(Frame(frame), &mut self.tx).map_err(Error::Tls)?;
        }
    }

    /// Sends `payload` as one message, a TCP frame at a time.
    pub async fn send(&mut self, kind: Kind, payload: &[u8]) -> Result<(), Error> {
        Ok(self.tx.send(kind, payload).await?)
    }

    /// Sends a close frame (`code`, `reason`), then close_notify and FIN. Receiving goes on
    /// until `recv` returns `None`, after the server's echo.
    pub async fn close(&mut self, code: u16, reason: &str) -> Result<(), Error> {
        let mut payload = code.to_be_bytes().to_vec();
        payload.extend_from_slice(reason.as_bytes());
        self.tx.close(&payload);
        self.tx.shutdown().await.map_err(Error::Send)
    }
}
