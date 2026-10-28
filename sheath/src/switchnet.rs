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

/// Most TCP payload per sent frame: the Ethernet MSS. A lower effective MSS makes conduit cut
/// and copy each frame, which stays correct.
const FRAME_PAYLOAD: usize = 1460;

/// The plaintext room advertised below TLS for frames of `capacity` bytes.
fn frame_room(capacity: usize) -> usize {
    capacity.min(FRAME_PAYLOAD)
}

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
    /// `frame_room` of the Stack's frames.
    room: usize,
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
        (0, self.room)
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

/// A TLS 1.3 connection over switchnet TCP, after the handshake: the receive half, the send
/// half, and the TCP receive half that feeds `rx`.
pub struct Tls {
    pub rx: TlsRx<Frame, SwitchTx>,
    pub tx: TlsTx<SwitchTx>,
    pub tcp: TcpRx,
}

impl Tls {
    /// Waits until `rx` holds plaintext to read through `rx.current()`/`rx.consume` (`Ok(true)`),
    /// or the server closed with close_notify (`Ok(false)`). A TLS error first sends the alert
    /// rustls queued for it, then FIN; the TCP stream ending without close_notify is truncation.
    pub async fn ready(&mut self) -> Result<bool, Error> {
        use relay::PlainRx;

        loop {
            let failed = match self.rx.chunk(&mut self.tx).map(|run| run.is_some()) {
                Ok(true) => return Ok(true),
                Ok(false) if self.rx.eof() => return Ok(false),
                Ok(false) => match self.tcp.recv().await {
                    Ok(frame) => self.rx.push(Frame(frame), &mut self.tx).err(),
                    Err(RecvError::Finished) => self.rx.fin(&mut self.tx).err(),
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

    /// Sends close_notify, then FIN. Receiving goes on.
    pub async fn close(&mut self) -> Result<(), Error> {
        let f = self.tx.alloc().await.map_err(Error::Send)?;
        let (_, room) = self.tx.plain();
        self.tx.room(room).await.map_err(Error::Send)?;
        if let Some(sealed) = self.tx.seal_close(f).map_err(Error::Send)? {
            send_sealed(&mut self.tx, sealed, room).await.map_err(Error::Send)?;
        }
        self.tx.shutdown();
        Ok(())
    }
}

/// After a TLS error: sends the alert rustls queued for it, if any, then FIN, and yields once so
/// the Net's next turn sends them before the caller can drop the socket (a drop with server data
/// unread aborts with RST and discards them). Best effort: the caller reports the TLS error,
/// which matters more than a failed send.
async fn abort(tx: &mut TlsTx<SwitchTx>) {
    if tx.has_pending() {
        let sent = async {
            let f = tx.alloc().await?;
            let (_, room) = tx.plain();
            tx.room(room).await?;
            if let Some(sealed) = tx.take_pending(f)? {
                send_sealed(tx, sealed, room).await?;
            }
            Ok::<(), TxError<SendError>>(())
        };
        let _ = sent.await;
    }
    tx.shutdown();
    switch::yield_now().await;
}

/// Sends `sealed`, waiting for room of `len` bytes each time it is refused.
async fn send_sealed<L: PlainTx>(lower: &mut L, mut sealed: L::Sealed, len: usize) -> Result<(), L::Error> {
    while let Err(s) = lower.try_send(sealed) {
        sealed = s;
        lower.room(len).await?;
    }
    Ok(())
}

/// The TLS handshake for `host` on `stream`. If rustls rejects the server's handshake, the alert
/// it queued goes out, then FIN, before the error returns.
pub async fn tls_connect(
    handle: &NetHandle,
    stream: TcpStream,
    config: Arc<ClientConfig>,
    host: &str,
) -> Result<Tls, Error> {
    let (mut tcp_rx, tcp) = stream.split();
    let room = frame_room(handle.tx_frame().await.capacity());
    let mut raw = SwitchTx { handle: handle.clone(), tcp, room };
    let name = ServerName::try_from(host.to_owned()).map_err(|e| Error::Upgrade(format!("server name {host}: {e}")))?;
    let mut conn = UnbufferedClientConnection::new(config, name).map_err(|e| Error::Tls(TlsError::Tls(e)))?;

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
        let UnbufferedStatus { discard, state } = conn.process_tls_records(&mut received);
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
            Ok(state) => return Err(Error::Upgrade(format!("TLS handshake ended in {state:?}"))),
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
            // the alert and FIN leave at the Net's next turn; dropping the socket before that,
            // with server data unread, would abort it with RST and discard them
            switch::yield_now().await;
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
        tx: TlsTx::new(conn, raw),
        rx: TlsRx::with_leftover(&received, stamp),
        tcp: tcp_rx,
    })
}

/// The TLS handshake for `host` on `stream`, connected to the server's `port`, then the
/// WebSocket upgrade for `path`.
pub async fn connect(
    handle: &NetHandle,
    stream: TcpStream,
    config: Arc<ClientConfig>,
    host: &str,
    port: u16,
    path: &str,
) -> Result<Wss, Error> {
    let mut key = [0; 16];
    config
        .crypto_provider()
        .secure_random
        .fill(&mut key)
        .map_err(|_| Error::Upgrade("no randomness for Sec-WebSocket-Key".into()))?;
    let mut tls = tls_connect(handle, stream, config, host).await?;

    // WebSocket upgrade (RFC 6455 §4.1).
    let key = data_encoding::BASE64.encode(&key);
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n",
        host = host_header(host, port),
    );
    send_bytes(&mut tls.tx, request.as_bytes()).await.map_err(Error::Send)?;
    let response = read_response(&mut tls).await?;
    check_response(&response, &key)?;

    Ok(Wss {
        rx: WsRx::new(tls.rx, Config::new(Role::Client)),
        tx: WsTx::new(tls.tx, Role::Client),
        tcp: tls.tcp,
    })
}

/// Sends `bytes` through `lower`, a frame at a time, each sent once there is room for it.
pub async fn send_bytes<L: PlainTx>(lower: &mut L, bytes: &[u8]) -> Result<(), L::Error> {
    let (head, room) = lower.plain();
    for piece in bytes.chunks(room) {
        let mut f = lower.alloc().await?;
        relay::TxFrame::room_mut(&mut f)[head..head + piece.len()].copy_from_slice(piece);
        lower.room(piece.len()).await?;
        let sealed = lower.seal(f, piece.len())?;
        send_sealed(lower, sealed, piece.len()).await?;
    }
    Ok(())
}

/// Reads the upgrade response's head, through the blank line; bytes after it stay unread for
/// the WebSocket receive.
async fn read_response(tls: &mut Tls) -> Result<Vec<u8>, Error> {
    use relay::PlainRx;

    let mut head = Vec::new();
    loop {
        if !tls.ready().await? {
            return Err(Error::Upgrade("server closed before the response".into()));
        }
        let before = head.len();
        head.extend_from_slice(tls.rx.current());
        let end = head.windows(4).position(|w| w == b"\r\n\r\n").map(|at| at + 4);
        let taken = end.unwrap_or(head.len());
        head.truncate(taken);
        tls.rx.consume(taken - before);
        if end.is_some() {
            return Ok(head);
        }
        if head.len() > MAX_RESPONSE {
            return Err(Error::Upgrade(format!("response head over {MAX_RESPONSE} B")));
        }
    }
}

/// RFC 6455 §4.2.2: status 101, `Upgrade: websocket`, `Connection: upgrade`, the accept key
/// derived from `key`, and no extension or subprotocol (we offer neither).
fn check_response(response: &[u8], key: &str) -> Result<(), Error> {
    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut parsed = httparse::Response::new(&mut headers);
    match parsed.parse(response) {
        Ok(httparse::Status::Complete(_)) => {}
        Ok(httparse::Status::Partial) => return Err(Error::Upgrade("incomplete response head".into())),
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
    if header("sec-websocket-accept") != Some(accept_key(key).as_bytes()) {
        return Err(Error::Upgrade("wrong Sec-WebSocket-Accept".into()));
    }
    // RFC 6455 §4.1: the server may select only what the client offered.
    for unrequested in ["Sec-WebSocket-Extensions", "Sec-WebSocket-Protocol"] {
        if header(unrequested).is_some() {
            return Err(Error::Upgrade(format!("unrequested {unrequested}")));
        }
    }
    Ok(())
}

/// Sec-WebSocket-Accept for `key` (RFC 6455 §4.2.2).
fn accept_key(key: &str) -> String {
    let mut sha1 = Sha1::new();
    sha1.update(key.as_bytes());
    sha1.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    data_encoding::BASE64.encode(&sha1.finalize())
}

/// RFC 7230 §5.4: the port only when it is not the scheme's default (wss: 443).
fn host_header(host: &str, port: u16) -> String {
    if port == 443 { host.to_owned() } else { format!("{host}:{port}") }
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
                Err(WsError::Lower(e)) => return Err(self.tls_failed(e).await),
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
            if let Err(e) = self.rx.push_via(Frame(frame), &mut self.tx) {
                return Err(self.tls_failed(e).await);
            }
        }
    }

    /// A TLS receive error: sends the alert rustls queued for it (the lower layer's pending
    /// record), best effort, and yields once so the Net's next turn sends it before the caller
    /// can drop the `Wss` (see `abort`). FIN follows when the `Wss` is dropped.
    async fn tls_failed(&mut self, e: TlsError) -> Error {
        let _ = self.tx.flush().await;
        switch::yield_now().await;
        Error::Tls(e)
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

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "dGhlIHNhbXBsZSBub25jZQ==";

    /// A valid 101 for `KEY` with `extra` header lines, minus any line naming `drop`.
    fn response(extra: &str, drop: &str) -> Vec<u8> {
        let lines = [
            "HTTP/1.1 101 Switching Protocols".to_owned(),
            "Upgrade: websocket".to_owned(),
            "Connection: Upgrade".to_owned(),
            format!("Sec-WebSocket-Accept: {}", accept_key(KEY)),
        ];
        let mut head = String::new();
        for line in lines.iter().filter(|l| drop.is_empty() || !l.starts_with(drop)) {
            head.push_str(line);
            head.push_str("\r\n");
        }
        head.push_str(extra);
        head.push_str("\r\n");
        head.into_bytes()
    }

    fn upgrade_error(response: &[u8]) -> String {
        match check_response(response, KEY) {
            Err(Error::Upgrade(e)) => e,
            other => panic!("expected an upgrade error: {other:?}"),
        }
    }

    #[test]
    fn accept_key_is_rfc_6455_example() {
        // RFC 6455 §1.3.
        assert_eq!(accept_key(KEY), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
    }

    #[test]
    fn valid_response_passes() {
        check_response(&response("", ""), KEY).unwrap();
        check_response(&response("Connection: keep-alive, Upgrade\r\n", "Connection"), KEY).unwrap();
    }

    #[test]
    fn bad_responses_are_refused() {
        assert!(upgrade_error(&response("Sec-WebSocket-Extensions: permessage-deflate\r\n", "")).contains("Extensions"));
        assert!(upgrade_error(&response("Sec-WebSocket-Protocol: chat\r\n", "")).contains("Protocol"));
        assert_eq!(upgrade_error(&response("", "Sec-WebSocket-Accept")), "wrong Sec-WebSocket-Accept");
        let wrong = format!("Sec-WebSocket-Accept: {}\r\n", accept_key("other"));
        assert_eq!(upgrade_error(&response(&wrong, "Sec-WebSocket-Accept")), "wrong Sec-WebSocket-Accept");
        let forbidden = String::from_utf8(response("", "")).unwrap().replace("101 Switching Protocols", "403 Forbidden");
        assert!(upgrade_error(forbidden.as_bytes()).starts_with("status Some(403)"));
        assert_eq!(upgrade_error(&response("", "Upgrade")), "no Upgrade: websocket");
        assert_eq!(upgrade_error(&response("", "Connection")), "no Connection: upgrade");
        assert_eq!(upgrade_error(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: web"), "incomplete response head");
    }

    #[test]
    fn host_header_names_the_port_unless_443() {
        assert_eq!(host_header("stream.example", 443), "stream.example");
        assert_eq!(host_header("stream.example", 9443), "stream.example:9443");
    }

    /// Frames below the Ethernet MSS (an IP MTU of 1280 gives 1252 B) are advertised as they
    /// are, so TLS never seals past the frame.
    #[test]
    fn frame_room_is_the_capacity_up_to_the_mss() {
        assert_eq!(frame_room(1472), 1460);
        assert_eq!(frame_room(1460), 1460);
        assert_eq!(frame_room(1252), 1252);
    }
}
