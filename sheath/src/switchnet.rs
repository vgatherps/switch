//! TLS 1.3 over switchnet TCP: [`tls_connect`] runs the handshake (a rare path, which
//! allocates), then [`Tls`] holds the two halves and the TCP receive half feeding them. Its
//! futures run as switch tasks. switchnet's frames are this crate's [`RxFrame`]/[`TxFrame`].

use std::fmt;
use std::sync::Arc;

use rustls::ClientConfig;
use rustls::client::UnbufferedClientConnection;
use rustls::pki_types::ServerName;
use rustls::unbuffered::{ConnectionState, UnbufferedStatus};
use switchnet::{NetHandle, RecvError, SendError, TcpFrame, TcpRx, TcpStream, TcpTx};

use crate::{Error as TlsError, FrameTx, RxFrame, TlsRx, TlsTx, TxError, TxFrame, encode_into};

/// Most TCP payload per sent frame: the Ethernet MSS. A lower effective MSS makes conduit cut
/// and copy each frame, which stays correct.
const FRAME_PAYLOAD: usize = 1460;

/// The plaintext room advertised for frames of `capacity` bytes.
fn frame_room(capacity: usize) -> usize {
    capacity.min(FRAME_PAYLOAD)
}

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

impl TxFrame for switchnet::TxFrame {
    fn room_mut(&mut self) -> &mut [u8] {
        switchnet::TxFrame::room_mut(self)
    }
}

/// Raw TCP: frames go out as they are. Below [`TlsTx`], or alone for a plain stream.
pub struct SwitchTx {
    handle: NetHandle,
    tcp: TcpTx,
    /// `frame_room` of the Stack's frames.
    room: usize,
}

impl SwitchTx {
    /// Sends on `tcp`, in frames from `handle`, sized by the Stack's frame capacity (one frame
    /// is taken to read it).
    pub async fn new(handle: &NetHandle, tcp: TcpTx) -> SwitchTx {
        let room = frame_room(handle.tx_frame().await.capacity());
        SwitchTx { handle: handle.clone(), tcp, room }
    }
}

impl FrameTx for SwitchTx {
    type Frame = switchnet::TxFrame;
    type Sealed = switchnet::TxFrame;
    type Error = SendError;

    async fn alloc(&mut self) -> Result<switchnet::TxFrame, SendError> {
        Ok(self.handle.tx_frame().await)
    }

    async fn room(&mut self, len: usize) -> Result<(), SendError> {
        self.tcp.room(len).await
    }

    fn plain(&self) -> (usize, usize) {
        (0, self.room)
    }

    fn seal(&mut self, mut f: switchnet::TxFrame, len: usize) -> Result<switchnet::TxFrame, SendError> {
        f.set_len(len);
        Ok(f)
    }

    fn try_send(&mut self, f: switchnet::TxFrame) -> Result<(), switchnet::TxFrame> {
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
    Recv(RecvError),
    /// The TLS handshake or a received record failed.
    Tls(TlsError),
    /// Sending failed (TLS or TCP).
    Send(TxError<SendError>),
    /// The handshake could not start (a bad server name) or ended in a state other than
    /// application traffic.
    Handshake(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Recv(e) => write!(f, "tcp receive: {e}"),
            Self::Tls(e) => write!(f, "{e}"),
            Self::Send(e) => write!(f, "send: {e}"),
            Self::Handshake(e) => write!(f, "tls handshake: {e}"),
        }
    }
}

impl std::error::Error for Error {}

/// A TLS 1.3 connection over switchnet TCP, after the handshake: the receive half, the send
/// half, and the TCP receive half that feeds `rx`.
pub struct Tls {
    pub rx: TlsRx<TcpFrame, SwitchTx>,
    pub tx: TlsTx<SwitchTx>,
    pub tcp: TcpRx,
}

impl Tls {
    /// Waits until `rx` holds plaintext to read through `rx.current()`/`rx.consume` (`Ok(true)`),
    /// or the server closed with close_notify (`Ok(false)`). A TLS error first sends the alert
    /// rustls queued for it, then FIN; the TCP stream ending without close_notify is truncation.
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
        let sealed = self.tx.close_notify(f).map_err(Error::Send)?;
        send_sealed(&mut self.tx, sealed, room).await.map_err(Error::Send)?;
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
async fn send_sealed<L: FrameTx>(lower: &mut L, mut sealed: L::Sealed, len: usize) -> Result<(), L::Error> {
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
    let mut raw = SwitchTx::new(handle, tcp).await;
    let name = ServerName::try_from(host.to_owned()).map_err(|e| Error::Handshake(format!("server name {host}: {e}")))?;
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Frames below the Ethernet MSS (an IP MTU of 1280 gives 1252 B) are advertised as they
    /// are, so TLS never seals past the frame.
    #[test]
    fn frame_room_is_the_capacity_up_to_the_mss() {
        assert_eq!(frame_room(1472), 1460);
        assert_eq!(frame_room(1460), 1460);
        assert_eq!(frame_room(1252), 1252);
    }
}
