//! relay (WebSocket client) over sheath (TLS 1.3) over in-memory frames, against tungstenite
//! as the WebSocket server on a rustls `ServerConnection`.
//!
//! Server-to-client TLS bytes are cut into frames stamped 0, 1, 2, ... in stream order, so a
//! message's expected stamp is the index of the frame holding the first byte of the record its
//! first WebSocket header byte is in.

use std::io::{self, Read, Write};

use super::peer::*;
use super::*;
use rustls::ServerConnection;
use sheath::{TlsRx, TlsTx};
use tungstenite::protocol::frame::coding::{CloseCode, Data, OpCode};
use tungstenite::protocol::frame::{CloseFrame, Frame};
use tungstenite::protocol::{Role as TRole, WebSocket};
use tungstenite::{Error as TError, Message as TMessage};

/// The server's application-data plaintext, as tungstenite's byte stream.
struct ServerIo(ServerConnection);

impl Read for ServerIo {
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        self.0.reader().read(b)
    }
}

impl Write for ServerIo {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.0.writer().write(b)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
struct Got {
    kind: Kind,
    payload: Vec<u8>,
    stamp: u64,
}

struct Wss {
    rx: WsRx<TlsRx<VecFrame, TestTx>>,
    tx: TxCore<TlsTx<TestTx>>,
    /// Client-to-server TLS bytes.
    wire: Wire,
    server: WebSocket<ServerIo>,
    /// tungstenite finished the close handshake; it reads nothing more.
    server_done: bool,
    /// Server-to-client TLS bytes not yet framed.
    to_client: Vec<u8>,
    /// Stream offset of `to_client[0]`.
    framed: usize,
    /// Stream offset where each pushed frame starts; frame `i` is stamped `i`.
    frame_starts: Vec<usize>,
    /// Where each pushed frame's bytes are in memory.
    frame_bytes: Vec<std::ops::Range<*const u8>>,
}

impl Wss {
    /// Handshake done, the session tickets delivered.
    fn new() -> Wss {
        Wss::with(connect())
    }

    fn with(peer: Peer) -> Wss {
        let Peer { tx, wire, mut server } = peer;
        // tungstenite writes whole messages; rustls would stop taking plaintext at 64 KiB.
        server.set_buffer_limit(None);
        let mut w = Wss {
            rx: WsRx::new(TlsRx::new(), Config::new(Role::Client, Limits::default())),
            tx: TxCore::new(tx, &Config::new(Role::Client, Limits::default())),
            wire,
            server: WebSocket::from_raw_socket(ServerIo(server), TRole::Server, None),
            server_done: false,
            to_client: Vec::new(),
            framed: 0,
            frame_starts: Vec::new(),
            frame_bytes: Vec::new(),
        };
        w.server_flush();
        assert!(w.client_read(FRAME_ROOM).is_empty());
        w
    }

    fn conn(&mut self) -> &mut ServerConnection {
        &mut self.server.get_mut().0
    }

    /// Moves the server's queued TLS records to `to_client`; the stream offset after them.
    fn server_flush(&mut self) -> usize {
        while self.conn().wants_write() {
            let mut out = Vec::new();
            self.conn().write_tls(&mut out).unwrap();
            self.to_client.extend_from_slice(&out);
        }
        self.framed + self.to_client.len()
    }

    /// Writes `msgs` with tungstenite and flushes; the stream offset where they start.
    fn server_send(&mut self, msgs: Vec<TMessage>) -> usize {
        let at = self.server_flush();
        for m in msgs {
            self.server.write(m).unwrap();
        }
        self.server.flush().unwrap();
        self.server_flush();
        at
    }

    /// Hands the server what the client sent, letting tungstenite read as it goes (rustls
    /// holds at most 16 KiB of received plaintext); the messages read and whether rustls saw
    /// close_notify. A completed close handshake ends the reading.
    fn server_deliver(&mut self) -> (Vec<TMessage>, bool) {
        let sent = self.wire.sent.take();
        let mut tls = &sent[..];
        let mut out = Vec::new();
        let mut closed = false;
        while !tls.is_empty() {
            self.conn().read_tls(&mut tls).unwrap();
            closed = self.conn().process_new_packets().unwrap().peer_has_closed();
            while !self.server_done {
                match self.server.read() {
                    Ok(m) => out.push(m),
                    Err(TError::Io(e)) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(TError::ConnectionClosed) => self.server_done = true,
                    Err(e) => panic!("tungstenite: {e}"),
                }
            }
        }
        (out, closed)
    }

    /// What tungstenite reads from what the client sent.
    fn server_read(&mut self) -> Vec<TMessage> {
        let (out, closed) = self.server_deliver();
        assert!(!closed);
        out
    }

    /// Pushes the next `len` bytes of `to_client` (all if fewer) as one frame.
    fn push(&mut self, len: usize) {
        let len = len.min(self.to_client.len());
        let data: Vec<u8> = self.to_client.drain(..len).collect();
        let stamp = self.frame_starts.len() as u64;
        self.frame_starts.push(self.framed);
        self.framed += len;
        self.frame_bytes.push(data.as_ptr_range());
        self.rx.push(VecFrame { data, stamp });
    }

    /// Pushes `to_client` in frames of `frame_len`, reading every message.
    fn client_read(&mut self, frame_len: usize) -> Vec<Got> {
        let mut got = Vec::new();
        loop {
            match self.rx.recv(self.tx.lower_mut()) {
                Ok(Some(m)) => {
                    // runs text()'s debug_assert over TLS frame and scratch runs
                    match m.kind() {
                        Kind::Text => assert_eq!(m.text().map(str::as_bytes), Some(m.bytes())),
                        Kind::Binary => assert_eq!(m.text(), None),
                    }
                    got.push(Got { kind: m.kind(), payload: m.bytes().to_vec(), stamp: m.rx_timestamp() })
                }
                Ok(None) if self.to_client.is_empty() => return got,
                Ok(None) => self.push(frame_len),
                Err(e) => panic!("relay: {e:?}"),
            }
        }
    }

    /// Stamp of the frame holding stream offset `at`.
    fn stamp_of(&self, at: usize) -> u64 {
        (self.frame_starts.iter().filter(|&&s| s <= at).count() - 1) as u64
    }

    fn client_send(&mut self, kind: Kind, payload: &[u8]) {
        assert_eq!(ready(self.tx.send(kind, payload)), Ok(()));
    }

    /// Answers a ping the client received.
    fn client_pong(&mut self) {
        if let Some(p) = self.rx.take_pong() {
            self.tx.ping(p);
        }
        ready(self.tx.finish());
    }
}

fn to_tmessage(kind: Kind, payload: &[u8]) -> TMessage {
    match kind {
        Kind::Text => TMessage::Text(String::from_utf8(payload.to_vec()).unwrap().into()),
        Kind::Binary => TMessage::Binary(payload.to_vec().into()),
    }
}

/// `len` bytes of UTF-8 with multibyte characters, so record and frame boundaries cut code
/// points.
fn text(len: usize) -> Vec<u8> {
    let mut s = "€ü".repeat(len / 5).into_bytes();
    s.resize(len, b'a');
    s
}

const SIZES: [usize; 6] = [0, 1, 125, 126, 65535, 70000];

/// Client sends, tungstenite echoes, client reads it back; for every size, both kinds, frames
/// of an MSS and of 100 B.
#[test]
#[cfg_attr(miri, ignore)]
fn echo() {
    for frame_len in [FRAME_ROOM, 100] {
        let mut w = Wss::new();
        for len in SIZES {
            for (kind, payload) in [(Kind::Binary, message(len, len)), (Kind::Text, text(len))] {
                w.client_send(kind, &payload);
                let msgs = w.server_read();
                assert_eq!(msgs, [to_tmessage(kind, &payload)], "len {len}");
                let at = w.server_send(msgs);
                let got = w.client_read(frame_len);
                let want = Got { kind, payload, stamp: w.stamp_of(at) };
                assert_eq!(got, [want], "len {len} frame_len {frame_len}");
            }
        }
    }
}

/// Fragmented messages both ways, with a ping between the server's fragments answered by the
/// client between its own.
#[test]
#[cfg_attr(miri, ignore)]
fn fragments_and_ping() {
    let mut w = Wss::new();
    let at = w.server_send(vec![
        TMessage::Frame(Frame::message(&b"frag"[..], OpCode::Data(Data::Text), false)),
        TMessage::Frame(Frame::ping(&b"hb"[..])),
        TMessage::Frame(Frame::message(&b"ment"[..], OpCode::Data(Data::Continue), false)),
        TMessage::Frame(Frame::message(&[b'!'; 2000][..], OpCode::Data(Data::Continue), true)),
    ]);
    let got = w.client_read(FRAME_ROOM);
    let mut want = b"fragment".to_vec();
    want.extend_from_slice(&[b'!'; 2000]);
    assert_eq!(got, [Got { kind: Kind::Text, payload: want, stamp: w.stamp_of(at) }]);

    // The client answers the ping, then sends a message of three TLS records.
    w.client_pong();
    assert_eq!(ready(w.tx.send(Kind::Binary, &[7; 3000])), Ok(()));
    assert_eq!(w.server_read(), [TMessage::Pong(b"hb".to_vec().into()), TMessage::Binary(vec![7; 3000].into())]);
}

#[test]
#[cfg_attr(miri, ignore)]
fn close_started_by_the_client() {
    let mut w = Wss::new();
    assert_eq!(ready(w.tx.close(1000, "done")), Ok(()));
    let msgs = w.server_read();
    assert!(matches!(&msgs[..], [TMessage::Close(Some(c))] if c.code == CloseCode::Normal && c.reason == "done"));
    // tungstenite wrote its echo while reading; it reaches the client.
    w.server_flush();
    w.push(FRAME_ROOM);
    assert!(matches!(w.rx.recv(w.tx.lower_mut()), Err(WsError::Closed)));
    assert_eq!(w.rx.peer_close(), Some(&[&1000u16.to_be_bytes()[..], b"done"].concat()[..]));
    // Then the client ends the TLS stream.
    w.tx.peer_close(w.rx.peer_close().unwrap());
    ready(w.tx.finish());
    assert!(w.wire.shut.get(), "no FIN after close_notify");
    let (msgs, closed) = w.server_deliver();
    assert!(msgs.is_empty() && closed, "no close_notify");
}

#[test]
#[cfg_attr(miri, ignore)]
fn close_started_by_the_server() {
    let mut w = Wss::new();
    w.server.close(Some(CloseFrame { code: CloseCode::Away, reason: "bye".into() })).unwrap();
    w.server.flush().unwrap();
    w.server_flush();
    w.push(FRAME_ROOM);
    assert!(matches!(w.rx.recv(w.tx.lower_mut()), Err(WsError::Closed)));
    let echo = w.rx.peer_close().unwrap().to_vec();
    assert_eq!(echo, [&1001u16.to_be_bytes()[..], b"bye"].concat());
    // Echo the close, then end the TLS stream with close_notify behind it.
    w.tx.peer_close(&echo);
    ready(w.tx.finish());
    let (msgs, closed) = w.server_deliver();
    assert!(closed, "no close_notify");
    assert!(matches!(&msgs[..], [TMessage::Close(Some(c))] if c.code == CloseCode::Away && c.reason == "bye"));
    assert!(w.server_done);
}

/// One record cut at every offset across two frames: the message comes out of sheath's scratch
/// copy, stamped with the frame holding the record's first byte.
#[test]
#[cfg_attr(miri, ignore)]
fn record_split_across_frames() {
    let payload = message(3, 109);
    let mut w = Wss::new();
    let mut record = None;
    for cut in 1.. {
        // A fresh record each round: same shape, next sequence number.
        let at = w.server_send(vec![TMessage::Binary(payload.clone().into())]);
        let len = *record.get_or_insert(w.to_client.len());
        assert_eq!(w.to_client.len(), len);
        if cut == len {
            break;
        }
        w.push(cut);
        assert!(w.rx.recv(w.tx.lower_mut()).unwrap().is_none(), "cut {cut}");
        w.push(len);
        let want = w.stamp_of(at);
        assert_eq!(want, w.frame_starts.len() as u64 - 2);
        let m = w.rx.recv(w.tx.lower_mut()).unwrap().unwrap();
        assert_eq!(m.rx_timestamp(), want, "cut {cut}");
        assert_eq!(m.bytes(), payload);
        assert!(w.rx.recv(w.tx.lower_mut()).unwrap().is_none());
    }
    assert_eq!(record, Some(5 + 2 + 109 + 17));
}

/// A message inside one record inside one frame is read in place, where rustls decrypted it in
/// the frame; two records in one frame both come out of it.
#[test]
#[cfg_attr(miri, ignore)]
fn message_inside_one_record_is_read_in_the_frame() {
    let mut w = Wss::new();
    let payload = message(5, 109);
    let at = w.server_send(vec![TMessage::Binary(payload.clone().into())]);
    w.push(FRAME_ROOM);
    let want = w.stamp_of(at);
    let frame = w.frame_bytes.last().unwrap().clone();
    let m = w.rx.recv(w.tx.lower_mut()).unwrap().unwrap();
    assert_eq!(m.rx_timestamp(), want);
    assert_eq!(m.bytes(), payload);
    // Record header (5) + WebSocket header (2): the payload sits right behind them.
    assert_eq!(m.bytes().as_ptr(), frame.start.wrapping_add(7));
    assert!(w.rx.recv(w.tx.lower_mut()).unwrap().is_none());

    let first = w.server_send(vec![TMessage::Binary(b"one".to_vec().into())]);
    w.server_send(vec![TMessage::Binary(b"two".to_vec().into())]);
    w.push(FRAME_ROOM);
    let want = w.stamp_of(first);
    let frame = w.frame_bytes.last().unwrap().clone();
    for expected in [&b"one"[..], b"two"] {
        let m = w.rx.recv(w.tx.lower_mut()).unwrap().unwrap();
        assert_eq!((m.bytes(), m.rx_timestamp()), (expected, want));
        assert!(frame.contains(&m.bytes().as_ptr()), "{expected:?} not read in the frame");
    }
}

/// The server updates its keys and asks the client to (KeyUpdate with update_requested)
/// between messages; messages keep arriving, and the client's next send carries its own
/// KeyUpdate in front, which the server accepts.
#[test]
#[cfg_attr(miri, ignore)]
fn server_key_update_mid_stream() {
    let mut w = Wss::new();
    let before = w.server_send(vec![TMessage::Text("before".into())]);
    w.conn().refresh_traffic_keys().unwrap();
    let after = w.server_send(vec![TMessage::Binary(message(1, 3000).into())]);
    assert!(after > before);
    let got = w.client_read(FRAME_ROOM);
    assert_eq!(
        got,
        [
            Got { kind: Kind::Text, payload: b"before".to_vec(), stamp: w.stamp_of(before) },
            Got { kind: Kind::Binary, payload: message(1, 3000), stamp: w.stamp_of(after) },
        ]
    );
    // The first send carries the client's KeyUpdate record in front of the data record.
    w.client_send(Kind::Text, b"after the update");
    assert_eq!(records(&w.wire.sent.borrow()).len(), 2);
    assert_eq!(w.server_read(), [TMessage::Text("after the update".into())]);
    w.client_send(Kind::Text, b"next");
    assert_eq!(records(&w.wire.sent.borrow()).len(), 1);
    assert_eq!(w.server_read(), [TMessage::Text("next".into())]);
    // Both directions keep working under the new keys.
    let at = w.server_send(vec![TMessage::Text("again".into())]);
    assert_eq!(w.client_read(FRAME_ROOM), [Got { kind: Kind::Text, payload: b"again".to_vec(), stamp: w.stamp_of(at) }]);
}

/// A client configured with a small `max_fragment_size` (256: 251 B of plaintext per record,
/// still above the largest control frame): every seal stays one record, so a 1384 B message
/// (the plaintext room of a 1460 B frame without the limit) goes out in records of at most
/// 251 B of plaintext and arrives whole. (Before, sheath advertised the frame's room, rustls
/// split one seal into 6 records past the frame, and the seal panicked.)
#[test]
#[cfg_attr(miri, ignore)]
fn small_max_fragment_size_keeps_each_seal_one_record() {
    let mut w = Wss::with(connect_with(Some(256)));
    w.client_send(Kind::Binary, &message(4, 1384));
    assert!(records(&w.wire.sent.borrow()).iter().all(|r| r.len() <= 5 + 251 + 17));
    assert_eq!(w.server_read(), [TMessage::Binary(message(4, 1384).into())]);
}
