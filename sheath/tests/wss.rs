//! relay (WebSocket client) over sheath (TLS 1.3) over in-memory frames, against tungstenite
//! as the WebSocket server on a rustls `ServerConnection`.
//!
//! Server-to-client TLS bytes are cut into frames stamped 0, 1, 2, ... in stream order, so a
//! message's expected stamp is the index of the frame holding the first byte of the record its
//! first WebSocket header byte is in.

mod common;

use std::io::{self, Read, Write};

use common::*;
use relay::{Config, Kind, Role, WsError, WsRx, WsTx};
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
    tx: WsTx<TlsTx<TestTx>>,
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
}

impl Wss {
    /// Handshake done, the session tickets delivered.
    fn new() -> Wss {
        let Peer { tx, wire, mut server } = connect(None);
        // tungstenite writes whole messages; rustls would stop taking plaintext at 64 KiB.
        server.set_buffer_limit(None);
        let mut w = Wss {
            rx: WsRx::new(TlsRx::new(), Config::new(Role::Client)),
            tx: WsTx::new(tx, Role::Client),
            wire,
            server: WebSocket::from_raw_socket(ServerIo(server), TRole::Server, None),
            server_done: false,
            to_client: Vec::new(),
            framed: 0,
            frame_starts: Vec::new(),
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
        self.rx.push_via(VecFrame { data, stamp }, &mut self.tx).unwrap();
    }

    /// Pushes `to_client` in frames of `frame_len`, reading every message.
    fn client_read(&mut self, frame_len: usize) -> Vec<Got> {
        let mut got = Vec::new();
        loop {
            match self.rx.recv_via(&mut self.tx) {
                Ok(Some(m)) => got.push(Got { kind: m.kind(), payload: m.bytes().to_vec(), stamp: m.rx_timestamp() }),
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
        let mut rest = payload;
        assert_eq!(self.tx.send(kind, &mut rest), Ok(true));
    }

    /// Answers a ping the client received.
    fn client_pong(&mut self) {
        if let Some(p) = self.rx.take_pong() {
            self.tx.pong(p);
        }
        assert_eq!(self.tx.flush(), Ok(true));
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

    // The client opens a fragmented message, answers the ping inside it, then finishes it.
    let mut f = w.tx.frame(Kind::Binary).unwrap().unwrap();
    f.room()[..3].copy_from_slice(b"abc");
    f.commit(3, false).unwrap();
    w.client_pong();
    let mut rest: &[u8] = &[7; 3000];
    assert_eq!(w.tx.send(Kind::Binary, &mut rest), Ok(true));
    let mut want = b"abc".to_vec();
    want.extend_from_slice(&[7; 3000]);
    assert_eq!(w.server_read(), [TMessage::Pong(b"hb".to_vec().into()), TMessage::Binary(want.into())]);
}

/// tungstenite pings; the client answers once with the same payload.
#[test]
#[cfg_attr(miri, ignore)]
fn server_ping_is_answered() {
    let mut w = Wss::new();
    w.server_send(vec![TMessage::Ping(b"p1".to_vec().into())]);
    assert!(w.client_read(FRAME_ROOM).is_empty());
    w.client_pong();
    assert_eq!(w.server_read(), [TMessage::Pong(b"p1".to_vec().into())]);
    // Nothing more is owed.
    assert_eq!(w.rx.take_pong(), None);
}

#[test]
#[cfg_attr(miri, ignore)]
fn close_started_by_the_client() {
    let mut w = Wss::new();
    w.tx.close(&[&1000u16.to_be_bytes()[..], b"done"].concat());
    assert_eq!(w.tx.flush(), Ok(true));
    let msgs = w.server_read();
    assert!(matches!(&msgs[..], [TMessage::Close(Some(c))] if c.code == CloseCode::Normal && c.reason == "done"));
    // tungstenite wrote its echo while reading; it reaches the client.
    w.server_flush();
    w.push(FRAME_ROOM);
    assert!(matches!(w.rx.recv_via(&mut w.tx), Err(WsError::Closed)));
    assert_eq!(w.rx.peer_close(), Some(&[&1000u16.to_be_bytes()[..], b"done"].concat()[..]));
    // Then the client ends the TLS stream.
    assert_eq!(w.tx.shutdown(), Ok(true));
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
    assert!(matches!(w.rx.recv_via(&mut w.tx), Err(WsError::Closed)));
    let echo = w.rx.peer_close().unwrap().to_vec();
    assert_eq!(echo, [&1001u16.to_be_bytes()[..], b"bye"].concat());
    // Echo the close, then end the TLS stream with close_notify behind it.
    w.tx.close(&echo);
    assert_eq!(w.tx.shutdown(), Ok(true));
    let (msgs, closed) = w.server_deliver();
    assert!(closed, "no close_notify");
    assert!(matches!(&msgs[..], [TMessage::Close(Some(c))] if c.code == CloseCode::Away && c.reason == "bye"));
    assert!(w.server_done);
}

/// One record cut at every offset across two frames: the message comes out of sheath's scratch
/// copy, stamped with the frame holding the record's first byte, and cannot be taken.
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
        assert!(w.rx.recv_via(&mut w.tx).unwrap().is_none(), "cut {cut}");
        w.push(len);
        let want = w.stamp_of(at);
        assert_eq!(want, w.frame_starts.len() as u64 - 2);
        let m = w.rx.recv_via(&mut w.tx).unwrap().unwrap();
        assert_eq!(m.rx_timestamp(), want, "cut {cut}");
        let Err(m) = m.take() else { panic!("message from a straddling record taken, cut {cut}") };
        assert_eq!(m.bytes(), payload);
        assert!(w.rx.recv_via(&mut w.tx).unwrap().is_none());
    }
    assert_eq!(record, Some(5 + 2 + 109 + 17));
}

/// A message inside one record inside one frame is taken with its frame: the payload is read
/// in place, where rustls decrypted it.
#[test]
#[cfg_attr(miri, ignore)]
fn take_message_inside_one_record() {
    let mut w = Wss::new();
    let payload = message(5, 109);
    let at = w.server_send(vec![TMessage::Binary(payload.clone().into())]);
    w.push(FRAME_ROOM);
    let want = w.stamp_of(at);
    let m = w.rx.recv_via(&mut w.tx).unwrap().unwrap();
    assert_eq!(m.rx_timestamp(), want);
    let Ok(t) = m.take() else { panic!("direct message not taken") };
    assert_eq!(t.bytes(), payload);
    assert_eq!(t.rx_timestamp(), want);
    let p = t.bytes().as_ptr();
    let frame = t.into_frame();
    assert!(frame.data.as_ptr_range().contains(&p), "payload not inside the frame");
    // Record header (5) + WebSocket header (2): the payload sits right behind them.
    assert_eq!(p, frame.data[7..].as_ptr());
    assert!(w.rx.recv_via(&mut w.tx).unwrap().is_none());

    // Two records in one frame: taking the first keeps the second readable.
    let first = w.server_send(vec![TMessage::Binary(b"one".to_vec().into())]);
    w.server_send(vec![TMessage::Binary(b"two".to_vec().into())]);
    w.push(FRAME_ROOM);
    let want = w.stamp_of(first);
    let Ok(t) = w.rx.recv_via(&mut w.tx).unwrap().unwrap().take() else { panic!() };
    assert_eq!(t.bytes(), b"one");
    assert_eq!(t.rx_timestamp(), want);
    let m = w.rx.recv_via(&mut w.tx).unwrap().unwrap();
    assert_eq!(m.bytes(), b"two");
    assert_eq!(m.rx_timestamp(), want);
}

/// A message spanning records (rustls cuts plaintext at 16 KiB) is assembled in relay's
/// scratch and given back by `take`.
#[test]
#[cfg_attr(miri, ignore)]
fn take_message_spanning_records_gives_it_back() {
    let mut w = Wss::new();
    let payload = message(9, 40_000);
    let at = w.server_send(vec![TMessage::Binary(payload.clone().into())]);
    assert!(records(&w.to_client).len() >= 3);
    let mut taken_back = false;
    loop {
        let want = w.stamp_of(at);
        match w.rx.recv_via(&mut w.tx).unwrap() {
            Some(m) => {
                assert_eq!(m.rx_timestamp(), want);
                let Err(m) = m.take() else { panic!("spanning message taken") };
                assert_eq!(m.bytes(), payload);
                taken_back = true;
            }
            None if w.to_client.is_empty() => break,
            None => w.push(FRAME_ROOM),
        }
    }
    assert!(taken_back);
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
