//! Differential against tungstenite 0.30 reading the same bytes from a
//! Cursor: same data messages, same newest ping, same ending (clean end,
//! close code, or failure class).

mod common;

use std::io::Cursor;

use common::*;
use relay::{Config, Kind, ProtocolError, Raw, Role, WsError, WsRx};
use tungstenite::protocol::{Role as TRole, WebSocket, WebSocketConfig};
use tungstenite::{Error as TError, Message as TMessage};

const MAX_FRAME: usize = 300;
const MAX_MESSAGE: usize = 600;

#[derive(Debug, PartialEq, Eq)]
enum End {
    /// The bytes ran out between frames.
    Eof,
    Close(Option<u16>),
    /// Close code of the failure: 1002, 1007, 1009.
    Fail(u16),
}

#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    msgs: Vec<(Kind, Vec<u8>)>,
    ping: Option<Vec<u8>>,
    end: End,
}

fn tungstenite(role: Role, wire: &[u8]) -> Outcome {
    let cfg = WebSocketConfig::default().max_frame_size(Some(MAX_FRAME)).max_message_size(Some(MAX_MESSAGE));
    let trole = if role == Role::Server { TRole::Server } else { TRole::Client };
    let mut ws = WebSocket::from_raw_socket(Duplex(Cursor::new(wire.to_vec())), trole, Some(cfg));
    let mut out = Outcome { msgs: Vec::new(), ping: None, end: End::Eof };
    loop {
        match ws.read() {
            Ok(TMessage::Text(t)) => out.msgs.push((Kind::Text, t.as_bytes().to_vec())),
            Ok(TMessage::Binary(b)) => out.msgs.push((Kind::Binary, b.to_vec())),
            Ok(TMessage::Ping(p)) => out.ping = Some(p.to_vec()),
            Ok(TMessage::Pong(_)) => {}
            Ok(TMessage::Close(c)) => {
                out.end = End::Close(c.map(|c| c.code.into()));
                return out;
            }
            Ok(TMessage::Frame(_)) => unreachable!(),
            Err(TError::Protocol(tungstenite::error::ProtocolError::ResetWithoutClosingHandshake)) => return out,
            Err(TError::Protocol(_)) => {
                out.end = End::Fail(1002);
                return out;
            }
            Err(TError::Utf8(_)) => {
                out.end = End::Fail(1007);
                return out;
            }
            Err(TError::Capacity(_)) => {
                out.end = End::Fail(1009);
                return out;
            }
            Err(e) => panic!("unexpected tungstenite error {e:?}"),
        }
    }
}

fn relay(role: Role, wire: &[u8], cuts: &[usize]) -> Outcome {
    let cfg = Config { max_frame: MAX_FRAME, max_message: MAX_MESSAGE, ..Config::new(role) };
    let mut rx = WsRx::new(Raw::new(), cfg);
    let run = drive(&mut rx, &split(wire, cuts));
    let end = match run.end {
        None => End::Eof,
        Some(WsError::Closed) => {
            let p = rx.peer_close().unwrap();
            End::Close((p.len() >= 2).then(|| u16::from_be_bytes([p[0], p[1]])))
        }
        // tungstenite reports a disallowed close code as a close with 1002.
        Some(WsError::Protocol(ProtocolError::InvalidCloseCode(_))) => End::Close(Some(1002)),
        Some(WsError::Protocol(e)) => End::Fail(e.close_code()),
        Some(WsError::Lower(e)) => match e {},
    };
    Outcome { msgs: run.msgs.into_iter().map(|g| (g.kind, g.payload)).collect(), ping: run.pong, end }
}

const CHARS: [&str; 8] = ["a", "b", "{", "\"", "0", "€", "𝄞", "ß"];

fn text(rng: &mut Rng, max_chars: usize) -> Vec<u8> {
    let n = rng.below(max_chars + 1);
    (0..n).flat_map(|_| CHARS[rng.below(CHARS.len())].bytes()).collect()
}

/// A random stream of frames, mostly valid, with occasional violations.
fn stream(rng: &mut Rng, role: Role) -> Vec<u8> {
    let mut wire = Vec::new();
    // The open fragmented message: its opcode kind and the bytes still to send.
    let mut open: Option<(Kind, Vec<u8>)> = None;
    for _ in 0..1 + rng.below(16) {
        let mut mask = (role == Role::Server).then(|| rng.next().to_be_bytes()[..4].try_into().unwrap());
        let r = rng.below(100);
        let (b0, payload) = if r < 55 {
            match open.take() {
                Some((kind, rest)) => {
                    let last = rng.below(3) == 0;
                    let n = if last { rest.len() } else { rng.below(rest.len() + 1) };
                    if !last {
                        open = Some((kind, rest[n..].to_vec()));
                    }
                    (if last { fin(CONT) } else { CONT }, rest[..n].to_vec())
                }
                None => {
                    let kind = if rng.below(2) == 0 { Kind::Text } else { Kind::Binary };
                    let len = if rng.below(10) == 0 { 100 + rng.below(250) } else { rng.below(40) };
                    let body = match kind {
                        Kind::Text => text(rng, len / 2),
                        Kind::Binary => rng.bytes(len),
                    };
                    if rng.below(3) == 0 {
                        let n = rng.below(body.len() + 1);
                        open = Some((kind, body[n..].to_vec()));
                        (kind_op(kind), body[..n].to_vec())
                    } else {
                        (fin(kind_op(kind)), body)
                    }
                }
            }
        } else if r < 75 {
            let n = rng.below(20);
            (fin(if rng.below(2) == 0 { PING } else { PONG }), rng.bytes(n))
        } else if r < 78 {
            let mut p = [1000u16, 1001, 1011, 3000, 4999][rng.below(5)].to_be_bytes().to_vec();
            p.extend_from_slice(&text(rng, 5));
            (fin(CLOSE), if rng.below(4) == 0 { Vec::new() } else { p })
        } else {
            match rng.below(12) {
                0 => (fin(BINARY) | [0x40, 0x20, 0x10][rng.below(3)], rng.bytes(3)),
                1 => (fin([3, 4, 7, 11, 15][rng.below(5)]), rng.bytes(2)),
                2 => {
                    mask = if mask.is_some() { None } else { Some([1, 2, 3, 4]) };
                    (fin(BINARY), rng.bytes(4))
                }
                3 => (fin(CONT), b"c".to_vec()),
                4 => (fin(TEXT), b"t".to_vec()),
                5 => (PING, Vec::new()),
                6 => (fin(PING), rng.bytes(126)),
                7 => {
                    let n = MAX_FRAME + 1 + rng.below(50);
                    (fin(BINARY), rng.bytes(n))
                }
                8 => {
                    let mut t = text(rng, 6);
                    let at = rng.below(t.len() + 1);
                    t.insert(at, [0xff, 0x80, 0xc0, 0xed][rng.below(4)]);
                    (fin(TEXT), t)
                }
                9 => (fin(CLOSE), vec![3]),
                10 => {
                    let code = [0u16, 999, 1004, 1005, 1006, 1015, 2999, 5000][rng.below(8)];
                    (fin(CLOSE), code.to_be_bytes().to_vec())
                }
                _ => (fin(CLOSE), vec![0x03, 0xe8, 0xff]),
            }
        };
        wire.extend_from_slice(&encode(b0, mask, &payload));
    }
    wire
}

#[test]
#[cfg_attr(miri, ignore)]
fn same_outcome_as_tungstenite() {
    let mut rng = Rng(0x0ddb_1a5e_5bad_5eed);
    let mut ends = std::collections::BTreeMap::<String, usize>::new();
    for i in 0..4000 {
        let role = if i % 2 == 0 { Role::Client } else { Role::Server };
        let wire = stream(&mut rng, role);
        let want = tungstenite(role, &wire);
        for max in [1, 5, 64, wire.len().max(1)] {
            let cuts = random_cuts(&mut rng, wire.len(), max);
            assert_eq!(relay(role, &wire, &cuts), want, "case {i} {role:?} cuts {cuts:?} wire {wire:x?}");
        }
        *ends.entry(format!("{:?}", want.end)).or_default() += 1;
    }
    // Every ending class shows up, so the comparison is not vacuous.
    for key in ["Eof", "Fail(1002)", "Fail(1007)", "Fail(1009)", "Close(None)", "Close(Some(1000))", "Close(Some(1002))"] {
        assert!(ends.get(key).copied().unwrap_or(0) > 10, "{key}: {ends:?}");
    }
}
