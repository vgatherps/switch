//! The generator's ws:// client: just enough of RFC 6455 for the bench, over plain TCP halves, so
//! that only the DUT side runs relay. A fixed-key upgrade request, a check of the 101 response,
//! masked single-frame binary messages out, unmasked single-frame binary echoes in. Every bench
//! message has the same size, so a connection's echo stream is a sequence of equal-size frames
//! (cut by `load::messages`); anything else from the DUT (ping, close, text, fragments) breaks the
//! sequence and is reported as an error.

use crate::wire::HEADER;

/// Largest bench ws message: client header (8 B) and payload in one TCP frame at the 1460 B MSS.
pub const MAX_SIZE: usize = 1400;
/// The upgrade request's Sec-WebSocket-Key (any 16 bytes, base64).
pub const KEY: &str = "dGhlIHNhbXBsZSBub25jZQ==";
/// Every client frame's masking key.
const MASK: [u8; 4] = [0x37, 0xfa, 0x21, 0x3d];
/// FIN + binary.
const FIN_BINARY: u8 = 0x82;

pub fn check_size(size: usize) -> Result<(), String> {
    if (HEADER..=MAX_SIZE).contains(&size) {
        Ok(())
    } else {
        Err(format!(
            "--size: ws messages are {HEADER}..={MAX_SIZE} B (one frame, one TCP segment)"
        ))
    }
}

/// The upgrade request for `host:port`.
pub fn request(host: &str, port: u16) -> String {
    format!(
        "GET / HTTP/1.1\r\nHost: {host}:{port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Key: {KEY}\r\nSec-WebSocket-Version: 13\r\n\r\n"
    )
}

/// Checks a complete response head (through the blank line): status 101 and the accept key for
/// [`KEY`].
pub fn check_response(head: &[u8]) -> Result<(), String> {
    let text = std::str::from_utf8(head).map_err(|_| "upgrade response is not UTF-8".to_owned())?;
    let mut lines = text.split("\r\n");
    let status = lines.next().unwrap_or_default();
    if status.split(' ').nth(1) != Some("101") {
        return Err(format!("upgrade refused: {status:?}"));
    }
    let want = tungstenite::handshake::derive_accept_key(KEY.as_bytes());
    let accept = lines
        .filter_map(|l| l.split_once(':'))
        .find(|(name, _)| name.trim().eq_ignore_ascii_case("sec-websocket-accept"))
        .map(|(_, v)| v.trim());
    match accept {
        Some(a) if a == want => Ok(()),
        other => Err(format!(
            "upgrade: Sec-WebSocket-Accept {other:?}, want {want:?}"
        )),
    }
}

/// Length of the response head in `buf` (through `\r\n\r\n`), once it is all there.
pub fn head_len(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4)
}

fn len_field(size: usize) -> usize {
    if size < 126 { 0 } else { 2 }
}

/// Bytes of a server (unmasked) frame carrying a `size`-byte message.
pub fn server_frame_len(size: usize) -> usize {
    2 + len_field(size) + size
}

/// Writes a masked binary frame of a `size`-byte message into `room`: `payload` fills the
/// message's bytes, then they are masked. Returns the frame's length.
pub fn encode(room: &mut [u8], size: usize, payload: impl FnOnce(&mut [u8])) -> usize {
    let head = 2 + len_field(size) + 4;
    room[0] = FIN_BINARY;
    if size < 126 {
        room[1] = 0x80 | size as u8;
    } else {
        room[1] = 0x80 | 126;
        room[2..4].copy_from_slice(&(size as u16).to_be_bytes());
    }
    room[head - 4..head].copy_from_slice(&MASK);
    let body = &mut room[head..head + size];
    payload(body);
    for (i, b) in body.iter_mut().enumerate() {
        *b ^= MASK[i % 4];
    }
    head + size
}

/// The message in one server frame of `server_frame_len(size)` bytes.
pub fn decode(frame: &[u8], size: usize) -> Result<&[u8], String> {
    let head = 2 + len_field(size);
    let len_ok = if size < 126 {
        frame[1] == size as u8
    } else {
        frame[1] == 126 && frame[2..4] == (size as u16).to_be_bytes()
    };
    if frame[0] != FIN_BINARY || !len_ok {
        return Err(format!(
            "ws: unexpected frame from the DUT (head {:02x?}, want a {size} B unmasked binary frame)",
            &frame[..head]
        ));
    }
    Ok(&frame[head..])
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Read, Write};
    use tungstenite::protocol::{Role, WebSocket};
    use tungstenite::{Message, Utf8Bytes};

    /// Reads `input`, collects writes.
    struct Duplex {
        input: Cursor<Vec<u8>>,
        output: Vec<u8>,
    }
    impl Read for Duplex {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.input.read(buf)
        }
    }
    impl Write for Duplex {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.output.write(buf)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn duplex(input: Vec<u8>) -> Duplex {
        Duplex {
            input: Cursor::new(input),
            output: Vec::new(),
        }
    }

    #[test]
    fn tungstenite_accepts_the_request_and_its_response_checks_out() {
        let req = request("10.0.0.2", 7000);
        let ws =
            tungstenite::accept(duplex(req.into_bytes())).expect("tungstenite accepts the upgrade");
        let response = &ws.get_ref().output;
        let n = head_len(response).expect("a complete head");
        assert_eq!(n, response.len());
        check_response(&response[..n]).unwrap();
    }

    #[test]
    fn bad_responses_are_refused() {
        let ok = format!(
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {}\r\n\r\n",
            tungstenite::handshake::derive_accept_key(KEY.as_bytes())
        );
        check_response(ok.as_bytes()).unwrap();
        assert!(check_response(ok.replace("101", "400").as_bytes()).is_err());
        assert!(check_response(b"HTTP/1.1 101 OK\r\nSec-WebSocket-Accept: xyz\r\n\r\n").is_err());
        assert!(check_response(b"HTTP/1.1 101 OK\r\n\r\n").is_err());
        assert_eq!(head_len(b"HTTP/1.1 101 OK\r\n\r"), None);
    }

    /// Our frames, as a server (tungstenite) reads them, at both length encodings.
    #[test]
    fn encoded_frames_read_back_as_binary_messages() {
        for size in [HEADER, 125, 126, 1000, MAX_SIZE] {
            let mut room = vec![0u8; 2048];
            let message: Vec<u8> = (0..size).map(|i| (i * 7) as u8).collect();
            let n = encode(&mut room, size, |p| p.copy_from_slice(&message));
            let mut server =
                WebSocket::from_raw_socket(duplex(room[..n].to_vec()), Role::Server, None);
            match server.read().unwrap() {
                Message::Binary(b) => assert_eq!(&b[..], &message[..], "size {size}"),
                other => panic!("size {size}: {other:?}"),
            }
        }
    }

    /// Echo frames a server (tungstenite) writes decode to the message; others are refused.
    #[test]
    fn server_frames_decode_and_others_are_refused() {
        for size in [HEADER, 125, 126, MAX_SIZE] {
            let message: Vec<u8> = (0..size).map(|i| i as u8).collect();
            let mut server = WebSocket::from_raw_socket(duplex(Vec::new()), Role::Server, None);
            server
                .send(Message::Binary(message.clone().into()))
                .unwrap();
            let wire = server.get_ref().output.clone();
            assert_eq!(wire.len(), server_frame_len(size));
            assert_eq!(decode(&wire, size).unwrap(), &message[..]);
            let mut text = WebSocket::from_raw_socket(duplex(Vec::new()), Role::Server, None);
            let s = "x".repeat(size);
            text.send(Message::Text(Utf8Bytes::from(s))).unwrap();
            assert!(
                decode(&text.get_ref().output, size).is_err(),
                "text frame accepted"
            );
        }
        let mut server = WebSocket::from_raw_socket(duplex(Vec::new()), Role::Server, None);
        server.send(Message::Binary(vec![0; 100].into())).unwrap();
        assert!(
            decode(&server.get_ref().output, 101).is_err(),
            "wrong length accepted"
        );
    }

    #[test]
    fn sizes_must_fit_one_segment() {
        assert!(check_size(HEADER).is_ok() && check_size(MAX_SIZE).is_ok());
        assert!(check_size(HEADER - 1).is_err() && check_size(MAX_SIZE + 1).is_err());
    }
}
