//! The client's WebSocket upgrade (RFC 6455 §4.1, §4.2.2), without I/O: the request to send,
//! the response head gathered from received runs, and its check. Bytes after the head stay with
//! the lower layer for the WebSocket receive.

use sha1::{Digest, Sha1};

/// Most bytes of the server's upgrade response head accepted.
const MAX_HEAD: usize = 16 * 1024;

/// The upgrade request for `path` on `host:port` with the 16 random bytes `key`, and the
/// base64 key the response must answer. `default_port` is the scheme's (wss 443, ws 80).
pub(crate) fn request(host: &str, port: u16, default_port: u16, path: &str, key: [u8; 16]) -> (String, String) {
    let key = data_encoding::BASE64.encode(&key);
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n",
        host = host_header(host, port, default_port),
    );
    (request, key)
}

/// Moves the bytes of `run` that belong to the response head into `head`: how many it took,
/// and whether the head (through its blank line) is now complete.
pub(crate) fn read_head(head: &mut Vec<u8>, run: &[u8]) -> Result<(usize, bool), String> {
    let before = head.len();
    head.extend_from_slice(run);
    let end = head.windows(4).position(|w| w == b"\r\n\r\n").map(|at| at + 4);
    let taken = end.unwrap_or(head.len());
    head.truncate(taken);
    if end.is_none() && head.len() > MAX_HEAD {
        return Err(format!("response head over {MAX_HEAD} B"));
    }
    Ok((taken - before, end.is_some()))
}

/// RFC 6455 §4.2.2: status 101, `Upgrade: websocket`, `Connection: upgrade`, the accept key
/// derived from `key`, and no extension or subprotocol (we offer neither).
pub(crate) fn check(head: &[u8], key: &str) -> Result<(), String> {
    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut parsed = httparse::Response::new(&mut headers);
    match parsed.parse(head) {
        Ok(httparse::Status::Complete(_)) => {}
        Ok(httparse::Status::Partial) => return Err("incomplete response head".into()),
        Err(e) => return Err(format!("response: {e}")),
    }
    if parsed.code != Some(101) {
        return Err(format!("status {:?} {:?}", parsed.code, parsed.reason));
    }
    let header = |name: &str| parsed.headers.iter().find(|h| h.name.eq_ignore_ascii_case(name)).map(|h| h.value);
    if !header("upgrade").is_some_and(|v| v.eq_ignore_ascii_case(b"websocket")) {
        return Err("no Upgrade: websocket".into());
    }
    let connection = header("connection").unwrap_or_default();
    if !connection.split(|&b| b == b',').any(|t| t.trim_ascii().eq_ignore_ascii_case(b"upgrade")) {
        return Err("no Connection: upgrade".into());
    }
    if header("sec-websocket-accept") != Some(accept_key(key).as_bytes()) {
        return Err("wrong Sec-WebSocket-Accept".into());
    }
    // RFC 6455 §4.1: the server may select only what the client offered.
    for unrequested in ["Sec-WebSocket-Extensions", "Sec-WebSocket-Protocol"] {
        if header(unrequested).is_some() {
            return Err(format!("unrequested {unrequested}"));
        }
    }
    Ok(())
}

/// The server's answer to a client's upgrade request head (RFC 6455 §4.2.1, §4.2.2): a GET
/// with `Upgrade: websocket`, `Connection: upgrade`, `Sec-WebSocket-Version: 13` and a
/// `Sec-WebSocket-Key` gets the 101 response. No extension or subprotocol is selected, so
/// offers of them are ignored.
pub(crate) fn accept(head: &[u8]) -> Result<String, String> {
    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut parsed = httparse::Request::new(&mut headers);
    match parsed.parse(head) {
        Ok(httparse::Status::Complete(_)) => {}
        Ok(httparse::Status::Partial) => return Err("incomplete request head".into()),
        Err(e) => return Err(format!("request: {e}")),
    }
    if parsed.method != Some("GET") {
        return Err(format!("method {:?}", parsed.method));
    }
    let header = |name: &str| parsed.headers.iter().find(|h| h.name.eq_ignore_ascii_case(name)).map(|h| h.value);
    if !header("upgrade").is_some_and(|v| v.eq_ignore_ascii_case(b"websocket")) {
        return Err("no Upgrade: websocket".into());
    }
    let connection = header("connection").unwrap_or_default();
    if !connection.split(|&b| b == b',').any(|t| t.trim_ascii().eq_ignore_ascii_case(b"upgrade")) {
        return Err("no Connection: upgrade".into());
    }
    if header("sec-websocket-version") != Some(b"13") {
        return Err("no Sec-WebSocket-Version: 13".into());
    }
    let key = header("sec-websocket-key").ok_or("no Sec-WebSocket-Key")?;
    let key = core::str::from_utf8(key).map_err(|_| "Sec-WebSocket-Key not ASCII")?;
    Ok(format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Accept: {}\r\n\r\n",
        accept_key(key.trim())
    ))
}

/// Sec-WebSocket-Accept for `key` (RFC 6455 §4.2.2).
fn accept_key(key: &str) -> String {
    let mut sha1 = Sha1::new();
    sha1.update(key.as_bytes());
    sha1.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    data_encoding::BASE64.encode(&sha1.finalize())
}

/// RFC 7230 §5.4: the port only when it is not the scheme's default.
fn host_header(host: &str, port: u16, default_port: u16) -> String {
    if port == default_port { host.to_owned() } else { format!("{host}:{port}") }
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

    fn error(head: &[u8]) -> String {
        check(head, KEY).expect_err("an upgrade error")
    }

    #[test]
    // sha1 uses aarch64 SHA intrinsics Miri does not run; no unsafe of ours here
    #[cfg_attr(miri, ignore)]
    fn accept_key_is_rfc_6455_example() {
        // RFC 6455 §1.3.
        assert_eq!(accept_key(KEY), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
    }

    #[test]
    // sha1 uses aarch64 SHA intrinsics Miri does not run; no unsafe of ours here
    #[cfg_attr(miri, ignore)]
    fn valid_response_passes() {
        check(&response("", ""), KEY).unwrap();
        check(&response("Connection: keep-alive, Upgrade\r\n", "Connection"), KEY).unwrap();
    }

    #[test]
    // sha1 uses aarch64 SHA intrinsics Miri does not run; no unsafe of ours here
    #[cfg_attr(miri, ignore)]
    fn bad_responses_are_refused() {
        assert!(error(&response("Sec-WebSocket-Extensions: permessage-deflate\r\n", "")).contains("Extensions"));
        assert!(error(&response("Sec-WebSocket-Protocol: chat\r\n", "")).contains("Protocol"));
        assert_eq!(error(&response("", "Sec-WebSocket-Accept")), "wrong Sec-WebSocket-Accept");
        let wrong = format!("Sec-WebSocket-Accept: {}\r\n", accept_key("other"));
        assert_eq!(error(&response(&wrong, "Sec-WebSocket-Accept")), "wrong Sec-WebSocket-Accept");
        let forbidden = String::from_utf8(response("", "")).unwrap().replace("101 Switching Protocols", "403 Forbidden");
        assert!(error(forbidden.as_bytes()).starts_with("status Some(403)"));
        assert_eq!(error(&response("", "Upgrade")), "no Upgrade: websocket");
        assert_eq!(error(&response("", "Connection")), "no Connection: upgrade");
        assert_eq!(error(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: web"), "incomplete response head");
    }

    #[test]
    fn host_header_names_the_port_unless_the_default() {
        assert_eq!(host_header("stream.example", 443, 443), "stream.example");
        assert_eq!(host_header("stream.example", 9443, 443), "stream.example:9443");
        assert_eq!(host_header("stream.example", 80, 80), "stream.example");
        assert_eq!(host_header("stream.example", 443, 80), "stream.example:443");
    }

    #[test]
    fn request_carries_the_key_and_host() {
        let (request, key) = request("stream.example", 9443, 443, "/ws", [0; 16]);
        assert_eq!(key, "AAAAAAAAAAAAAAAAAAAAAA==");
        assert_eq!(
            request,
            "GET /ws HTTP/1.1\r\nHost: stream.example:9443\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
             Sec-WebSocket-Key: AAAAAAAAAAAAAAAAAAAAAA==\r\nSec-WebSocket-Version: 13\r\n\r\n"
        );
    }

    /// The head is found across runs cut anywhere, even inside the blank line; the bytes after
    /// it are not taken.
    #[test]
    // sha1 (in the response) uses aarch64 SHA intrinsics Miri does not run; a sweep anyway
    #[cfg_attr(miri, ignore)]
    fn read_head_stops_at_the_blank_line_across_runs() {
        let mut stream = response("", "");
        let head_len = stream.len();
        stream.extend_from_slice(b"\x81\x02hi");
        for cut in 1..stream.len() {
            let mut head = Vec::new();
            let (first, done) = read_head(&mut head, &stream[..cut]).unwrap();
            let taken = if done {
                first
            } else {
                let (second, done) = read_head(&mut head, &stream[cut..]).unwrap();
                assert!(done, "cut {cut}");
                first + second
            };
            assert_eq!(taken, head_len, "cut {cut}");
            assert_eq!(head, stream[..head_len], "cut {cut}");
        }
        let mut head = Vec::new();
        assert!(read_head(&mut head, &[b'x'; MAX_HEAD + 1]).is_err());
    }

    /// Our own request is accepted, and the 101 answers its key (RFC 6455 §1.3 example).
    #[test]
    #[cfg_attr(miri, ignore)]
    fn accept_answers_a_valid_request() {
        let req = "GET /chat HTTP/1.1\r\nHost: server.example.com\r\nUpgrade: websocket\r\n\
                   Connection: keep-alive, Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
                   Sec-WebSocket-Version: 13\r\nSec-WebSocket-Extensions: permessage-deflate\r\n\r\n";
        let response = accept(req.as_bytes()).unwrap();
        assert_eq!(
            response,
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
             Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n"
        );
        // and it passes the client's own check, without an extension selected
        check(response.as_bytes(), KEY).unwrap();
        let (ours, key) = request("h", 80, 80, "/", [7; 16]);
        check(accept(ours.as_bytes()).unwrap().as_bytes(), &key).unwrap();
    }

    #[test]
    fn accept_refuses_bad_requests() {
        let good = "GET / HTTP/1.1\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
                    Sec-WebSocket-Key: k\r\nSec-WebSocket-Version: 13\r\n\r\n";
        let without = |line: &str| good.replace(&format!("{line}\r\n"), "");
        assert_eq!(accept(without("Upgrade: websocket").as_bytes()).unwrap_err(), "no Upgrade: websocket");
        assert_eq!(accept(without("Connection: Upgrade").as_bytes()).unwrap_err(), "no Connection: upgrade");
        assert_eq!(accept(without("Sec-WebSocket-Key: k").as_bytes()).unwrap_err(), "no Sec-WebSocket-Key");
        let v8 = good.replace("Version: 13", "Version: 8");
        assert_eq!(accept(v8.as_bytes()).unwrap_err(), "no Sec-WebSocket-Version: 13");
        assert!(accept(good.replace("GET", "POST").as_bytes()).unwrap_err().starts_with("method"));
        assert_eq!(accept(b"GET / HTTP/1.1\r\nUpgr").unwrap_err(), "incomplete request head");
    }
}
