//! The WebSocket upgrade (RFC 6455 §4.1, §4.2), without I/O. Client: the request to send, the
//! response head gathered from received runs, and its check. Server: the request's check and
//! the 101 to send. Bytes after the head stay with the lower layer for the WebSocket receive.

use std::fmt;

use sha1::{Digest, Sha1};

/// Most bytes of an upgrade head (request or response) accepted.
const MAX_HEAD: usize = 16 * 1024;

/// Why an upgrade failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpgradeError {
    /// The head is over `MAX_HEAD` bytes.
    HeadTooBig,
    /// The stream ended before the head did.
    Closed,
    /// Not HTTP: httparse's reason.
    Malformed(String),
    /// A head without its blank line.
    Incomplete,
    /// The response's status is not 101.
    Status(Option<u16>),
    /// The request is not a GET.
    Method(Option<String>),
    /// The request is not HTTP/1.1.
    NotHttp11,
    NoHost,
    NoUpgradeWebsocket,
    NoConnectionUpgrade,
    /// The request's `Sec-WebSocket-Version` is not 13.
    Version,
    NoKey,
    /// The request's key is not base64 of 16 bytes.
    BadKey,
    /// The response's `Sec-WebSocket-Accept` does not answer our key.
    WrongAccept,
    /// The response selects an extension or subprotocol we did not offer (the header named).
    Unrequested(&'static str),
}

impl fmt::Display for UpgradeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HeadTooBig => write!(f, "head over {MAX_HEAD} B"),
            Self::Closed => f.write_str("stream ended before the head"),
            Self::Malformed(e) => write!(f, "malformed head: {e}"),
            Self::Incomplete => f.write_str("incomplete head"),
            Self::Status(code) => write!(f, "status {code:?}, not 101"),
            Self::Method(m) => write!(f, "method {m:?}, not GET"),
            Self::NotHttp11 => f.write_str("not HTTP/1.1"),
            Self::NoHost => f.write_str("no Host"),
            Self::NoUpgradeWebsocket => f.write_str("no Upgrade: websocket"),
            Self::NoConnectionUpgrade => f.write_str("no Connection: upgrade"),
            Self::Version => f.write_str("no Sec-WebSocket-Version: 13"),
            Self::NoKey => f.write_str("no Sec-WebSocket-Key"),
            Self::BadKey => f.write_str("Sec-WebSocket-Key not base64 of 16 bytes"),
            Self::WrongAccept => f.write_str("wrong Sec-WebSocket-Accept"),
            Self::Unrequested(h) => write!(f, "unrequested {h}"),
        }
    }
}

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

/// Moves the bytes of `run` that belong to the head into `head`: how many it took,
/// and whether the head (through its blank line) is now complete.
pub(crate) fn read_head(head: &mut Vec<u8>, run: &[u8]) -> Result<(usize, bool), UpgradeError> {
    let before = head.len();
    head.extend_from_slice(run);
    let end = head.windows(4).position(|w| w == b"\r\n\r\n").map(|at| at + 4);
    let taken = end.unwrap_or(head.len());
    head.truncate(taken);
    if end.is_none() && head.len() > MAX_HEAD {
        return Err(UpgradeError::HeadTooBig);
    }
    Ok((taken - before, end.is_some()))
}

/// RFC 6455 §4.2.2: status 101, `Upgrade: websocket`, `Connection: upgrade`, the accept key
/// derived from `key`, and no extension or subprotocol (we offer neither).
pub(crate) fn check(head: &[u8], key: &str) -> Result<(), UpgradeError> {
    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut parsed = httparse::Response::new(&mut headers);
    match parsed.parse(head) {
        Ok(httparse::Status::Complete(_)) => {}
        Ok(httparse::Status::Partial) => return Err(UpgradeError::Incomplete),
        Err(e) => return Err(UpgradeError::Malformed(e.to_string())),
    }
    if parsed.code != Some(101) {
        return Err(UpgradeError::Status(parsed.code));
    }
    let header = |name: &str| parsed.headers.iter().find(|h| h.name.eq_ignore_ascii_case(name)).map(|h| h.value);
    if !header("upgrade").is_some_and(|v| v.eq_ignore_ascii_case(b"websocket")) {
        return Err(UpgradeError::NoUpgradeWebsocket);
    }
    if !connection_upgrade(parsed.headers) {
        return Err(UpgradeError::NoConnectionUpgrade);
    }
    if header("sec-websocket-accept") != Some(accept_key(key).as_bytes()) {
        return Err(UpgradeError::WrongAccept);
    }
    // RFC 6455 §4.1: the server may select only what the client offered.
    for unrequested in ["Sec-WebSocket-Extensions", "Sec-WebSocket-Protocol"] {
        if header(unrequested).is_some() {
            return Err(UpgradeError::Unrequested(unrequested));
        }
    }
    Ok(())
}

/// The server's answer to a client's upgrade request head (RFC 6455 §4.2.1, §4.2.2): an
/// HTTP/1.1 GET with `Host`, `Upgrade: websocket`, `Connection: upgrade`,
/// `Sec-WebSocket-Version: 13` and a `Sec-WebSocket-Key` that is base64 of 16 bytes gets the 101
/// response. No extension or subprotocol is selected, so offers of them are ignored.
pub(crate) fn accept(head: &[u8]) -> Result<String, UpgradeError> {
    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut parsed = httparse::Request::new(&mut headers);
    match parsed.parse(head) {
        Ok(httparse::Status::Complete(_)) => {}
        Ok(httparse::Status::Partial) => return Err(UpgradeError::Incomplete),
        Err(e) => return Err(UpgradeError::Malformed(e.to_string())),
    }
    if parsed.method != Some("GET") {
        return Err(UpgradeError::Method(parsed.method.map(str::to_owned)));
    }
    // httparse reads HTTP/1.0 (0) and HTTP/1.1 (1) only
    if parsed.version != Some(1) {
        return Err(UpgradeError::NotHttp11);
    }
    let header = |name: &str| parsed.headers.iter().find(|h| h.name.eq_ignore_ascii_case(name)).map(|h| h.value);
    if !header("upgrade").is_some_and(|v| v.eq_ignore_ascii_case(b"websocket")) {
        return Err(UpgradeError::NoUpgradeWebsocket);
    }
    if !connection_upgrade(parsed.headers) {
        return Err(UpgradeError::NoConnectionUpgrade);
    }
    if header("host").is_none() {
        return Err(UpgradeError::NoHost);
    }
    if header("sec-websocket-version") != Some(b"13") {
        return Err(UpgradeError::Version);
    }
    let key = header("sec-websocket-key").ok_or(UpgradeError::NoKey)?.trim_ascii();
    if data_encoding::BASE64.decode(key).map(|k| k.len()) != Ok(16) {
        return Err(UpgradeError::BadKey);
    }
    let key = core::str::from_utf8(key).expect("valid base64 is ASCII");
    Ok(format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Accept: {}\r\n\r\n",
        accept_key(key)
    ))
}

/// Whether the `Connection` fields, together one comma-separated list (RFC 9110 §5.2, §5.3),
/// hold the token `upgrade`.
fn connection_upgrade(headers: &[httparse::Header<'_>]) -> bool {
    headers
        .iter()
        .filter(|h| h.name.eq_ignore_ascii_case("connection"))
        .flat_map(|h| h.value.split(|&b| b == b','))
        .any(|t| t.trim_ascii().eq_ignore_ascii_case(b"upgrade"))
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

    fn error(head: &[u8]) -> UpgradeError {
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
        check(&response("Connection: keep-alive\r\nConnection: Upgrade\r\n", "Connection"), KEY).unwrap();
    }

    #[test]
    // sha1 uses aarch64 SHA intrinsics Miri does not run; no unsafe of ours here
    #[cfg_attr(miri, ignore)]
    fn bad_responses_are_refused() {
        use UpgradeError as E;
        assert_eq!(error(&response("Sec-WebSocket-Extensions: permessage-deflate\r\n", "")), E::Unrequested("Sec-WebSocket-Extensions"));
        assert_eq!(error(&response("Sec-WebSocket-Protocol: chat\r\n", "")), E::Unrequested("Sec-WebSocket-Protocol"));
        assert_eq!(error(&response("", "Sec-WebSocket-Accept")), E::WrongAccept);
        let wrong = format!("Sec-WebSocket-Accept: {}\r\n", accept_key("other"));
        assert_eq!(error(&response(&wrong, "Sec-WebSocket-Accept")), E::WrongAccept);
        let forbidden = String::from_utf8(response("", "")).unwrap().replace("101 Switching Protocols", "403 Forbidden");
        assert_eq!(error(forbidden.as_bytes()), E::Status(Some(403)));
        assert_eq!(error(&response("", "Upgrade")), E::NoUpgradeWebsocket);
        assert_eq!(error(&response("", "Connection")), E::NoConnectionUpgrade);
        assert_eq!(error(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: web"), E::Incomplete);
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

    /// The request every row of `accept_checks_the_request` edits.
    const GOOD: &str = "GET / HTTP/1.1\r\nHost: h\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
                        Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";

    /// Each request is `GOOD` with one edit (`from` replaced by `to`), and `accept`'s verdict.
    #[test]
    // sha1 (on the accepted rows) uses aarch64 SHA intrinsics Miri does not run
    #[cfg_attr(miri, ignore)]
    fn accept_checks_the_request() {
        use UpgradeError as E;
        let rows: [(&str, &str, Result<(), E>); 15] = [
            ("", "", Ok(())),
            // RFC 9110 §5.2: repeated Connection fields are one list
            ("Connection: Upgrade\r\n", "Connection: keep-alive\r\nConnection: Upgrade\r\n", Ok(())),
            ("Connection: Upgrade", "Connection: keep-alive, upgrade", Ok(())),
            ("Connection: Upgrade", "Connection: keep-alive", Err(E::NoConnectionUpgrade)),
            ("Connection: Upgrade\r\n", "", Err(E::NoConnectionUpgrade)),
            ("Upgrade: websocket\r\n", "", Err(E::NoUpgradeWebsocket)),
            ("HTTP/1.1", "HTTP/1.0", Err(E::NotHttp11)),
            ("Host: h\r\n", "", Err(E::NoHost)),
            ("Version: 13", "Version: 8", Err(E::Version)),
            ("Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n", "", Err(E::NoKey)),
            ("dGhlIHNhbXBsZSBub25jZQ==", "k", Err(E::BadKey)),
            ("dGhlIHNhbXBsZSBub25jZQ==", "", Err(E::BadKey)),
            // base64 of 15 and of 17 bytes
            ("dGhlIHNhbXBsZSBub25jZQ==", "AAAAAAAAAAAAAAAAAAAA", Err(E::BadKey)),
            ("dGhlIHNhbXBsZSBub25jZQ==", "AAAAAAAAAAAAAAAAAAAAAAA=", Err(E::BadKey)),
            ("GET", "POST", Err(E::Method(Some("POST".into())))),
        ];
        for (from, to, want) in rows {
            let request = GOOD.replacen(from, to, 1);
            assert_eq!(accept(request.as_bytes()).map(drop), want, "{request:?}");
        }
        assert_eq!(accept(b"GET / HTTP/1.1\r\nUpgr").unwrap_err(), E::Incomplete);
    }
}
