//! TlsRx over an in-process rustls server: records in the stream are cut into frames in every
//! way that matters, and each record must come out once, in order, with the stamp of the frame
//! holding its first byte, decrypted inside that frame unless it straddles frames.

// ring's assembly cannot run under Miri, and sheath itself has no unsafe code.
#![cfg(not(miri))]

use std::io::Write;
use std::ops::Range;
use std::sync::{Arc, LazyLock};

use rustls::client::UnbufferedClientConnection;
use rustls::crypto::cipher::{MessageEncrypter, OutboundChunks, OutboundPlainMessage};
use rustls::crypto::ring::default_provider;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use rustls::unbuffered::{ConnectionState, UnbufferedStatus};
use rustls::version::TLS13;
use rustls::{
    AlertDescription, ClientConfig, ConnectionTrafficSecrets, ContentType, ProtocolVersion,
    RootCertStore, ServerConfig, ServerConnection, SupportedCipherSuite,
};
use sheath::{Error, RxFrame, TlsRx};

const HOST: &str = "sheath.test";

struct VecFrame {
    data: Vec<u8>,
    stamp: u64,
}

impl RxFrame for VecFrame {
    fn payload(&self) -> &[u8] {
        &self.data
    }

    fn payload_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    fn rx_timestamp(&self) -> u64 {
        self.stamp
    }
}

fn configs(max_fragment_size: Option<usize>) -> (Arc<ClientConfig>, Arc<ServerConfig>) {
    static CERT: LazyLock<(CertificateDer<'static>, PrivatePkcs8KeyDer<'static>)> =
        LazyLock::new(|| {
            let rcgen::CertifiedKey { cert, signing_key } =
                rcgen::generate_simple_self_signed(vec![HOST.to_owned()]).unwrap();
            (
                cert.der().clone(),
                PrivatePkcs8KeyDer::from(signing_key.serialize_der()),
            )
        });
    let (cert, key) = &*CERT;

    let provider = Arc::new(default_provider());
    let mut server = ServerConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![cert.clone()], PrivateKeyDer::Pkcs8(key.clone_key()))
        .unwrap();
    server.max_fragment_size = max_fragment_size;
    // lets `Sealer` take over the server's sending side
    server.enable_secret_extraction = true;

    let mut roots = RootCertStore::empty();
    roots.add(cert.clone()).unwrap();
    let client = ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    (Arc::new(client), Arc::new(server))
}

struct Peer {
    client: UnbufferedClientConnection,
    server: ServerConnection,
}

/// Completes the handshake. The server's records sent after it (session tickets) stay queued
/// for the stream under test.
fn connect(max_fragment_size: Option<usize>) -> Peer {
    enum Next {
        Continue,
        Blocked,
        Done,
    }

    let (client_config, server_config) = configs(max_fragment_size);
    let mut client =
        UnbufferedClientConnection::new(client_config, ServerName::try_from(HOST).unwrap())
            .unwrap();
    let mut server = ServerConnection::new(server_config).unwrap();
    let mut to_client = Vec::new();
    loop {
        let UnbufferedStatus { discard, state } = client.process_tls_records(&mut to_client);
        let next = match state.unwrap() {
            ConnectionState::EncodeTlsData(mut data) => {
                let mut buf = vec![0; 1 << 15];
                let len = data.encode(&mut buf).unwrap();
                let mut tls = &buf[..len];
                while !tls.is_empty() {
                    server.read_tls(&mut tls).unwrap();
                }
                Next::Continue
            }
            ConnectionState::TransmitTlsData(data) => {
                data.done();
                server.process_new_packets().unwrap();
                Next::Continue
            }
            ConnectionState::BlockedHandshake => Next::Blocked,
            ConnectionState::WriteTraffic(_) => Next::Done,
            state => panic!("unexpected client state {state:?}"),
        };
        to_client.drain(..discard);
        match next {
            Next::Continue => {}
            Next::Blocked => {
                assert!(server.wants_write(), "handshake stalled");
                while server.wants_write() {
                    server.write_tls(&mut to_client).unwrap();
                }
            }
            Next::Done => break,
        }
    }
    assert!(to_client.is_empty());
    assert!(!server.is_handshaking());
    Peer { client, server }
}

/// Server-to-client bytes and the application data records among them.
#[derive(Default)]
struct Stream {
    bytes: Vec<u8>,
    records: Vec<Sent>,
}

struct Sent {
    wire: Range<usize>,
    plaintext: Vec<u8>,
}

impl Peer {
    /// Moves the server's queued records (tickets, key updates, alerts) to the stream.
    fn flush(&mut self, stream: &mut Stream) {
        while self.server.wants_write() {
            self.server.write_tls(&mut stream.bytes).unwrap();
        }
    }

    fn send(&mut self, stream: &mut Stream, msg: &[u8]) {
        self.flush(stream);
        let at = stream.bytes.len();
        self.server.writer().write_all(msg).unwrap();
        self.flush(stream);

        // TLS 1.3 records carry 1 B of content type and 16 B of tag; rustls adds no padding
        let mut sent = 0;
        for wire in records(&stream.bytes[at..]) {
            let len = wire.len() - 5 - 17;
            stream.records.push(Sent {
                wire: at + wire.start..at + wire.end,
                plaintext: msg[sent..sent + len].to_vec(),
            });
            sent += len;
        }
        assert_eq!(sent, msg.len());
    }

    fn close_notify(&mut self, stream: &mut Stream) {
        self.server.send_close_notify();
        self.flush(stream);
    }

    /// Makes the server send a fatal decode_error alert.
    fn fatal_alert(&mut self, stream: &mut Stream) {
        self.server.read_tls(&mut &[0xff; 5][..]).unwrap();
        assert!(self.server.process_new_packets().is_err());
        self.flush(stream);
    }
}

/// Wire ranges of the records in `bytes`, which holds whole records.
fn records(bytes: &[u8]) -> Vec<Range<usize>> {
    let mut at = 0;
    let mut out = Vec::new();
    while at < bytes.len() {
        let end = at + 5 + usize::from(u16::from_be_bytes([bytes[at + 3], bytes[at + 4]]));
        out.push(at..end);
        at = end;
    }
    assert_eq!(at, bytes.len());
    out
}

fn message(seed: usize, len: usize) -> Vec<u8> {
    (0..len).map(|i| (seed * 31 + i * 7) as u8).collect()
}

/// Tickets, application data of several sizes, a key update, more data, close_notify.
fn standard(peer: &mut Peer) -> Stream {
    let mut stream = Stream::default();
    peer.flush(&mut stream);
    for (seed, len) in [1, 2, 31, 200].into_iter().enumerate() {
        peer.send(&mut stream, &message(seed, len));
    }
    peer.server.refresh_traffic_keys().unwrap();
    peer.send(&mut stream, &message(10, 17));
    peer.send(&mut stream, &message(11, 64));
    peer.close_notify(&mut stream);
    stream
}

#[derive(Debug, PartialEq)]
struct Got {
    plaintext: Vec<u8>,
    stamp: u64,
    place: Place,
}

/// Where a record's plaintext was found.
#[derive(Debug, PartialEq)]
enum Place {
    /// Inside frame `index` at `offset`.
    Frame { index: usize, offset: usize },
    /// Outside every frame (the scratch buffer).
    Elsewhere(usize),
}

struct Received {
    got: Vec<Got>,
    error: Option<Error>,
    rx: TlsRx<VecFrame>,
}

/// Pushes `frames` (frame `i` stamped `i`) and collects every chunk until the first error.
fn receive(client: &mut UnbufferedClientConnection, frames: Vec<Vec<u8>>) -> Received {
    let ranges: Vec<Range<usize>> = frames
        .iter()
        .map(|f| {
            let r = f.as_ptr_range();
            r.start as usize..r.end as usize
        })
        .collect();
    let mut rx = TlsRx::new(client);
    let mut got = Vec::new();
    for (index, data) in frames.into_iter().enumerate() {
        rx.push(VecFrame {
            data,
            stamp: index as u64,
        });
        loop {
            match rx.chunk(client) {
                Ok(Some(chunk)) => {
                    let ptr = chunk.bytes.as_ptr() as usize;
                    let place = match ranges.iter().position(|r| r.contains(&ptr)) {
                        Some(index) => Place::Frame {
                            index,
                            offset: ptr - ranges[index].start,
                        },
                        None => Place::Elsewhere(ptr),
                    };
                    got.push(Got {
                        plaintext: chunk.bytes.to_vec(),
                        stamp: chunk.rx_timestamp,
                        place,
                    });
                }
                Ok(None) => break,
                Err(error) => {
                    return Received {
                        got,
                        error: Some(error),
                        rx,
                    };
                }
            }
        }
    }
    Received {
        got,
        error: None,
        rx,
    }
}

fn split(bytes: &[u8], cuts: &[usize]) -> Vec<Vec<u8>> {
    let mut bounds = vec![0];
    bounds.extend_from_slice(cuts);
    bounds.push(bytes.len());
    bounds
        .windows(2)
        .map(|w| bytes[w[0]..w[1]].to_vec())
        .collect()
}

/// `got` must be `records` in order, each stamped with the index of the frame holding its first
/// byte, and found in that frame right after its header unless it straddles frames.
fn assert_received(got: &[Got], records: &[Sent], cuts: &[usize]) {
    assert_eq!(got.len(), records.len(), "cuts {cuts:?}");
    for (got, sent) in got.iter().zip(records) {
        let index = cuts.iter().filter(|&&c| c <= sent.wire.start).count();
        let start = if index == 0 { 0 } else { cuts[index - 1] };
        let end = cuts.get(index).copied().unwrap_or(usize::MAX);
        assert_eq!(got.plaintext, sent.plaintext, "cuts {cuts:?}");
        assert_eq!(got.stamp, index as u64, "cuts {cuts:?}");
        if sent.wire.end <= end {
            let offset = sent.wire.start - start + 5;
            assert_eq!(got.place, Place::Frame { index, offset }, "cuts {cuts:?}");
        } else {
            assert!(matches!(got.place, Place::Elsewhere(_)), "cuts {cuts:?}");
        }
    }
}

/// Builds a stream with `build` on a fresh connection, cuts it at `cuts(stream)`, and checks
/// every record and the clean close.
fn check(
    max_fragment_size: Option<usize>,
    build: fn(&mut Peer) -> Stream,
    cuts: impl Fn(&Stream) -> Vec<usize>,
) {
    let mut peer = connect(max_fragment_size);
    let stream = build(&mut peer);
    let cuts = cuts(&stream);
    let mut received = receive(&mut peer.client, split(&stream.bytes, &cuts));
    assert_eq!(received.error, None, "cuts {cuts:?}");
    assert_received(&received.got, &stream.records, &cuts);
    assert!(received.rx.eof(), "cuts {cuts:?}");
    assert_eq!(received.rx.fin(), Ok(()));
}

fn stream_len(max_fragment_size: Option<usize>, build: fn(&mut Peer) -> Stream) -> usize {
    build(&mut connect(max_fragment_size)).bytes.len()
}

#[test]
fn one_frame_holds_the_whole_stream() {
    check(None, standard, |_| vec![]);
}

#[test]
#[cfg_attr(miri, ignore)]
fn every_split_point_across_two_frames() {
    let len = stream_len(None, standard);
    for cut in 1..len {
        check(None, standard, |_| vec![cut]);
    }
}

#[test]
fn every_byte_its_own_frame() {
    check(None, standard, |s| (1..s.bytes.len()).collect());
}

#[test]
#[cfg_attr(miri, ignore)]
fn random_partitions() {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    let mut next = move |bound: usize| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        (state % bound as u64) as usize
    };
    let len = stream_len(None, standard);
    for _ in 0..200 {
        let mut cuts: Vec<usize> = (0..1 + next(12)).map(|_| 1 + next(len - 1)).collect();
        cuts.sort_unstable();
        cuts.dedup();
        check(None, standard, |_| cuts.clone());
    }
}

#[test]
fn many_records_per_frame() {
    for per_frame in 1..=8 {
        check(None, standard, |s| {
            records(&s.bytes)
                .iter()
                .skip(per_frame - 1)
                .step_by(per_frame)
                .map(|r| r.end)
                .filter(|&end| end < s.bytes.len())
                .collect()
        });
    }
}

fn fragmented(peer: &mut Peer) -> Stream {
    let mut stream = Stream::default();
    peer.flush(&mut stream);
    assert!(
        records(&stream.bytes).len() > 2,
        "tickets are split across records"
    );
    peer.send(&mut stream, &message(1, 100));
    peer.close_notify(&mut stream);
    stream
}

/// Small records split the server's session tickets across records, so rustls keeps the
/// first fragments undiscarded until the message completes, across frames.
#[test]
#[cfg_attr(miri, ignore)]
fn handshake_messages_fragmented_across_records() {
    let len = stream_len(Some(64), fragmented);
    for cut in 1..len {
        check(Some(64), fragmented, |_| vec![cut]);
    }
    check(Some(64), fragmented, |s| (1..s.bytes.len()).collect());
}

fn max_size(peer: &mut Peer) -> Stream {
    let mut stream = Stream::default();
    peer.flush(&mut stream);
    peer.send(&mut stream, &message(1, 1 << 14));
    peer.send(&mut stream, &message(2, 1 << 14));
    peer.close_notify(&mut stream);
    stream
}

#[test]
fn max_size_records() {
    check(None, max_size, |_| vec![]);
    for at in [
        1,
        4,
        5,
        6,
        1000,
        (1 << 14) + 5,
        (1 << 14) + 6,
        (1 << 14) + 21,
    ] {
        check(None, max_size, |s| vec![s.records[0].wire.start + at]);
    }
}

#[test]
fn scratch_buffer_is_reused() {
    let mut peer = connect(None);
    let stream = max_size(&mut peer);
    let cuts: Vec<usize> = stream.records.iter().map(|r| r.wire.start + 100).collect();
    let received = receive(&mut peer.client, split(&stream.bytes, &cuts));
    assert_received(&received.got, &stream.records, &cuts);
    let [first, second] = &received.got[..] else {
        panic!("two records");
    };
    assert!(matches!(first.place, Place::Elsewhere(_)));
    assert_eq!(first.place, second.place);
}

#[test]
fn bad_tag_is_rejected_without_releasing_plaintext() {
    for straddle in [false, true] {
        let mut peer = connect(None);
        let mut stream = standard(&mut peer);
        let bad = &stream.records[2];
        stream.bytes[bad.wire.end - 1] ^= 1;
        let cuts = if straddle {
            vec![bad.wire.start + 3]
        } else {
            vec![]
        };

        let mut received = receive(&mut peer.client, split(&stream.bytes, &cuts));
        assert_received(&received.got, &stream.records[..2], &cuts);
        let error = Some(Error::Tls(rustls::Error::DecryptError));
        assert_eq!(received.error, error);
        assert_eq!(received.rx.chunk(&mut peer.client).err(), error);
        assert_eq!(received.rx.fin().err(), error);
    }
}

#[test]
fn fatal_alert_is_an_error() {
    let mut peer = connect(None);
    let mut stream = Stream::default();
    peer.send(&mut stream, b"hello");
    peer.fatal_alert(&mut stream);

    let received = receive(&mut peer.client, vec![stream.bytes]);
    assert_eq!(received.got.len(), 1);
    assert_eq!(received.got[0].plaintext, b"hello");
    assert_eq!(
        received.error,
        Some(Error::Tls(rustls::Error::AlertReceived(
            AlertDescription::DecodeError
        )))
    );
}

#[test]
fn bytes_after_close_notify_are_ignored() {
    let mut peer = connect(None);
    let mut stream = Stream::default();
    peer.send(&mut stream, b"hello");
    peer.close_notify(&mut stream);
    let after = stream.bytes.len();
    stream
        .bytes
        .extend_from_slice(&[0x17, 0x03, 0x03, 0x00, 0x10]);

    let mut received = receive(&mut peer.client, split(&stream.bytes, &[after + 2]));
    assert_eq!(received.error, None);
    assert_eq!(received.got.len(), 1);
    assert!(received.rx.eof());

    received.rx.push(VecFrame {
        data: b"ignored".to_vec(),
        stamp: 9,
    });
    assert!(received.rx.chunk(&mut peer.client).unwrap().is_none());
    assert_eq!(received.rx.fin(), Ok(()));
}

#[test]
fn fin_without_close_notify_is_truncation() {
    for partial in [false, true] {
        let mut peer = connect(None);
        let mut stream = Stream::default();
        peer.send(&mut stream, b"hello");
        peer.send(&mut stream, b"world");
        let end = if partial {
            stream.records[1].wire.start + 3
        } else {
            stream.records[0].wire.end
        };

        let mut received = receive(&mut peer.client, vec![stream.bytes[..end].to_vec()]);
        assert_eq!(received.error, None);
        assert_eq!(received.got.len(), 1);
        assert!(!received.rx.eof());
        assert_eq!(received.rx.fin(), Err(Error::Truncated));
        assert_eq!(
            received.rx.chunk(&mut peer.client).err(),
            Some(Error::Truncated)
        );
    }
}

#[test]
#[should_panic(expected = "push before chunk returned None")]
fn push_before_chunk_drained_panics() {
    let mut peer = connect(None);
    let mut stream = Stream::default();
    peer.send(&mut stream, b"hello");
    let mut rx = TlsRx::new(&peer.client);
    rx.push(VecFrame {
        data: stream.bytes,
        stamp: 0,
    });
    assert!(rx.chunk(&mut peer.client).unwrap().is_some());
    rx.push(VecFrame {
        data: vec![],
        stamp: 1,
    });
}

#[test]
#[should_panic(expected = "handshake is complete")]
fn new_needs_completed_handshake() {
    let (client_config, _) = configs(None);
    let client =
        UnbufferedClientConnection::new(client_config, ServerName::try_from(HOST).unwrap())
            .unwrap();
    TlsRx::<VecFrame>::new(&client);
}

/// The length in a record header is checked against the TLS 1.3 bound of 2^14 + 256 bytes as
/// soon as the header is complete: inside a frame, split across frames at every offset, and
/// before any of the body has arrived.
#[test]
fn record_length_is_bounded_at_its_header() {
    for (len, error) in [
        (16640, rustls::Error::DecryptError),
        (16641, rustls::Error::PeerSentOversizedRecord),
        (0xffff, rustls::Error::PeerSentOversizedRecord),
    ] {
        for with_body in [true, false] {
            for header_cut in 0..=5 {
                let mut peer = connect(None);
                let mut stream = Stream::default();
                peer.send(&mut stream, b"hello");
                let at = stream.bytes.len();
                let [hi, lo] = u16::try_from(len).unwrap().to_be_bytes();
                stream.bytes.extend_from_slice(&[0x17, 0x03, 0x03, hi, lo]);
                if with_body {
                    stream.bytes.resize(at + 5 + len, 0);
                }
                let cuts = if header_cut == 0 {
                    vec![]
                } else {
                    vec![at + header_cut]
                };

                let received = receive(&mut peer.client, split(&stream.bytes, &cuts));
                assert_received(&received.got, &stream.records, &cuts);
                // a body of zeros fails authentication once it is all there
                let expected = (len > 16640 || with_body).then(|| Error::Tls(error.clone()));
                assert_eq!(received.error, expected, "len {len} cuts {cuts:?}");
            }
        }
    }
}

/// Seals records with the server's traffic keys, for streams the rustls server API does not
/// produce.
struct Sealer {
    encrypter: Box<dyn MessageEncrypter>,
    seq: u64,
}

impl Sealer {
    /// Takes over the server's sending side once its queued records are in `stream`.
    fn new(mut peer: Peer, stream: &mut Stream) -> (UnbufferedClientConnection, Self) {
        peer.flush(stream);
        let Some(SupportedCipherSuite::Tls13(suite)) = peer.server.negotiated_cipher_suite() else {
            panic!("TLS 1.3 suite negotiated");
        };
        let (seq, secrets) = peer.server.dangerous_extract_secrets().unwrap().tx;
        let (key, iv) = match secrets {
            ConnectionTrafficSecrets::Aes128Gcm { key, iv }
            | ConnectionTrafficSecrets::Aes256Gcm { key, iv }
            | ConnectionTrafficSecrets::Chacha20Poly1305 { key, iv } => (key, iv),
            _ => unreachable!("TLS 1.3 suites"),
        };
        let encrypter = suite.aead_alg.encrypter(key, iv);
        (peer.client, Self { encrypter, seq })
    }

    fn seal(&mut self, typ: ContentType, payload: &[u8], stream: &mut Stream) -> Range<usize> {
        let msg = OutboundPlainMessage {
            typ,
            version: ProtocolVersion::TLSv1_2,
            payload: OutboundChunks::Single(payload),
        };
        let at = stream.bytes.len();
        stream
            .bytes
            .extend(self.encrypter.encrypt(msg, self.seq).unwrap().encode());
        self.seq += 1;
        at..stream.bytes.len()
    }
}

/// A NewSessionTicket message carrying a `len`-byte ticket.
fn ticket(len: usize) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend(3600u32.to_be_bytes()); // lifetime
    body.extend(0u32.to_be_bytes()); // age_add
    body.push(0); // empty nonce
    body.extend(u16::try_from(len).unwrap().to_be_bytes());
    body.resize(body.len() + len, 7);
    body.extend(0u16.to_be_bytes()); // no extensions
    let mut msg = vec![4];
    msg.extend(&u32::try_from(body.len()).unwrap().to_be_bytes()[1..]);
    msg.extend(body);
    msg
}

/// `tickets` session tickets in 3000 B records, so every record ends inside a ticket and
/// rustls must keep all of them until the last; then "hello" and close_notify. Returns the
/// wire size of the ticket records, which rustls retains whole.
fn chained_tickets(tickets: usize) -> (UnbufferedClientConnection, Stream, usize) {
    let mut stream = Stream::default();
    let (client, mut sealer) = Sealer::new(connect(None), &mut stream);
    let chain: Vec<u8> = (0..tickets).flat_map(|_| ticket(4000)).collect();
    let at = stream.bytes.len();
    for record in chain.chunks(3000) {
        sealer.seal(ContentType::Handshake, record, &mut stream);
    }
    let retained = stream.bytes.len() - at;
    let wire = sealer.seal(ContentType::ApplicationData, b"hello", &mut stream);
    stream.records.push(Sent {
        wire,
        plaintext: b"hello".to_vec(),
    });
    sealer.seal(ContentType::Alert, &[1, 0], &mut stream);
    (client, stream, retained)
}

/// rustls keeps every record of a chain of handshake messages that never ends at a record
/// boundary. Up to 0xffff retained bytes (the bound of rustls's buffered API) the chain is
/// accepted; beyond, it is an error, whatever the framing.
#[test]
fn retained_handshake_records_are_bounded() {
    for every_byte in [false, true] {
        let cuts = |stream: &Stream| -> Vec<usize> {
            if every_byte {
                (1..stream.bytes.len()).collect()
            } else {
                vec![]
            }
        };

        let (mut client, stream, retained) = chained_tickets(10);
        assert!(retained < 0xffff);
        let cuts_below = cuts(&stream);
        let mut received = receive(&mut client, split(&stream.bytes, &cuts_below));
        assert_eq!(received.error, None);
        assert_received(&received.got, &stream.records, &cuts_below);
        assert_eq!(received.rx.fin(), Ok(()));

        let (mut client, stream, retained) = chained_tickets(20);
        assert!(retained > 0xffff);
        let cuts = cuts(&stream);
        let received = receive(&mut client, split(&stream.bytes, &cuts));
        assert!(received.got.is_empty());
        assert_eq!(
            received.error,
            Some(Error::Tls(rustls::Error::InvalidMessage(
                rustls::InvalidMessage::MessageTooLarge
            )))
        );
    }
}
