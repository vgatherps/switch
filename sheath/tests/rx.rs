//! TlsRx over an in-process rustls server: records in the stream are cut into frames in every
//! way that matters, and each record must come out once, in order, with the stamp of the frame
//! holding its first byte, decrypted inside that frame unless it straddles frames.

// ring's assembly cannot run under Miri, and sheath itself has no unsafe code.
#![cfg(not(miri))]

mod common;

use std::ops::Range;

use common::*;
use rustls::crypto::cipher::{MessageEncrypter, OutboundChunks, OutboundPlainMessage};
use rustls::{
    AlertDescription, ConnectionTrafficSecrets, ContentType, ProtocolVersion, SupportedCipherSuite,
};
use sheath::{Error, TlsRx, TlsTx};

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
    let mut received = receive(&mut peer.tx, split(&stream.bytes, &cuts));
    assert_eq!(received.error, None, "cuts {cuts:?}");
    assert_received(&received.got, &stream.records, &cuts);
    assert!(received.rx.eof(), "cuts {cuts:?}");
    assert_eq!(received.rx.fin(&mut peer.tx), Ok(()));
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
    let received = receive(&mut peer.tx, split(&stream.bytes, &cuts));
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

        let mut received = receive(&mut peer.tx, split(&stream.bytes, &cuts));
        assert_received(&received.got, &stream.records[..2], &cuts);
        let error = Some(Error::Tls(rustls::Error::DecryptError));
        assert_eq!(received.error, error);
        assert_eq!(received.rx.chunk(&mut peer.tx).err(), error);
        assert_eq!(received.rx.fin(&mut peer.tx).err(), error);
    }
}

#[test]
fn fatal_alert_is_an_error() {
    let mut peer = connect(None);
    let mut stream = Stream::default();
    peer.send(&mut stream, b"hello");
    peer.fatal_alert(&mut stream);

    let received = receive(&mut peer.tx, vec![stream.bytes]);
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

    let mut received = receive(&mut peer.tx, split(&stream.bytes, &[after + 2]));
    assert_eq!(received.error, None);
    assert_eq!(received.got.len(), 1);
    assert!(received.rx.eof());

    let ignored = VecFrame {
        data: b"ignored".to_vec(),
        stamp: 9,
    };
    received.rx.push(ignored);
    assert!(received.rx.chunk(&mut peer.tx).unwrap().is_none());
    assert_eq!(received.rx.fin(&mut peer.tx), Ok(()));
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

        let mut received = receive(&mut peer.tx, vec![stream.bytes[..end].to_vec()]);
        assert_eq!(received.error, None);
        assert_eq!(received.got.len(), 1);
        assert!(!received.rx.eof());
        assert_eq!(received.rx.fin(&mut peer.tx), Err(Error::Truncated));
        assert_eq!(
            received.rx.chunk(&mut peer.tx).err(),
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
    let mut rx = TlsRx::<VecFrame, TestTx>::new();
    let first = VecFrame {
        data: stream.bytes,
        stamp: 0,
    };
    rx.push(first);
    assert!(rx.chunk(&mut peer.tx).unwrap().is_some());
    let second = VecFrame {
        data: vec![],
        stamp: 1,
    };
    rx.push(second);
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

                let received = receive(&mut peer.tx, split(&stream.bytes, &cuts));
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
    fn new(mut peer: Peer, stream: &mut Stream) -> (TlsTx<TestTx>, Self) {
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
        (peer.tx, Self { encrypter, seq })
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
fn chained_tickets(tickets: usize) -> (TlsTx<TestTx>, Stream, usize) {
    let mut stream = Stream::default();
    let (tx, mut sealer) = Sealer::new(connect(None), &mut stream);
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
    (tx, stream, retained)
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

        let (mut tx, stream, retained) = chained_tickets(10);
        assert!(retained < 0xffff);
        let cuts_below = cuts(&stream);
        let mut received = receive(&mut tx, split(&stream.bytes, &cuts_below));
        assert_eq!(received.error, None);
        assert_received(&received.got, &stream.records, &cuts_below);
        assert_eq!(received.rx.fin(&mut tx), Ok(()));

        let (mut tx, stream, retained) = chained_tickets(20);
        assert!(retained > 0xffff);
        let cuts = cuts(&stream);
        let received = receive(&mut tx, split(&stream.bytes, &cuts));
        assert!(received.got.is_empty());
        assert_eq!(
            received.error,
            Some(Error::Tls(rustls::Error::InvalidMessage(
                rustls::InvalidMessage::MessageTooLarge
            )))
        );
    }
}

/// A frame that counts itself live while held.
struct LiveFrame {
    data: Vec<u8>,
    _live: std::rc::Rc<()>,
}

impl sheath::RxFrame for LiveFrame {
    fn payload(&self) -> &[u8] {
        &self.data
    }
    fn payload_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
    fn rx_timestamp(&self) -> u64 {
        0
    }
}

/// `discard` with a record's run held in the frame and another record behind it: the frame
/// is released, nothing is current, and `chunk` returns nothing more.
#[test]
fn discard_releases_the_frame_and_the_run() {
    let mut peer = connect(None);
    let mut stream = Stream::default();
    peer.flush(&mut stream);
    peer.send(&mut stream, b"hello");
    peer.send(&mut stream, b"world");
    let live = std::rc::Rc::new(());
    let mut rx: TlsRx<LiveFrame, TestTx> = TlsRx::new();
    rx.push(LiveFrame { data: stream.bytes, _live: live.clone() });
    assert_eq!(rx.chunk(&mut peer.tx).unwrap().unwrap().bytes, b"hello");
    rx.discard();
    assert_eq!(std::rc::Rc::strong_count(&live), 1, "the frame is still held");
    assert_eq!(rx.current(), b"");
    assert!(rx.chunk(&mut peer.tx).unwrap().is_none());
    assert!(!rx.eof());
}

/// A fresh receive half with frame `stamp` holding `data`.
fn pushed(data: Vec<u8>, stamp: u64) -> TlsRx<VecFrame, TestTx> {
    let mut rx = TlsRx::new();
    rx.push(VecFrame { data, stamp });
    rx
}

/// The run contract: `chunk` again returns the same unconsumed bytes, with the caller's
/// changes; `consume` advances within the record; the next record comes only once the run is
/// used up.
#[test]
fn a_run_is_returned_again_until_consumed() {
    let mut peer = connect(None);
    let mut stream = Stream::default();
    peer.flush(&mut stream);
    peer.send(&mut stream, b"hello");
    peer.send(&mut stream, b"world");
    let mut rx = pushed(stream.bytes, 0);

    let chunk = rx.chunk(&mut peer.tx).unwrap().unwrap();
    assert_eq!(chunk.bytes, b"hello");
    chunk.bytes[0] = b'H';
    assert_eq!(rx.chunk(&mut peer.tx).unwrap().unwrap().bytes, b"Hello");
    assert_eq!(rx.current(), b"Hello");
    rx.consume(2);
    assert_eq!(rx.current(), b"llo");
    assert_eq!(rx.chunk(&mut peer.tx).unwrap().unwrap().bytes, b"llo");
    rx.consume(3);
    assert_eq!(rx.current(), b"");
    assert_eq!(rx.chunk(&mut peer.tx).unwrap().unwrap().bytes, b"world");
    rx.consume(5);
    assert!(rx.chunk(&mut peer.tx).unwrap().is_none());
    assert_eq!(rx.current(), b"");
}

#[test]
#[should_panic(expected = "consume past the current run")]
fn consume_past_the_run_panics() {
    let mut peer = connect(None);
    let mut stream = Stream::default();
    peer.flush(&mut stream);
    peer.send(&mut stream, b"hi");
    let mut rx = pushed(stream.bytes, 0);
    assert!(rx.chunk(&mut peer.tx).unwrap().is_some());
    rx.consume(3);
}

/// A record straddling frames is decrypted in the scratch buffer: `current()` is exactly that
/// record's plaintext, and stays so until consumed (relay reads validated text back through it).
#[test]
fn current_of_a_straddling_record_is_its_plaintext() {
    let mut peer = connect(None);
    let mut stream = Stream::default();
    peer.flush(&mut stream);
    peer.send(&mut stream, "split €".as_bytes());
    let cut = stream.records[0].wire.start + 3;
    let mut rx = pushed(stream.bytes[..cut].to_vec(), 0);
    assert!(rx.chunk(&mut peer.tx).unwrap().is_none());
    rx.push(VecFrame { data: stream.bytes[cut..].to_vec(), stamp: 1 });
    assert_eq!(rx.chunk(&mut peer.tx).unwrap().unwrap().bytes, "split €".as_bytes());
    assert_eq!(rx.current(), "split €".as_bytes());
    assert_eq!(rx.chunk(&mut peer.tx).unwrap().unwrap().rx_timestamp, 0);
    assert_eq!(rx.current(), "split €".as_bytes());
    rx.consume(6);
    assert_eq!(rx.current(), "€".as_bytes());
}
