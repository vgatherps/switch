//! TlsTx against an in-process rustls server: each sealed frame is one record (behind the
//! KeyUpdates rustls must send first), the server decrypts it, and no sealed frame is lost.

// ring's assembly cannot run under Miri, and sheath itself has no unsafe code.
#![cfg(not(miri))]

mod common;

use std::sync::LazyLock;

use common::*;
use sheath::{FrameTx, TxFrame};
use rustls::client::UnbufferedClientConnection;
use rustls::crypto::ring::{cipher_suite, default_provider};
use rustls::crypto::{CipherSuiteCommon, CryptoProvider};
use rustls::pki_types::ServerName;
use rustls::{AlertDescription, SupportedCipherSuite, Tls13CipherSuite};
use sheath::{Error, Sealed, TlsRx, TlsTx, TxError};

/// Record overhead: 5 B header, 1 B content type, 16 B tag.
const RECORD: usize = 22;
/// A KeyUpdate record.
const KEY_UPDATE: usize = 27;

/// A frame with `payload` as its plaintext.
fn frame(tx: &mut TlsTx<TestTx>, payload: &[u8]) -> TestFrame {
    let mut f = ready(tx.alloc()).unwrap();
    let (head, _) = tx.plain();
    f.room_mut()[head..head + payload.len()].copy_from_slice(payload);
    f
}

fn seal(tx: &mut TlsTx<TestTx>, payload: &[u8]) -> Sealed<TestFrame> {
    let f = frame(tx, payload);
    tx.seal(f, payload.len()).unwrap()
}

/// Sends `sealed`; returns its length on the wire.
fn send(peer: &mut Peer, sealed: Sealed<TestFrame>) -> usize {
    let before = peer.wire.sent.borrow().len();
    peer.tx.try_send(sealed).unwrap();
    peer.wire.sent.borrow().len() - before
}

fn seal_and_send(peer: &mut Peer, payload: &[u8]) -> usize {
    let sealed = seal(&mut peer.tx, payload);
    send(peer, sealed)
}

/// Feeds the server's queued records to the client; they must carry no application data.
fn deliver_to_client(peer: &mut Peer) -> Result<(), Error> {
    let mut stream = Stream::default();
    peer.flush(&mut stream);
    let received = receive(&mut peer.tx, vec![stream.bytes]);
    assert!(received.got.is_empty());
    received.error.map_or(Ok(()), Err)
}

#[test]
fn sealed_records_decrypt_on_the_server() {
    let mut peer = connect(None);
    deliver_to_client(&mut peer).unwrap(); // tickets
    let (head, room) = peer.tx.plain();
    assert_eq!((head, room), (5, FRAME_ROOM - RECORD - 2 * KEY_UPDATE));
    for len in [1, 109, room] {
        let msg = message(len, len);
        assert_eq!(seal_and_send(&mut peer, &msg), RECORD + len);
        assert_eq!(peer.deliver().unwrap(), msg);
    }
}

/// The server updates its keys and requests ours: rustls answers with a KeyUpdate record
/// written in front of the next data record, in the same frame.
#[test]
fn key_update_request_is_answered_in_front_of_the_next_record() {
    let mut peer = connect(None);
    deliver_to_client(&mut peer).unwrap();
    peer.server.refresh_traffic_keys().unwrap();
    deliver_to_client(&mut peer).unwrap();

    // a full frame of plaintext still fits: the room reserves space for the KeyUpdate
    let (_, room) = peer.tx.plain();
    let msg = message(1, room);
    assert_eq!(seal_and_send(&mut peer, &msg), KEY_UPDATE + RECORD + room);
    assert_eq!(peer.deliver().unwrap(), msg);
    assert_eq!(seal_and_send(&mut peer, b"again"), RECORD + 5);
    assert_eq!(peer.deliver().unwrap(), b"again");

    // the server's new keys are in use on the receive side too
    let mut stream = Stream::default();
    peer.send(&mut stream, b"after");
    let received = receive(&mut peer.tx, vec![stream.bytes]);
    assert_eq!(received.error, None);
    assert_eq!(received.got[0].plaintext, b"after");
}

/// AES-128-GCM with a confidentiality limit of 1024 records, so rustls updates our keys soon.
fn limited_provider() -> CryptoProvider {
    static LIMITED: LazyLock<Tls13CipherSuite> = LazyLock::new(|| {
        let suite = cipher_suite::TLS13_AES_128_GCM_SHA256
            .tls13()
            .expect("a TLS 1.3 suite");
        Tls13CipherSuite {
            common: CipherSuiteCommon {
                confidentiality_limit: 1024,
                ..suite.common
            },
            ..*suite
        }
    });
    CryptoProvider {
        cipher_suites: vec![SupportedCipherSuite::Tls13(&LIMITED)],
        ..default_provider()
    }
}

/// The send that carries rustls's own KeyUpdate under `limited_provider`.
const OWN_UPDATE_AT: usize = 1025;

/// At the confidentiality limit rustls updates our sending keys: its KeyUpdate goes out in
/// front of the next data record, once, and the server follows it.
#[test]
fn own_key_update_goes_in_front_of_the_next_record() {
    let mut peer = connect_with(limited_provider(), None);
    deliver_to_client(&mut peer).unwrap();
    for i in 0..1100 {
        let msg = format!("{i:08}");
        let expected = if i == OWN_UPDATE_AT {
            KEY_UPDATE + RECORD + 8
        } else {
            RECORD + 8
        };
        assert_eq!(
            seal_and_send(&mut peer, msg.as_bytes()),
            expected,
            "send {i}"
        );
        assert_eq!(peer.deliver().unwrap(), msg.as_bytes());
    }
}

/// The server requests a key update just as rustls is due for its own: both KeyUpdates go in
/// front of the same data record, and a full frame of plaintext still fits.
#[test]
fn both_key_updates_fit_in_front_of_one_full_record() {
    let mut peer = connect_with(limited_provider(), None);
    deliver_to_client(&mut peer).unwrap();
    for i in 0..OWN_UPDATE_AT {
        seal_and_send(&mut peer, format!("{i:08}").as_bytes());
        peer.deliver().unwrap();
    }
    peer.server.refresh_traffic_keys().unwrap();
    deliver_to_client(&mut peer).unwrap();

    let (_, room) = peer.tx.plain();
    let msg = message(2, room);
    assert_eq!(
        seal_and_send(&mut peer, &msg),
        2 * KEY_UPDATE + RECORD + room
    );
    assert_eq!(2 * KEY_UPDATE + RECORD + room, FRAME_ROOM);
    assert_eq!(peer.deliver().unwrap(), msg);
    assert_eq!(seal_and_send(&mut peer, b"next"), RECORD + 4);
    assert_eq!(peer.deliver().unwrap(), b"next");
}

#[test]
fn close_notify_ends_sending_only() {
    let mut peer = connect(None);
    deliver_to_client(&mut peer).unwrap();
    let f = ready(peer.tx.alloc()).unwrap();
    let sealed = peer.tx.close_notify(f).unwrap();
    assert_eq!(send(&mut peer, sealed), RECORD + 2);
    assert_eq!(peer.deliver().unwrap(), b"");
    assert!(peer.server.process_new_packets().unwrap().peer_has_closed());

    let f = frame(&mut peer.tx, b"x");
    assert_eq!(peer.tx.seal(f, 1).err(), Some(TxError::Closed));

    let mut stream = Stream::default();
    peer.send(&mut stream, b"still");
    let received = receive(&mut peer.tx, vec![stream.bytes]);
    assert_eq!(received.got[0].plaintext, b"still");
}

/// After a bad record, rustls's bad_record_mac alert goes out through `take_pending`, behind a
/// frame sealed before the error, and sealing stops.
#[test]
fn alert_after_a_receive_error_follows_earlier_frames() {
    let mut peer = connect(None);
    deliver_to_client(&mut peer).unwrap();

    let sealed = seal(&mut peer.tx, b"before");
    peer.wire.refuse.set(1);
    let sealed = peer.tx.try_send(sealed).unwrap_err();

    let mut stream = Stream::default();
    peer.send(&mut stream, b"hello");
    let last = stream.bytes.len() - 1;
    stream.bytes[last] ^= 1;
    let received = receive(&mut peer.tx, vec![stream.bytes]);
    let error = Error::Tls(rustls::Error::DecryptError);
    assert_eq!(received.error, Some(error.clone()));

    assert_eq!(send(&mut peer, sealed), RECORD + 6);
    let f = frame(&mut peer.tx, b"x");
    assert_eq!(peer.tx.seal(f, 1).err(), Some(TxError::Received(error)));
    let f = ready(peer.tx.alloc()).unwrap();
    let alert = peer.tx.take_pending(f).unwrap().expect("the alert");
    assert_eq!(send(&mut peer, alert), RECORD + 2);
    let f = ready(peer.tx.alloc()).unwrap();
    assert!(peer.tx.take_pending(f).unwrap().is_none());

    let sent = peer.wire.sent.take();
    peer.wire.sent.replace(sent[..RECORD + 6].to_vec());
    assert_eq!(peer.deliver().unwrap(), b"before");
    peer.wire.sent.replace(sent[RECORD + 6..].to_vec());
    assert_eq!(
        peer.deliver(),
        Err(rustls::Error::AlertReceived(AlertDescription::BadRecordMac))
    );
}

/// A record over 2^14 + 256 B (16641 B here) fails the connection with a record_overflow alert
/// to the server (RFC 8446 §5.2), whether the record arrives in one frame or across two.
#[test]
fn oversized_record_gets_a_record_overflow_alert() {
    for straddle in [false, true] {
        let mut peer = connect(None);
        deliver_to_client(&mut peer).unwrap();
        let mut stream = Stream::default();
        peer.send(&mut stream, b"hello");
        let at = stream.bytes.len();
        let [hi, lo] = 16641u16.to_be_bytes();
        stream.bytes.extend_from_slice(&[0x17, 0x03, 0x03, hi, lo]);
        stream.bytes.resize(at + 5 + 16641, 0);
        let cuts = if straddle { vec![at + 100] } else { vec![] };

        let received = receive(&mut peer.tx, split(&stream.bytes, &cuts));
        assert_eq!(received.got.len(), 1, "straddle {straddle}");
        assert_eq!(received.error, Some(Error::Tls(rustls::Error::PeerSentOversizedRecord)), "straddle {straddle}");
        let f = ready(peer.tx.alloc()).unwrap();
        let alert = peer.tx.take_pending(f).unwrap().expect("the alert");
        send(&mut peer, alert);
        assert_eq!(
            peer.deliver(),
            Err(rustls::Error::AlertReceived(AlertDescription::RecordOverflow)),
            "straddle {straddle}"
        );
    }
}

/// TCP FIN without the server's close_notify is truncation, which stops sending.
#[test]
fn truncation_stops_sending() {
    let mut peer = connect(None);
    deliver_to_client(&mut peer).unwrap();
    let mut rx = TlsRx::<VecFrame, TestTx>::new();
    assert_eq!(rx.fin(&mut peer.tx), Err(Error::Truncated));
    let f = frame(&mut peer.tx, b"x");
    assert_eq!(
        peer.tx.seal(f, 1).err(),
        Some(TxError::Received(Error::Truncated))
    );
    let f = ready(peer.tx.alloc()).unwrap();
    assert_eq!(
        peer.tx.close_notify(f).err(),
        Some(TxError::Received(Error::Truncated))
    );
}

/// A refused frame comes back unchanged and goes out later as it is.
#[test]
fn refused_frame_is_sent_later() {
    let mut peer = connect(None);
    deliver_to_client(&mut peer).unwrap();
    let sealed = seal(&mut peer.tx, b"one");
    peer.wire.refuse.set(2);
    let sealed = peer.tx.try_send(sealed).unwrap_err();
    let sealed = peer.tx.try_send(sealed).unwrap_err();
    assert!(peer.wire.sent.borrow().is_empty());
    assert_eq!(send(&mut peer, sealed), RECORD + 3);
    assert_eq!(peer.deliver().unwrap(), b"one");
    seal_and_send(&mut peer, b"two");
    assert_eq!(peer.deliver().unwrap(), b"two");
}

/// The frame sender failing after rustls encrypted a record leaves a gap in the sequence
/// numbers: no record may be encrypted after it.
#[test]
fn lower_failure_after_encryption_breaks_the_connection() {
    for close in [false, true] {
        let mut peer = connect(None);
        deliver_to_client(&mut peer).unwrap();
        peer.wire.fail_seal.set(true);
        let error = if close {
            let f = ready(peer.tx.alloc()).unwrap();
            peer.tx.close_notify(f).err()
        } else {
            let f = frame(&mut peer.tx, b"lost");
            peer.tx.seal(f, 4).err()
        };
        assert_eq!(error, Some(TxError::Lower(SealFailed)));

        let f = frame(&mut peer.tx, b"x");
        assert_eq!(peer.tx.seal(f, 1).err(), Some(TxError::Broken));
        let f = ready(peer.tx.alloc()).unwrap();
        assert_eq!(peer.tx.close_notify(f).err(), Some(TxError::Broken));
        let f = ready(peer.tx.alloc()).unwrap();
        assert_eq!(peer.tx.take_pending(f).err(), Some(TxError::Broken));
        assert!(peer.wire.sent.borrow().is_empty());
    }
}

#[test]
#[should_panic(expected = "sealing an empty record")]
fn sealing_nothing_panics() {
    let mut peer = connect(None);
    let f = ready(peer.tx.alloc()).unwrap();
    let _ = peer.tx.seal(f, 0);
}

/// Frame A is sealed and dropped unsent: its sequence number is lost, so the sender is broken.
/// The next seal fails with `Broken` instead of panicking, and so does every later call that
/// would make a record. (Before, the next seal panicked.)
#[test]
fn sealing_after_dropping_a_sealed_frame_is_broken() {
    let mut peer = connect(None);
    drop(seal(&mut peer.tx, b"a"));
    let f = frame(&mut peer.tx, b"b");
    assert!(matches!(peer.tx.seal(f, 1), Err(TxError::Broken)));
    let f = ready(peer.tx.alloc()).unwrap();
    assert!(matches!(peer.tx.take_pending(f), Err(TxError::Broken)));
    let f = ready(peer.tx.alloc()).unwrap();
    assert!(matches!(peer.tx.close_notify(f), Err(TxError::Broken)));
    assert!(peer.wire.sent.borrow().is_empty());
}

/// A second record while the first sealed frame is still held unsent: the sender is broken
/// (the held one's records can no longer be followed), with `Broken`, not a panic.
#[test]
fn two_sealed_frames_cannot_exist() {
    let mut peer = connect(None);
    let _a = seal(&mut peer.tx, b"a");
    let f = ready(peer.tx.alloc()).unwrap();
    assert!(matches!(peer.tx.take_pending(f), Err(TxError::Broken)));
    let f = frame(&mut peer.tx, b"b");
    assert!(matches!(peer.tx.seal(f, 1), Err(TxError::Broken)));
}

#[test]
#[should_panic(expected = "handshake is complete")]
fn new_needs_completed_handshake() {
    let (client_config, _) = configs(default_provider(), None);
    let client =
        UnbufferedClientConnection::new(client_config, ServerName::try_from(HOST).unwrap())
            .unwrap();
    TlsTx::new(client, TestTx(Wire::default()), None);
}

/// The alert rustls queues on a receive error is visible to `has_pending` as soon as the
/// receive step returns, before anything else asks rustls.
#[test]
fn has_pending_sees_the_alert_of_a_receive_error() {
    let mut peer = connect(None);
    deliver_to_client(&mut peer).unwrap();
    assert!(!peer.tx.has_pending());
    let mut stream = Stream::default();
    peer.send(&mut stream, b"hello");
    let last = stream.bytes.len() - 1;
    stream.bytes[last] ^= 1;
    let received = receive(&mut peer.tx, vec![stream.bytes]);
    assert_eq!(
        received.error,
        Some(Error::Tls(rustls::Error::DecryptError))
    );
    assert!(peer.tx.has_pending());
    let f = ready(peer.tx.alloc()).unwrap();
    let alert = peer.tx.take_pending(f).unwrap().unwrap();
    assert_eq!(send(&mut peer, alert), RECORD + 2);
    assert!(!peer.tx.has_pending());
}

/// Sending while the server's session ticket is split across records, half received: rustls
/// holds the first half, and every call to it must start with those bytes.
#[test]
fn sealing_while_a_handshake_message_is_half_received() {
    let mut peer = connect(Some(64));
    let mut stream = Stream::default();
    peer.flush(&mut stream);
    peer.send(&mut stream, b"after the ticket");
    let tickets = records(&stream.bytes);
    assert!(tickets.len() > 2, "the ticket spans records");
    let cut = tickets[0].end;

    let mut rx = TlsRx::<VecFrame, TestTx>::new();
    let first = VecFrame {
        data: stream.bytes[..cut].to_vec(),
        stamp: 0,
    };
    rx.push(first);
    assert!(rx.chunk(&mut peer.tx).unwrap().is_none());

    assert_eq!(seal_and_send(&mut peer, b"mid"), RECORD + 3);
    assert_eq!(peer.deliver().unwrap(), b"mid");
    assert!(!peer.tx.has_pending());

    let rest = VecFrame {
        data: stream.bytes[cut..].to_vec(),
        stamp: 1,
    };
    rx.push(rest);
    let chunk = rx.chunk(&mut peer.tx).unwrap().unwrap();
    assert_eq!(chunk.bytes, b"after the ticket");
    let n = chunk.bytes.len();
    rx.consume(n);
    assert!(rx.chunk(&mut peer.tx).unwrap().is_none());
    assert_eq!(seal_and_send(&mut peer, b"end"), RECORD + 3);
    assert_eq!(peer.deliver().unwrap(), b"end");
}

/// Over a lower layer with 64 KiB frames and the default fragment limit (None: rustls's 16 KiB
/// records), a seal of the whole advertised plaintext room is one record that fits the frame.
/// (Before, the room was the frame less the overhead, 65,460 B: rustls split it into four
/// records past the frame and the seal panicked.)
#[test]
fn default_fragment_limit_caps_the_room_at_one_record() {
    let mut peer = connect_over(default_provider(), None, Wire { frame_room: 1 << 16, ..Wire::default() });
    let (_, room) = peer.tx.plain();
    assert_eq!(room, 1 << 14);
    let payload = vec![7; room];
    let wire_len = seal_and_send(&mut peer, &payload);
    assert_eq!(wire_len, RECORD + room);
    let mut tls = &peer.wire.sent.borrow()[..];
    while !tls.is_empty() {
        peer.server.read_tls(&mut tls).unwrap();
    }
    peer.server.process_new_packets().unwrap();
    let mut got = vec![0; room];
    std::io::Read::read_exact(&mut peer.server.reader(), &mut got).unwrap();
    assert_eq!(got, payload);
}
