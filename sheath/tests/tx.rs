//! TlsTx against an in-process rustls server: each sealed frame is one record (behind the
//! KeyUpdates rustls must send first), the server decrypts it, and no sealed frame is lost.

// ring's assembly cannot run under Miri, and sheath itself has no unsafe code.
#![cfg(not(miri))]

mod common;

use std::sync::LazyLock;

use common::*;
use rustls::client::UnbufferedClientConnection;
use rustls::crypto::ring::{cipher_suite, default_provider};
use rustls::crypto::{CipherSuiteCommon, CryptoProvider};
use rustls::pki_types::ServerName;
use rustls::{AlertDescription, SupportedCipherSuite, Tls13CipherSuite};
use sheath::{Error, PlainTx, Sealed, TlsRx, TlsTx, TxError, TxFrame};

/// Record overhead: 5 B header, 1 B content type, 16 B tag.
const RECORD: usize = 22;
/// A KeyUpdate record.
const KEY_UPDATE: usize = 27;

/// A frame with `payload` as its plaintext.
fn frame(tx: &mut TlsTx<TestTx>, payload: &[u8]) -> TestFrame {
    let mut f = tx.alloc().unwrap();
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
    let f = peer.tx.alloc().unwrap();
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
    let f = peer.tx.alloc().unwrap();
    let alert = peer.tx.take_pending(f).unwrap().expect("the alert");
    assert_eq!(send(&mut peer, alert), RECORD + 2);
    let f = peer.tx.alloc().unwrap();
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

/// TCP FIN without the server's close_notify is truncation, which stops sending.
#[test]
fn truncation_stops_sending() {
    let mut peer = connect(None);
    deliver_to_client(&mut peer).unwrap();
    let mut rx = TlsRx::<VecFrame>::new();
    assert_eq!(rx.fin(&mut peer.tx), Err(Error::Truncated));
    let f = frame(&mut peer.tx, b"x");
    assert_eq!(
        peer.tx.seal(f, 1).err(),
        Some(TxError::Received(Error::Truncated))
    );
    let f = peer.tx.alloc().unwrap();
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
            let f = peer.tx.alloc().unwrap();
            peer.tx.close_notify(f).err()
        } else {
            let f = frame(&mut peer.tx, b"lost");
            peer.tx.seal(f, 4).err()
        };
        assert_eq!(error, Some(TxError::Lower(SealFailed)));

        let f = frame(&mut peer.tx, b"x");
        assert_eq!(peer.tx.seal(f, 1).err(), Some(TxError::Broken));
        let f = peer.tx.alloc().unwrap();
        assert_eq!(peer.tx.close_notify(f).err(), Some(TxError::Broken));
        let f = peer.tx.alloc().unwrap();
        assert_eq!(peer.tx.take_pending(f).err(), Some(TxError::Broken));
        assert!(peer.wire.sent.borrow().is_empty());
    }
}

#[test]
#[should_panic(expected = "sealing an empty record")]
fn sealing_nothing_panics() {
    let mut peer = connect(None);
    let f = peer.tx.alloc().unwrap();
    let _ = peer.tx.seal(f, 0);
}

/// The review's case: frame A is sealed, another frame goes out, A is dropped. Only a
/// `Sealed` can be sent, and only one exists at a time, so A's loss is caught.
#[test]
#[should_panic(expected = "the last sealed frame was not sent")]
fn sealing_after_dropping_a_sealed_frame_panics() {
    let mut peer = connect(None);
    let a = seal(&mut peer.tx, b"a");
    drop(a);
    let _ = seal(&mut peer.tx, b"b");
}

#[test]
#[should_panic(expected = "the last sealed frame was not sent")]
fn two_sealed_frames_cannot_exist() {
    let mut peer = connect(None);
    let _a = seal(&mut peer.tx, b"a");
    let f = peer.tx.alloc().unwrap();
    let _ = peer.tx.take_pending(f);
}

#[test]
#[should_panic(expected = "handshake is complete")]
fn new_needs_completed_handshake() {
    let (client_config, _) = configs(default_provider(), None);
    let client =
        UnbufferedClientConnection::new(client_config, ServerName::try_from(HOST).unwrap())
            .unwrap();
    TlsTx::new(client, TestTx(Wire::default()));
}
