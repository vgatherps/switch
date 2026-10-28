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
use sheath::{Error, PlainTx, TlsTx, TxError, TxFrame};

/// Record overhead: 5 B header, 1 B content type, 16 B tag.
const RECORD: usize = 22;
/// A KeyUpdate record.
const KEY_UPDATE: usize = 27;

/// Seals `payload` into one frame and sends it; returns the frame's length on the wire.
fn seal_and_send(tx: &mut TlsTx<TestTx>, payload: &[u8]) -> usize {
    let mut f = tx.alloc().unwrap();
    let (head, _) = tx.plain();
    f.room_mut()[head..head + payload.len()].copy_from_slice(payload);
    tx.seal(&mut f, payload.len()).unwrap();
    let len = f.bytes().len();
    tx.try_send(f).unwrap();
    len
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
        assert_eq!(seal_and_send(&mut peer.tx, &msg), RECORD + len);
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
    assert_eq!(
        seal_and_send(&mut peer.tx, &msg),
        KEY_UPDATE + RECORD + room
    );
    assert_eq!(peer.deliver().unwrap(), msg);
    assert_eq!(seal_and_send(&mut peer.tx, b"again"), RECORD + 5);
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

/// At the confidentiality limit rustls updates our sending keys: its KeyUpdate goes out in
/// front of the next data record, once, and the server follows it.
#[test]
fn own_key_update_goes_in_front_of_the_next_record() {
    let mut peer = connect_with(limited_provider(), None);
    deliver_to_client(&mut peer).unwrap();
    let mut updates = 0;
    for i in 0..1100 {
        let msg = format!("{i:08}");
        match seal_and_send(&mut peer.tx, msg.as_bytes()) {
            n if n == RECORD + 8 => {}
            n if n == KEY_UPDATE + RECORD + 8 => updates += 1,
            n => panic!("frame {i} is {n} B"),
        }
        assert_eq!(peer.deliver().unwrap(), msg.as_bytes());
    }
    assert_eq!(updates, 1);
}

#[test]
fn close_notify_ends_sending_only() {
    let mut peer = connect(None);
    deliver_to_client(&mut peer).unwrap();
    let mut f = peer.tx.alloc().unwrap();
    peer.tx.close_notify(&mut f).unwrap();
    assert_eq!(f.bytes().len(), RECORD + 2);
    peer.tx.try_send(f).unwrap();
    assert_eq!(peer.deliver().unwrap(), b"");
    assert!(peer.server.process_new_packets().unwrap().peer_has_closed());

    let mut f = peer.tx.alloc().unwrap();
    assert_eq!(peer.tx.seal(&mut f, 1), Err(TxError::Closed));

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

    let mut sealed = peer.tx.alloc().unwrap();
    let (head, _) = peer.tx.plain();
    sealed.room_mut()[head..head + 6].copy_from_slice(b"before");
    peer.tx.seal(&mut sealed, 6).unwrap();
    peer.wire.refuse.set(1);
    let sealed = peer.tx.try_send(sealed).unwrap_err();

    let mut stream = Stream::default();
    peer.send(&mut stream, b"hello");
    let last = stream.bytes.len() - 1;
    stream.bytes[last] ^= 1;
    let received = receive(&mut peer.tx, vec![stream.bytes]);
    let error = rustls::Error::DecryptError;
    assert_eq!(received.error, Some(Error::Tls(error.clone())));

    peer.tx.try_send(sealed).unwrap();
    let mut f = peer.tx.alloc().unwrap();
    assert_eq!(peer.tx.seal(&mut f, 1), Err(TxError::Tls(error)));
    assert_eq!(peer.tx.take_pending(&mut f), Ok(true));
    assert_eq!(f.bytes().len(), RECORD + 2);
    peer.tx.try_send(f).unwrap();
    let mut f = peer.tx.alloc().unwrap();
    assert_eq!(peer.tx.take_pending(&mut f), Ok(false));

    let sent = peer.wire.sent.take();
    peer.wire.sent.replace(sent[..RECORD + 6].to_vec());
    assert_eq!(peer.deliver().unwrap(), b"before");
    peer.wire.sent.replace(sent[RECORD + 6..].to_vec());
    assert_eq!(
        peer.deliver(),
        Err(rustls::Error::AlertReceived(AlertDescription::BadRecordMac))
    );
}

/// A refused frame comes back unchanged and goes out later as it is.
#[test]
fn refused_frame_is_sent_later() {
    let mut peer = connect(None);
    deliver_to_client(&mut peer).unwrap();
    let mut f = peer.tx.alloc().unwrap();
    let (head, _) = peer.tx.plain();
    f.room_mut()[head..head + 3].copy_from_slice(b"one");
    peer.tx.seal(&mut f, 3).unwrap();
    peer.wire.refuse.set(2);
    let f = peer.tx.try_send(f).unwrap_err();
    let f = peer.tx.try_send(f).unwrap_err();
    assert!(peer.wire.sent.borrow().is_empty());
    peer.tx.try_send(f).unwrap();
    assert_eq!(peer.deliver().unwrap(), b"one");
    seal_and_send(&mut peer.tx, b"two");
    assert_eq!(peer.deliver().unwrap(), b"two");
}

#[test]
#[should_panic(expected = "was not sent")]
fn sealing_after_dropping_a_sealed_frame_panics() {
    let mut peer = connect(None);
    let mut f = peer.tx.alloc().unwrap();
    peer.tx.seal(&mut f, 1).unwrap();
    drop(f);
    let mut f = peer.tx.alloc().unwrap();
    let _ = peer.tx.seal(&mut f, 1);
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
