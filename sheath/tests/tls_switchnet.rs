//! sheath's TLS client over switchnet TCP (as switch tasks) against a kernel rustls server
//! across net_tap: the alert for a rejected record leaves before the socket is dropped. Needs
//! CAP_NET_ADMIN and /dev/net/tun (the Docker test container).

use std::io::{Read, Write};
use std::sync::Arc;

use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::version::TLS13;
use rustls::{ClientConfig, RootCertStore, ServerConfig, ServerConnection};
use sheath::switchnet::{Error as TlsError, Tls as ClientTls, tls_connect};
use switchnet::testkit::{TapConfig, client_server};
use switchnet::{NetHandle, TcpStream, tcp};

const SERVER_PORT: u16 = 8443;
const HOST: &str = "tls.test";

const TAP: TapConfig = TapConfig {
    name: "tls",
    socket: tcp::Config { rx_capacity: 64 * 1024, tx_capacity: 64 * 1024, mss_cap: None },
    max_sockets: 1,
    random_seed: 1,
};

fn tls_configs() -> (Arc<ServerConfig>, Arc<ClientConfig>) {
    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec![HOST.to_owned()]).expect("generate test certificate");
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(signing_key.serialize_der()));
    let server = ServerConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![cert.der().clone()], key)
        .unwrap();
    let mut roots = RootCertStore::empty();
    roots.add(cert.der().clone()).unwrap();
    let client = ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    (Arc::new(server), Arc::new(client))
}

/// One client against one kernel peer (see `client_server`), with the TLS configs they use.
fn run<T: Send + 'static, F: Future<Output = ()> + 'static>(
    serve: impl FnOnce(std::net::TcpStream, Arc<ServerConfig>) -> T + Send + 'static,
    client: impl FnOnce(NetHandle, TcpStream, Arc<ClientConfig>) -> F + 'static,
) -> T {
    let (server_tls, client_tls) = tls_configs();
    client_server(
        &TAP,
        SERVER_PORT,
        |_| {},
        move |stream| serve(stream, server_tls),
        move |handle, stream| client(handle, stream, client_tls),
        || {},
    )
}

/// Bytes the server sends after the record the client rejects: still unread when the client
/// drops its socket, which then aborts with RST (as Linux `close()` does).
const TRAILING: usize = 16 * 1024;

/// Everything the client sends until its FIN or RST, given to the server's `conn`; returns the
/// error the server's rustls saw (the client's alert, if it got out before an RST).
fn read_alert(conn: &mut ServerConnection, stream: &mut std::net::TcpStream) -> Result<(), rustls::Error> {
    let mut sent = Vec::new();
    let mut buf = [0; 4096];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => sent.extend_from_slice(&buf[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => break,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => panic!("read the client's records: {e}"),
        }
    }
    let mut rest = &sent[..];
    while !rest.is_empty() {
        conn.read_tls(&mut rest).expect("buffer the client's records");
    }
    conn.process_new_packets().map(|_| ())
}

const BAD_RECORD_MAC: Result<(), rustls::Error> =
    Err(rustls::Error::AlertReceived(rustls::AlertDescription::BadRecordMac));

/// Runs the server handshake until it has its first flight to write; returns that flight.
fn server_flight(conn: &mut ServerConnection, stream: &mut std::net::TcpStream) -> Vec<u8> {
    while !conn.wants_write() {
        conn.read_tls(stream).expect("read the ClientHello");
        conn.process_new_packets().expect("process the ClientHello");
    }
    let mut flight = Vec::new();
    while conn.wants_write() {
        conn.write_tls(&mut flight).unwrap();
    }
    flight
}

/// The server's handshake flight arrives with its last record (the one carrying Finished)
/// corrupted, and more data behind it. rustls rejects the record; `tls_connect` sends rustls's
/// bad_record_mac alert and FIN, and they reach the server before the RST that dropping the
/// socket with the trailing data unread causes.
#[zhida_test_support::pinned_test]
fn rejected_handshake_sends_the_alert_before_the_rst() {
    let seen = run(
        |mut stream, config| {
            let mut conn = ServerConnection::new(config).unwrap();
            let mut flight = server_flight(&mut conn, &mut stream);
            *flight.last_mut().unwrap() ^= 1;
            flight.resize(flight.len() + TRAILING, 0);
            stream.write_all(&flight).unwrap();
            read_alert(&mut conn, &mut stream)
        },
        |handle, stream, config| async move {
            match tls_connect(&handle, stream, config, HOST).await {
                Err(TlsError::Tls(sheath::Error::Tls(rustls::Error::DecryptError))) => {}
                Err(e) => panic!("unexpected error {e}"),
                Ok(_) => panic!("the corrupted Finished record was accepted"),
            }
        },
    );
    assert_eq!(seen, BAD_RECORD_MAC);
}

/// A corrupted application data record after the handshake, with more data behind it:
/// `Tls::ready` returns the error after the alert and FIN went out, so the client may drop the
/// connection at once.
#[zhida_test_support::pinned_test]
fn receive_error_after_the_handshake_sends_the_alert_before_the_rst() {
    let seen = run(
        |mut stream, config| {
            let mut conn = ServerConnection::new(config).unwrap();
            while conn.is_handshaking() {
                conn.complete_io(&mut stream).expect("server handshake");
            }
            conn.writer().write_all(b"corrupted").unwrap();
            let mut record = Vec::new();
            while conn.wants_write() {
                conn.write_tls(&mut record).unwrap();
            }
            *record.last_mut().unwrap() ^= 1;
            record.resize(record.len() + TRAILING, 0);
            stream.write_all(&record).unwrap();
            read_alert(&mut conn, &mut stream)
        },
        |handle, stream, config| async move {
            let mut tls: ClientTls = tls_connect(&handle, stream, config, HOST).await.expect("handshake");
            match tls.ready().await {
                Err(TlsError::Tls(sheath::Error::Tls(rustls::Error::DecryptError))) => {}
                other => panic!("unexpected {other:?}"),
            }
        },
    );
    assert_eq!(seen, BAD_RECORD_MAC);
}

