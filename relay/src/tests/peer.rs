//! An in-process TLS 1.3 peer for relay over sheath: a stock rustls `ServerConnection` with an
//! rcgen certificate, and a sheath client whose raw sender collects the bytes it sends.

use std::cell::RefCell;
use std::ops::Range;
use std::rc::Rc;
use std::sync::Arc;

use rustls::client::UnbufferedClientConnection;
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use rustls::unbuffered::{ConnectionState, UnbufferedStatus};
use rustls::version::TLS13;
use rustls::{ClientConfig, RootCertStore, ServerConfig, ServerConnection};
use sheath::TlsTx;

use super::*;

const HOST: &str = "relay.test";

/// Room of a test send frame: an Ethernet MSS.
pub const FRAME_ROOM: usize = 1460;

pub struct TestFrame {
    room: Vec<u8>,
    len: usize,
}

impl TxFrame for TestFrame {
    fn room_mut(&mut self) -> &mut [u8] {
        &mut self.room
    }
}

/// What the raw sender below `TlsTx` put on the wire, shared with the test.
#[derive(Clone, Default)]
pub struct Wire {
    pub sent: Rc<RefCell<Vec<u8>>>,
    /// `shutdown` was called (TCP FIN).
    pub shut: Rc<std::cell::Cell<bool>>,
}

/// Raw TCP sender: frames go out as they are, always at once.
pub struct TestTx(pub Wire);

impl FrameTx for TestTx {
    type Frame = TestFrame;
    type Sealed = TestFrame;
    type Error = Infallible;

    async fn alloc(&mut self) -> Result<TestFrame, Infallible> {
        Ok(TestFrame { room: vec![0; FRAME_ROOM], len: 0 })
    }

    async fn room(&mut self, _: usize) -> Result<(), Infallible> {
        Ok(())
    }

    fn plain(&self) -> (usize, usize) {
        (0, FRAME_ROOM)
    }

    fn seal(&mut self, mut f: TestFrame, len: usize) -> Result<TestFrame, Infallible> {
        f.len = len;
        Ok(f)
    }

    fn try_send(&mut self, f: TestFrame) -> Result<(), TestFrame> {
        self.0.sent.borrow_mut().extend_from_slice(&f.room[..f.len]);
        Ok(())
    }

    fn shutdown(&mut self) {
        self.0.shut.set(true);
    }

    async fn delivered(&mut self) {}
}

pub struct Peer {
    pub tx: TlsTx<TestTx>,
    pub wire: Wire,
    pub server: ServerConnection,
}

fn configs() -> (Arc<ClientConfig>, Arc<ServerConfig>) {
    let rcgen::CertifiedKey { cert, signing_key } = rcgen::generate_simple_self_signed(vec![HOST.to_owned()]).unwrap();
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(signing_key.serialize_der()));
    let provider = Arc::new(rustls::crypto::ring::default_provider());
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
    (Arc::new(client), Arc::new(server))
}

/// Completes the handshake. The server's records sent after it (session tickets) stay queued
/// for the stream under test.
pub fn connect() -> Peer {
    enum Next {
        Continue,
        Blocked,
        Done,
    }

    let (client_config, server_config) = configs();
    let mut client = UnbufferedClientConnection::new(client_config, ServerName::try_from(HOST).unwrap()).unwrap();
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
    let wire = Wire::default();
    Peer { tx: TlsTx::new(client, TestTx(wire.clone())), wire, server }
}

/// Wire ranges of the records in `bytes`, which holds whole records.
pub fn records(bytes: &[u8]) -> Vec<Range<usize>> {
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

pub fn message(seed: usize, len: usize) -> Vec<u8> {
    (0..len).map(|i| (seed * 31 + i * 7) as u8).collect()
}
