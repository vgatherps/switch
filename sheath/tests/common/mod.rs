//! A sheath client over in-process rustls: a stock `ServerConnection` with an rcgen
//! certificate on the other end, frames as `Vec`s.

// each test crate uses part of this
#![allow(dead_code)]

use std::cell::{Cell, RefCell};
use std::io::{ErrorKind, Read, Write};
use std::ops::Range;
use std::rc::Rc;
use std::sync::{Arc, LazyLock};

use sheath::{FrameTx, RxFrame, TxFrame};
use rustls::client::UnbufferedClientConnection;
use rustls::crypto::CryptoProvider;
use rustls::crypto::ring::default_provider;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use rustls::unbuffered::{ConnectionState, UnbufferedStatus};
use rustls::version::TLS13;
use rustls::{ClientConfig, RootCertStore, ServerConfig, ServerConnection};
use sheath::{Error, TlsRx, TlsTx};

pub const HOST: &str = "sheath.test";

/// Room of a test send frame: an Ethernet MSS.
pub const FRAME_ROOM: usize = 1460;

pub struct VecFrame {
    pub data: Vec<u8>,
    pub stamp: u64,
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

#[derive(Debug)]
pub struct TestFrame {
    room: Vec<u8>,
    len: usize,
}

impl TxFrame for TestFrame {
    fn room_mut(&mut self) -> &mut [u8] {
        &mut self.room
    }
}

impl TestFrame {
    pub fn bytes(&self) -> &[u8] {
        &self.room[..self.len]
    }
}

/// What the raw sender below `TlsTx` put on the wire, shared with the test.
#[derive(Clone, Default)]
pub struct Wire {
    pub sent: Rc<RefCell<Vec<u8>>>,
    /// The next this many `try_send`s are refused.
    pub refuse: Rc<Cell<usize>>,
    /// The next `seal` fails.
    pub fail_seal: Rc<Cell<bool>>,
    /// `shutdown` was called (TCP FIN).
    pub shut: Rc<Cell<bool>>,
}

/// Raw TCP sender: frames go out as they are.
pub struct TestTx(pub Wire);

#[derive(Clone, Debug, PartialEq)]
pub struct SealFailed;

impl FrameTx for TestTx {
    type Frame = TestFrame;
    type Sealed = TestFrame;
    type Error = SealFailed;

    async fn alloc(&mut self) -> Result<TestFrame, SealFailed> {
        Ok(TestFrame {
            room: vec![0; FRAME_ROOM],
            len: 0,
        })
    }

    async fn room(&mut self, _: usize) -> Result<(), SealFailed> {
        Ok(())
    }

    fn plain(&self) -> (usize, usize) {
        (0, FRAME_ROOM)
    }

    fn seal(&mut self, mut f: TestFrame, len: usize) -> Result<TestFrame, SealFailed> {
        if self.0.fail_seal.take() {
            return Err(SealFailed);
        }
        f.len = len;
        Ok(f)
    }

    fn try_send(&mut self, f: TestFrame) -> Result<(), TestFrame> {
        if self.0.refuse.get() > 0 {
            self.0.refuse.set(self.0.refuse.get() - 1);
            return Err(f);
        }
        self.0.sent.borrow_mut().extend_from_slice(f.bytes());
        Ok(())
    }

    fn shutdown(&mut self) {
        self.0.shut.set(true);
    }

    async fn delivered(&mut self) {}
}

pub fn configs(
    provider: CryptoProvider,
    max_fragment_size: Option<usize>,
) -> (Arc<ClientConfig>, Arc<ServerConfig>) {
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

    let provider = Arc::new(provider);
    let mut server = ServerConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![cert.clone()], PrivateKeyDer::Pkcs8(key.clone_key()))
        .unwrap();
    server.max_fragment_size = max_fragment_size;
    // lets tests take over the server's sending side
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

pub struct Peer {
    pub tx: TlsTx<TestTx>,
    pub wire: Wire,
    pub server: ServerConnection,
}

pub fn connect(max_fragment_size: Option<usize>) -> Peer {
    connect_with(default_provider(), max_fragment_size)
}

/// Completes the handshake. The server's records sent after it (session tickets) stay queued
/// for the stream under test.
pub fn connect_with(provider: CryptoProvider, max_fragment_size: Option<usize>) -> Peer {
    enum Next {
        Continue,
        Blocked,
        Done,
    }

    let (client_config, server_config) = configs(provider, max_fragment_size);
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
    let wire = Wire::default();
    Peer {
        tx: TlsTx::new(client, TestTx(wire.clone()), None),
        wire,
        server,
    }
}

/// Server-to-client bytes and the application data records among them.
#[derive(Default)]
pub struct Stream {
    pub bytes: Vec<u8>,
    pub records: Vec<Sent>,
}

pub struct Sent {
    pub wire: Range<usize>,
    pub plaintext: Vec<u8>,
}

impl Peer {
    /// Moves the server's queued records (tickets, key updates, alerts) to the stream.
    pub fn flush(&mut self, stream: &mut Stream) {
        while self.server.wants_write() {
            self.server.write_tls(&mut stream.bytes).unwrap();
        }
    }

    pub fn send(&mut self, stream: &mut Stream, msg: &[u8]) {
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

    pub fn close_notify(&mut self, stream: &mut Stream) {
        self.server.send_close_notify();
        self.flush(stream);
    }

    /// Makes the server send a fatal decode_error alert.
    pub fn fatal_alert(&mut self, stream: &mut Stream) {
        self.server.read_tls(&mut &[0xff; 5][..]).unwrap();
        assert!(self.server.process_new_packets().is_err());
        self.flush(stream);
    }

    /// Hands the server what the client sent; returns the plaintext it received.
    pub fn deliver(&mut self) -> Result<Vec<u8>, rustls::Error> {
        let sent = self.wire.sent.take();
        let mut tls = &sent[..];
        while !tls.is_empty() {
            self.server.read_tls(&mut tls).unwrap();
            self.server.process_new_packets()?;
        }
        let mut plaintext = Vec::new();
        match self.server.reader().read_to_end(&mut plaintext) {
            Ok(_) => {}
            Err(e) if e.kind() == ErrorKind::WouldBlock => {}
            Err(e) => panic!("server read: {e}"),
        }
        Ok(plaintext)
    }
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

#[derive(Debug, PartialEq)]
pub struct Got {
    pub plaintext: Vec<u8>,
    pub stamp: u64,
    pub place: Place,
}

/// Where a record's plaintext was found.
#[derive(Debug, PartialEq)]
pub enum Place {
    /// Inside frame `index` at `offset`.
    Frame { index: usize, offset: usize },
    /// Outside every frame (the scratch buffer).
    Elsewhere(usize),
}

pub struct Received {
    pub got: Vec<Got>,
    pub error: Option<Error>,
    pub rx: TlsRx<VecFrame, TestTx>,
}

/// Pushes `frames` (frame `i` stamped `i`) and collects every run, consuming it whole, until
/// the first error.
pub fn receive(tx: &mut TlsTx<TestTx>, frames: Vec<Vec<u8>>) -> Received {
    let ranges: Vec<Range<usize>> = frames
        .iter()
        .map(|f| {
            let r = f.as_ptr_range();
            r.start as usize..r.end as usize
        })
        .collect();
    let mut rx = TlsRx::new();
    let mut got = Vec::new();
    for (index, data) in frames.into_iter().enumerate() {
        rx.push(
            VecFrame {
                data,
                stamp: index as u64,
            });
        loop {
            match rx.chunk(tx) {
                Ok(Some(chunk)) => {
                    let ptr = chunk.bytes.as_ptr() as usize;
                    let place = match ranges.iter().position(|r| r.contains(&ptr)) {
                        Some(index) => Place::Frame {
                            index,
                            offset: ptr - ranges[index].start,
                        },
                        None => Place::Elsewhere(ptr),
                    };
                    let n = chunk.bytes.len();
                    got.push(Got {
                        plaintext: chunk.bytes.to_vec(),
                        stamp: chunk.rx_timestamp,
                        place,
                    });
                    rx.consume(n);
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

pub fn split(bytes: &[u8], cuts: &[usize]) -> Vec<Vec<u8>> {
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
pub fn assert_received(got: &[Got], records: &[Sent], cuts: &[usize]) {
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

/// `f`'s output; panics unless it is ready on its first poll (the test lower layer never waits).
pub fn ready<F: Future>(f: F) -> F::Output {
    match std::pin::pin!(f).poll(&mut std::task::Context::from_waker(std::task::Waker::noop())) {
        std::task::Poll::Ready(out) => out,
        std::task::Poll::Pending => panic!("future pending though the test lower layer never waits"),
    }
}
