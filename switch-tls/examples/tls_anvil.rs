//! TLS-only client for TLS-Anvil's client tests (`make docker-tls-anvil`): switch-tls's TLS 1.3
//! client over switch-net TCP on net_tap, one connection per trigger.
//!
//! usage: tls_anvil <run-dir>
//!
//! Takes 192.0.2.1/24 (TLS-Anvil's address) on the tap netdev's kernel side and listens for
//! triggers on 127.0.0.1:8090: TLS-Anvil's trigger script, in a container sharing this
//! network namespace, opens a TCP connection there before each handshake. Then
//! `<run-dir>/ready` is written. Each trigger starts one connection from 192.0.2.2 to
//! 192.0.2.1:8443, after the previous one ended: the TLS handshake, a little application
//! data, reading until close_notify, then close_notify and FIN. On a TLS error switch-tls sends
//! rustls's alert and FIN itself. Connection outcomes are printed, one line each; they do not fail
//! the run, since TLS-Anvil expects rejections. Runs until `<run-dir>/done` exists, then
//! prints a summary and exits 0; a setup or driver failure panics.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::poll_fn;
use std::io::ErrorKind;
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Poll;
use std::thread;
use std::time::{Duration, Instant};

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms, verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::version::TLS13;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use switch_tls::switch_net::{send_bytes, tls_connect};
use switch::{Runtime, Spawner};
use switch_net::testkit::{Backend, KERNEL_ADDR, ROUND_SLEEP, TapConfig, with_tap_net};
use switch_net::{IpEndpoint, IpListenEndpoint, NetHandle, tcp};

/// TLS-Anvil's address: the kernel side of the tap netdev.
const ANVIL_ADDR: Ipv4Addr = KERNEL_ADDR;
const ANVIL_PORT: u16 = 8443;
const TRIGGER: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8090);
/// Server name sent; TLS-Anvil's certificate is accepted whatever its names (`AnyCert`).
const HOST: &str = "tls-anvil";
const REQUEST: &[u8] = b"GET / HTTP/1.1\r\nHost: tls-anvil\r\n\r\n";
/// Runtime ticks are milliseconds since start; a connection is abandoned after this many.
const CONNECTION_LIMIT_MS: u64 = 10_000;
/// How often the main loop looks for `<run-dir>/done`.
const DONE_CHECK: Duration = Duration::from_millis(100);

/// An ended connection keeps its socket until the TCP close completes (TIME-WAIT, or never if
/// TLS-Anvil leaves it half closed), and TLS-Anvil starts several a second.
const TAP: TapConfig = TapConfig {
    name: "anvil",
    socket: tcp::Config { rx_capacity: 16 * 1024, tx_capacity: 16 * 1024, mss_cap: None },
    max_sockets: 512,
    random_seed: 0,
};

/// Accepts TLS-Anvil's certificate, whatever it is, but checks the handshake signatures with it,
/// so CertificateVerify still has to be right. Test-only: no chain or name validation.
#[derive(Debug)]
struct AnyCert(WebPkiSupportedAlgorithms);

impl ServerCertVerifier for AnyCert {
    fn verify_server_cert(
        &self,
        _: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(message, cert, dss, &self.0)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(message, cert, dss, &self.0)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_schemes()
    }
}

/// A fresh config per connection: TLS-Anvil assumes every ClientHello is the same, and a shared
/// config remembers the last server's key exchange group (and session tickets) and offers it next.
fn client_config() -> Arc<ClientConfig> {
    let provider: CryptoProvider = rustls::crypto::ring::default_provider();
    let verifier = Arc::new(AnyCert(provider.signature_verification_algorithms));
    let config = ClientConfig::builder_with_provider(Arc::new(provider))
        .with_protocol_versions(&[&TLS13])
        .expect("ring supports TLS 1.3")
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    Arc::new(config)
}

// ANCHOR: connection
/// One connection: handshake, request, read until close_notify, then close_notify and FIN.
/// switch-tls sends the alert for a TLS error itself. Returns how the connection ended.
async fn connection(handle: &NetHandle, local_port: u16, config: Arc<ClientConfig>) -> String {
    let remote = IpEndpoint::new(ANVIL_ADDR.into(), ANVIL_PORT);
    let local = IpListenEndpoint { addr: None, port: local_port };
    let stream = match handle.tcp_connect(TAP.socket, remote, local).await {
        Ok(stream) => stream,
        Err(e) => return format!("TCP connect: {e:?}"),
    };
    let mut tls = match tls_connect(handle, stream, config, HOST, 1).await {
        Ok(tls) => tls,
        Err(e) => return format!("handshake: {e}"),
    };
    if let Err(e) = send_bytes(&mut tls.tx, REQUEST).await {
        return format!("send request: {e}");
    }
    loop {
        match tls.ready().await {
            Ok(true) => {
                let n = tls.rx.current().len();
                tls.rx.consume(n);
            }
            Ok(false) => break,
            Err(e) => return format!("receive: {e}"),
        }
    }
    match tls.close().await {
        Ok(()) => "ended by close_notify".to_owned(),
        Err(e) => format!("ended by close_notify; closing: {e}"),
    }
}
// ANCHOR_END: connection

/// `connection`, abandoned after `CONNECTION_LIMIT_MS`; how it ended goes to `ended`.
#[switch::task]
async fn bounded(
    spawner: Spawner,
    handle: NetHandle,
    local_port: u16,
    config: Arc<ClientConfig>,
    ended: Rc<RefCell<Option<String>>>,
) {
    let mut work = pin!(connection(&handle, local_port, config));
    let mut limit = pin!(spawner.sleep(CONNECTION_LIMIT_MS).expect("the runtime is open"));
    let outcome = poll_fn(|cx| {
        if let Poll::Ready(ended) = work.as_mut().poll(cx) {
            return Poll::Ready(ended);
        }
        if limit.as_mut().poll(cx).is_ready() {
            return Poll::Ready("abandoned after the time limit".to_owned());
        }
        Poll::Pending
    });
    *ended.borrow_mut() = Some(outcome.await);
}

fn marker(dir: &Path, name: &str) {
    let tmp = dir.join(format!("{name}.tmp"));
    std::fs::write(&tmp, "").and_then(|()| std::fs::rename(&tmp, dir.join(name))).expect("write marker");
}

fn main() {
    let dir = PathBuf::from(std::env::args_os().nth(1).expect("usage: tls_anvil <run-dir>"));
    let tap = TapConfig { random_seed: u64::from(std::process::id()), ..TAP };
    let summary = with_tap_net(Backend::Conduit, &tap, |net, clock| {
        let triggers = TcpListener::bind(TRIGGER).expect("bind the trigger port");
        triggers.set_nonblocking(true).expect("non-blocking trigger port");
        let mut rt = Runtime::new(0);
        let handle = net.handle();
        marker(&dir, "ready");
        println!("tls_anvil: ready; triggers on {TRIGGER}, connecting to {ANVIL_ADDR}:{ANVIL_PORT}");

        let mut waiting = 0usize;
        let mut started = 0usize;
        // the outcome of the running connection, `Some` once it ended
        let mut running: Option<Rc<RefCell<Option<String>>>> = None;
        let mut outcomes: BTreeMap<String, usize> = BTreeMap::new();
        let mut checked = Instant::now();
        loop {
            loop {
                match triggers.accept() {
                    Ok(_) => waiting += 1,
                    Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                    Err(e) => panic!("trigger accept: {e}"),
                }
            }
            if let Some(ended) = running.as_ref().and_then(|slot| slot.borrow_mut().take()) {
                println!("tls_anvil: connection {started}: {ended}");
                *outcomes.entry(ended).or_default() += 1;
                running = None;
            }
            if running.is_none() && waiting > 0 {
                waiting -= 1;
                started += 1;
                let slot: Rc<RefCell<Option<String>>> = Rc::default();
                running = Some(slot.clone());
                // successive local ports, so no connection meets the previous one's TIME-WAIT
                let local_port = 49152 + u16::try_from(started % 16384).unwrap();
                let task = bounded(rt.spawner(), handle.clone(), local_port, client_config(), slot);
                rt.spawner().spawn(task).expect("spawn a connection");
            }
            net.turn(clock.now()).expect("turn the Net");
            rt.poll(clock.ms()).expect("poll the connections");
            if checked.elapsed() >= DONE_CHECK {
                checked = Instant::now();
                if dir.join("done").exists() {
                    break;
                }
            }
            thread::sleep(ROUND_SLEEP);
        }
        rt.shutdown();
        drop(handle);
        format!("{started} connections, {waiting} triggers left waiting; outcomes {outcomes:#?}")
    });
    println!("tls_anvil: {summary}");
}
