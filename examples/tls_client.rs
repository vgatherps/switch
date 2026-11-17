//! Send a line over TLS 1.3 and print the reversed reply from OpenSSL.
//!
//! ```text
//! cargo build -p switch-examples --features tls --example tls_client
//! openssl req -x509 -newkey rsa:2048 -nodes -keyout /tmp/switch-key.pem -out /tmp/switch-cert.pem -days 1 -subj /CN=localhost -addext subjectAltName=DNS:localhost -addext basicConstraints=critical,CA:FALSE
//! openssl s_server -accept 7000 -cert /tmp/switch-cert.pem -key /tmp/switch-key.pem -tls1_3 -rev -quiet &
//! taskset -c 0 target/debug/examples/tls_client examples/tap.json localhost 7000 /tmp/switch-cert.pem 192.0.2.1
//! ```
//!
//! `localhost` names the certificate; `192.0.2.1` is the peer's IPv4 address.
//! Start the client in the background, then assign `192.0.2.1/24` to `cdnic0`
//! and bring it up. The kernel-side peer is `openssl s_server -rev`.

use std::cell::RefCell;
use std::net::{Ipv4Addr, SocketAddr, ToSocketAddrs};
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant as WallInstant};

use switch::tls::rustls::pki_types::{CertificateDer, pem::PemObject};
use switch::tls::rustls::{self, ClientConfig, RootCertStore};
use switch::Runtime;
use switch::net::{Instant, IpEndpoint, IpListenEndpoint, Net, NetConfig, NetHandle, tcp};
use switch::nic::{Config, DEFAULT_NEIGHBOR_CAPACITY, Sizing, open};
use switch::tls::net::{send_bytes, tls_connect};

#[switch::task]
async fn request(
    handle: NetHandle,
    server: IpEndpoint,
    socket: tcp::Config,
    tls: Arc<ClientConfig>,
    host: String,
    done: Rc<RefCell<Option<Result<(), String>>>>,
) {
    let result = async {
        let stream = handle
            .tcp_connect(
                socket,
                server,
                IpListenEndpoint {
                    addr: None,
                    port: 40000,
                },
            )
            .await
            .map_err(|error| error.to_string())?;
        let mut tls = tls_connect(&handle, stream, tls, &host, 6)
            .await
            .map_err(|error| error.to_string())?;
        send_bytes(&mut tls.tx, b"hello\n")
            .await
            .map_err(|error| error.to_string())?;
        while tls.ready().await.map_err(|error| error.to_string())? {
            let bytes = tls.rx.current();
            print!("{}", String::from_utf8_lossy(bytes));
            let end = bytes.contains(&b'\n');
            let len = bytes.len();
            tls.rx.consume(len);
            if end {
                break;
            }
        }
        tls.close().await.map_err(|error| error.to_string())?;
        println!("TLS closed");
        Ok(())
    }
    .await;
    *done.borrow_mut() = Some(result);
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let (Some(config), Some(host), Some(port), Some(roots)) =
        (args.next(), args.next(), args.next(), args.next())
    else {
        return Err("usage: tls_client CONFIG HOST PORT CA_PEM [IPV4]".into());
    };
    let port = port.parse()?;
    let address = match args.next() {
        Some(ip) => ip.parse::<Ipv4Addr>()?,
        None => (host.as_str(), port)
            .to_socket_addrs()?
            .find_map(|addr| match addr {
                SocketAddr::V4(addr) => Some(*addr.ip()),
                SocketAddr::V6(_) => None,
            })
            .ok_or("host has no IPv4 address")?,
    };
    let mut root_store = RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(roots)? {
        root_store.add(cert?)?;
    }
    let tls = Arc::new(
        ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_root_certificates(root_store)
            .with_no_client_auth(),
    );
    let config = Config::load(Path::new(&config))?;
    let socket = tcp::Config {
        rx_capacity: 64 * 1024,
        tx_capacity: 64 * 1024,
        mss_cap: Some(config.path_mtu(address) - 40),
    };
    let sizing = Sizing {
        frame_share: tcp::tx_mbuf_budget(&socket, usize::from(config.mtu)),
        max_sockets: 1,
        max_udp_sockets: 0,
        rx_staging: 32,
        tx_pending: 32,
        neighbor_capacity: DEFAULT_NEIGHBOR_CAPACITY,
    };
    let start = WallInstant::now();
    // SAFETY: this thread owns the device and its packets; EAL outlives the port.
    let opened = unsafe {
        open(&config, &sizing, None, &mut || {
            Instant::from_micros(i64::try_from(start.elapsed().as_micros()).unwrap())
        })
    }?;
    println!("{}", opened.report);
    let _eal = opened.eal;
    let mut net = Net::new(opened.stack, NetConfig { ingress_budget: 32 });
    let mut runtime = Runtime::new(0);
    let done = Rc::new(RefCell::new(None));
    runtime
        .spawner()
        .spawn(request(
            net.handle(),
            IpEndpoint::new(address.into(), port),
            socket,
            tls,
            host,
            done.clone(),
        ))
        .unwrap();
    let result = loop {
        if let Some(result) = done.borrow_mut().take() {
            break result;
        }
        if start.elapsed() > Duration::from_secs(30) {
            break Err("request timed out".into());
        }
        let tick = u64::try_from(start.elapsed().as_nanos())?;
        let now = Instant::from_micros(i64::try_from(tick / 1_000)?);
        if !switch::net::round(&mut net, &mut runtime, now, tick)
            .map_err(|error| format!("round: {error:?}"))?
        {
            std::thread::sleep(Duration::from_millis(1));
        }
    };
    runtime.shutdown();
    let stack = net
        .into_stack()
        .map_err(|_| "a task still owns a socket or handle")?;
    stack
        .into_port()
        .try_close()
        .map_err(|(_, error)| format!("port close: {error}"))?;
    result.map_err(Into::into)
}
