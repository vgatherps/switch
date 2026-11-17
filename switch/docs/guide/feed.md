# A complete program

The example `wss_client` connects to Binance's public book-ticker feed over `wss://`, prints a few updates, closes the connection and exits.

```toml
[package]
name = "feed"
version = "0.1.0"
edition = "2024"

[dependencies]
switch = { path = "<clone>/switch", features = ["ws"] }
rustls = { version = "=0.23.43", default-features = false, features = ["ring", "std"] }

[patch.crates-io]
rustls = { path = "<clone>/vendor/rustls" }
```

## Imports and limits

```rust,no_run
use std::cell::RefCell;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant as WallInstant};

use rustls::pki_types::{CertificateDer, pem::PemObject};
use rustls::{ClientConfig, RootCertStore};
use switch::net::{Instant, IpEndpoint, IpListenEndpoint, Net, NetConfig, NetHandle, tcp, within};
use switch::nic::{Config, DEFAULT_NEIGHBOR_CAPACITY, Sizing, open};
use switch::ws::Limits;
use switch::{Runtime, Spawner};

const WAIT: Duration = Duration::from_secs(10);
const BUFFER: usize = 64 * 1024;
const LIMITS: Limits = Limits {
    max_message: 64 * 1024,
    max_send: 4096,
};

struct Feed {
    host: String,
    path: String,
    messages: usize,
    tls: Arc<ClientConfig>,
}
# #[switch::task]
# async fn read_feed(
#     handle: NetHandle,
#     spawner: Spawner,
#     socket: tcp::Config,
#     server: IpEndpoint,
#     feed: Feed,
#     done: Rc<RefCell<Option<Result<(), String>>>>,
# ) {
#     let result = async {
#         let connecting = async {
#             let local = IpListenEndpoint {
#                 addr: None,
#                 port: 40000,
#             };
#             let stream = handle
#                 .tcp_connect(socket, server, local)
#                 .await
#                 .map_err(|error| error.to_string())?;
#             switch::ws::net::connect(
#                 &handle,
#                 stream,
#                 feed.tls,
#                 &feed.host,
#                 server.port,
#                 &feed.path,
#                 LIMITS,
#             )
#             .await
#             .map_err(|error| error.to_string())
#         };
#         let mut ws = within(&spawner, WAIT, connecting)
#             .await
#             .ok_or("connect timed out")??;
#         for _ in 0..feed.messages {
#             let message = within(&spawner, WAIT, ws.recv())
#                 .await
#                 .ok_or("receive timed out")?
#                 .map_err(|error| error.to_string())?
#                 .ok_or("feed closed before the requested message count")?;
#             println!("{}", message.text().ok_or("feed sent a binary message")?);
#         }
#         let closing = async {
#             ws.close(1000, "done")
#                 .await
#                 .map_err(|error| error.to_string())?;
#             while ws
#                 .recv()
#                 .await
#                 .map_err(|error| error.to_string())?
#                 .is_some()
#             {}
#             Ok::<(), String>(())
#         };
#         within(&spawner, WAIT, closing)
#             .await
#             .ok_or("close timed out")??;
#         println!("WebSocket closed");
#         Ok(())
#     }
#     .await;
#     *done.borrow_mut() = Some(result);
# }
# fn main() -> Result<(), Box<dyn std::error::Error>> {
#     let mut args = std::env::args().skip(1);
#     let config = args
#         .next()
#         .ok_or("usage: wss_client CONFIG [HOST PORT PATH MESSAGES CA_PEM]")?;
#     let host = args.next().unwrap_or_else(|| "stream.binance.com".into());
#     let port = args.next().unwrap_or_else(|| "9443".into()).parse()?;
#     let path = args
#         .next()
#         .unwrap_or_else(|| "/ws/btcusdt@bookTicker".into());
#     let messages = args.next().unwrap_or_else(|| "3".into()).parse()?;
#     let roots = args
#         .next()
#         .unwrap_or_else(|| "/etc/ssl/certs/ca-certificates.crt".into());
#     // Resolve and load certificates before opening the NIC: both use the kernel.
#     let server = (host.as_str(), port)
#         .to_socket_addrs()?
#         .find_map(|address| match address {
#             SocketAddr::V4(address) => Some(address),
#             SocketAddr::V6(_) => None,
#         })
#         .ok_or("host has no IPv4 address")?;
#     let mut root_store = RootCertStore::empty();
#     for cert in CertificateDer::pem_file_iter(roots)? {
#         root_store.add(cert?)?;
#     }
#     let tls = Arc::new(
#         ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
#             .with_root_certificates(root_store)
#             .with_no_client_auth(),
#     );
#     let feed = Feed {
#         host,
#         path,
#         messages,
#         tls,
#     };
#     let config = Config::load(Path::new(&config))?;
#     let socket = tcp::Config {
#         rx_capacity: BUFFER,
#         tx_capacity: BUFFER,
#         mss_cap: Some(config.path_mtu(*server.ip()) - 40),
#     };
#     let sizing = Sizing {
#         frame_share: tcp::tx_mbuf_budget(&socket, usize::from(config.mtu)),
#         max_sockets: 1,
#         max_udp_sockets: 0,
#         rx_staging: 32,
#         tx_pending: 32,
#         neighbor_capacity: DEFAULT_NEIGHBOR_CAPACITY,
#     };
#     let start = WallInstant::now();
#     // SAFETY: this thread owns the device and its packets; EAL outlives the port.
#     let opened = unsafe {
#         open(&config, &sizing, None, &mut || {
#             Instant::from_micros(i64::try_from(start.elapsed().as_micros()).unwrap())
#         })
#     }?;
#     println!("{}", opened.report);
#     let _eal = opened.eal;
#     let mut net = Net::new(opened.stack, NetConfig { ingress_budget: 32 });
#     let mut runtime = Runtime::new(0);
#     let spawner = runtime.spawner();
#     let done = Rc::new(RefCell::new(None));
#     spawner
#         .spawn(read_feed(
#             net.handle(),
#             spawner.clone(),
#             socket,
#             IpEndpoint::new((*server.ip()).into(), port),
#             feed,
#             done.clone(),
#         ))
#         .unwrap();
#     let result = loop {
#         if let Some(result) = done.borrow_mut().take() {
#             break result;
#         }
#         let tick = u64::try_from(start.elapsed().as_nanos())?;
#         let now = Instant::from_micros(i64::try_from(tick / 1_000)?);
#         if !switch::net::round(&mut net, &mut runtime, now, tick)
#             .map_err(|error| format!("round: {error:?}"))?
#         {
#             std::thread::sleep(Duration::from_millis(1));
#         }
#     };
#     runtime.shutdown();
#     let stack = net
#         .into_stack()
#         .map_err(|_| "a task still owns a socket or handle")?;
#     stack
#         .into_port()
#         .try_close()
#         .map_err(|(_, error)| format!("port close: {error}"))?;
#     result.map_err(Into::into)
# }
```

`WAIT` is the time limit for every step that depends on the server. `LIMITS` accepts messages up to 64 KiB where the default would accept 64 MiB.

## The task

```rust,no_run
# use std::cell::RefCell;
# use std::net::{SocketAddr, ToSocketAddrs};
# use std::path::Path;
# use std::rc::Rc;
# use std::sync::Arc;
# use std::time::{Duration, Instant as WallInstant};
# 
# use rustls::pki_types::{CertificateDer, pem::PemObject};
# use rustls::{ClientConfig, RootCertStore};
# use switch::net::{Instant, IpEndpoint, IpListenEndpoint, Net, NetConfig, NetHandle, tcp, within};
# use switch::nic::{Config, DEFAULT_NEIGHBOR_CAPACITY, Sizing, open};
# use switch::ws::Limits;
# use switch::{Runtime, Spawner};
# 
# const WAIT: Duration = Duration::from_secs(10);
# const BUFFER: usize = 64 * 1024;
# const LIMITS: Limits = Limits {
#     max_message: 64 * 1024,
#     max_send: 4096,
# };
# 
# struct Feed {
#     host: String,
#     path: String,
#     messages: usize,
#     tls: Arc<ClientConfig>,
# }
#[switch::task]
async fn read_feed(
    handle: NetHandle,
    spawner: Spawner,
    socket: tcp::Config,
    server: IpEndpoint,
    feed: Feed,
    done: Rc<RefCell<Option<Result<(), String>>>>,
) {
    let result = async {
        let connecting = async {
            let local = IpListenEndpoint {
                addr: None,
                port: 40000,
            };
            let stream = handle
                .tcp_connect(socket, server, local)
                .await
                .map_err(|error| error.to_string())?;
            switch::ws::net::connect(
                &handle,
                stream,
                feed.tls,
                &feed.host,
                server.port,
                &feed.path,
                LIMITS,
            )
            .await
            .map_err(|error| error.to_string())
        };
        let mut ws = within(&spawner, WAIT, connecting)
            .await
            .ok_or("connect timed out")??;
        for _ in 0..feed.messages {
            let message = within(&spawner, WAIT, ws.recv())
                .await
                .ok_or("receive timed out")?
                .map_err(|error| error.to_string())?
                .ok_or("feed closed before the requested message count")?;
            println!("{}", message.text().ok_or("feed sent a binary message")?);
        }
        let closing = async {
            ws.close(1000, "done")
                .await
                .map_err(|error| error.to_string())?;
            while ws
                .recv()
                .await
                .map_err(|error| error.to_string())?
                .is_some()
            {}
            Ok::<(), String>(())
        };
        within(&spawner, WAIT, closing)
            .await
            .ok_or("close timed out")??;
        println!("WebSocket closed");
        Ok(())
    }
    .await;
    *done.borrow_mut() = Some(result);
}
# fn main() -> Result<(), Box<dyn std::error::Error>> {
#     let mut args = std::env::args().skip(1);
#     let config = args
#         .next()
#         .ok_or("usage: wss_client CONFIG [HOST PORT PATH MESSAGES CA_PEM]")?;
#     let host = args.next().unwrap_or_else(|| "stream.binance.com".into());
#     let port = args.next().unwrap_or_else(|| "9443".into()).parse()?;
#     let path = args
#         .next()
#         .unwrap_or_else(|| "/ws/btcusdt@bookTicker".into());
#     let messages = args.next().unwrap_or_else(|| "3".into()).parse()?;
#     let roots = args
#         .next()
#         .unwrap_or_else(|| "/etc/ssl/certs/ca-certificates.crt".into());
#     // Resolve and load certificates before opening the NIC: both use the kernel.
#     let server = (host.as_str(), port)
#         .to_socket_addrs()?
#         .find_map(|address| match address {
#             SocketAddr::V4(address) => Some(address),
#             SocketAddr::V6(_) => None,
#         })
#         .ok_or("host has no IPv4 address")?;
#     let mut root_store = RootCertStore::empty();
#     for cert in CertificateDer::pem_file_iter(roots)? {
#         root_store.add(cert?)?;
#     }
#     let tls = Arc::new(
#         ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
#             .with_root_certificates(root_store)
#             .with_no_client_auth(),
#     );
#     let feed = Feed {
#         host,
#         path,
#         messages,
#         tls,
#     };
#     let config = Config::load(Path::new(&config))?;
#     let socket = tcp::Config {
#         rx_capacity: BUFFER,
#         tx_capacity: BUFFER,
#         mss_cap: Some(config.path_mtu(*server.ip()) - 40),
#     };
#     let sizing = Sizing {
#         frame_share: tcp::tx_mbuf_budget(&socket, usize::from(config.mtu)),
#         max_sockets: 1,
#         max_udp_sockets: 0,
#         rx_staging: 32,
#         tx_pending: 32,
#         neighbor_capacity: DEFAULT_NEIGHBOR_CAPACITY,
#     };
#     let start = WallInstant::now();
#     // SAFETY: this thread owns the device and its packets; EAL outlives the port.
#     let opened = unsafe {
#         open(&config, &sizing, None, &mut || {
#             Instant::from_micros(i64::try_from(start.elapsed().as_micros()).unwrap())
#         })
#     }?;
#     println!("{}", opened.report);
#     let _eal = opened.eal;
#     let mut net = Net::new(opened.stack, NetConfig { ingress_budget: 32 });
#     let mut runtime = Runtime::new(0);
#     let spawner = runtime.spawner();
#     let done = Rc::new(RefCell::new(None));
#     spawner
#         .spawn(read_feed(
#             net.handle(),
#             spawner.clone(),
#             socket,
#             IpEndpoint::new((*server.ip()).into(), port),
#             feed,
#             done.clone(),
#         ))
#         .unwrap();
#     let result = loop {
#         if let Some(result) = done.borrow_mut().take() {
#             break result;
#         }
#         let tick = u64::try_from(start.elapsed().as_nanos())?;
#         let now = Instant::from_micros(i64::try_from(tick / 1_000)?);
#         if !switch::net::round(&mut net, &mut runtime, now, tick)
#             .map_err(|error| format!("round: {error:?}"))?
#         {
#             std::thread::sleep(Duration::from_millis(1));
#         }
#     };
#     runtime.shutdown();
#     let stack = net
#         .into_stack()
#         .map_err(|_| "a task still owns a socket or handle")?;
#     stack
#         .into_port()
#         .try_close()
#         .map_err(|(_, error)| format!("port close: {error}"))?;
#     result.map_err(Into::into)
# }
```

Each phase of the task is under [`within`](crate::guide::timeouts). The limit on each `recv` is needed because [TCP has no keep-alive](crate::guide::tcp), so a feed that had gone quiet would otherwise keep the task waiting.

Closing is as in [WebSocket](crate::guide::websocket). The feed keeps sending updates until it has seen the close, and the loop reads past them.

## Before the card is opened

```rust,no_run
# use std::cell::RefCell;
# use std::net::{SocketAddr, ToSocketAddrs};
# use std::path::Path;
# use std::rc::Rc;
# use std::sync::Arc;
# use std::time::{Duration, Instant as WallInstant};
# 
# use rustls::pki_types::{CertificateDer, pem::PemObject};
# use rustls::{ClientConfig, RootCertStore};
# use switch::net::{Instant, IpEndpoint, IpListenEndpoint, Net, NetConfig, NetHandle, tcp, within};
# use switch::nic::{Config, DEFAULT_NEIGHBOR_CAPACITY, Sizing, open};
# use switch::ws::Limits;
# use switch::{Runtime, Spawner};
# 
# const WAIT: Duration = Duration::from_secs(10);
# const BUFFER: usize = 64 * 1024;
# const LIMITS: Limits = Limits {
#     max_message: 64 * 1024,
#     max_send: 4096,
# };
# 
# struct Feed {
#     host: String,
#     path: String,
#     messages: usize,
#     tls: Arc<ClientConfig>,
# }
# #[switch::task]
# async fn read_feed(
#     handle: NetHandle,
#     spawner: Spawner,
#     socket: tcp::Config,
#     server: IpEndpoint,
#     feed: Feed,
#     done: Rc<RefCell<Option<Result<(), String>>>>,
# ) {
#     let result = async {
#         let connecting = async {
#             let local = IpListenEndpoint {
#                 addr: None,
#                 port: 40000,
#             };
#             let stream = handle
#                 .tcp_connect(socket, server, local)
#                 .await
#                 .map_err(|error| error.to_string())?;
#             switch::ws::net::connect(
#                 &handle,
#                 stream,
#                 feed.tls,
#                 &feed.host,
#                 server.port,
#                 &feed.path,
#                 LIMITS,
#             )
#             .await
#             .map_err(|error| error.to_string())
#         };
#         let mut ws = within(&spawner, WAIT, connecting)
#             .await
#             .ok_or("connect timed out")??;
#         for _ in 0..feed.messages {
#             let message = within(&spawner, WAIT, ws.recv())
#                 .await
#                 .ok_or("receive timed out")?
#                 .map_err(|error| error.to_string())?
#                 .ok_or("feed closed before the requested message count")?;
#             println!("{}", message.text().ok_or("feed sent a binary message")?);
#         }
#         let closing = async {
#             ws.close(1000, "done")
#                 .await
#                 .map_err(|error| error.to_string())?;
#             while ws
#                 .recv()
#                 .await
#                 .map_err(|error| error.to_string())?
#                 .is_some()
#             {}
#             Ok::<(), String>(())
#         };
#         within(&spawner, WAIT, closing)
#             .await
#             .ok_or("close timed out")??;
#         println!("WebSocket closed");
#         Ok(())
#     }
#     .await;
#     *done.borrow_mut() = Some(result);
# }
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let config = args
        .next()
        .ok_or("usage: wss_client CONFIG [HOST PORT PATH MESSAGES CA_PEM]")?;
    let host = args.next().unwrap_or_else(|| "stream.binance.com".into());
    let port = args.next().unwrap_or_else(|| "9443".into()).parse()?;
    let path = args
        .next()
        .unwrap_or_else(|| "/ws/btcusdt@bookTicker".into());
    let messages = args.next().unwrap_or_else(|| "3".into()).parse()?;
    let roots = args
        .next()
        .unwrap_or_else(|| "/etc/ssl/certs/ca-certificates.crt".into());
    // Look up the server and load the certificates before opening the card.
    let server = (host.as_str(), port)
        .to_socket_addrs()?
        .find_map(|address| match address {
            SocketAddr::V4(address) => Some(address),
            SocketAddr::V6(_) => None,
        })
        .ok_or("host has no IPv4 address")?;
    let mut root_store = RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(roots)? {
        root_store.add(cert?)?;
    }
    let tls = Arc::new(
        ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_root_certificates(root_store)
            .with_no_client_auth(),
    );
    let feed = Feed {
        host,
        path,
        messages,
        tls,
    };
#     let config = Config::load(Path::new(&config))?;
#     let socket = tcp::Config {
#         rx_capacity: BUFFER,
#         tx_capacity: BUFFER,
#         mss_cap: Some(config.path_mtu(*server.ip()) - 40),
#     };
#     let sizing = Sizing {
#         frame_share: tcp::tx_mbuf_budget(&socket, usize::from(config.mtu)),
#         max_sockets: 1,
#         max_udp_sockets: 0,
#         rx_staging: 32,
#         tx_pending: 32,
#         neighbor_capacity: DEFAULT_NEIGHBOR_CAPACITY,
#     };
#     let start = WallInstant::now();
#     // SAFETY: this thread owns the device and its packets; EAL outlives the port.
#     let opened = unsafe {
#         open(&config, &sizing, None, &mut || {
#             Instant::from_micros(i64::try_from(start.elapsed().as_micros()).unwrap())
#         })
#     }?;
#     println!("{}", opened.report);
#     let _eal = opened.eal;
#     let mut net = Net::new(opened.stack, NetConfig { ingress_budget: 32 });
#     let mut runtime = Runtime::new(0);
#     let spawner = runtime.spawner();
#     let done = Rc::new(RefCell::new(None));
#     spawner
#         .spawn(read_feed(
#             net.handle(),
#             spawner.clone(),
#             socket,
#             IpEndpoint::new((*server.ip()).into(), port),
#             feed,
#             done.clone(),
#         ))
#         .unwrap();
#     let result = loop {
#         if let Some(result) = done.borrow_mut().take() {
#             break result;
#         }
#         let tick = u64::try_from(start.elapsed().as_nanos())?;
#         let now = Instant::from_micros(i64::try_from(tick / 1_000)?);
#         if !switch::net::round(&mut net, &mut runtime, now, tick)
#             .map_err(|error| format!("round: {error:?}"))?
#         {
#             std::thread::sleep(Duration::from_millis(1));
#         }
#     };
#     runtime.shutdown();
#     let stack = net
#         .into_stack()
#         .map_err(|_| "a task still owns a socket or handle")?;
#     stack
#         .into_port()
#         .try_close()
#         .map_err(|(_, error)| format!("port close: {error}"))?;
#     result.map_err(Into::into)
# }
```

The blocking setup, here DNS resolution and reading the certificate file, comes before the card is opened, as in [Your first network program](crate::guide::first_connection).

## Opening the card

```rust,no_run
# use std::cell::RefCell;
# use std::net::{SocketAddr, ToSocketAddrs};
# use std::path::Path;
# use std::rc::Rc;
# use std::sync::Arc;
# use std::time::{Duration, Instant as WallInstant};
# 
# use rustls::pki_types::{CertificateDer, pem::PemObject};
# use rustls::{ClientConfig, RootCertStore};
# use switch::net::{Instant, IpEndpoint, IpListenEndpoint, Net, NetConfig, NetHandle, tcp, within};
# use switch::nic::{Config, DEFAULT_NEIGHBOR_CAPACITY, Sizing, open};
# use switch::ws::Limits;
# use switch::{Runtime, Spawner};
# 
# const WAIT: Duration = Duration::from_secs(10);
# const BUFFER: usize = 64 * 1024;
# const LIMITS: Limits = Limits {
#     max_message: 64 * 1024,
#     max_send: 4096,
# };
# 
# struct Feed {
#     host: String,
#     path: String,
#     messages: usize,
#     tls: Arc<ClientConfig>,
# }
# #[switch::task]
# async fn read_feed(
#     handle: NetHandle,
#     spawner: Spawner,
#     socket: tcp::Config,
#     server: IpEndpoint,
#     feed: Feed,
#     done: Rc<RefCell<Option<Result<(), String>>>>,
# ) {
#     let result = async {
#         let connecting = async {
#             let local = IpListenEndpoint {
#                 addr: None,
#                 port: 40000,
#             };
#             let stream = handle
#                 .tcp_connect(socket, server, local)
#                 .await
#                 .map_err(|error| error.to_string())?;
#             switch::ws::net::connect(
#                 &handle,
#                 stream,
#                 feed.tls,
#                 &feed.host,
#                 server.port,
#                 &feed.path,
#                 LIMITS,
#             )
#             .await
#             .map_err(|error| error.to_string())
#         };
#         let mut ws = within(&spawner, WAIT, connecting)
#             .await
#             .ok_or("connect timed out")??;
#         for _ in 0..feed.messages {
#             let message = within(&spawner, WAIT, ws.recv())
#                 .await
#                 .ok_or("receive timed out")?
#                 .map_err(|error| error.to_string())?
#                 .ok_or("feed closed before the requested message count")?;
#             println!("{}", message.text().ok_or("feed sent a binary message")?);
#         }
#         let closing = async {
#             ws.close(1000, "done")
#                 .await
#                 .map_err(|error| error.to_string())?;
#             while ws
#                 .recv()
#                 .await
#                 .map_err(|error| error.to_string())?
#                 .is_some()
#             {}
#             Ok::<(), String>(())
#         };
#         within(&spawner, WAIT, closing)
#             .await
#             .ok_or("close timed out")??;
#         println!("WebSocket closed");
#         Ok(())
#     }
#     .await;
#     *done.borrow_mut() = Some(result);
# }
# fn main() -> Result<(), Box<dyn std::error::Error>> {
#     let mut args = std::env::args().skip(1);
#     let config = args
#         .next()
#         .ok_or("usage: wss_client CONFIG [HOST PORT PATH MESSAGES CA_PEM]")?;
#     let host = args.next().unwrap_or_else(|| "stream.binance.com".into());
#     let port = args.next().unwrap_or_else(|| "9443".into()).parse()?;
#     let path = args
#         .next()
#         .unwrap_or_else(|| "/ws/btcusdt@bookTicker".into());
#     let messages = args.next().unwrap_or_else(|| "3".into()).parse()?;
#     let roots = args
#         .next()
#         .unwrap_or_else(|| "/etc/ssl/certs/ca-certificates.crt".into());
#     // Resolve and load certificates before opening the NIC: both use the kernel.
#     let server = (host.as_str(), port)
#         .to_socket_addrs()?
#         .find_map(|address| match address {
#             SocketAddr::V4(address) => Some(address),
#             SocketAddr::V6(_) => None,
#         })
#         .ok_or("host has no IPv4 address")?;
#     let mut root_store = RootCertStore::empty();
#     for cert in CertificateDer::pem_file_iter(roots)? {
#         root_store.add(cert?)?;
#     }
#     let tls = Arc::new(
#         ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
#             .with_root_certificates(root_store)
#             .with_no_client_auth(),
#     );
#     let feed = Feed {
#         host,
#         path,
#         messages,
#         tls,
#     };
    let config = Config::load(Path::new(&config))?;
    let socket = tcp::Config {
        rx_capacity: BUFFER,
        tx_capacity: BUFFER,
        mss_cap: Some(config.path_mtu(*server.ip()) - 40),
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
    // SAFETY: this thread alone uses the card, and `_eal` lives until it is closed.
    let opened = unsafe {
        open(&config, &sizing, None, &mut || {
            Instant::from_micros(i64::try_from(start.elapsed().as_micros()).unwrap())
        })
    }?;
    println!("{}", opened.report);
    let _eal = opened.eal;
    let mut net = Net::new(opened.stack, NetConfig { ingress_budget: 32 });
#     let mut runtime = Runtime::new(0);
#     let spawner = runtime.spawner();
#     let done = Rc::new(RefCell::new(None));
#     spawner
#         .spawn(read_feed(
#             net.handle(),
#             spawner.clone(),
#             socket,
#             IpEndpoint::new((*server.ip()).into(), port),
#             feed,
#             done.clone(),
#         ))
#         .unwrap();
#     let result = loop {
#         if let Some(result) = done.borrow_mut().take() {
#             break result;
#         }
#         let tick = u64::try_from(start.elapsed().as_nanos())?;
#         let now = Instant::from_micros(i64::try_from(tick / 1_000)?);
#         if !switch::net::round(&mut net, &mut runtime, now, tick)
#             .map_err(|error| format!("round: {error:?}"))?
#         {
#             std::thread::sleep(Duration::from_millis(1));
#         }
#     };
#     runtime.shutdown();
#     let stack = net
#         .into_stack()
#         .map_err(|_| "a task still owns a socket or handle")?;
#     stack
#         .into_port()
#         .try_close()
#         .map_err(|(_, error)| format!("port close: {error}"))?;
#     result.map_err(Into::into)
# }
```

This is the setup of [the first network program](crate::guide::first_connection). The server is on the internet, so the config file must name a `gateway`, and `open` fails if the gateway does not answer ARP within three seconds.

## The loop and the end

```rust,no_run
# use std::cell::RefCell;
# use std::net::{SocketAddr, ToSocketAddrs};
# use std::path::Path;
# use std::rc::Rc;
# use std::sync::Arc;
# use std::time::{Duration, Instant as WallInstant};
# 
# use rustls::pki_types::{CertificateDer, pem::PemObject};
# use rustls::{ClientConfig, RootCertStore};
# use switch::net::{Instant, IpEndpoint, IpListenEndpoint, Net, NetConfig, NetHandle, tcp, within};
# use switch::nic::{Config, DEFAULT_NEIGHBOR_CAPACITY, Sizing, open};
# use switch::ws::Limits;
# use switch::{Runtime, Spawner};
# 
# const WAIT: Duration = Duration::from_secs(10);
# const BUFFER: usize = 64 * 1024;
# const LIMITS: Limits = Limits {
#     max_message: 64 * 1024,
#     max_send: 4096,
# };
# 
# struct Feed {
#     host: String,
#     path: String,
#     messages: usize,
#     tls: Arc<ClientConfig>,
# }
# #[switch::task]
# async fn read_feed(
#     handle: NetHandle,
#     spawner: Spawner,
#     socket: tcp::Config,
#     server: IpEndpoint,
#     feed: Feed,
#     done: Rc<RefCell<Option<Result<(), String>>>>,
# ) {
#     let result = async {
#         let connecting = async {
#             let local = IpListenEndpoint {
#                 addr: None,
#                 port: 40000,
#             };
#             let stream = handle
#                 .tcp_connect(socket, server, local)
#                 .await
#                 .map_err(|error| error.to_string())?;
#             switch::ws::net::connect(
#                 &handle,
#                 stream,
#                 feed.tls,
#                 &feed.host,
#                 server.port,
#                 &feed.path,
#                 LIMITS,
#             )
#             .await
#             .map_err(|error| error.to_string())
#         };
#         let mut ws = within(&spawner, WAIT, connecting)
#             .await
#             .ok_or("connect timed out")??;
#         for _ in 0..feed.messages {
#             let message = within(&spawner, WAIT, ws.recv())
#                 .await
#                 .ok_or("receive timed out")?
#                 .map_err(|error| error.to_string())?
#                 .ok_or("feed closed before the requested message count")?;
#             println!("{}", message.text().ok_or("feed sent a binary message")?);
#         }
#         let closing = async {
#             ws.close(1000, "done")
#                 .await
#                 .map_err(|error| error.to_string())?;
#             while ws
#                 .recv()
#                 .await
#                 .map_err(|error| error.to_string())?
#                 .is_some()
#             {}
#             Ok::<(), String>(())
#         };
#         within(&spawner, WAIT, closing)
#             .await
#             .ok_or("close timed out")??;
#         println!("WebSocket closed");
#         Ok(())
#     }
#     .await;
#     *done.borrow_mut() = Some(result);
# }
# fn main() -> Result<(), Box<dyn std::error::Error>> {
#     let mut args = std::env::args().skip(1);
#     let config = args
#         .next()
#         .ok_or("usage: wss_client CONFIG [HOST PORT PATH MESSAGES CA_PEM]")?;
#     let host = args.next().unwrap_or_else(|| "stream.binance.com".into());
#     let port = args.next().unwrap_or_else(|| "9443".into()).parse()?;
#     let path = args
#         .next()
#         .unwrap_or_else(|| "/ws/btcusdt@bookTicker".into());
#     let messages = args.next().unwrap_or_else(|| "3".into()).parse()?;
#     let roots = args
#         .next()
#         .unwrap_or_else(|| "/etc/ssl/certs/ca-certificates.crt".into());
#     // Resolve and load certificates before opening the NIC: both use the kernel.
#     let server = (host.as_str(), port)
#         .to_socket_addrs()?
#         .find_map(|address| match address {
#             SocketAddr::V4(address) => Some(address),
#             SocketAddr::V6(_) => None,
#         })
#         .ok_or("host has no IPv4 address")?;
#     let mut root_store = RootCertStore::empty();
#     for cert in CertificateDer::pem_file_iter(roots)? {
#         root_store.add(cert?)?;
#     }
#     let tls = Arc::new(
#         ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
#             .with_root_certificates(root_store)
#             .with_no_client_auth(),
#     );
#     let feed = Feed {
#         host,
#         path,
#         messages,
#         tls,
#     };
#     let config = Config::load(Path::new(&config))?;
#     let socket = tcp::Config {
#         rx_capacity: BUFFER,
#         tx_capacity: BUFFER,
#         mss_cap: Some(config.path_mtu(*server.ip()) - 40),
#     };
#     let sizing = Sizing {
#         frame_share: tcp::tx_mbuf_budget(&socket, usize::from(config.mtu)),
#         max_sockets: 1,
#         max_udp_sockets: 0,
#         rx_staging: 32,
#         tx_pending: 32,
#         neighbor_capacity: DEFAULT_NEIGHBOR_CAPACITY,
#     };
#     let start = WallInstant::now();
#     // SAFETY: this thread owns the device and its packets; EAL outlives the port.
#     let opened = unsafe {
#         open(&config, &sizing, None, &mut || {
#             Instant::from_micros(i64::try_from(start.elapsed().as_micros()).unwrap())
#         })
#     }?;
#     println!("{}", opened.report);
#     let _eal = opened.eal;
#     let mut net = Net::new(opened.stack, NetConfig { ingress_budget: 32 });
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let done = Rc::new(RefCell::new(None));
    spawner
        .spawn(read_feed(
            net.handle(),
            spawner.clone(),
            socket,
            IpEndpoint::new((*server.ip()).into(), port),
            feed,
            done.clone(),
        ))
        .unwrap();
    let result = loop {
        if let Some(result) = done.borrow_mut().take() {
            break result;
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
```

The [loop](crate::guide::drive_loop) sleeps a millisecond after an idle round; without the sleep it busy-polls.

The loop needs no time limit, because every wait in the task has one. When the task ends, the program [shuts down](crate::guide::shutdown) in order.

## Running it

On a machine whose card reaches the internet (`docs/operations/host-setup.md` in the repository prepares one and writes its config file):

```sh
cargo build -p switch-examples --features ws --example wss_client
taskset -c 0 target/debug/examples/wss_client /etc/switch/dut.json
```

The other arguments default to the Binance feed, three messages and the system's certificate file. To run it in a container on the tap device, the container has to forward the program's packets to the internet; `examples/README.md` in the repository has the steps. After the report on the card, a run prints:

```text
{"u":103008143177,"s":"BTCUSDT","b":"64604.23000000","B":"2.17781000","a":"64604.24000000","A":"0.16129000"}
{"u":103008143187,"s":"BTCUSDT","b":"64604.23000000","B":"2.17781000","a":"64604.24000000","A":"0.16121000"}
{"u":103008143188,"s":"BTCUSDT","b":"64604.23000000","B":"2.17781000","a":"64604.24000000","A":"0.16129000"}
WebSocket closed
```
