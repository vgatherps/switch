# A complete program

The example `wss_client` is a small market-data client. It connects to Binance's public book-ticker feed over `wss://`, prints a few updates, closes the connection and exits. It is shown here whole, in the order of the file.

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

`WAIT` is the one time limit the program uses, for every step that depends on the server. A book-ticker update is a short line of JSON, so `LIMITS` accepts messages up to 64 KiB where the default would accept 64 MiB.

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

Each phase of the task is under [`within`](crate::guide::timeouts).

Connecting is one block: the TCP connect, then [`connect`](crate::ws::net::connect) for TLS and the WebSocket upgrade. If the limit passes, the block is dropped wherever it was, and the socket it had opened is closed with it.

The message loop puts a limit on every `recv`. A book-ticker feed is never quiet for long, so ten seconds of silence means the connection is dead even though TCP has reported nothing; [TCP has no keep-alive](crate::guide::tcp). Each `message` borrows the connection and is released at the end of the loop body. Server pings are answered inside `recv`.

Closing follows [the WebSocket chapter](crate::guide::websocket): send the close frame, then receive until `None`. The feed keeps sending updates until it has seen the close, and the loop reads past them.

The task's errors are turned into strings so that the three kinds, from TCP, from WebSocket and from the limits, fit one `Result`. The whole body is an inner `async` block so that `?` can be used and the outcome still lands in `done`, where `main` finds it.

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
    // Resolve and load certificates before opening the NIC: both use the kernel.
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

Looking up the server's name and reading the certificate file are done by the kernel and can block. They come first, while nothing depends on the loop going round. switch has no name lookup of its own.

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
    // SAFETY: this thread owns the device and its packets; EAL outlives the port.
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

This is the setup of [the first network program](crate::guide::first_connection): one TCP socket, sized for one connection. The server is on the internet, so the config file must name a `gateway`. `open` finds the gateway's hardware address before it returns.

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

The [loop](crate::guide::drive_loop) reads the clock once per round, in nanoseconds for the runtime and microseconds for the network. It sleeps a millisecond after an idle round, which suits a program that prints to a terminal. A feed handler that cares about every microsecond removes the sleep and gives the loop a CPU of its own.

The loop needs no time limit, because every wait in the task has one and the task always ends. When it has, the program [shuts down](crate::guide::shutdown) in order.

## Running it

On a machine whose card reaches the internet, with a config file that has a gateway:

```sh
cargo build -p switch-examples --features ws --example wss_client
taskset -c 0 target/debug/examples/wss_client /etc/switch/nic.json
```

The other arguments default to the Binance feed, three messages and the system's certificate file. To run it in a container on the tap device, the container has to forward the program's packets to the internet; `examples/README.md` in the repository has the commands. After the report on the card, a run prints:

```text
{"u":103008143177,"s":"BTCUSDT","b":"64604.23000000","B":"2.17781000","a":"64604.24000000","A":"0.16129000"}
{"u":103008143187,"s":"BTCUSDT","b":"64604.23000000","B":"2.17781000","a":"64604.24000000","A":"0.16121000"}
{"u":103008143188,"s":"BTCUSDT","b":"64604.23000000","B":"2.17781000","a":"64604.24000000","A":"0.16129000"}
WebSocket closed
```
