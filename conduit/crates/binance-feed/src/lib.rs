//! Client of Binance's bookTicker stream: TLS 1.3 and WebSocket (relay's wss client over
//! sheath) on one switchnet TCP socket, run as a switch task. The Runtime's ticks are
//! milliseconds ([`Clock::ms`]).

use std::cell::RefCell;
use std::fmt;
use std::future::Future;
use std::net::SocketAddrV4;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant as WallInstant};

use relay::switchnet::{Error as WsError, Wss};
use rustls::version::TLS13;
use rustls::{ClientConfig, RootCertStore};
use switch::{oneshot, Runtime, RuntimeError, Spawner};
use switchnet::{
    first, tcp, Instant, IpEndpoint, IpListenEndpoint, Net, NetHandle, OpenError, RoundError,
    StackError,
};

pub const INGRESS_BUDGET: usize = 32;
/// RX and TX buffer bytes of the feed's socket.
pub const SOCKET_BUFFER: usize = 256 * 1024;

/// Wall clock since `start`: Stack time in microseconds (panics past i64 microseconds) and
/// Runtime ticks in milliseconds.
pub struct Clock {
    start: WallInstant,
}

impl Clock {
    pub fn new(start: WallInstant) -> Self {
        Self { start }
    }

    pub fn now(&self) -> Instant {
        let micros = i64::try_from(self.start.elapsed().as_micros())
            .expect("elapsed wall time fits i64 microseconds");
        Instant::from_micros(micros)
    }

    pub fn ms(&self) -> u64 {
        u64::try_from(self.start.elapsed().as_millis())
            .expect("elapsed wall time fits u64 milliseconds")
    }
}

/// `addr` is the resolved IPv4 endpoint, `host` the TLS name and HTTP Host,
/// and `path` the request path.
#[derive(Clone)]
pub struct Target {
    pub addr: SocketAddrV4,
    pub host: String,
    pub path: String,
}

impl Target {
    /// `wss://{host}:{port}{path}` with the port of `addr`.
    pub fn url(&self) -> String {
        format!("wss://{}:{}{}", self.host, self.addr.port(), self.path)
    }
}

/// One bookTicker update; prices and quantities keep Binance's decimal strings.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub struct BookTicker {
    #[serde(rename = "u")]
    pub update_id: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "b")]
    pub bid_price: String,
    #[serde(rename = "B")]
    pub bid_qty: String,
    #[serde(rename = "a")]
    pub ask_price: String,
    #[serde(rename = "A")]
    pub ask_qty: String,
}

impl fmt::Display for BookTicker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "u={} {} bid {} x {} ask {} x {}",
            self.update_id, self.symbol, self.bid_price, self.bid_qty, self.ask_price, self.ask_qty,
        )
    }
}

/// rustls with ring, TLS 1.3 only (sheath's protocol), `roots` and no client auth.
pub fn client_config(roots: RootCertStore) -> Arc<ClientConfig> {
    Arc::new(
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_protocol_versions(&[&TLS13])
            .expect("ring supports TLS 1.3")
            .with_root_certificates(roots)
            .with_no_client_auth(),
    )
}

/// `client_config` with the Mozilla roots from webpki-roots.
pub fn webpki_client_config() -> Arc<ClientConfig> {
    client_config(RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    })
}

/// Configures feed RX and TX buffers and an optional MSS cap.
pub fn socket(mss_cap: Option<u16>) -> tcp::Config {
    tcp::Config {
        rx_capacity: SOCKET_BUFFER,
        tx_capacity: SOCKET_BUFFER,
        mss_cap,
    }
}

#[derive(Debug)]
pub enum FeedError {
    /// The Stack refused the socket (no free slot, pools too small) or the connect.
    Open(OpenError),
    /// TCP closed before it was established.
    Closed {
        addr: SocketAddrV4,
        reset_by_peer: bool,
    },
    /// TLS, the WebSocket upgrade or protocol, or TCP under them.
    Ws(WsError),
    /// `step` did not finish within `limit`.
    Timeout {
        step: &'static str,
        limit: Duration,
    },
    Parse {
        text: String,
        error: serde_json::Error,
    },
    /// A binary message; bookTicker updates are text.
    Binary,
    /// The server closed the WebSocket with this status and reason (its close was echoed).
    ServerClosed { code: u16, reason: String },
    Stack(StackError),
    Runtime(RuntimeError),
}

impl fmt::Display for FeedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Open(error) => write!(f, "opening the TCP socket: {error:?}"),
            Self::Closed {
                addr,
                reset_by_peer: true,
            } => {
                write!(
                    f,
                    "TCP connect to {addr}: reset by the peer (RST answering the SYN)"
                )
            }
            Self::Closed {
                addr,
                reset_by_peer: false,
            } => {
                write!(f, "TCP connect to {addr}: closed before it was established")
            }
            Self::Ws(error) => write!(f, "{error}"),
            Self::Timeout { step, limit } => write!(f, "{step} not finished within {limit:?}"),
            Self::Parse { text, error } => write!(f, "not a bookTicker message ({error}): {text}"),
            Self::Binary => write!(f, "unexpected WebSocket binary message"),
            Self::ServerClosed { code, reason } => write!(f, "server closed the WebSocket: {code} {reason}"),
            Self::Stack(error) => write!(f, "Zhida stack: {error}"),
            Self::Runtime(error) => write!(f, "switch runtime: {error:?}"),
        }
    }
}

impl std::error::Error for FeedError {}

/// Runs `client(handle, spawner, stop)` as a switch task on a new Runtime, turning `net` until it
/// finishes, and returns its output. `stop` finishes once `should_stop()`, asked each round,
/// first returns true. The Runtime is shut down before this returns, dropping the task if a
/// turn failed.
pub fn run<T: 'static, F: Future<Output = T> + 'static>(
    net: &mut Net,
    clock: &Clock,
    mut should_stop: impl FnMut() -> bool,
    client: impl FnOnce(NetHandle, Spawner, oneshot::Receiver<()>) -> F,
) -> Result<T, FeedError> {
    let mut rt = Runtime::new(clock.ms());
    let (stop_tx, stop_rx) = oneshot::channel();
    let done = Rc::new(RefCell::new(None));
    let slot = done.clone();
    let future = client(net.handle(), rt.spawner(), stop_rx);
    let task = switchnet::boxed(Box::pin(
        async move { *slot.borrow_mut() = Some(future.await) },
    ));
    rt.spawner()
        .spawn(task)
        .map_err(|error| FeedError::Runtime(error.reason))?;
    let mut stop_tx = Some(stop_tx);
    let result = loop {
        if let Some(out) = done.borrow_mut().take() {
            break Ok(out);
        }
        if let Err(error) = switchnet::round(net, &mut rt, clock.now(), clock.ms()) {
            break Err(match error {
                RoundError::Stack(error) => FeedError::Stack(error),
                RoundError::Runtime(error) => FeedError::Runtime(error),
            });
        }
        if stop_tx.is_some() && should_stop() {
            let _ = stop_tx.take().expect("checked above").send(());
        }
    };
    rt.shutdown();
    result
}

/// `work`, or `Timeout { step, limit }` once `limit` passed; dropping `work` drops what it holds.
async fn within<T>(
    spawner: &Spawner,
    step: &'static str,
    limit: Duration,
    work: impl Future<Output = Result<T, FeedError>>,
) -> Result<T, FeedError> {
    switchnet::within(spawner, limit, work)
        .await
        .unwrap_or(Err(FeedError::Timeout { step, limit }))
}

/// Opens TCP from `local_port`, then TLS and WebSocket, within one `limit`. On any error the
/// socket is dropped (a socket that never got a SYN-ACK closes without sending anything).
pub async fn connect(
    handle: &NetHandle,
    spawner: &Spawner,
    target: &Target,
    socket: tcp::Config,
    tls: Arc<ClientConfig>,
    local_port: u16,
    limit: Duration,
) -> Result<Feed, FeedError> {
    let open = async {
        let remote = IpEndpoint::new((*target.addr.ip()).into(), target.addr.port());
        let local = IpListenEndpoint {
            addr: None,
            port: local_port,
        };
        let stream = handle
            .tcp_connect(socket, remote, local)
            .map_err(FeedError::Open)?
            .await
            .map_err(|closed| FeedError::Closed {
                addr: target.addr,
                reset_by_peer: closed.reset_by_peer,
            })?;
        let wss = relay::switchnet::connect(
            handle,
            stream,
            tls,
            &target.host,
            target.addr.port(),
            &target.path,
        )
        .await
        .map_err(FeedError::Ws)?;
        Ok(Feed(wss))
    };
    within(spawner, "TCP, TLS and WebSocket connect", limit, open).await
}

pub struct Feed(Wss);

impl Feed {
    /// The next update, or `None` once `stop` finishes; messages already received come first.
    /// Server pings are answered inside.
    pub async fn next(&mut self, stop: impl Future) -> Result<Option<BookTicker>, FeedError> {
        first(self.recv(), stop).await.transpose()
    }

    async fn recv(&mut self) -> Result<BookTicker, FeedError> {
        let Some(message) = self.0.recv().await.map_err(FeedError::Ws)? else {
            let (code, reason) = self.0.peer_close().expect("recv returned None after the server's close");
            return Err(FeedError::ServerClosed { code, reason: reason.to_owned() });
        };
        let text = message.text().ok_or(FeedError::Binary)?;
        serde_json::from_str(text).map_err(|error| FeedError::Parse {
            text: text.to_owned(),
            error,
        })
    }

    /// Sends the WebSocket close (1000) and reads until the server's close echo, dropping the
    /// updates before it; relay then sends close_notify and FIN and returns once the server acked
    /// them, so the Net may be dropped right after. All within `limit`.
    pub async fn close(mut self, spawner: &Spawner, limit: Duration) -> Result<(), FeedError> {
        let close = async {
            self.0.close(1000, "").await.map_err(FeedError::Ws)?;
            while self.0.recv().await.map_err(FeedError::Ws)?.is_some() {}
            Ok(())
        };
        within(spawner, "WebSocket close", limit, close).await
    }
}
