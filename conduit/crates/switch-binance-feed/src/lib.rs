//! Client of Binance's bookTicker stream: TLS 1.3 and WebSocket (switch-ws's wss client over
//! switch-tls) on one switch-net TCP socket, run as a switch task. It is a worked example of the
//! protocol crates and the market-data feed behind `switch-nic feed`.
//!
//! [`feed`] is the whole client as one `#[switch_runtime::task]`: it connects within
//! [`CONNECT_LIMIT`], reads updates until its `stop` oneshot fires, then runs the WebSocket close
//! within [`CLOSE_LIMIT`], printing `{name}: {key}: {value}` lines and storing a [`Summary`]
//! with a latency histogram (wall clock at hand-over minus the message's hardware timestamp).
//! [`connect`] alone opens TCP, TLS and WebSocket under one time limit; [`resolve`],
//! [`client_config`], [`socket`] and [`Clock`] are the setup pieces. Runtime ticks are monotonic
//! nanoseconds ([`Clock::ns`]).
//!
//! The `switch-binance-feed` binary runs the feed on a tap device behind a helper process (the
//! tester) that gives the tap's kernel side its address and NATs the feed's traffic out:
//!
//! ```text
//! switch-binance-feed [--host HOST] [--port PORT] [--path PATH] [--print N] -- <tester> [args...]
//! ```
//!
//! Defaults: host `stream.binance.com`, port 9443, path `/ws/btcusdt@bookTicker`, 10 updates
//! printed; each option at most once. Exit status: 0 the tester exited 0; 1 the tester failed;
//! 2 packet buffers still in use at port close; 3 bad arguments, setup, feed or teardown error;
//! 101 panic. Statuses 2 and 3 take precedence over the tester's. Run it with no arguments for
//! the full usage, or through `make -C conduit docker-binance BINANCE_SECONDS=30`.
//!
//! Book: See the switch book, chapter "Walkthrough: a market-data feed".

use std::borrow::Cow;
use std::cell::Cell;
use std::fmt;
use std::future::Future;
use std::io::{self, Read as _, Write as _};
use std::net::{SocketAddr, SocketAddrV4, ToSocketAddrs};
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant as WallInstant, SystemTime, UNIX_EPOCH};

use switch_ws::switch_net::{Error as WsError, Wss};
use rustls::version::TLS13;
use rustls::{ClientConfig, RootCertStore};
use switch_runtime::{oneshot, Spawner};
use switch_net::{first, tcp, Error, Instant, IpEndpoint, IpListenEndpoint, NetHandle};

pub const INGRESS_BUDGET: usize = 128;
/// The feed's defaults, shared by switch-binance-feed and `switch-nic feed`. The host speaks TLS 1.3;
/// data-stream.binance.vision speaks TLS 1.2 only, which switch-tls does not.
pub const DEFAULT_HOST: &str = "stream.binance.com";
pub const DEFAULT_PORT: u16 = 9443;
pub const DEFAULT_PATH: &str = "/ws/btcusdt@bookTicker";
/// Updates printed.
pub const DEFAULT_PRINT: usize = 10;
/// RX and TX buffer bytes of the feed's socket.
pub const SOCKET_BUFFER: usize = 256 * 1024;
/// The feed's local port is drawn from here to 65535 (IANA dynamic ports).
pub const FIRST_LOCAL_PORT: u16 = 49152;
/// TCP, TLS and WebSocket connect, together.
pub const CONNECT_LIMIT: Duration = Duration::from_secs(20);
/// The WebSocket close handshake, close_notify and FIN.
pub const CLOSE_LIMIT: Duration = Duration::from_secs(10);

/// Monotonic time since `start`: Stack time in microseconds and Runtime ticks in nanoseconds.
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

    pub fn ns(&self) -> u64 {
        u64::try_from(self.start.elapsed().as_nanos())
            .expect("elapsed time fits u64 nanoseconds")
    }

    /// Stack time and Runtime ticks from the same clock sample.
    pub fn sample(&self) -> (Instant, u64) {
        let nanos = self.ns();
        let micros = i64::try_from(nanos / 1_000).expect("elapsed time fits i64 microseconds");
        (Instant::from_micros(micros), nanos)
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

/// `host` resolved by the kernel resolver to its first IPv4 address, at `port`, with `path`.
pub fn resolve(host: &str, port: u16, path: &str) -> Result<Target, String> {
    let addresses = (host, port)
        .to_socket_addrs()
        .map_err(|error| format!("DNS: resolving {host}: {error}"))?;
    let addr = addresses
        .into_iter()
        .find_map(|addr| match addr {
            SocketAddr::V4(addr) => Some(addr),
            SocketAddr::V6(_) => None,
        })
        .ok_or_else(|| format!("DNS: {host} has no IPv4 address"))?;
    Ok(Target {
        addr,
        host: host.to_owned(),
        path: path.to_owned(),
    })
}

/// A local port from FIRST_LOCAL_PORT to 65535, from OS entropy, so a run does not reuse the
/// previous run's 4-tuple (Docker Desktop's network once answered a repeated SYN with segments
/// of the earlier connection; the Stack's ISNs are now seeded per run, so this is a precaution).
pub fn random_local_port() -> std::io::Result<u16> {
    let mut bytes = [0; 2];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    let span = u16::MAX - FIRST_LOCAL_PORT + 1;
    Ok(FIRST_LOCAL_PORT + u16::from_ne_bytes(bytes) % span)
}

/// One bookTicker update; prices and quantities keep Binance's decimal strings. The strings
/// borrow the message text; one with a JSON escape (which Binance does not send) is unescaped
/// into an owned copy.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub struct BookTicker<'a> {
    #[serde(rename = "u")]
    pub update_id: u64,
    #[serde(rename = "s", borrow)]
    pub symbol: Cow<'a, str>,
    #[serde(rename = "b", borrow)]
    pub bid_price: Cow<'a, str>,
    #[serde(rename = "B", borrow)]
    pub bid_qty: Cow<'a, str>,
    #[serde(rename = "a", borrow)]
    pub ask_price: Cow<'a, str>,
    #[serde(rename = "A", borrow)]
    pub ask_qty: Cow<'a, str>,
}

impl fmt::Display for BookTicker<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "u={} {} bid {} x {} ask {} x {}",
            self.update_id, self.symbol, self.bid_price, self.bid_qty, self.ask_price, self.ask_qty,
        )
    }
}

// ANCHOR: client_config
/// rustls with ring, TLS 1.3 only (switch-tls's protocol), `roots` and no client auth.
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
// ANCHOR_END: client_config

// ANCHOR: socket
/// Configures feed RX and TX buffers and an optional MSS cap.
pub fn socket(mss_cap: Option<u16>) -> tcp::Config {
    tcp::Config {
        rx_capacity: SOCKET_BUFFER,
        tx_capacity: SOCKET_BUFFER,
        mss_cap,
    }
}
// ANCHOR_END: socket

#[derive(Debug)]
pub enum FeedError {
    /// The Stack refused the socket (no free slot, pools too small) or the connect.
    Open(Error),
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
}

fn connect_error(addr: SocketAddrV4, error: Error) -> FeedError {
    match error {
        Error::Refused | Error::Closed => FeedError::Closed {
            addr,
            reset_by_peer: error == Error::Refused,
        },
        error => FeedError::Open(error),
    }
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
        }
    }
}

impl std::error::Error for FeedError {}

// ANCHOR: within
// ANCHOR: within_step
/// `work`, or `Timeout { step, limit }` once `limit` passed; dropping `work` drops what it holds.
async fn within<T>(
    spawner: &Spawner,
    step: &'static str,
    limit: Duration,
    work: impl Future<Output = Result<T, FeedError>>,
) -> Result<T, FeedError> {
    switch_net::within(spawner, limit, work)
        .await
        .unwrap_or(Err(FeedError::Timeout { step, limit }))
}
// ANCHOR_END: within_step
// ANCHOR_END: within

// ANCHOR: connect
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
) -> Result<Wss, FeedError> {
    let open = async {
        let remote = IpEndpoint::new((*target.addr.ip()).into(), target.addr.port());
        let local = IpListenEndpoint {
            addr: None,
            port: local_port,
        };
        let stream = handle
            .tcp_connect(socket, remote, local)
            .await
            .map_err(|error| connect_error(target.addr, error))?;
        let wss = switch_ws::switch_net::connect(
            handle,
            stream,
            tls,
            &target.host,
            target.addr.port(),
            &target.path,
            switch_ws::Limits::default(),
        )
        .await
        .map_err(FeedError::Ws)?;
        Ok(wss)
    };
    within(spawner, "TCP, TLS and WebSocket connect", limit, open).await
}
// ANCHOR_END: connect

/// What a feed saw: the updates read, their first and last update ids, and the latency of every
/// message received.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Summary {
    pub count: u64,
    pub update_ids: Option<(u64, u64)>,
    pub latencies: Latencies,
}

/// CLOCK_REALTIME in ns since the Unix epoch (`clock_gettime`, the vDSO on Linux).
pub fn realtime_ns() -> u64 {
    let since = SystemTime::now().duration_since(UNIX_EPOCH).expect("the clock is after 1970");
    u64::try_from(since.as_nanos()).expect("realtime fits u64 ns")
}

/// Per message: CLOCK_REALTIME when switch-ws handed the complete message over, minus the retained
/// RX-buffer stamp (NIC PHC, ns since the epoch) of the TLS record holding its first WebSocket
/// byte. The host's chrony disciplines CLOCK_REALTIME to that PHC (deploy/hft-host.sh).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Latencies {
    /// Messages without a NIC stamp.
    pub no_stamp: u64,
    /// Samples by ns: bucket 0 holds ns <= 0 (a stamp at or after now), bucket i holds
    /// [2^(i-1), 2^i), bucket 31 holds ns >= 2^30 (about 1.07 s).
    pub buckets: [u64; 32],
}

impl Latencies {
    fn record(&mut self, stamp: u64, now_ns: u64) {
        if stamp == 0 {
            self.no_stamp += 1;
            return;
        }
        let bits = 64 - now_ns.saturating_sub(stamp).leading_zeros();
        self.buckets[bits.min(31) as usize] += 1;
    }
}

/// `no_stamp=N`, then `<=0:N`, `<lo>..<hi>:N` and `1073741824..:N` (ns) for each nonempty bucket.
impl fmt::Display for Latencies {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "no_stamp={}", self.no_stamp)?;
        for (i, &count) in self.buckets.iter().enumerate().filter(|(_, &count)| count > 0) {
            match i {
                0 => write!(f, " <=0:{count}")?,
                31 => write!(f, " {}..:{count}", 1u64 << 30)?,
                _ => write!(f, " {}..{}:{count}", 1u64 << (i - 1), 1u64 << i)?,
            }
        }
        Ok(())
    }
}

/// Where [`feed`] puts what it read and its result.
pub type Outcome = Rc<Cell<Option<(Summary, Result<(), FeedError>)>>>;

// ANCHOR: feed
/// The feed, as a switch task: connects within CONNECT_LIMIT, reads updates until `stop`
/// finishes, `read_for` passed since the connect, or a read fails, then attempts the close within
/// CLOSE_LIMIT, after a failed read too. Prints `{name}: {key}: {value}` lines: feed_connected,
/// feed_message for the first `print` updates, feed_count, feed_latency, feed_update_ids, then
/// feed_close or feed_close_error; feed_stopped if `stop` finished before the connect did. Puts
/// what it read and the read error, else the connect or close error, into `out`.
/// `clock_reset` is called once, immediately before reporting latency, to synchronously detect
/// a device reset during the measurement. A reset suppresses the histogram.
#[allow(clippy::too_many_arguments)]
#[switch_runtime::task]
pub async fn feed(
    handle: NetHandle,
    spawner: Spawner,
    target: Target,
    socket: tcp::Config,
    tls: Arc<ClientConfig>,
    local_port: u16,
    name: &'static str,
    print: usize,
    read_for: Option<Duration>,
    mut stop: oneshot::Receiver<()>,
    out: Outcome,
    clock_reset: impl FnOnce() -> bool + 'static,
) {
    let mut summary = Summary::default();
    let connecting = connect(&handle, &spawner, &target, socket, tls, local_port, CONNECT_LIMIT);
    let result = match first(connecting, &mut stop).await {
        None => {
            line(name, "feed_stopped", "before the feed connected");
            Ok(())
        }
        Some(Err(error)) => Err(error),
        Some(Ok(mut wss)) => {
            let url = target.url();
            line(name, "feed_connected", format_args!("{url} local_port={local_port} TLS 1.3"));
            let reading = read(&mut wss, &mut stop, &mut summary, name, print);
            let read = match read_for {
                Some(limit) => switch_net::within(&spawner, limit, reading).await.unwrap_or(Ok(())),
                None => reading.await,
            };
            line(name, "feed_count", summary.count);
            let _ = write_latency(&mut io::stdout(), name, summary.latencies, clock_reset);
            match summary.update_ids {
                Some((first, last)) => line(name, "feed_update_ids", format_args!("{first}..{last}")),
                None => line(name, "feed_update_ids", "none"),
            }
            let closed = close(wss, &spawner).await;
            match (&read, &closed) {
                (_, Ok(())) => line(name, "feed_close", "WebSocket close handshake, TLS close_notify, FIN"),
                (Err(_), Err(error)) => line(name, "feed_close_error", error),
                (Ok(()), Err(_)) => {}
            }
            read.and(closed)
        }
    };
    out.set(Some((summary, result)));
}
// ANCHOR_END: feed

fn line(name: &str, key: &str, value: impl fmt::Display) {
    let _ = writeln!(io::stdout(), "{name}: {key}: {value}");
}

fn write_latency(
    out: &mut impl io::Write,
    name: &str,
    latencies: Latencies,
    clock_reset: impl FnOnce() -> bool,
) -> io::Result<()> {
    if clock_reset() {
        writeln!(out, "{name}: feed_latency: latency invalid: NIC clock jumped")
    } else {
        writeln!(out, "{name}: feed_latency: {latencies}")
    }
}

// ANCHOR: read
// ANCHOR: read_loop
/// Reads updates into `summary` until `stop` finishes, printing the first `print`; messages
/// already received come first. Server pings are answered inside `recv`.
async fn read(
    wss: &mut Wss,
    stop: &mut oneshot::Receiver<()>,
    summary: &mut Summary,
    name: &str,
    print: usize,
) -> Result<(), FeedError> {
    loop {
        let Some(received) = first(wss.recv(), &mut *stop).await else {
            return Ok(());
        };
        let Some(message) = received.map_err(FeedError::Ws)? else {
            let (code, reason) = wss.peer_close().expect("recv returned None after the server's close");
            return Err(FeedError::ServerClosed { code, reason: reason.to_owned() });
        };
        summary.latencies.record(message.rx_timestamp(), realtime_ns());
        let text = message.text().ok_or(FeedError::Binary)?;
        let ticker: BookTicker = serde_json::from_str(text).map_err(|error| FeedError::Parse {
            text: text.to_owned(),
            error,
        })?;
        summary.count += 1;
        let first = summary.update_ids.map_or(ticker.update_id, |(first, _)| first);
        summary.update_ids = Some((first, ticker.update_id));
        if summary.count <= print as u64 {
            line(name, "feed_message", format_args!("#{} {ticker}", summary.count));
        }
    }
}
// ANCHOR_END: read_loop
// ANCHOR_END: read

// ANCHOR: close
/// Sends the WebSocket close (1000) and reads until the server's close echo, dropping the updates
/// before it; switch-ws then sends close_notify and FIN and returns once the server acked them, so the
/// Net may be dropped right after. All within CLOSE_LIMIT.
async fn close(mut wss: Wss, spawner: &Spawner) -> Result<(), FeedError> {
    let close = async {
        wss.close(1000, "").await.map_err(FeedError::Ws)?;
        while wss.recv().await.map_err(FeedError::Ws)?.is_some() {}
        Ok(())
    };
    within(spawner, "WebSocket close", CLOSE_LIMIT, close).await
}
// ANCHOR_END: close

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flat_connect_errors_preserve_failure_meaning() {
        let addr = SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, 443);
        for (error, reset) in [(Error::Refused, true), (Error::Closed, false)] {
            assert!(matches!(
                connect_error(addr, error),
                FeedError::Closed { addr: actual, reset_by_peer }
                    if actual == addr && reset_by_peer == reset
            ));
        }
        for error in [Error::SocketsFull, Error::PortInUse, Error::InvalidConfig, Error::Unaddressable] {
            assert!(matches!(connect_error(addr, error), FeedError::Open(actual) if actual == error));
        }
        assert!(connect_error(addr, Error::Refused).to_string().contains("reset by the peer"));
        assert!(connect_error(addr, Error::Closed).to_string().contains("closed before it was established"));
    }

    #[test]
    fn clock_sample_uses_one_nanosecond_clock_for_both_times() {
        let start = WallInstant::now() - Duration::from_millis(123);
        let clock = Clock::new(start);
        let before = start.elapsed().as_nanos();
        let (stack, ticks) = clock.sample();
        let after = start.elapsed().as_nanos();
        assert!((before..=after).contains(&u128::from(ticks)));
        assert_eq!(stack, Instant::from_micros(i64::try_from(ticks / 1_000).unwrap()));
    }

    #[test]
    fn resolve_takes_the_first_ipv4_address() {
        let target = resolve("127.0.0.1", 9443, "/ws/x").unwrap();
        assert_eq!(target.url(), "wss://127.0.0.1:9443/ws/x");
        assert_eq!(target.addr, "127.0.0.1:9443".parse().unwrap());
        assert_eq!(resolve("::1", 9443, "/").err().unwrap(), "DNS: ::1 has no IPv4 address");
        assert!(resolve("no-such-host.invalid", 9443, "/")
            .err()
            .unwrap()
            .starts_with("DNS: resolving no-such-host.invalid: "));
    }

    /// A sample is now minus the stamp; a stamp at or after now lands in `<=0`.
    #[test]
    fn latencies_bucket_realtime_minus_stamp() {
        const STAMP: u64 = 1_700_000_000_000_000_000;
        let mut latencies = Latencies::default();
        latencies.record(0, STAMP);
        latencies.record(STAMP, STAMP + 10_000);
        latencies.record(STAMP, STAMP + 16_384);
        latencies.record(STAMP, STAMP - 5);
        latencies.record(STAMP, STAMP + 2_000_000_000);
        assert_eq!(latencies.to_string(), "no_stamp=1 <=0:1 8192..16384:1 16384..32768:1 1073741824..:1");
    }

    #[test]
    fn local_tap_without_hardware_timestamps_keeps_no_stamp_report() {
        let mut latencies = Latencies::default();
        latencies.record(0, 1);
        latencies.record(0, 1);
        let mut output = Vec::new();
        write_latency(&mut output, "test", latencies, || false).unwrap();
        assert_eq!(
            String::from_utf8(output).unwrap(),
            "test: feed_latency: no_stamp=2\n"
        );
    }
}
