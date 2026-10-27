use std::collections::HashMap;
use std::env;
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use bench_core::{
    DEFAULT_MAX_PAYLOAD_BYTES, DEFAULT_PAYLOAD_BYTES, FrameDecoder, FrameHeader, HEADER_LEN,
    SenderSummary, XorShift64, encode_frame_parts, monotonic_ns, percentile, poisson_gap_ns,
};
use mio::net::TcpStream;
use mio::{Events, Interest, Poll, Registry, Token};

const READ_BUF_LEN: usize = 64 * 1024;
const MAX_DUE_SENDS_PER_TICK: usize = 4096;
const DRAIN_AFTER_SEND_SECS: u64 = 1;
const DEFAULT_MAX_PENDING_BYTES: usize = 64 * 1024 * 1024;

struct Config {
    target: SocketAddr,
    connections: usize,
    rate_total: f64,
    duration_secs: u64,
    payload_bytes: usize,
    max_pending_bytes: usize,
    seed: u64,
}

struct Conn {
    socket: TcpStream,
    decoder: FrameDecoder,
    write_buf: Vec<u8>,
    write_pos: usize,
    stream_id: u32,
    next_seq: u64,
    next_send_at_ns: u64,
    alive: bool,
    connected: bool,
    wants_write: bool,
    in_flight: HashMap<u64, u64>,
    rng: XorShift64,
}

impl Conn {
    fn new(socket: TcpStream, stream_id: u32, seed: u64) -> Self {
        Self {
            socket,
            decoder: FrameDecoder::new(DEFAULT_MAX_PAYLOAD_BYTES),
            write_buf: Vec::new(),
            write_pos: 0,
            stream_id,
            next_seq: 0,
            next_send_at_ns: 0,
            alive: true,
            connected: false,
            wants_write: true,
            in_flight: HashMap::new(),
            rng: XorShift64::new(seed),
        }
    }

    fn sync_interest(&mut self, registry: &Registry, token: Token) -> io::Result<()> {
        if !self.alive {
            return Ok(());
        }

        let wants_write = !self.connected || self.write_pos < self.write_buf.len();
        if wants_write == self.wants_write {
            return Ok(());
        }

        let interest = if wants_write {
            Interest::READABLE | Interest::WRITABLE
        } else {
            Interest::READABLE
        };
        registry.reregister(&mut self.socket, token, interest)?;
        self.wants_write = wants_write;
        Ok(())
    }

    fn mark_connected(&mut self, rate_per_conn: f64, now_ns: u64) {
        self.connected = true;
        self.next_send_at_ns = now_ns + poisson_gap_ns(rate_per_conn, &mut self.rng);
    }

    fn queue_due(
        &mut self,
        now_ns: u64,
        end_ns: u64,
        rate_per_conn: f64,
        payload: &[u8],
        max_pending_bytes: usize,
        sent: &mut u64,
    ) -> io::Result<usize> {
        if !self.connected {
            return Ok(0);
        }

        let mut queued = 0_usize;
        while self.next_send_at_ns <= now_ns
            && self.next_send_at_ns < end_ns
            && queued < MAX_DUE_SENDS_PER_TICK
        {
            let frame_len = HEADER_LEN + payload.len();
            if self
                .pending_frame_bytes(frame_len)
                .saturating_add(frame_len)
                > max_pending_bytes
            {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "sender max pending bytes exceeded",
                ));
            }
            let send_time_ns = monotonic_ns();
            let header = FrameHeader::new(
                self.next_seq,
                send_time_ns,
                payload.len() as u32,
                self.stream_id,
            );
            encode_frame_parts(header, payload, &mut self.write_buf).map_err(io::Error::other)?;
            self.in_flight.insert(self.next_seq, send_time_ns);
            self.next_seq += 1;
            *sent += 1;
            queued += 1;
            self.next_send_at_ns += poisson_gap_ns(rate_per_conn, &mut self.rng);
        }

        Ok(queued)
    }

    fn pending_frame_bytes(&self, frame_len: usize) -> usize {
        self.in_flight.len().saturating_mul(frame_len)
    }

    fn flush(&mut self, summary: &mut SenderSummary) -> io::Result<bool> {
        while self.write_pos < self.write_buf.len() {
            match self.socket.write(&self.write_buf[self.write_pos..]) {
                Ok(0) => return Ok(false),
                Ok(n) => {
                    self.write_pos += n;
                    summary.write_calls += 1;
                    summary.write_bytes += n as u64;
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                Err(err) => return Err(err),
            }
        }

        if self.write_pos == self.write_buf.len() {
            self.write_buf.clear();
            self.write_pos = 0;
        } else if self.write_pos > 64 * 1024 {
            self.write_buf.drain(..self.write_pos);
            self.write_pos = 0;
        }

        Ok(true)
    }
}

fn main() -> io::Result<()> {
    let config = parse_config()?;
    if config.connections == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--connections must be > 0",
        ));
    }
    if !(config.rate_total.is_finite() && config.rate_total > 0.0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--rate-total must be finite and > 0",
        ));
    }
    if config.payload_bytes > DEFAULT_MAX_PAYLOAD_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("--payload-bytes must be <= {}", DEFAULT_MAX_PAYLOAD_BYTES),
        ));
    }
    if config.max_pending_bytes < HEADER_LEN + config.payload_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--max-pending-bytes must fit at least one frame",
        ));
    }
    if config.seed == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--seed must be > 0",
        ));
    }

    let mut poll = Poll::new()?;
    let mut conns = Vec::with_capacity(config.connections);
    for idx in 0..config.connections {
        let mut socket = TcpStream::connect(config.target)?;
        socket.set_nodelay(true)?;
        poll.registry().register(
            &mut socket,
            Token(idx + 1),
            Interest::READABLE | Interest::WRITABLE,
        )?;
        conns.push(Conn::new(
            socket,
            idx as u32,
            stream_seed(config.seed, idx as u64),
        ));
    }

    let payload = vec![0_u8; config.payload_bytes];
    let rate_per_conn = config.rate_total / config.connections as f64;
    let started = Instant::now();
    let start_ns = monotonic_ns();
    let end_ns = start_ns + Duration::from_secs(config.duration_secs).as_nanos() as u64;

    let mut summary = SenderSummary {
        target: config.target.to_string(),
        connections: config.connections,
        payload_bytes: config.payload_bytes,
        max_pending_bytes: config.max_pending_bytes,
        offered_rate_total: config.rate_total,
        elapsed_secs: 0.0,
        active_elapsed_secs: 0.0,
        total_elapsed_secs: 0.0,
        expected_sent: 0.0,
        offered_ratio: 0.0,
        sent: 0,
        received: 0,
        lost: 0,
        achieved_rate: 0.0,
        read_calls: 0,
        write_calls: 0,
        write_bytes: 0,
        readiness_events: 0,
        connection_errors: 0,
        validation_errors: 0,
        pending_limit_errors: 0,
        rtt_p50_ns: None,
        rtt_p90_ns: None,
        rtt_p99_ns: None,
        rtt_p999_ns: None,
        rtt_p9999_ns: None,
        rtt_max_ns: None,
    };
    let mut rtts = Vec::new();
    let mut events = Events::with_capacity(1024);
    let mut read_buf = vec![0_u8; READ_BUF_LEN];

    while monotonic_ns() < end_ns {
        let timeout = next_poll_timeout(&conns, end_ns);
        poll.poll(&mut events, timeout)?;

        for event in events.iter() {
            let idx = event.token().0.saturating_sub(1);
            let Some(conn) = conns.get_mut(idx) else {
                continue;
            };

            summary.readiness_events += 1;
            let mut keep = true;

            if event.is_writable() {
                if !conn.connected {
                    match conn.socket.take_error() {
                        Ok(Some(_err)) => {
                            keep = false;
                        }
                        Ok(None) => conn.mark_connected(rate_per_conn, monotonic_ns()),
                        Err(_err) => {
                            keep = false;
                        }
                    }
                }
                if keep {
                    match conn.flush(&mut summary) {
                        Ok(write_keep) => keep = write_keep,
                        Err(_err) => {
                            keep = false;
                        }
                    }
                }
            }

            if keep && event.is_readable() {
                match handle_read(conn, &mut summary, &mut rtts, &mut read_buf) {
                    Ok(read_keep) => keep = read_keep,
                    Err(_err) => {
                        keep = false;
                    }
                }
            }

            if !keep {
                summary.connection_errors += 1;
                conn.alive = false;
                let _ = poll.registry().deregister(&mut conn.socket);
            } else {
                conn.sync_interest(poll.registry(), event.token())?;
            }
        }

        let now_ns = monotonic_ns();
        for conn in &mut conns {
            if !conn.alive {
                continue;
            }
            let token = Token(conn.stream_id as usize + 1);
            let mut keep = true;
            if let Err(err) = conn.queue_due(
                now_ns,
                end_ns,
                rate_per_conn,
                &payload,
                config.max_pending_bytes,
                &mut summary.sent,
            ) {
                if err.kind() == io::ErrorKind::WouldBlock {
                    summary.pending_limit_errors += 1;
                } else {
                    summary.connection_errors += 1;
                }
                conn.alive = false;
                let _ = poll.registry().deregister(&mut conn.socket);
                continue;
            }
            if keep {
                match conn.flush(&mut summary) {
                    Ok(write_keep) => keep = write_keep,
                    Err(_err) => keep = false,
                }
            }
            if keep {
                conn.sync_interest(poll.registry(), token)?;
            } else {
                summary.connection_errors += 1;
                conn.alive = false;
                let _ = poll.registry().deregister(&mut conn.socket);
            }
        }
    }

    let active_elapsed_secs = started.elapsed().as_secs_f64();
    let drain_end_ns =
        monotonic_ns() + Duration::from_secs(DRAIN_AFTER_SEND_SECS).as_nanos() as u64;
    while conns.iter().any(|conn| {
        conn.alive && (!conn.in_flight.is_empty() || conn.write_pos < conn.write_buf.len())
    }) && monotonic_ns() < drain_end_ns
    {
        poll.poll(&mut events, Some(Duration::from_millis(1)))?;

        for event in events.iter() {
            let idx = event.token().0.saturating_sub(1);
            let Some(conn) = conns.get_mut(idx) else {
                continue;
            };
            if !conn.alive {
                continue;
            }

            summary.readiness_events += 1;
            let mut keep = true;
            if event.is_writable() {
                match conn.flush(&mut summary) {
                    Ok(write_keep) => keep = write_keep,
                    Err(_err) => keep = false,
                }
            }
            if keep && event.is_readable() {
                match handle_read(conn, &mut summary, &mut rtts, &mut read_buf) {
                    Ok(read_keep) => keep = read_keep,
                    Err(_err) => keep = false,
                }
            }
            if keep {
                conn.sync_interest(poll.registry(), event.token())?;
            } else {
                summary.connection_errors += 1;
                conn.alive = false;
                let _ = poll.registry().deregister(&mut conn.socket);
            }
        }

        for conn in &mut conns {
            if !conn.alive {
                continue;
            }
            let token = Token(conn.stream_id as usize + 1);
            match conn.flush(&mut summary) {
                Ok(true) => conn.sync_interest(poll.registry(), token)?,
                Ok(false) | Err(_) => {
                    summary.connection_errors += 1;
                    conn.alive = false;
                    let _ = poll.registry().deregister(&mut conn.socket);
                }
            }
        }
    }

    summary.elapsed_secs = active_elapsed_secs;
    summary.active_elapsed_secs = active_elapsed_secs;
    summary.total_elapsed_secs = started.elapsed().as_secs_f64();
    summary.expected_sent = config.rate_total * config.duration_secs as f64;
    summary.offered_ratio = ratio(summary.sent as f64, summary.expected_sent);
    summary.lost = summary.sent.saturating_sub(summary.received);
    summary.achieved_rate = if summary.elapsed_secs > 0.0 {
        summary.received as f64 / summary.elapsed_secs
    } else {
        0.0
    };
    rtts.sort_unstable();
    summary.rtt_p50_ns = percentile(&rtts, 50.0);
    summary.rtt_p90_ns = percentile(&rtts, 90.0);
    summary.rtt_p99_ns = percentile(&rtts, 99.0);
    summary.rtt_p999_ns = percentile(&rtts, 99.9);
    summary.rtt_p9999_ns = percentile(&rtts, 99.99);
    summary.rtt_max_ns = rtts.last().copied();

    println!(
        "{}",
        serde_json::to_string_pretty(&summary).map_err(io::Error::other)?
    );
    Ok(())
}

fn handle_read(
    conn: &mut Conn,
    summary: &mut SenderSummary,
    rtts: &mut Vec<u64>,
    read_buf: &mut [u8],
) -> io::Result<bool> {
    loop {
        match conn.socket.read(read_buf) {
            Ok(0) => return Ok(false),
            Ok(n) => {
                summary.read_calls += 1;
                conn.decoder.push(&read_buf[..n]);
                loop {
                    match conn.decoder.next_frame() {
                        Ok(Some(frame)) => {
                            if frame.header.stream_id != conn.stream_id {
                                summary.validation_errors += 1;
                                continue;
                            }
                            if conn.in_flight.remove(&frame.header.seq).is_none() {
                                summary.validation_errors += 1;
                                continue;
                            }
                            let now_ns = monotonic_ns();
                            rtts.push(now_ns.saturating_sub(frame.header.send_time_ns));
                            summary.received += 1;
                        }
                        Ok(None) => break,
                        Err(_err) => {
                            summary.validation_errors += 1;
                            return Ok(false);
                        }
                    }
                }
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
            Err(err) => return Err(err),
        }
    }

    Ok(true)
}

fn ratio(numerator: f64, denominator: f64) -> f64 {
    if denominator > 0.0 {
        numerator / denominator
    } else {
        0.0
    }
}

fn stream_seed(run_seed: u64, idx: u64) -> u64 {
    run_seed ^ (idx + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15)
}

fn next_poll_timeout(conns: &[Conn], end_ns: u64) -> Option<Duration> {
    let now_ns = monotonic_ns();
    let next_due = conns
        .iter()
        .filter(|conn| conn.alive && conn.connected)
        .map(|conn| conn.next_send_at_ns)
        .min()
        .unwrap_or(end_ns)
        .min(end_ns);

    if next_due <= now_ns {
        return Some(Duration::ZERO);
    }

    Some(Duration::from_nanos((next_due - now_ns).min(10_000_000)))
}

fn parse_config() -> io::Result<Config> {
    let mut target: Option<SocketAddr> = None;
    let mut connections = 1_usize;
    let mut rate_total = 1_000.0_f64;
    let mut duration_secs = 10_u64;
    let mut payload_bytes = DEFAULT_PAYLOAD_BYTES;
    let mut max_pending_bytes = DEFAULT_MAX_PENDING_BYTES;
    let mut seed = 1_u64;
    let mut args = env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--target" => {
                target = Some(parse_next(&mut args, "--target")?);
            }
            "--connections" => {
                connections = parse_next(&mut args, "--connections")?;
            }
            "--rate-total" => {
                rate_total = parse_next(&mut args, "--rate-total")?;
            }
            "--duration-secs" => {
                duration_secs = parse_next(&mut args, "--duration-secs")?;
            }
            "--payload-bytes" => {
                payload_bytes = parse_next(&mut args, "--payload-bytes")?;
            }
            "--max-pending-bytes" => {
                max_pending_bytes = parse_next(&mut args, "--max-pending-bytes")?;
            }
            "--seed" => {
                seed = parse_next(&mut args, "--seed")?;
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown argument: {arg}"),
                ));
            }
        }
    }

    Ok(Config {
        target: target.ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "missing --target HOST:PORT")
        })?,
        connections,
        rate_total,
        duration_secs,
        payload_bytes,
        max_pending_bytes,
        seed,
    })
}

fn parse_next<T: std::str::FromStr>(
    args: &mut impl Iterator<Item = String>,
    name: &str,
) -> io::Result<T>
where
    T::Err: std::fmt::Display,
{
    args.next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, format!("missing {name}")))?
        .parse()
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, format!("{name}: {err}")))
}

fn print_usage() {
    eprintln!(
        "usage: bench-sender-mio --target HOST:PORT [--connections N] [--rate-total N] [--duration-secs N] [--payload-bytes N] [--max-pending-bytes N] [--seed N]"
    );
}
