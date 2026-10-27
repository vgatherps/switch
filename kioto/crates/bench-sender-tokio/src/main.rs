use std::collections::HashMap;
use std::env;
use std::io;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use bench_core::{
    DEFAULT_MAX_PAYLOAD_BYTES, DEFAULT_PAYLOAD_BYTES, FrameDecoder, FrameHeader, HEADER_LEN,
    SenderSummary, XorShift64, encode_frame_parts, monotonic_ns, percentile, poisson_gap_ns,
};
use tokio::net::TcpStream;

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

#[derive(Default)]
struct ConnResult {
    sent: u64,
    received: u64,
    read_calls: u64,
    write_calls: u64,
    write_bytes: u64,
    readiness_events: u64,
    connection_errors: u64,
    validation_errors: u64,
    pending_limit_errors: u64,
    rtts: Vec<u64>,
}

struct ConnState {
    socket: TcpStream,
    decoder: FrameDecoder,
    write_buf: Vec<u8>,
    write_pos: usize,
    stream_id: u32,
    next_seq: u64,
    next_send_at_ns: u64,
    alive: bool,
    in_flight: HashMap<u64, u64>,
    rng: XorShift64,
    result: ConnResult,
}

fn main() -> io::Result<()> {
    let config = parse_config()?;
    validate_config(&config)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()?;
    runtime.block_on(run(config))
}

async fn run(config: Config) -> io::Result<()> {
    let started = Instant::now();
    let start_ns = monotonic_ns();
    let end_ns = start_ns + Duration::from_secs(config.duration_secs).as_nanos() as u64;
    let rate_per_conn = config.rate_total / config.connections as f64;
    let payload = vec![0_u8; config.payload_bytes];
    let mut handles = Vec::with_capacity(config.connections);

    for idx in 0..config.connections {
        let target = config.target;
        let payload = payload.clone();
        let seed = stream_seed(config.seed, idx as u64);
        let max_pending_bytes = config.max_pending_bytes;
        handles.push(tokio::spawn(async move {
            run_conn(
                idx as u32,
                target,
                rate_per_conn,
                end_ns,
                payload,
                max_pending_bytes,
                seed,
            )
            .await
        }));
    }

    let mut combined = ConnResult::default();
    for handle in handles {
        match handle.await {
            Ok(result) => merge_result(&mut combined, result),
            Err(_err) => combined.connection_errors += 1,
        }
    }

    let active_elapsed_secs = config.duration_secs as f64;
    let expected_sent = config.rate_total * active_elapsed_secs;
    combined.rtts.sort_unstable();
    let total_elapsed_secs = started.elapsed().as_secs_f64();
    let summary = SenderSummary {
        target: config.target.to_string(),
        connections: config.connections,
        payload_bytes: config.payload_bytes,
        max_pending_bytes: config.max_pending_bytes,
        offered_rate_total: config.rate_total,
        elapsed_secs: active_elapsed_secs,
        active_elapsed_secs,
        total_elapsed_secs,
        expected_sent,
        offered_ratio: ratio(combined.sent as f64, expected_sent),
        sent: combined.sent,
        received: combined.received,
        lost: combined.sent.saturating_sub(combined.received),
        achieved_rate: if active_elapsed_secs > 0.0 {
            combined.received as f64 / active_elapsed_secs
        } else {
            0.0
        },
        read_calls: combined.read_calls,
        write_calls: combined.write_calls,
        write_bytes: combined.write_bytes,
        readiness_events: combined.readiness_events,
        connection_errors: combined.connection_errors,
        validation_errors: combined.validation_errors,
        pending_limit_errors: combined.pending_limit_errors,
        rtt_p50_ns: percentile(&combined.rtts, 50.0),
        rtt_p90_ns: percentile(&combined.rtts, 90.0),
        rtt_p99_ns: percentile(&combined.rtts, 99.0),
        rtt_p999_ns: percentile(&combined.rtts, 99.9),
        rtt_p9999_ns: percentile(&combined.rtts, 99.99),
        rtt_max_ns: combined.rtts.last().copied(),
    };

    println!(
        "{}",
        serde_json::to_string_pretty(&summary).map_err(io::Error::other)?
    );
    Ok(())
}

async fn run_conn(
    stream_id: u32,
    target: SocketAddr,
    rate_per_conn: f64,
    end_ns: u64,
    payload: Vec<u8>,
    max_pending_bytes: usize,
    seed: u64,
) -> ConnResult {
    let socket = match TcpStream::connect(target).await {
        Ok(socket) => socket,
        Err(_err) => {
            let mut result = ConnResult::default();
            result.connection_errors += 1;
            return result;
        }
    };
    if socket.set_nodelay(true).is_err() {
        let mut result = ConnResult::default();
        result.connection_errors += 1;
        return result;
    }

    let mut state = ConnState {
        socket,
        decoder: FrameDecoder::new(DEFAULT_MAX_PAYLOAD_BYTES),
        write_buf: Vec::new(),
        write_pos: 0,
        stream_id,
        next_seq: 0,
        next_send_at_ns: monotonic_ns(),
        alive: true,
        in_flight: HashMap::new(),
        rng: XorShift64::new(seed),
        result: ConnResult::default(),
    };
    state.next_send_at_ns += poisson_gap_ns(rate_per_conn, &mut state.rng);

    let mut read_buf = vec![0_u8; READ_BUF_LEN];
    while state.alive && monotonic_ns() < end_ns {
        if let Err(err) = queue_due(
            &mut state,
            monotonic_ns(),
            end_ns,
            rate_per_conn,
            &payload,
            max_pending_bytes,
        ) {
            if err.kind() != io::ErrorKind::WouldBlock {
                state.result.connection_errors += 1;
            }
            state.alive = false;
            break;
        }
        if step_io(&mut state, &mut read_buf, end_ns).await.is_err() {
            state.result.connection_errors += 1;
            state.alive = false;
            break;
        }
    }

    let drain_end_ns =
        monotonic_ns() + Duration::from_secs(DRAIN_AFTER_SEND_SECS).as_nanos() as u64;
    while state.alive
        && (!state.in_flight.is_empty() || state.write_pos < state.write_buf.len())
        && monotonic_ns() < drain_end_ns
    {
        if step_io(&mut state, &mut read_buf, drain_end_ns)
            .await
            .is_err()
        {
            state.result.connection_errors += 1;
            state.alive = false;
            break;
        }
    }

    state.result
}

async fn step_io(state: &mut ConnState, read_buf: &mut [u8], wait_until_ns: u64) -> io::Result<()> {
    if state.write_pos < state.write_buf.len() {
        flush(state)?;
    }
    state.alive = read_ready(state, read_buf)?;
    if !state.alive {
        return Ok(());
    }

    let now_ns = monotonic_ns();
    let next_due_ns = state.next_send_at_ns.min(wait_until_ns);
    if next_due_ns <= now_ns {
        return Ok(());
    }
    let timeout = Duration::from_nanos((next_due_ns - now_ns).min(10_000_000));

    tokio::select! {
        res = state.socket.readable() => {
            res?;
            state.result.readiness_events += 1;
            state.alive = read_ready(state, read_buf)?;
        }
        res = state.socket.writable(), if state.write_pos < state.write_buf.len() => {
            res?;
            state.result.readiness_events += 1;
            flush(state)?;
        }
        _ = tokio::time::sleep(timeout) => {}
    }

    Ok(())
}

fn queue_due(
    state: &mut ConnState,
    now_ns: u64,
    end_ns: u64,
    rate_per_conn: f64,
    payload: &[u8],
    max_pending_bytes: usize,
) -> io::Result<()> {
    let mut queued = 0_usize;
    while state.next_send_at_ns <= now_ns
        && state.next_send_at_ns < end_ns
        && queued < MAX_DUE_SENDS_PER_TICK
    {
        let frame_len = HEADER_LEN + payload.len();
        if pending_frame_bytes(state, frame_len).saturating_add(frame_len) > max_pending_bytes {
            state.result.pending_limit_errors += 1;
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "sender max pending bytes exceeded",
            ));
        }
        let send_time_ns = monotonic_ns();
        let header = FrameHeader::new(
            state.next_seq,
            send_time_ns,
            payload.len() as u32,
            state.stream_id,
        );
        encode_frame_parts(header, payload, &mut state.write_buf).map_err(io::Error::other)?;
        state.in_flight.insert(state.next_seq, send_time_ns);
        state.next_seq += 1;
        state.result.sent += 1;
        queued += 1;
        state.next_send_at_ns += poisson_gap_ns(rate_per_conn, &mut state.rng);
    }

    Ok(())
}

fn pending_frame_bytes(state: &ConnState, frame_len: usize) -> usize {
    state.in_flight.len().saturating_mul(frame_len)
}

fn read_ready(state: &mut ConnState, read_buf: &mut [u8]) -> io::Result<bool> {
    loop {
        match state.socket.try_read(read_buf) {
            Ok(0) => {
                state.result.connection_errors += 1;
                return Ok(false);
            }
            Ok(n) => {
                state.result.read_calls += 1;
                state.decoder.push(&read_buf[..n]);
                loop {
                    match state.decoder.next_frame() {
                        Ok(Some(frame)) => {
                            if frame.header.stream_id != state.stream_id {
                                state.result.validation_errors += 1;
                                continue;
                            }
                            if state.in_flight.remove(&frame.header.seq).is_none() {
                                state.result.validation_errors += 1;
                                continue;
                            }
                            let now_ns = monotonic_ns();
                            state
                                .result
                                .rtts
                                .push(now_ns.saturating_sub(frame.header.send_time_ns));
                            state.result.received += 1;
                        }
                        Ok(None) => break,
                        Err(_err) => {
                            state.result.validation_errors += 1;
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

fn flush(state: &mut ConnState) -> io::Result<()> {
    while state.write_pos < state.write_buf.len() {
        match state.socket.try_write(&state.write_buf[state.write_pos..]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "write returned zero",
                ));
            }
            Ok(n) => {
                state.write_pos += n;
                state.result.write_calls += 1;
                state.result.write_bytes += n as u64;
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
            Err(err) => return Err(err),
        }
    }

    if state.write_pos == state.write_buf.len() {
        state.write_buf.clear();
        state.write_pos = 0;
    } else if state.write_pos > 64 * 1024 {
        state.write_buf.drain(..state.write_pos);
        state.write_pos = 0;
    }

    Ok(())
}

fn merge_result(combined: &mut ConnResult, mut result: ConnResult) {
    combined.sent += result.sent;
    combined.received += result.received;
    combined.read_calls += result.read_calls;
    combined.write_calls += result.write_calls;
    combined.write_bytes += result.write_bytes;
    combined.readiness_events += result.readiness_events;
    combined.connection_errors += result.connection_errors;
    combined.validation_errors += result.validation_errors;
    combined.pending_limit_errors += result.pending_limit_errors;
    combined.rtts.append(&mut result.rtts);
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

fn validate_config(config: &Config) -> io::Result<()> {
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
    Ok(())
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
        "usage: bench-sender-tokio --target HOST:PORT [--connections N] [--rate-total N] [--duration-secs N] [--payload-bytes N] [--max-pending-bytes N] [--seed N]"
    );
}
