use std::cell::Cell;
use std::env;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::{Duration, Instant};

use bench_core::{
    DEFAULT_MAX_PAYLOAD_BYTES, FrameDecoder, HEADER_LEN, VictimSummary, encode_frame,
};
use tokio::io::Interest;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::LocalSet;

const READ_BUF_LEN: usize = 64 * 1024;
const DEFAULT_READ_BUDGET_BYTES: usize = 256 * 1024;
const DEFAULT_MAX_PENDING_BYTES: usize = 64 * 1024 * 1024;

struct Config {
    listen: SocketAddr,
    duration_secs: Option<u64>,
    read_budget_bytes: usize,
    max_pending_bytes: usize,
}

#[derive(Default)]
struct Counters {
    connections_closed: Cell<u64>,
    frames: Cell<u64>,
    bytes: Cell<u64>,
    read_calls: Cell<u64>,
    write_calls: Cell<u64>,
    readiness_events: Cell<u64>,
    parse_errors: Cell<u64>,
    connection_errors: Cell<u64>,
    pending_limit_errors: Cell<u64>,
}

type SharedCounters = Rc<Counters>;

fn main() -> io::Result<()> {
    let config = parse_config()?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()?;
    let local = LocalSet::new();
    runtime.block_on(local.run_until(run(config)))
}

async fn run(config: Config) -> io::Result<()> {
    let listener = TcpListener::bind(config.listen).await?;
    let started = Instant::now();
    let mut handles = Vec::new();
    let mut connections_accepted = 0_u64;
    let mut counters_by_conn: Vec<SharedCounters> = Vec::new();
    let stop = stop_future(config.duration_secs);
    tokio::pin!(stop);

    loop {
        tokio::select! {
            res = listener.accept() => {
                let (socket, _addr) = res?;
                socket.set_nodelay(true)?;
                connections_accepted += 1;
                let counters_for_task = Rc::new(Counters::default());
                counters_by_conn.push(Rc::clone(&counters_for_task));
                let read_budget_bytes = config.read_budget_bytes;
                let max_pending_bytes = config.max_pending_bytes;
                handles.push(tokio::task::spawn_local(async move {
                    if let Err(err) = handle_conn(socket, read_budget_bytes, max_pending_bytes, Rc::clone(&counters_for_task)).await {
                        if err.kind() == io::ErrorKind::WouldBlock {
                            inc(&counters_for_task.pending_limit_errors);
                        } else {
                            inc(&counters_for_task.connection_errors);
                        }
                    }
                    inc(&counters_for_task.connections_closed);
                }));
            }
            _ = &mut stop => break,
        }
    }

    for handle in handles {
        handle.abort();
    }

    let summary = VictimSummary {
        listen: config.listen.to_string(),
        elapsed_secs: started.elapsed().as_secs_f64(),
        connections_accepted,
        connections_closed: counters_by_conn
            .iter()
            .map(|counters| counters.connections_closed.get())
            .sum(),
        frames: counters_by_conn
            .iter()
            .map(|counters| counters.frames.get())
            .sum(),
        bytes: counters_by_conn
            .iter()
            .map(|counters| counters.bytes.get())
            .sum(),
        read_calls: counters_by_conn
            .iter()
            .map(|counters| counters.read_calls.get())
            .sum(),
        write_calls: counters_by_conn
            .iter()
            .map(|counters| counters.write_calls.get())
            .sum(),
        readiness_events: counters_by_conn
            .iter()
            .map(|counters| counters.readiness_events.get())
            .sum(),
        parse_errors: counters_by_conn
            .iter()
            .map(|counters| counters.parse_errors.get())
            .sum(),
        connection_errors: counters_by_conn
            .iter()
            .map(|counters| counters.connection_errors.get())
            .sum(),
        pending_limit_errors: counters_by_conn
            .iter()
            .map(|counters| counters.pending_limit_errors.get())
            .sum(),
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&summary).map_err(io::Error::other)?
    );
    Ok(())
}

async fn handle_conn(
    socket: TcpStream,
    read_budget_bytes: usize,
    max_pending_bytes: usize,
    counters: SharedCounters,
) -> io::Result<()> {
    let mut decoder = FrameDecoder::new(DEFAULT_MAX_PAYLOAD_BYTES);
    let mut read_buf = vec![0_u8; READ_BUF_LEN];
    let mut write_buf = Vec::new();
    let mut write_pos = 0_usize;

    loop {
        let interest = if write_pos < write_buf.len() {
            Interest::READABLE | Interest::WRITABLE
        } else {
            Interest::READABLE
        };
        let ready = socket.ready(interest).await?;
        inc(&counters.readiness_events);

        if ready.is_readable() {
            let mut bytes_read = 0_usize;
            while bytes_read < read_budget_bytes {
                match socket.try_read(&mut read_buf) {
                    Ok(0) => return Ok(()),
                    Ok(n) => {
                        bytes_read += n;
                        inc(&counters.read_calls);
                        decoder.push(&read_buf[..n]);

                        loop {
                            match decoder.next_frame() {
                                Ok(Some(frame)) => {
                                    let frame_len = HEADER_LEN + frame.payload.len();
                                    if pending_write_bytes(&write_buf, write_pos)
                                        .saturating_add(frame_len)
                                        > max_pending_bytes
                                    {
                                        return Err(io::Error::new(
                                            io::ErrorKind::WouldBlock,
                                            "victim max pending bytes exceeded",
                                        ));
                                    }
                                    inc(&counters.frames);
                                    add(&counters.bytes, frame_len as u64);
                                    encode_frame(&frame, &mut write_buf)
                                        .map_err(io::Error::other)?;
                                }
                                Ok(None) => break,
                                Err(_err) => {
                                    inc(&counters.parse_errors);
                                    return Ok(());
                                }
                            }
                        }
                    }
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                    Err(err) => return Err(err),
                }
            }
        }

        if write_pos < write_buf.len() {
            flush(&socket, &mut write_buf, &mut write_pos, &counters)?;
        }
    }
}

fn pending_write_bytes(write_buf: &[u8], write_pos: usize) -> usize {
    write_buf.len().saturating_sub(write_pos)
}

fn flush(
    socket: &TcpStream,
    write_buf: &mut Vec<u8>,
    write_pos: &mut usize,
    counters: &SharedCounters,
) -> io::Result<()> {
    while *write_pos < write_buf.len() {
        match socket.try_write(&write_buf[*write_pos..]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "write returned zero",
                ));
            }
            Ok(n) => {
                *write_pos += n;
                inc(&counters.write_calls);
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
            Err(err) => return Err(err),
        }
    }

    if *write_pos == write_buf.len() {
        write_buf.clear();
        *write_pos = 0;
    } else if *write_pos > 64 * 1024 {
        write_buf.drain(..*write_pos);
        *write_pos = 0;
    }

    Ok(())
}

fn inc(counter: &Cell<u64>) {
    counter.set(counter.get() + 1);
}

fn add(counter: &Cell<u64>, value: u64) {
    counter.set(counter.get() + value);
}

async fn stop_future(duration_secs: Option<u64>) {
    match duration_secs {
        Some(secs) => tokio::time::sleep(Duration::from_secs(secs)).await,
        None => {
            let _ = tokio::signal::ctrl_c().await;
        }
    }
}

fn parse_config() -> io::Result<Config> {
    let mut listen: Option<SocketAddr> = None;
    let mut duration_secs = None;
    let mut read_budget_bytes = DEFAULT_READ_BUDGET_BYTES;
    let mut max_pending_bytes = DEFAULT_MAX_PENDING_BYTES;
    let mut args = env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--listen" => {
                listen = Some(parse_next(&mut args, "--listen")?);
            }
            "--duration-secs" => {
                duration_secs = Some(parse_next(&mut args, "--duration-secs")?);
            }
            "--read-budget-bytes" => {
                read_budget_bytes = parse_next(&mut args, "--read-budget-bytes")?;
            }
            "--max-pending-bytes" => {
                max_pending_bytes = parse_next(&mut args, "--max-pending-bytes")?;
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

    if max_pending_bytes == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--max-pending-bytes must be > 0",
        ));
    }

    Ok(Config {
        listen: listen.ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "missing --listen HOST:PORT")
        })?,
        duration_secs,
        read_budget_bytes,
        max_pending_bytes,
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
        "usage: bench-victim-tokio --listen HOST:PORT [--duration-secs N] [--read-budget-bytes N] [--max-pending-bytes N]"
    );
}
