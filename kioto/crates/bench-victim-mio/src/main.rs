use std::env;
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use bench_core::{
    DEFAULT_MAX_PAYLOAD_BYTES, FrameDecoder, HEADER_LEN, VictimSummary, encode_frame,
};
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Registry, Token};

const LISTENER: Token = Token(0);
const READ_BUF_LEN: usize = 64 * 1024;
const DEFAULT_READ_BUDGET_BYTES: usize = 256 * 1024;
const DEFAULT_MAX_PENDING_BYTES: usize = 64 * 1024 * 1024;

struct Config {
    listen: SocketAddr,
    duration_secs: Option<u64>,
    read_budget_bytes: usize,
    max_pending_bytes: usize,
}

struct Conn {
    socket: TcpStream,
    decoder: FrameDecoder,
    write_buf: Vec<u8>,
    write_pos: usize,
    wants_write: bool,
}

impl Conn {
    fn new(socket: TcpStream) -> Self {
        Self {
            socket,
            decoder: FrameDecoder::new(DEFAULT_MAX_PAYLOAD_BYTES),
            write_buf: Vec::new(),
            write_pos: 0,
            wants_write: false,
        }
    }

    fn sync_interest(&mut self, registry: &Registry, token: Token) -> io::Result<()> {
        let wants_write = self.write_pos < self.write_buf.len();
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

    fn queue_echo(&mut self, frame: bench_core::Frame) -> io::Result<()> {
        encode_frame(&frame, &mut self.write_buf).map_err(io::Error::other)
    }

    fn pending_write_bytes(&self) -> usize {
        self.write_buf.len().saturating_sub(self.write_pos)
    }

    fn flush(&mut self, summary: &mut VictimSummary) -> io::Result<bool> {
        while self.write_pos < self.write_buf.len() {
            match self.socket.write(&self.write_buf[self.write_pos..]) {
                Ok(0) => return Ok(false),
                Ok(n) => {
                    self.write_pos += n;
                    summary.write_calls += 1;
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
    let mut listener = TcpListener::bind(config.listen)?;
    let mut poll = Poll::new()?;
    poll.registry()
        .register(&mut listener, LISTENER, Interest::READABLE)?;

    let running = Arc::new(AtomicBool::new(true));
    {
        let running = Arc::clone(&running);
        ctrlc::set_handler(move || running.store(false, Ordering::SeqCst))
            .map_err(io::Error::other)?;
    }

    let started = Instant::now();
    let deadline = config
        .duration_secs
        .map(|secs| started + Duration::from_secs(secs));
    let mut summary = VictimSummary {
        listen: config.listen.to_string(),
        elapsed_secs: 0.0,
        connections_accepted: 0,
        connections_closed: 0,
        frames: 0,
        bytes: 0,
        read_calls: 0,
        write_calls: 0,
        readiness_events: 0,
        parse_errors: 0,
        connection_errors: 0,
        pending_limit_errors: 0,
    };

    let mut events = Events::with_capacity(1024);
    let mut conns: Vec<Option<Conn>> = Vec::new();
    let mut free: Vec<usize> = Vec::new();
    let mut read_buf = vec![0_u8; READ_BUF_LEN];

    while running.load(Ordering::SeqCst)
        && deadline.is_none_or(|deadline| Instant::now() < deadline)
    {
        poll.poll(&mut events, Some(Duration::from_millis(100)))?;

        for event in events.iter() {
            if event.token() == LISTENER {
                accept_ready(&poll, &mut listener, &mut conns, &mut free, &mut summary)?;
                continue;
            }

            let idx = event.token().0.saturating_sub(1);
            let Some(Some(conn)) = conns.get_mut(idx) else {
                continue;
            };

            summary.readiness_events += 1;
            let mut keep = true;

            if event.is_readable() {
                match handle_read(
                    conn,
                    &mut summary,
                    &mut read_buf,
                    config.read_budget_bytes,
                    config.max_pending_bytes,
                ) {
                    Ok(read_keep) => keep = read_keep,
                    Err(err) => {
                        if err.kind() != io::ErrorKind::WouldBlock {
                            summary.connection_errors += 1;
                        }
                        keep = false;
                    }
                }
            }

            if keep {
                match conn.flush(&mut summary) {
                    Ok(write_keep) => keep = write_keep,
                    Err(_err) => {
                        summary.connection_errors += 1;
                        keep = false;
                    }
                }
            }

            if keep {
                conn.sync_interest(poll.registry(), event.token())?;
            } else {
                close_conn(&poll, &mut conns, &mut free, idx, &mut summary);
            }
        }
    }

    summary.elapsed_secs = started.elapsed().as_secs_f64();
    println!(
        "{}",
        serde_json::to_string_pretty(&summary).map_err(io::Error::other)?
    );
    Ok(())
}

fn accept_ready(
    poll: &Poll,
    listener: &mut TcpListener,
    conns: &mut Vec<Option<Conn>>,
    free: &mut Vec<usize>,
    summary: &mut VictimSummary,
) -> io::Result<()> {
    loop {
        match listener.accept() {
            Ok((mut socket, _addr)) => {
                socket.set_nodelay(true)?;
                let idx = free.pop().unwrap_or_else(|| {
                    conns.push(None);
                    conns.len() - 1
                });
                poll.registry()
                    .register(&mut socket, Token(idx + 1), Interest::READABLE)?;
                conns[idx] = Some(Conn::new(socket));
                summary.connections_accepted += 1;
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => return Ok(()),
            Err(err) => {
                summary.connection_errors += 1;
                return Err(err);
            }
        }
    }
}

fn handle_read(
    conn: &mut Conn,
    summary: &mut VictimSummary,
    read_buf: &mut [u8],
    read_budget_bytes: usize,
    max_pending_bytes: usize,
) -> io::Result<bool> {
    let mut bytes_read = 0_usize;

    while bytes_read < read_budget_bytes {
        match conn.socket.read(read_buf) {
            Ok(0) => return Ok(false),
            Ok(n) => {
                bytes_read += n;
                summary.read_calls += 1;
                conn.decoder.push(&read_buf[..n]);

                loop {
                    match conn.decoder.next_frame() {
                        Ok(Some(frame)) => {
                            let frame_len = HEADER_LEN + frame.payload.len();
                            if conn.pending_write_bytes().saturating_add(frame_len)
                                > max_pending_bytes
                            {
                                summary.pending_limit_errors += 1;
                                return Err(io::Error::new(
                                    io::ErrorKind::WouldBlock,
                                    "victim max pending bytes exceeded",
                                ));
                            }
                            summary.frames += 1;
                            summary.bytes += frame_len as u64;
                            conn.queue_echo(frame)?;
                        }
                        Ok(None) => break,
                        Err(_err) => {
                            summary.parse_errors += 1;
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

fn close_conn(
    poll: &Poll,
    conns: &mut [Option<Conn>],
    free: &mut Vec<usize>,
    idx: usize,
    summary: &mut VictimSummary,
) {
    if let Some(mut conn) = conns[idx].take() {
        let _ = poll.registry().deregister(&mut conn.socket);
        free.push(idx);
        summary.connections_closed += 1;
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
        "usage: bench-victim-mio --listen HOST:PORT [--duration-secs N] [--read-budget-bytes N] [--max-pending-bytes N]"
    );
}
