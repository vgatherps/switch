//! One open-loop generator worker: a Zhida Stack on its own Port and core. It stays up for a whole
//! sweep and runs the steps the coordinator sends on stdin, one JSON line each way (see
//! `Command` and `StepResult`). Between steps it turns the Net and reads stdin (non-blocking);
//! during a step the loop makes no syscall.

use std::cell::{Cell, RefCell};
use std::io::{ErrorKind, Read};
use std::net::SocketAddrV4;
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use switchnet::{
    IpEndpoint, IpListenEndpoint, NetHandle, SendFrameError, TcpTx, TxFrame, UdpConfig,
    UdpSendError, UdpSendFrameError, UdpTx, boxed, tcp,
};
use zhida_open::Sizing;

use crate::hist::{Histogram, Sparse};
use crate::host::{self, Clock, EthStats, Host, TurnSums};
use crate::pace::{Outcome, Pacer};
use crate::record::Recorder;
use crate::schedule::Schedule;
use crate::wire::{HEADER, Header};
use crate::wsclient;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Proto {
    Udp,
    Tcp,
    Ws,
}

impl Proto {
    pub fn name(self) -> &'static str {
        match self {
            Proto::Udp => "udp",
            Proto::Tcp => "tcp",
            Proto::Ws => "ws",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Args {
    pub config: PathBuf,
    pub proto: Proto,
    pub target: SocketAddrV4,
    pub senders: u32,
    pub size: usize,
    pub send_budget: u32,
    /// First local port; sender i uses `local_port + i`.
    pub local_port: u16,
}

/// Datagrams each UDP socket may hold before the Stack drops (counted as `rx_dropped_full`).
const UDP_QUEUE: usize = 256;
/// Receive and send buffer of each generator TCP connection.
pub const TCP_BUFFER: usize = 64 * 1024;
const CONNECT_LIMIT: Duration = Duration::from_secs(10);

/// A step to run: this worker's share of the rate, starting at a wall-clock time common to all
/// workers.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Command {
    pub step: u32,
    pub rate_pps: u64,
    pub start_unix_ns: u64,
    pub warmup_ms: u64,
    pub step_ms: u64,
    pub drain_ms: u64,
}

/// What a worker reports for one step.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepResult {
    pub step: u32,
    pub offered_pps: f64,
    /// Scheduled times of the measured messages, wall clock.
    pub window_unix_ns: [u64; 2],
    pub measured: u64,
    pub received: u64,
    pub duplicates: u64,
    pub stale: u64,
    pub malformed: u64,
    pub warmup_received: u64,
    pub latency: Sparse,
    /// Actual send - scheduled time of messages sent fresh off the schedule.
    pub send_lag: Sparse,
    /// The same for messages a full connection owed (TCP flow control): the receiver's doing.
    pub owed_wait: Sparse,
    pub backlog_at_end: u64,
    pub owed_at_end: u64,
    /// The worker's loop over the step: wall time per round (Net turn + Runtime poll).
    pub ns_per_round: u64,
    pub turns: TurnSums,
    pub eth: EthStats,
    pub rx_dropped_full: u64,
}

/// What the send task leaves for the loop once it has sent the whole step.
struct Sent {
    lag: Histogram,
    owed_wait: Histogram,
    backlog_at_end: u64,
    owed_at_end: u64,
}

/// The senders' send halves (ws: the upgraded connections' TCP halves).
enum Senders {
    Udp(Vec<UdpTx>),
    Tcp(Vec<TcpTx>),
    Ws(Vec<TcpTx>),
}

impl Senders {
    /// Sends `frame` as message `k` from `sender`.
    #[inline]
    fn send(&mut self, sender: u32, frame: TxFrame, target: IpEndpoint) -> Outcome {
        match self {
            Senders::Udp(txs) => match txs[sender as usize].send(Some(target), frame) {
                Ok(()) => Outcome::Sent,
                Err((
                    _,
                    UdpSendFrameError::Udp(
                        UdpSendError::WouldBlock | UdpSendError::NeighborPending,
                    ),
                )) => Outcome::Stop,
                Err((_, e)) => panic!("udp send: {e:?}"),
            },
            Senders::Tcp(txs) | Senders::Ws(txs) => match txs[sender as usize].try_send(frame) {
                Ok(()) => Outcome::Sent,
                Err((_, SendFrameError::Full)) => Outcome::Blocked,
                Err((_, e)) => panic!("tcp send on connection {sender}: {e:?}"),
            },
        }
    }

    fn rx_dropped_full(&self) -> u64 {
        match self {
            Senders::Udp(txs) => txs.iter().map(|tx| tx.stats().rx_dropped_full).sum(),
            Senders::Tcp(_) | Senders::Ws(_) => 0,
        }
    }
}

struct Worker {
    host: Host,
    handle: NetHandle,
    senders: Rc<RefCell<Senders>>,
    recorder: Rc<RefCell<Option<Recorder>>>,
    /// TCP/ws connections' receive side, for the stall dump (empty for UDP).
    conns: Rc<Vec<ConnStats>>,
    target: IpEndpoint,
    args: Args,
}

/// A step that has not finished this long after its drain fails, with a state dump.
const STALL_LIMIT_NS: u64 = 5_000_000_000;

/// One TCP/ws connection's receive side, for the stall dump.
#[derive(Default)]
struct ConnStats {
    rx_bytes: Cell<u64>,
    last_rx_ns: Cell<u64>,
    ended: Cell<bool>,
}

/// Opens the Stack, binds or connects the senders, prints `{"kind":"ready"}`, then serves
/// commands from stdin until it closes or SIGINT/SIGTERM.
pub fn run(args: &Args, out: &mut dyn FnMut(&str)) -> Result<(), String> {
    let n = args.senders as usize;
    if args.size < HEADER {
        return Err(format!("--size must be at least {HEADER}"));
    }
    if args.proto == Proto::Ws {
        wsclient::check_size(args.size)?;
    }
    let config = Host::config(&args.config)?;
    let tcp_config = tcp::Config {
        rx_capacity: TCP_BUFFER,
        tx_capacity: TCP_BUFFER,
        mss_cap: None,
    };
    let mtu = usize::from(config.mtu);
    let sizing = match args.proto {
        Proto::Udp => Sizing {
            rx_share: n * UDP_QUEUE,
            frame_share: 64,
            max_sockets: 1,
            max_udp_sockets: n,
            rx_staging: 64,
            tx_pending: 64,
            neighbor_capacity: 256,
        },
        Proto::Tcp | Proto::Ws => Sizing {
            rx_share: n * tcp::rx_mbuf_budget(&tcp_config, mtu),
            frame_share: n * tcp::tx_mbuf_budget(&tcp_config, mtu) + 64,
            max_sockets: n,
            max_udp_sockets: 0,
            rx_staging: 64,
            tx_pending: 64,
            neighbor_capacity: 256,
        },
    };
    host::catch_stop()?;
    let mut host = Host::open(&config, &sizing)?;
    let handle = host.net.handle();
    let clock = host.clock;
    let recorder: Rc<RefCell<Option<Recorder>>> = Rc::default();
    let target = IpEndpoint::new((*args.target.ip()).into(), args.target.port());
    let conns: Rc<Vec<ConnStats>> = Rc::new(match args.proto {
        Proto::Udp => Vec::new(),
        Proto::Tcp | Proto::Ws => (0..n).map(|_| ConnStats::default()).collect(),
    });
    let senders = match args.proto {
        Proto::Udp => {
            let mut txs = Vec::with_capacity(n);
            for i in 0..args.senders {
                let port = local_port(args, i)?;
                let config = UdpConfig {
                    port,
                    local_addr: None,
                    rx_queue: UDP_QUEUE,
                    tx_checksum: true,
                    broadcast: false,
                };
                let (mut rx, tx) = handle
                    .udp_bind(config)
                    .map_err(|e| format!("udp bind {port}: {e:?}"))?
                    .split();
                txs.push(tx);
                let recorder = recorder.clone();
                spawn(&host, async move {
                    loop {
                        let datagram = rx.recv().await;
                        let now = clock.now_ns();
                        if let Some(r) = recorder.borrow_mut().as_mut() {
                            r.record(datagram.payload(), now);
                        }
                    }
                })?;
            }
            Senders::Udp(txs)
        }
        Proto::Tcp => Senders::Tcp(connect_all(
            &mut host, &handle, args, tcp_config, target, &recorder, &conns,
        )?),
        Proto::Ws => Senders::Ws(connect_all(
            &mut host, &handle, args, tcp_config, target, &recorder, &conns,
        )?),
    };
    let mut worker = Worker {
        host,
        handle,
        senders: Rc::new(RefCell::new(senders)),
        recorder,
        conns,
        target,
        args: args.clone(),
    };
    out(r#"{"kind":"ready"}"#);
    let mut stdin = Stdin::nonblocking()?;
    let mut idle = TurnSums::default();
    loop {
        match stdin.line()? {
            Some(line) if line.trim().is_empty() => {}
            Some(line) => {
                let command: Command =
                    serde_json::from_str(&line).map_err(|e| format!("command {line:?}: {e}"))?;
                let result = worker.step(command)?;
                out(&serde_json::to_string(&result).expect("result serializes"));
            }
            None if stdin.closed => break,
            None => {
                worker.host.round(&mut idle)?;
            }
        }
        if host::stopped() {
            break;
        }
    }
    let Worker {
        host,
        handle,
        senders,
        recorder,
        conns,
        ..
    } = worker;
    drop((handle, senders, recorder, conns));
    host.close()
}

fn local_port(args: &Args, i: u32) -> Result<u16, String> {
    u16::try_from(u32::from(args.local_port) + i).map_err(|_| "local ports past 65535".to_owned())
}

fn spawn(host: &Host, task: impl Future<Output = ()> + 'static) -> Result<(), String> {
    host.rt
        .spawner()
        .spawn(boxed(Box::pin(task)))
        .map_err(|e| format!("spawn: {:?}", e.reason))
}

/// Connects the N TCP senders (ws: and upgrades each), turning the Net until all are ready, and
/// starts a receive task on each that cuts the echoed byte stream into messages (ws: frames).
fn connect_all(
    host: &mut Host,
    handle: &NetHandle,
    args: &Args,
    config: tcp::Config,
    target: IpEndpoint,
    recorder: &Rc<RefCell<Option<Recorder>>>,
    conns: &Rc<Vec<ConnStats>>,
) -> Result<Vec<TcpTx>, String> {
    let n = args.senders as usize;
    let connected: Rc<RefCell<Vec<Option<TcpTx>>>> =
        Rc::new(RefCell::new((0..n).map(|_| None).collect()));
    let clock = host.clock;
    for i in 0..args.senders {
        let local = IpListenEndpoint {
            addr: None,
            port: local_port(args, i)?,
        };
        let connecting = handle
            .tcp_connect(config, target, local)
            .map_err(|e| format!("tcp connect: {e:?}"))?;
        let (connected, recorder, size) = (connected.clone(), recorder.clone(), args.size);
        let (ws, handle, peer) = (args.proto == Proto::Ws, handle.clone(), args.target);
        let conns = conns.clone();
        spawn(host, async move {
            let stream = match connecting.await {
                Ok(stream) => stream,
                Err(closed) => panic!("tcp connect {i}: {closed:?}"),
            };
            let (mut rx, mut tx) = stream.split();
            let mut partial = Vec::with_capacity(size);
            let unit = if ws {
                wsclient::server_frame_len(size)
            } else {
                size
            };
            if ws {
                let request = wsclient::request(&peer.ip().to_string(), peer.port());
                let mut frame = handle.tx_frame().await;
                frame.room_mut()[..request.len()].copy_from_slice(request.as_bytes());
                frame.set_len(request.len());
                if let Err((_, e)) = tx.try_send(frame) {
                    panic!("ws upgrade request {i}: {e:?}");
                }
                let mut head = Vec::new();
                let n = loop {
                    match rx.recv().await {
                        Ok(frame) => head.extend_from_slice(frame.payload()),
                        Err(e) => panic!("ws upgrade {i}: connection ended: {e:?}"),
                    }
                    if let Some(n) = wsclient::head_len(&head) {
                        break n;
                    }
                };
                if let Err(e) = wsclient::check_response(&head[..n]) {
                    panic!("ws upgrade {i}: {e}");
                }
                partial.extend_from_slice(&head[n..]);
            }
            connected.borrow_mut()[i as usize] = Some(tx);
            let stats = &conns[i as usize];
            while let Ok(frame) = rx.recv().await {
                let now = clock.now_ns();
                stats
                    .rx_bytes
                    .set(stats.rx_bytes.get() + frame.payload().len() as u64);
                stats.last_rx_ns.set(now);
                let mut r = recorder.borrow_mut();
                messages(&mut partial, frame.payload(), unit, |m| {
                    let m = if ws {
                        wsclient::decode(m, size).unwrap_or_else(|e| panic!("connection {i}: {e}"))
                    } else {
                        m
                    };
                    if let Some(r) = r.as_mut() {
                        r.record(m, now);
                    }
                });
            }
            stats.ended.set(true);
        })?;
    }
    let deadline = host.clock.now_ns() + CONNECT_LIMIT.as_nanos() as u64;
    let mut turns = TurnSums::default();
    while connected.borrow().iter().any(Option::is_none) {
        if host.round(&mut turns)? > deadline {
            return Err(format!(
                "{} connections not established within {CONNECT_LIMIT:?}",
                n
            ));
        }
    }
    Ok(connected
        .borrow_mut()
        .iter_mut()
        .map(|tx| tx.take().expect("checked above"))
        .collect())
}

/// Cuts `data` into `size`-byte messages, continuing the one in `partial`; calls `each` for every
/// complete message (from `data` itself when no partial message is pending).
#[inline]
pub fn messages(partial: &mut Vec<u8>, mut data: &[u8], size: usize, mut each: impl FnMut(&[u8])) {
    if !partial.is_empty() {
        let take = (size - partial.len()).min(data.len());
        partial.extend_from_slice(&data[..take]);
        data = &data[take..];
        if partial.len() < size {
            return;
        }
        each(partial);
        partial.clear();
    }
    while data.len() >= size {
        each(&data[..size]);
        data = &data[size..];
    }
    partial.extend_from_slice(data);
}

impl Worker {
    fn step(&mut self, c: Command) -> Result<StepResult, String> {
        let host = &mut self.host;
        let clock = host.clock;
        let eth_before = EthStats::read(host.port_id)?;
        let dropped_before = self.senders.borrow().rx_dropped_full();
        let origin = clock.mono_of_unix(c.start_unix_ns);
        let schedule = Schedule::for_rate(self.args.senders, c.rate_pps, origin);
        let measured_from = schedule.count_before(origin + c.warmup_ms * 1_000_000);
        let window_end = origin + (c.warmup_ms + c.step_ms) * 1_000_000;
        let end = schedule.count_before(window_end);
        *self.recorder.borrow_mut() = Some(Recorder::new(c.step, schedule, measured_from, end));
        let sent: Rc<RefCell<Option<Sent>>> = Rc::default();
        let pacer = Rc::new(RefCell::new(Pacer::new(
            schedule,
            end,
            self.args.send_budget,
        )));
        let phase = Rc::new(Cell::new("start"));
        let task = send_step(
            self.handle.clone(),
            self.senders.clone(),
            clock,
            pacer.clone(),
            phase.clone(),
            schedule,
            measured_from,
            c.step,
            self.args.size,
            self.target,
            sent.clone(),
        );
        spawn(host, task)?;
        let mut turns = TurnSums::default();
        let started_ns = clock.now_ns();
        let drain_ns = c.drain_ms * 1_000_000;
        let (mut last_round, mut max_gap) = (started_ns, 0);
        loop {
            let now = host.round(&mut turns)?;
            max_gap = max_gap.max(now - last_round);
            last_round = now;
            if host::stopped() {
                return Err("stopped".into());
            }
            if sent.borrow().is_some() && now >= window_end + drain_ns {
                break;
            }
            if now >= window_end + drain_ns + STALL_LIMIT_NS {
                let conns: Vec<_> = self
                    .conns
                    .iter()
                    .map(|c| {
                        serde_json::json!({"rx_bytes": c.rx_bytes.get(),
                            "last_rx_ago_ms": now.saturating_sub(c.last_rx_ns.get()) / 1_000_000,
                            "ended": c.ended.get()})
                    })
                    .collect();
                let received = self.recorder.borrow().as_ref().map(|r| r.received);
                eprintln!(
                    "{}",
                    serde_json::json!({"kind": "stall", "step": c.step, "rate_pps": c.rate_pps,
                        "past_drain_ms": (now - window_end - drain_ns) / 1_000_000,
                        "send_task": phase.get(), "pacer": format!("{:?}", pacer.borrow()),
                        "received": received, "max_round_gap_us": max_gap / 1000, "turns": turns,
                        "eth": EthStats::read(host.port_id)?.since(&eth_before), "conns": conns})
                );
                return Err(format!(
                    "step {} stalled: not done {} ms after its drain (state on stderr)",
                    c.step,
                    STALL_LIMIT_NS / 1_000_000
                ));
            }
        }
        let Sent {
            lag,
            owed_wait,
            backlog_at_end,
            owed_at_end,
        } = sent.borrow_mut().take().expect("checked above");
        let elapsed_ns = clock.now_ns() - started_ns;
        let r = self.recorder.borrow_mut().take().expect("set above");
        let eth = EthStats::read(host.port_id)?.since(&eth_before);
        let dropped_after = self.senders.borrow().rx_dropped_full();
        Ok(StepResult {
            step: c.step,
            offered_pps: schedule.offered_pps(),
            window_unix_ns: [
                clock.unix_ns(schedule.due_at(measured_from)),
                clock.unix_ns(window_end),
            ],
            measured: r.measured(),
            received: r.received,
            duplicates: r.duplicates,
            stale: r.stale,
            malformed: r.malformed,
            warmup_received: r.warmup_received,
            latency: r.latency.to_sparse(),
            send_lag: lag.to_sparse(),
            owed_wait: owed_wait.to_sparse(),
            backlog_at_end,
            owed_at_end,
            ns_per_round: elapsed_ns / turns.turns.max(1),
            turns,
            eth,
            rx_dropped_full: dropped_after - dropped_before,
        })
    }
}

/// Sends the step's messages on schedule through the pacer (at most its budget per poll),
/// yielding to the loop between batches. Send lag is recorded for measured messages only (the
/// warmup absorbs ARP and connection start); a full connection's owed messages are timed apart.
#[allow(clippy::too_many_arguments)]
async fn send_step(
    handle: NetHandle,
    senders: Rc<RefCell<Senders>>,
    clock: Clock,
    pacer: Rc<RefCell<Pacer>>,
    phase: Rc<Cell<&'static str>>,
    schedule: Schedule,
    measured_from: u64,
    step: u32,
    size: usize,
    target: IpEndpoint,
    done: Rc<RefCell<Option<Sent>>>,
) {
    let (mut lag, mut owed_wait) = (Histogram::new(), Histogram::new());
    let ws = matches!(&*senders.borrow(), Senders::Ws(_));
    while !pacer.borrow().done() {
        phase.set("pacing");
        pacer.borrow_mut().begin(clock.now_ns());
        loop {
            // no pacer borrow across the await: the stall dump reads it
            let next = pacer.borrow_mut().next_try();
            let Some(k) = next else { break };
            phase.set("frame");
            let mut frame = handle.tx_frame().await;
            phase.set("pacing");
            let (sender, seq) = schedule.split(k);
            let sched_ns = schedule.due_at(k);
            let header = Header {
                step,
                sender,
                seq,
                sched_ns,
            };
            let len = if ws {
                wsclient::encode(frame.room_mut(), size, |m| header.write(m))
            } else {
                header.write(&mut frame.room_mut()[..size]);
                size
            };
            frame.set_len(len);
            let owed = pacer.borrow().is_owed(k);
            let outcome = senders.borrow_mut().send(sender, frame, target);
            if outcome == Outcome::Sent && k >= measured_from {
                let wait = clock.now_ns().saturating_sub(sched_ns);
                if owed {
                    owed_wait.record(wait)
                } else {
                    lag.record(wait)
                }
            }
            pacer.borrow_mut().result(k, outcome);
        }
        pacer.borrow_mut().finish(clock.now_ns());
        phase.set("yield");
        switch::yield_now().await;
    }
    phase.set("done");
    let pacer = pacer.borrow();
    *done.borrow_mut() = Some(Sent {
        lag,
        owed_wait,
        backlog_at_end: pacer.backlog_at_end(),
        owed_at_end: pacer.owed_at_end(),
    });
}

/// Line-buffered non-blocking stdin.
struct Stdin {
    buf: Vec<u8>,
    closed: bool,
}

impl Stdin {
    fn nonblocking() -> Result<Stdin, String> {
        let fd = std::io::stdin().as_raw_fd();
        // SAFETY: fcntl on our own stdin descriptor with valid flags.
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
            return Err(format!(
                "stdin non-blocking: {}",
                std::io::Error::last_os_error()
            ));
        }
        Ok(Stdin {
            buf: Vec::new(),
            closed: false,
        })
    }

    /// A complete line if one is available, else `None` (and `closed` once stdin ended).
    fn line(&mut self) -> Result<Option<String>, String> {
        if let Some(pos) = self.buf.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = self.buf.drain(..=pos).collect();
            return String::from_utf8(line)
                .map(Some)
                .map_err(|_| "command not UTF-8".to_owned());
        }
        if self.closed {
            return Ok(None);
        }
        let mut chunk = [0u8; 4096];
        match std::io::stdin().lock().read(&mut chunk) {
            Ok(0) => self.closed = true,
            Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
            Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::Interrupted) => {}
            Err(e) => return Err(format!("stdin: {e}")),
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A byte stream cut anywhere comes out as the same messages, whole and in order.
    #[test]
    fn stream_cut_anywhere_gives_whole_messages() {
        let size = 24;
        let stream: Vec<u8> = (0..size * 10).map(|i| i as u8).collect();
        for cut in [1usize, 5, 23, 24, 25, 47, 100, 240] {
            let mut partial = Vec::new();
            let mut got = Vec::new();
            for chunk in stream.chunks(cut) {
                messages(&mut partial, chunk, size, |m| got.push(m.to_vec()));
            }
            assert!(partial.is_empty(), "cut {cut}: partial left");
            assert_eq!(got.concat(), stream, "cut {cut}");
            assert!(got.iter().all(|m| m.len() == size));
        }
    }
}
