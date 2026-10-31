//! One open-loop generator worker: a Zhida Stack on its own Port and core. It stays up for a whole
//! sweep and runs the steps the coordinator sends on stdin, one JSON line each way (see
//! `Command` and `StepResult`). Between steps it turns the Net and reads stdin (non-blocking);
//! during a step the loop makes no syscall.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::io::{ErrorKind, Read};
use std::net::SocketAddrV4;
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Poll;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use switchnet::{
    IpEndpoint, IpListenEndpoint, NetHandle, SendFrameError, TcpTx, TxFrame, UdpConfig,
    UdpSendError, UdpSendFrameError, UdpTx, boxed, tcp,
};
use zhida_open::Sizing;

use crate::hist::{Histogram, Sparse};
use crate::host::{self, Clock, Host};
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

/// Datagrams each UDP socket may hold before the Stack drops.
const UDP_QUEUE: usize = 256;
/// Receive and send buffer of each generator TCP connection.
pub const TCP_BUFFER: usize = 64 * 1024;
const CONNECT_LIMIT: Duration = Duration::from_secs(10);

/// A line from the coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Request {
    /// Run a step, unless its start time has already passed here (`Reply::Late`).
    Step(Command),
    /// Report this worker's wall clock (`Reply::Clock`), the one its steps start by.
    Clock,
}

/// A line to the coordinator.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Reply {
    /// Port open and senders bound or connected.
    Ready,
    Clock {
        unix_ns: u64,
    },
    /// The step's start time had passed when its command arrived: not run.
    Late {
        step: u32,
        late_ns: u64,
    },
    Step(StepResult),
}

/// A step to run: this worker's share of the rate, starting at a wall-clock time common to all
/// workers. Sending stops at the end of the window (warmup + step) whatever is still owed;
/// echoes are taken until the drain ends.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Command {
    pub step: u32,
    pub rate_pps: u64,
    pub start_unix_ns: u64,
    pub warmup_ms: u64,
    pub step_ms: u64,
    pub drain_ms: u64,
    /// TCP/ws: messages each connection may have sent and not yet had echoed; a message due
    /// beyond it waits (its latency still runs from its scheduled time). `None`: no limit.
    pub budget: Option<u32>,
}

/// What a worker reports for one step.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepResult {
    pub step: u32,
    pub offered_pps: f64,
    pub measured: u64,
    pub received: u64,
    /// Measured messages that never left the generator before the window ended (included in
    /// the loss: measured - received).
    pub unsent: u64,
    /// Messages actually sent, and this step's echoes received, within the window: the achieved
    /// rates, against `offered_pps`.
    pub sent_in_window: u64,
    pub received_in_window: u64,
    pub latency: Sparse,
    /// Actual send - scheduled time of measured messages sent fresh off the schedule (not those
    /// a connection owed: TCP flow control or the budget is the receiver's doing).
    pub send_lag: Sparse,
    /// Messages due and not sent, not owed by a blocked connection: the generator's backlog.
    pub backlog_at_end: u64,
    /// TCP/ws: echo time - actual send time.
    pub service_rtt: Sparse,
    /// Measured messages that waited at least once because their connection was at its budget.
    pub budget_waits: u64,
}

/// What the send task leaves for the loop once it has sent the whole step or been cut off.
struct Sent {
    lag: Histogram,
    backlog_at_end: u64,
    unsent: u64,
    in_window: u64,
    budget_waits: u64,
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
}

struct Worker {
    host: Host,
    handle: NetHandle,
    senders: Rc<RefCell<Senders>>,
    recorder: Rc<RefCell<Option<Recorder>>>,
    /// TCP/ws connections (empty for UDP).
    conns: Rc<Vec<Conn>>,
    target: IpEndpoint,
    args: Args,
}

/// One TCP/ws connection: the actual send times of its messages not yet echoed, oldest first
/// (TCP echoes in order), so its length is the connection's in-flight count.
struct Conn {
    in_flight: RefCell<VecDeque<u64>>,
}

/// Opens the Stack, binds or connects the senders, prints `Reply::Ready`, then serves requests
/// from stdin until it closes or SIGINT/SIGTERM.
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
    // in flight at most what the generator's and the receiver's send and receive buffers hold
    let in_flight_cap = 4 * TCP_BUFFER / args.size;
    let conns: Rc<Vec<Conn>> = Rc::new(match args.proto {
        Proto::Udp => Vec::new(),
        Proto::Tcp | Proto::Ws => (0..n)
            .map(|_| Conn {
                in_flight: RefCell::new(VecDeque::with_capacity(in_flight_cap)),
            })
            .collect(),
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
    let reply = |r: &Reply| serde_json::to_string(r).expect("reply serializes");
    out(&reply(&Reply::Ready));
    let mut stdin = Stdin::nonblocking()?;
    loop {
        match stdin.line()? {
            Some(line) if line.trim().is_empty() => {}
            Some(line) => {
                let request: Request =
                    serde_json::from_str(&line).map_err(|e| format!("request {line:?}: {e}"))?;
                worker.host.clock.resync();
                let clock = worker.host.clock;
                let now_unix_ns = clock.unix_ns(clock.now_ns());
                out(&reply(&match request {
                    Request::Clock => Reply::Clock {
                        unix_ns: now_unix_ns,
                    },
                    Request::Step(c) if now_unix_ns > c.start_unix_ns => Reply::Late {
                        step: c.step,
                        late_ns: now_unix_ns - c.start_unix_ns,
                    },
                    Request::Step(c) => Reply::Step(worker.step(c)?),
                }));
            }
            None if stdin.closed => break,
            None => {
                worker.host.round()?;
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
    conns: &Rc<Vec<Conn>>,
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
            let conn = &conns[i as usize];
            while let Ok(frame) = rx.recv().await {
                let now = clock.now_ns();
                let mut r = recorder.borrow_mut();
                let mut in_flight = conn.in_flight.borrow_mut();
                messages(&mut partial, frame.payload(), unit, |m| {
                    let m = if ws {
                        wsclient::decode(m, size).unwrap_or_else(|e| panic!("connection {i}: {e}"))
                    } else {
                        m
                    };
                    let sent_ns = in_flight.pop_front();
                    if let Some(r) = r.as_mut() {
                        r.record(m, now);
                        if let Some(sent_ns) = sent_ns {
                            r.service_rtt.record(now.saturating_sub(sent_ns));
                        }
                    }
                });
            }
        })?;
    }
    let deadline = host.clock.now_ns() + CONNECT_LIMIT.as_nanos() as u64;
    while connected.borrow().iter().any(Option::is_none) {
        if host.round()? > deadline {
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
        let origin = clock.mono_of_unix(c.start_unix_ns);
        let schedule = Schedule::for_rate(self.args.senders, c.rate_pps, origin);
        let window = [
            origin + c.warmup_ms * 1_000_000,
            origin + (c.warmup_ms + c.step_ms) * 1_000_000,
        ];
        let measured_from = schedule.count_before(window[0]);
        let end = schedule.count_before(window[1]);
        *self.recorder.borrow_mut() =
            Some(Recorder::new(c.step, schedule, measured_from, end, window));
        let sent: Rc<RefCell<Option<Sent>>> = Rc::default();
        let task = send_step(
            SendStep {
                handle: self.handle.clone(),
                senders: self.senders.clone(),
                conns: self.conns.clone(),
                clock,
                pacer: Pacer::new(schedule, end, self.args.send_budget),
                schedule,
                measured_from,
                window,
                budget: c.budget,
                step: c.step,
                size: self.args.size,
                target: self.target,
            },
            sent.clone(),
        );
        spawn(host, task)?;
        let drain_end = window[1] + c.drain_ms * 1_000_000;
        loop {
            let now = host.round()?;
            if host::stopped() {
                return Err("stopped".into());
            }
            if sent.borrow().is_some() && now >= drain_end {
                break;
            }
        }
        let r = self.recorder.borrow_mut().take().expect("set above");
        let s = sent.borrow_mut().take().expect("checked above");
        Ok(StepResult {
            step: c.step,
            offered_pps: schedule.offered_pps(),
            measured: r.measured(),
            received: r.received,
            unsent: s.unsent,
            sent_in_window: s.in_window,
            received_in_window: r.received_in_window,
            latency: r.latency.to_sparse(),
            send_lag: s.lag.to_sparse(),
            backlog_at_end: s.backlog_at_end,
            service_rtt: r.service_rtt.to_sparse(),
            budget_waits: s.budget_waits,
        })
    }
}

/// One step's send task.
struct SendStep {
    handle: NetHandle,
    senders: Rc<RefCell<Senders>>,
    conns: Rc<Vec<Conn>>,
    clock: Clock,
    pacer: Pacer,
    schedule: Schedule,
    measured_from: u64,
    /// Monotonic ns of the measurement window; sending stops at its end.
    window: [u64; 2],
    budget: Option<u32>,
    step: u32,
    size: usize,
    target: IpEndpoint,
}

/// An empty frame if the pool has one now; never waits (a cut-off send task must not be left
/// parked on the pool).
async fn try_tx_frame(handle: &NetHandle) -> Option<TxFrame> {
    let mut wait = handle.tx_frame();
    std::future::poll_fn(|cx| {
        Poll::Ready(match Pin::new(&mut wait).poll(cx) {
            Poll::Ready(frame) => Some(frame),
            Poll::Pending => None,
        })
    })
    .await
}

/// Sends the step's messages on schedule through the pacer (at most its budget per poll),
/// yielding to the loop between batches, until all are sent or the window ends. Send lag is
/// recorded for measured messages only (the warmup absorbs ARP and connection start), and not for
/// owed messages (a full connection, or one at its budget).
async fn send_step(s: SendStep, done: Rc<RefCell<Option<Sent>>>) {
    let SendStep {
        handle,
        senders,
        conns,
        clock,
        mut pacer,
        schedule,
        measured_from,
        window,
        budget,
        step,
        size,
        target,
    } = s;
    let mut out = Sent {
        lag: Histogram::new(),
        backlog_at_end: 0,
        unsent: 0,
        in_window: 0,
        budget_waits: 0,
    };
    // per sender, the last message refused for its budget (each counts once however often
    // it is retried)
    let mut refused = vec![u64::MAX; schedule.senders as usize];
    let ws = matches!(&*senders.borrow(), Senders::Ws(_));
    loop {
        let now = clock.now_ns();
        if pacer.done() || now >= window[1] {
            break;
        }
        pacer.begin(now);
        while let Some(k) = pacer.next_try() {
            let (sender, seq) = schedule.split(k);
            let conn = conns.get(sender as usize);
            if let (Some(cap), Some(conn)) = (budget, conn)
                && conn.in_flight.borrow().len() >= cap as usize
            {
                if k >= measured_from && refused[sender as usize] != k {
                    refused[sender as usize] = k;
                    out.budget_waits += 1;
                }
                pacer.result(k, Outcome::Blocked);
                continue;
            }
            let Some(mut frame) = try_tx_frame(&handle).await else {
                pacer.result(k, Outcome::Stop);
                continue;
            };
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
            let owed = pacer.is_owed(k);
            let outcome = senders.borrow_mut().send(sender, frame, target);
            if outcome == Outcome::Sent {
                let now = clock.now_ns();
                out.in_window += u64::from((window[0]..window[1]).contains(&now));
                if let Some(c) = conn {
                    c.in_flight.borrow_mut().push_back(now);
                }
                if k >= measured_from && !owed {
                    out.lag.record(now.saturating_sub(sched_ns));
                }
            }
            pacer.result(k, outcome);
        }
        pacer.finish(clock.now_ns());
        switch::yield_now().await;
    }
    out.backlog_at_end = pacer.backlog_at_end();
    out.unsent = pacer.unsent_from(measured_from);
    *done.borrow_mut() = Some(out);
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
