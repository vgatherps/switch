//! Autobahn testsuite driver for switch-ws (test tool, built with feature `e2e`). In the `server` role it runs a ws:// echo server for `wstest -m fuzzingclient`; in the `client` role it runs switch-ws's ws:// client against `wstest -m fuzzingserver`. Either role runs on a tap device with the local backend, or through `switch-junctiond` with `--backend junction`. Normally started by `make -C conduit docker-autobahn` (or `docker-autobahn-junction`), which then checks the report with `conduit/autobahn/check.py`.
//!
//! Usage: `switch-ws-autobahn server --expect-connections N | client --expect-cases N [--backend conduit|junction] [--junction-socket PATH] -- <tester> [args...]`
//!
//! See `docs/contributing/conformance.md`.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fmt;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process;
use std::rc::Rc;
use std::time::Duration;

use switch_junction::session::Session;
use switch_ws::{Kind, Limits};
use switch_ws::net::{Error, Failure as WsFailure, Ws, accept_ws, connect_ws};
use switch_runtime::{Runtime, Spawner};
use switch_net::{
    Error as NetError, Instant, IpEndpoint, IpListenEndpoint, Net, NetConfig, NetHandle, RemoteConfig,
    RoundError, round, tcp, within,
};
use switch_tap_echo::driver::{self, ECHO_PORT, EXIT_ERROR, IFACE, TESTER_IP, SWITCH_IP};

#[cfg(test)]
mod tests;

const NAME: &str = "switch-ws-autobahn";
/// The agent name in runCase and updateReports, and in the expected results.
const AGENT: &str = "switch-ws";
const BUFFER: usize = 128 * 1024;
const SOCKET: tcp::Config = tcp::Config { rx_capacity: BUFFER, tx_capacity: BUFFER, mss_cap: None };
const INGRESS_BUDGET: usize = 32;
/// Autobahn fuzzingserver's port on the tester address.
const FUZZINGSERVER_PORT: u16 = 9001;
/// Local port of the client's connection 0; connection i uses `FIRST_LOCAL_PORT + i`.
const FIRST_LOCAL_PORT: u16 = 49152;
/// The client role's marker directory, relative to the working directory: the tester writes
/// `server-ready` there once fuzzingserver listens; the client writes `client-done` once its
/// sequence completed, for peer.sh to stop fuzzingserver.
const CLIENT_RUN: &str = "autobahn/run/client";
/// Longest a connection may live, from its acceptance (server) or connect (client).
const DEADLINE: Duration = Duration::from_secs(120);
/// The longest sleep of a loop iteration that moved nothing.
const IDLE_SLEEP: Duration = Duration::from_millis(1);
/// Counted from the start of the client role. switch-tester.sh's own 60 s wait for
/// fuzzingserver starts once it saw the netdev up, which can be later; this limit can then
/// expire first by that gap, and expiry only fails the run.
const SERVER_READY_LIMIT: Duration = Duration::from_secs(60);
const SERVER_READY_POLL: Duration = Duration::from_millis(10);
/// Maximum wait for the host to acknowledge session retirement.
const FINISH_LIMIT: Duration = Duration::from_secs(60);

fn usage() -> String {
    format!(
        "\
usage: {NAME} server --expect-connections N [--backend conduit|junction] [--junction-socket PATH] -- <tester> [args...]
       {NAME} client --expect-cases N [--backend conduit|junction] [--junction-socket PATH] -- <tester> [args...]

server: runs switch-ws's WebSocket echo server on switch at ws://{SWITCH_IP}:{ECHO_PORT} until
<tester> exits: one connection at a time, Text and Binary echoed, max_sockets N + 1.
Fails (exit {EXIT_ERROR}) if a connection's upgrade fails, a connection outlives {DEADLINE:?},
or the accepted count is not N.

client: waits (at most {SERVER_READY_LIMIT:?}) for the file {CLIENT_RUN}/server-ready, then runs
switch-ws's client of Autobahn's fuzzingserver at ws://{TESTER_IP}:{FUZZINGSERVER_PORT} as agent
{AGENT}: getCaseCount (must report N), runCase 1..=N echoing Text and Binary, updateReports;
one connection at a time from local port {FIRST_LOCAL_PORT} + i, max_sockets N + 2. Writes the
file {CLIENT_RUN}/client-done once all completed, then turns the Net until <tester> exits.
Fails (exit {EXIT_ERROR}) if a connection fails to connect or upgrade, outlives {DEADLINE:?},
a control connection misbehaves, or the sequence did not complete.

Both print the connection count and how the connections ended on stdout, on netdev {IFACE},
otherwise with the switch-tap-echo driver's configuration and exit statuses (see switch-tap-echo's usage).
The backend defaults to conduit. Junction requires --junction-socket and a running switch-junctiond;
the app opens no EAL, tap device or Stack, and finishes its session before exiting.
"
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Role {
    /// Echo server; `connections` must be accepted.
    Server { connections: usize },
    /// fuzzingserver's client; markers in `run`.
    Client { cases: usize, run: PathBuf },
}

impl Role {
    /// The listener (server) or both control connections (client) on top of one socket per
    /// connection, each kept until reaped: a socket closed first keeps its TIME-WAIT.
    fn max_sockets(&self) -> usize {
        match self {
            Self::Server { connections } => connections + 1,
            Self::Client { cases, .. } => cases + 2,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Backend {
    Conduit,
    Junction(PathBuf),
}

fn parse(args: &[OsString]) -> Result<(Role, Backend, Vec<OsString>), String> {
    let [mode, flag, count, rest @ ..] = args else {
        return Err("expected: server|client --expect-... N [options] -- <tester> [args...]".to_owned());
    };
    let expected_flag = match mode.to_str() {
        Some("server") => "--expect-connections",
        Some("client") => "--expect-cases",
        _ => return Err(format!("unknown mode {mode:?}")),
    };
    if flag != expected_flag {
        return Err(format!("expected {expected_flag}, got {flag:?}"));
    }
    let n = count
        .to_str()
        .and_then(|text| text.parse::<usize>().ok())
        .filter(|&n| n >= 1)
        .ok_or_else(|| format!("{expected_flag} needs a positive integer, got {count:?}"))?;
    let mut rest = rest;
    let mut backend = None;
    let mut socket = None;
    while rest.first().is_some_and(|arg| arg != "--") {
        let [flag, value, tail @ ..] = rest else {
            return Err("backend options need a value".to_owned());
        };
        match flag.to_str() {
            Some("--backend") if backend.is_none() => backend = Some(value),
            Some("--junction-socket") if socket.is_none() && !value.is_empty() => socket = Some(PathBuf::from(value)),
            _ => return Err(format!("unknown or repeated option {flag:?}")),
        }
        rest = tail;
    }
    let [_, tester @ ..] = rest else {
        return Err("missing -- before tester command".to_owned());
    };
    if tester.is_empty() {
        return Err("missing tester command after --".to_owned());
    }
    let backend = match backend.map(|value| value.to_str().ok_or("backend must be UTF-8")).transpose()?.unwrap_or("conduit") {
        "conduit" if socket.is_none() => Backend::Conduit,
        "junction" => Backend::Junction(socket.ok_or("--backend junction requires --junction-socket PATH")?),
        other => return Err(format!("invalid backend or socket for backend {other:?}")),
    };
    let role = if mode == "server" {
        Role::Server { connections: n }
    } else {
        // connection cases + 1 (updateReports) needs a local port
        let last = u16::MAX - FIRST_LOCAL_PORT;
        if n >= usize::from(last) {
            return Err(format!("{n} cases: local ports from {FIRST_LOCAL_PORT} fit at most {}", last - 1));
        }
        Role::Client { cases: n, run: PathBuf::from(CLIENT_RUN) }
    };
    Ok((role, backend, tester.to_vec()))
}

fn main() {
    let args: Vec<OsString> = std::env::args_os().skip(1).collect();
    let (role, backend, tester) = match parse(&args) {
        Ok(parsed) => parsed,
        Err(message) => {
            driver::report(NAME, format_args!("{message}\n\n{}", usage()));
            process::exit(EXIT_ERROR);
        }
    };
    let code = match backend {
        Backend::Conduit => driver::run(NAME, &tester, role.max_sockets(), BUFFER, None, |stack, start, stop| {
            let mut net = Net::new(stack, NetConfig { ingress_budget: INGRESS_BUDGET });
            let result = drive(&mut net, &|| start.elapsed(), stop, &role, DEADLINE).map_err(|e| e.to_string());
            let stack = net.into_stack().ok().expect("drive dropped every handle and socket");
            (stack, result)
        }),
        Backend::Junction(socket) => driver::run_tester(NAME, &tester, |start, stop| {
            let max_sockets = u16::try_from(role.max_sockets()).map_err(|_| "junction socket count exceeds 65535")?;
            let session = Session::connect(&socket, max_sockets).map_err(|e| format!("connect junction: {e}"))?;
            let mut net = Net::remote(session, RemoteConfig {
                tx_frames: tcp::tx_mbuf_budget(&SOCKET, usize::from(driver::MTU)),
            });
            let driven = drive(&mut net, &|| start.elapsed(), stop, &role, DEADLINE).map_err(|e| e.to_string());
            let mut session = net.into_session().ok().expect("drive dropped every handle and socket");
            let finished = finish_session(&mut session);
            driven.and(finished)
        }),
    };
    process::exit(code);
}

fn finish_session(session: &mut Session) -> Result<(), String> {
    let start = std::time::Instant::now();
    while !session.closed() {
        let _ = session.finish();
        session.poll(|_, _| {}, |_, _, _| {}).map_err(|e| format!("finish junction session: {e}"))?;
        if start.elapsed() >= FINISH_LIMIT {
            return Err("junction session retirement timed out".to_owned());
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    Ok(())
}

/// How the connections went.
#[derive(Debug, Default)]
struct Tally {
    connections: usize,
    endings: BTreeMap<&'static str, usize>,
}

/// Why a run failed.
#[derive(Debug)]
enum Failure {
    Spawn,
    Round(RoundError),
    /// The server role's next listener.
    Listen(NetError),
    /// The tester's server-ready marker did not appear within `SERVER_READY_LIMIT`.
    NoServerReady,
    /// Writing the client-done marker.
    Marker(std::io::Error),
    /// Connection `n` (0-based) failed.
    Connection { n: usize, why: Why },
    /// The server role accepted `got` connections, not `want`.
    Accepted { got: usize, want: usize },
    /// The client role's sequence did not complete: `started` of `of` connections.
    Incomplete { started: usize, of: usize },
}

/// Why one connection failed.
#[derive(Debug)]
enum Why {
    Open(NetError),
    Upgrade(Error),
    /// Still open after this per-connection deadline.
    Deadline(Duration),
    /// getCaseCount's message, parsed, was not the expected count.
    CaseCount(Option<usize>),
    /// getCaseCount's connection ended without a message.
    NoCaseCount,
    /// Receiving getCaseCount's message failed.
    CaseCountRecv(Error),
    Close(Error),
    /// A control connection got a message after its own close.
    Unexpected { kind: Kind, len: usize },
    /// A control connection's close got no reply.
    NoCloseReply(Error),
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Spawn => f.write_str("spawn the role task"),
            Self::Round(e) => write!(f, "turn or poll: {e:?}"),
            Self::Listen(e) => write!(f, "listen: {e:?}"),
            Self::NoServerReady => write!(f, "no {CLIENT_RUN}/server-ready within {SERVER_READY_LIMIT:?}"),
            Self::Marker(e) => write!(f, "writing {CLIENT_RUN}/client-done: {e}"),
            Self::Connection { n, why } => write!(f, "connection {n}: {why}"),
            Self::Accepted { got, want } => write!(f, "accepted {got} connections, expected {want}"),
            Self::Incomplete { started, of } => write!(f, "sequence did not complete: {started} of {of} connections started"),
        }
    }
}

impl fmt::Display for Why {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Open(e) => write!(f, "open: {e:?}"),
            Self::Upgrade(e) => write!(f, "upgrade: {e}"),
            Self::Deadline(limit) => write!(f, "still open after {limit:?}"),
            Self::CaseCount(n) => write!(f, "case count {n:?}"),
            Self::NoCaseCount => f.write_str("closed without the case count"),
            Self::CaseCountRecv(e) => write!(f, "case count: {e}"),
            Self::Close(e) => write!(f, "close: {e}"),
            Self::Unexpected { kind, len } => write!(f, "unexpected {kind:?} message of {len} bytes after the close"),
            Self::NoCloseReply(e) => write!(f, "no close reply: {e}"),
        }
    }
}

/// Runs `role` as a switch task on `net`, turning it on `elapsed` (the time since the Stack's
/// zero) until `stop`. The server role must have accepted its connections by then; the client
/// role must have completed its sequence. A task error ends the run at once.
fn drive(
    net: &mut Net,
    elapsed: &dyn Fn() -> Duration,
    stop: &mut dyn FnMut() -> bool,
    role: &Role,
    deadline: Duration,
) -> Result<(), Failure> {
    drive_with(net.handle(), |rt, now, tick| {
        let moved = round(net, rt, now, tick)?;
        Ok((moved, net.poll_at()))
    }, elapsed, stop, role, deadline)
}

fn drive_with(
    handle: NetHandle,
    mut round: impl FnMut(&mut Runtime, Instant, u64) -> Result<(bool, Option<Instant>), RoundError>,
    elapsed: &dyn Fn() -> Duration,
    stop: &mut dyn FnMut() -> bool,
    role: &Role,
    deadline: Duration,
) -> Result<(), Failure> {
    let mut rt = Runtime::new(u64::try_from(elapsed().as_nanos()).expect("elapsed fits u64 ns"));
    let tally: Rc<RefCell<Tally>> = Rc::default();
    let outcome: Rc<RefCell<Option<Result<(), Failure>>>> = Rc::default();
    let task = run_role(handle, rt.spawner(), role.clone(), deadline, tally.clone(), outcome.clone());
    rt.spawner().spawn(task).map_err(|_| Failure::Spawn)?;
    let turned = loop {
        let since = elapsed();
        let now = Instant::from_micros(i64::try_from(since.as_micros()).expect("elapsed fits i64 microseconds"));
        let tick = u64::try_from(since.as_nanos()).expect("elapsed fits u64 ns");
        match round(&mut rt, now, tick) {
            Ok((true, _)) => {}
            // Nothing moved: sleep until the Stack's next timer, at most 1 ms (wstest shares the CPU).
            Ok((false, at)) => match at {
                Some(at) if at <= now => {}
                at => std::thread::sleep(at.map_or(IDLE_SLEEP, |at| {
                    Duration::from_micros((at - now).total_micros()).min(IDLE_SLEEP)
                })),
            },
            Err(e) => break Err(Failure::Round(e)),
        }
        if outcome.borrow().as_ref().is_some_and(Result::is_err) || stop() {
            break Ok(());
        }
    };
    rt.shutdown();
    let tally = tally.borrow();
    let _ = writeln!(std::io::stdout(), "{NAME}: {} connections; endings {:?}", tally.connections, tally.endings);
    turned?;
    match (role, outcome.borrow_mut().take()) {
        (_, Some(Err(e))) => Err(e),
        (&Role::Server { connections }, _) if tally.connections != connections => {
            Err(Failure::Accepted { got: tally.connections, want: connections })
        }
        (Role::Server { .. }, _) | (Role::Client { .. }, Some(Ok(()))) => Ok(()),
        (Role::Client { cases, .. }, None) => Err(Failure::Incomplete { started: tally.connections, of: cases + 2 }),
    }
}

/// `role`'s run as a switch task; its result goes to `outcome`.
#[switch_runtime::task]
async fn run_role(
    handle: NetHandle,
    spawner: Spawner,
    role: Role,
    deadline: Duration,
    tally: Rc<RefCell<Tally>>,
    outcome: Rc<RefCell<Option<Result<(), Failure>>>>,
) {
    let result = match role {
        Role::Server { .. } => server(&handle, &spawner, deadline, &tally).await,
        Role::Client { cases, run } => client(&handle, &spawner, cases, &run, deadline, &tally).await,
    };
    *outcome.borrow_mut() = Some(result);
}

/// The echo server on `ECHO_PORT`, one connection at a time, forever. The next connection's
/// listener opens as soon as one is accepted, so a client's next SYN finds it even while the
/// previous connection still closes.
async fn server(handle: &NetHandle, spawner: &Spawner, deadline: Duration, tally: &RefCell<Tally>) -> Result<(), Failure> {
    let local = IpListenEndpoint { addr: None, port: ECHO_PORT };
    let listen = || handle.tcp_accept(SOCKET, local);
    let mut next = listen();
    let mut buf = Vec::new();
    loop {
        let stream = next.await.map_err(Failure::Listen)?;
        next = listen();
        let n = tally.borrow().connections;
        tally.borrow_mut().connections += 1;
        let session = async {
            let mut ws = accept_ws(handle, stream, Limits::default()).await.map_err(Why::Upgrade)?;
            Ok(echo(&mut ws, &mut buf).await)
        };
        let ending = within(spawner, deadline, session).await.unwrap_or(Err(Why::Deadline(deadline)));
        let ending = ending.map_err(|why| Failure::Connection { n, why })?;
        *tally.borrow_mut().endings.entry(ending).or_default() += 1;
    }
}

/// fuzzingserver's client: once `run`/server-ready exists, getCaseCount (which must say
/// `cases`), runCase 1..=cases and updateReports, then writes `run`/client-done.
async fn client(
    handle: &NetHandle,
    spawner: &Spawner,
    cases: usize,
    run: &Path,
    deadline: Duration,
    tally: &RefCell<Tally>,
) -> Result<(), Failure> {
    let ready = run.join("server-ready");
    let waiting = std::time::Instant::now();
    while !ready.exists() {
        if waiting.elapsed() >= SERVER_READY_LIMIT {
            return Err(Failure::NoServerReady);
        }
        let poll = spawner.sleep(SERVER_READY_POLL.as_nanos() as u64).expect("the Runtime polling this task is open");
        poll.await.expect("a sleep in the task's own open Runtime");
    }
    let mut buf = Vec::new();
    for n in 0..=cases + 1 {
        let path = match n {
            0 => "/getCaseCount".to_owned(),
            n if n <= cases => format!("/runCase?case={n}&agent={AGENT}"),
            _ => format!("/updateReports?agent={AGENT}"),
        };
        tally.borrow_mut().connections += 1;
        let session = connection(handle, n, &path, cases, &mut buf);
        let ending = within(spawner, deadline, session).await.unwrap_or(Err(Why::Deadline(deadline)));
        let ending = ending.map_err(|why| Failure::Connection { n, why })?;
        *tally.borrow_mut().endings.entry(ending).or_default() += 1;
    }
    write_marker(&run.join("client-done")).map_err(Failure::Marker)
}

/// Client connection `n` to `path`. A runCase echoes until it ends, however it ends; a
/// control connection (getCaseCount, updateReports) closes, and must end with the close
/// handshake and no message besides getCaseCount's count.
async fn connection(handle: &NetHandle, n: usize, path: &str, cases: usize, buf: &mut Vec<u8>) -> Result<&'static str, Why> {
    let remote = IpEndpoint::new(TESTER_IP.into(), FUZZINGSERVER_PORT);
    let port = FIRST_LOCAL_PORT + u16::try_from(n).expect("parse bounded the cases");
    let local = IpListenEndpoint { addr: None, port };
    let stream = handle.tcp_connect(SOCKET, remote, local).await.map_err(Why::Open)?;
    let mut ws = connect_ws(handle, stream, &TESTER_IP.to_string(), FUZZINGSERVER_PORT, path, Limits::default()).await.map_err(Why::Upgrade)?;
    if (1..=cases).contains(&n) {
        return Ok(echo(&mut ws, buf).await);
    }
    if n == 0 {
        let count = match ws.recv().await {
            Ok(Some(m)) => m.text().and_then(|t| t.parse::<usize>().ok()),
            Ok(None) => return Err(Why::NoCaseCount),
            Err(e) => return Err(Why::CaseCountRecv(e)),
        };
        if count != Some(cases) {
            return Err(Why::CaseCount(count));
        }
    }
    ws.close(1000, "").await.map_err(Why::Close)?;
    match ws.recv().await {
        Ok(None) => Ok("close handshake"),
        Ok(Some(m)) => Err(Why::Unexpected { kind: m.kind(), len: m.bytes().len() }),
        Err(e) => Err(Why::NoCloseReply(e)),
    }
}

/// Echoes every Text and Binary message until the connection ends; returns how it ended. A
/// protocol error fails the connection with FIN and no close frame, and the peer's later bytes
/// are read and dropped until its FIN, so the socket closes without RST.
async fn echo(ws: &mut Ws, buf: &mut Vec<u8>) -> &'static str {
    loop {
        let kind = match ws.recv().await {
            Ok(Some(m)) => {
                buf.clear();
                buf.extend_from_slice(m.bytes());
                m.kind()
            }
            Ok(None) => return "close handshake",
            Err(Error::Failed(WsFailure::Protocol(_))) => {
                return match ws.linger().await {
                    Ok(()) => "protocol error",
                    Err(_) => "protocol error, then receive failed",
                };
            }
            Err(Error::Failed(WsFailure::PeerEof)) => return "peer ended the stream",
            Err(Error::Failed(_)) => return "connection failed",
            Err(e) => unreachable!("plain ws:// recv: {e}"),
        };
        if ws.send(kind, buf).await.is_err() {
            return "send failed";
        }
    }
}

/// Creates the empty file `path` atomically (write, then rename).
fn write_marker(path: &Path) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, "").and_then(|()| fs::rename(&tmp, path))
}
