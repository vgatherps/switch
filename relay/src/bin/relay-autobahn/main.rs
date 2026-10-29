//! relay's ws:// on Zhida for Autobahn (make docker-autobahn), as switch tasks on a switchnet
//! Net over the tap_echo driver: an echo server for fuzzingclient, or a client of
//! fuzzingserver. See `usage`.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::future::{Future, poll_fn};
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::process;
use std::rc::Rc;
use std::task::Poll;
use std::time::Duration;

use relay::switchnet::{Error, Ws, accept_ws, connect_ws};
use switch::{Runtime, Spawner};
use switchnet::testkit::boxed;
use switchnet::{Instant, IpEndpoint, IpListenEndpoint, Net, NetConfig, NetHandle, tcp};
use tap_echo::driver::{self, ECHO_PORT, EXIT_ERROR, IFACE, TESTER_IP, ZHIDA_IP};

#[cfg(test)]
mod tests;

const NAME: &str = "relay-autobahn";
/// The agent name in runCase and updateReports, and in the expected results.
const AGENT: &str = "relay";
const BUFFER: usize = 128 * 1024;
const SOCKET: tcp::Config = tcp::Config { rx_capacity: BUFFER, tx_capacity: BUFFER, mss_cap: None };
const INGRESS_BUDGET: usize = 32;
const MAX_IDLE_SLEEP: Duration = Duration::from_millis(1);
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
/// Counted from the start of the client role. zhida-tester.sh's own 60 s wait for
/// fuzzingserver starts once it saw the netdev up, which can be later; this limit can then
/// expire first by that gap, and expiry only fails the run.
const SERVER_READY_LIMIT: Duration = Duration::from_secs(60);
const SERVER_READY_POLL_MS: u64 = 10;

fn usage() -> String {
    format!(
        "\
usage: {NAME} server --expect-connections N -- <tester> [args...]
       {NAME} client --expect-cases N -- <tester> [args...]

server: runs relay's WebSocket echo server on Zhida at ws://{ZHIDA_IP}:{ECHO_PORT} until
<tester> exits: one connection at a time, Text and Binary echoed, max_sockets N + 1.
Fails (exit {EXIT_ERROR}) if a connection's upgrade fails, a connection outlives {DEADLINE:?},
or the accepted count is not N.

client: waits (at most {SERVER_READY_LIMIT:?}) for the file {CLIENT_RUN}/server-ready, then runs
relay's client of Autobahn's fuzzingserver at ws://{TESTER_IP}:{FUZZINGSERVER_PORT} as agent
{AGENT}: getCaseCount (must report N), runCase 1..=N echoing Text and Binary, updateReports;
one connection at a time from local port {FIRST_LOCAL_PORT} + i, max_sockets N + 2. Writes the
file {CLIENT_RUN}/client-done once all completed, then turns the Net until <tester> exits.
Fails (exit {EXIT_ERROR}) if a connection fails to connect or upgrade, outlives {DEADLINE:?},
a control connection misbehaves, or the sequence did not complete.

Both print the connection count and how the connections ended on stdout, on netdev {IFACE},
otherwise with the tap-echo driver's configuration and exit statuses (see tap-echo's usage).
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

fn parse(args: &[OsString]) -> Result<(Role, Vec<OsString>), String> {
    let [mode, flag, count, separator, tester @ ..] = args else {
        return Err("expected: server|client --expect-... N -- <tester> [args...]".to_owned());
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
    if separator != "--" {
        return Err(format!("expected --, got {separator:?}"));
    }
    if tester.is_empty() {
        return Err("missing tester command after --".to_owned());
    }
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
    Ok((role, tester.to_vec()))
}

fn main() {
    let args: Vec<OsString> = std::env::args_os().skip(1).collect();
    let (role, tester) = match parse(&args) {
        Ok(parsed) => parsed,
        Err(message) => {
            driver::report(NAME, format_args!("{message}\n\n{}", usage()));
            process::exit(EXIT_ERROR);
        }
    };
    let code = driver::run(NAME, &tester, role.max_sockets(), BUFFER, None, |stack, start, stop| {
        let mut net = Net::new(stack, NetConfig { ingress_budget: INGRESS_BUDGET });
        let result = drive(&mut net, &|| start.elapsed(), stop, &role, DEADLINE);
        let stack = net.into_stack().ok().expect("drive dropped every handle and socket");
        (stack, result)
    });
    process::exit(code);
}

/// How the connections went.
#[derive(Debug, Default)]
struct Tally {
    connections: usize,
    endings: BTreeMap<&'static str, usize>,
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
) -> Result<(), String> {
    let mut rt = Runtime::new(0);
    let tally: Rc<RefCell<Tally>> = Rc::default();
    let outcome: Rc<RefCell<Option<Result<(), String>>>> = Rc::default();
    let (handle, spawner) = (net.handle(), rt.spawner());
    let task = {
        let (tally, outcome, role) = (tally.clone(), outcome.clone(), role.clone());
        async move {
            let result = match role {
                Role::Server { .. } => server(&handle, &spawner, deadline, &tally).await,
                Role::Client { cases, run } => client(&handle, &spawner, cases, &run, deadline, &tally).await,
            };
            *outcome.borrow_mut() = Some(result);
        }
    };
    rt.spawner().spawn(boxed(Box::pin(task))).map_err(|_| "spawn the role task".to_owned())?;
    let result = turn_until(net, &mut rt, elapsed, stop, &outcome);
    rt.shutdown();
    let tally = tally.borrow();
    println!("{NAME}: {} connections; endings {:?}", tally.connections, tally.endings);
    result?;
    match (role, outcome.borrow_mut().take()) {
        (Role::Server { connections }, _) if tally.connections != *connections => {
            Err(format!("accepted {} connections, expected {connections}", tally.connections))
        }
        (Role::Server { .. }, _) | (Role::Client { .. }, Some(Ok(()))) => Ok(()),
        (Role::Client { cases, .. }, _) => {
            Err(format!("sequence did not complete: {} of {} connections started", tally.connections, cases + 2))
        }
    }
}

/// Turns the Net and polls the runtime until `stop`, or until the task failed. Sleeps (at most
/// `MAX_IDLE_SLEEP`, never past the Stack's next timer) only after a round that moved nothing.
fn turn_until(
    net: &mut Net,
    rt: &mut Runtime,
    elapsed: &dyn Fn() -> Duration,
    stop: &mut dyn FnMut() -> bool,
    outcome: &RefCell<Option<Result<(), String>>>,
) -> Result<(), String> {
    loop {
        let since = elapsed();
        let now = Instant::from_micros(i64::try_from(since.as_micros()).expect("elapsed fits i64 microseconds"));
        let turn = net.turn(now).map_err(|e| format!("turn: {e:?}"))?;
        let polled = rt.poll(u64::try_from(since.as_millis()).expect("elapsed fits u64 ms"));
        let polled = polled.map_err(|e| format!("runtime: {e:?}"))?;
        if let Some(Err(e)) = &*outcome.borrow() {
            return Err(e.clone());
        }
        if stop() {
            return Ok(());
        }
        if turn.moved() || turn.tx_pending > 0 || turn.rx_staged > 0 || polled.tasks_polled > 0 {
            continue;
        }
        let sleep = match net.poll_at() {
            None => MAX_IDLE_SLEEP,
            Some(at) if at <= now => continue,
            Some(at) => Duration::from_micros((at - now).total_micros()).min(MAX_IDLE_SLEEP),
        };
        std::thread::sleep(sleep);
    }
}

/// `f`, or an error once `limit` passed first (dropping `f`, and the connection with it).
async fn within<T>(spawner: &Spawner, limit: Duration, f: impl Future<Output = T>) -> Result<T, String> {
    let ms = u64::try_from(limit.as_millis()).expect("limit fits u64 ms");
    let sleep = spawner.sleep(ms).map_err(|e| format!("sleep: {e:?}"))?;
    let (mut f, mut sleep) = (pin!(f), pin!(sleep));
    poll_fn(|cx| match f.as_mut().poll(cx) {
        Poll::Ready(v) => Poll::Ready(Ok(v)),
        Poll::Pending => sleep.as_mut().poll(cx).map(|slept| match slept {
            Ok(()) => Err(format!("still open after {limit:?}")),
            Err(e) => Err(format!("sleep: {e:?}")),
        }),
    })
    .await
}

/// The echo server on `ECHO_PORT`, one connection at a time, forever. The next connection's
/// listener opens as soon as one is accepted, so a client's next SYN finds it even while the
/// previous connection still closes.
async fn server(handle: &NetHandle, spawner: &Spawner, deadline: Duration, tally: &RefCell<Tally>) -> Result<(), String> {
    let local = IpListenEndpoint { addr: None, port: ECHO_PORT };
    let listen = || handle.tcp_accept(SOCKET, local).map_err(|e| format!("listen: {e:?}"));
    let mut next = listen()?;
    let mut buf = Vec::new();
    loop {
        let stream = next.await;
        next = listen()?;
        let n = tally.borrow().connections;
        tally.borrow_mut().connections += 1;
        let session = async {
            let mut ws = accept_ws(handle, stream).await.map_err(|e| format!("upgrade: {e}"))?;
            Ok::<_, String>(echo(&mut ws, &mut buf).await)
        };
        let ending = within(spawner, deadline, session).await.and_then(|r| r).map_err(|e| format!("connection {n}: {e}"))?;
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
) -> Result<(), String> {
    let ready = run.join("server-ready");
    let mut waited = Duration::ZERO;
    while !ready.exists() {
        if waited >= SERVER_READY_LIMIT {
            return Err(format!("no {} within {SERVER_READY_LIMIT:?}", ready.display()));
        }
        spawner.sleep(SERVER_READY_POLL_MS).map_err(|e| format!("sleep: {e:?}"))?.await.map_err(|e| format!("sleep: {e:?}"))?;
        waited += Duration::from_millis(SERVER_READY_POLL_MS);
    }
    let mut buf = Vec::new();
    for i in 0..=cases + 1 {
        let path = match i {
            0 => "/getCaseCount".to_owned(),
            i if i <= cases => format!("/runCase?case={i}&agent={AGENT}"),
            _ => format!("/updateReports?agent={AGENT}"),
        };
        tally.borrow_mut().connections += 1;
        let session = connection(handle, i, &path, cases, &mut buf);
        let ending = within(spawner, deadline, session).await.and_then(|r| r).map_err(|e| format!("connection {i} {path}: {e}"))?;
        *tally.borrow_mut().endings.entry(ending).or_default() += 1;
    }
    write_marker(&run.join("client-done"))
}

/// Client connection `i` to `path`. A runCase echoes until it ends, however it ends; a
/// control connection (getCaseCount, updateReports) closes, and must end with the close
/// handshake and no message besides getCaseCount's count.
async fn connection(handle: &NetHandle, i: usize, path: &str, cases: usize, buf: &mut Vec<u8>) -> Result<&'static str, String> {
    let remote = IpEndpoint::new(TESTER_IP.into(), FUZZINGSERVER_PORT);
    let port = FIRST_LOCAL_PORT + u16::try_from(i).expect("parse bounded the cases");
    let local = IpListenEndpoint { addr: None, port };
    let connecting = handle.tcp_connect(SOCKET, remote, local).map_err(|e| format!("connect: {e:?}"))?;
    let stream = connecting.await.map_err(|e| format!("not established: {e:?}"))?;
    let mut ws =
        connect_ws(handle, stream, &TESTER_IP.to_string(), FUZZINGSERVER_PORT, path).await.map_err(|e| format!("upgrade: {e}"))?;
    if (1..=cases).contains(&i) {
        return Ok(echo(&mut ws, buf).await);
    }
    if i == 0 {
        let count = match ws.recv().await {
            Ok(Some(m)) => m.text().and_then(|t| t.parse::<usize>().ok()),
            Ok(None) => return Err("closed without the case count".to_owned()),
            Err(e) => return Err(format!("case count: {e}")),
        };
        if count != Some(cases) {
            return Err(format!("case count {count:?}, expected {cases}"));
        }
    }
    ws.close(1000, "").await.map_err(|e| format!("close: {e}"))?;
    match ws.recv().await {
        Ok(None) => Ok("close handshake"),
        Ok(Some(m)) => Err(format!("unexpected {:?} message of {} bytes", m.kind(), m.bytes().len())),
        Err(e) => Err(format!("no close reply: {e}")),
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
            Err(Error::Protocol(_)) => {
                return match ws.linger().await {
                    Ok(()) => "protocol error",
                    Err(_) => "protocol error, then receive failed",
                };
            }
            Err(Error::Recv(_)) => return "receive failed",
            Err(Error::Send(_)) => return "send failed",
            Err(e) => unreachable!("plain ws:// recv: {e}"),
        };
        if ws.send(kind, buf).await.is_err() {
            return "send failed";
        }
    }
}

/// Creates the empty file `path` atomically (write, then rename).
fn write_marker(path: &Path) -> Result<(), String> {
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, "").and_then(|()| fs::rename(&tmp, path)).map_err(|e| format!("writing {}: {e}", path.display()))
}
