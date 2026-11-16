//! The same TCP client as `tcp_client_local.rs`, connected through a running
//! `switch-junctiond` instead of owning the NIC.
//!
//! ```text
//! cargo build -p switch-examples --features net --example tcp_client_junction
//! taskset -c 2 target/debug/examples/tcp_client_junction /tmp/example-junction/control.sock 192.0.2.1:7000
//! ```
//!
//! Pin the process before it connects: `Session::connect` starts a background thread
//! that inherits the caller's CPU affinity.
//!
//! The peer is the Python TCP server in `tcp_client_local.rs`. Start the daemon with
//! `taskset -c 0 target/debug/switch-junctiond --config examples/tap.json --dir /tmp/example-junction --control-cpus 1`,
//! then assign `192.0.2.1/24` to `cdnic0` and bring it up before starting the client.

use std::cell::Cell;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::Path;
use std::rc::Rc;
use std::time::{Duration, Instant as WallInstant};

use switch::Runtime;
use switch::junction::session::Session;
use switch::net::{tcp, Error, Instant, IpEndpoint, IpListenEndpoint, Net, NetHandle, RemoteConfig};

const BUFFER: usize = 64 * 1024;
const IDLE_SLEEP: Duration = Duration::from_millis(1);
const LIMIT: Duration = Duration::from_secs(30);
/// Socket slots this session asks the daemon for.
const MAX_SOCKETS: u16 = 1;

/// Monotonic time since `start`: Stack time in microseconds and runtime ticks in nanoseconds.
struct Clock {
    start: WallInstant,
}

impl Clock {
    fn ns(&self) -> u64 {
        u64::try_from(self.start.elapsed().as_nanos()).expect("elapsed time fits u64 nanoseconds")
    }

    /// Stack time and runtime tick from the same clock sample.
    fn sample(&self) -> (Instant, u64) {
        let nanos = self.ns();
        let micros = i64::try_from(nanos / 1_000).expect("elapsed time fits i64 microseconds");
        (Instant::from_micros(micros), nanos)
    }
}

/// There is no ephemeral port allocator; the local port must be nonzero.
const LOCAL_PORT: u16 = 40000;

/// Runs the conversation and leaves its result in `done` for the drive loop.
#[switch::task]
async fn hello(
    handle: NetHandle,
    server: IpEndpoint,
    socket: tcp::Config,
    done: Rc<Cell<Option<Result<(), Error>>>>,
) {
    done.set(Some(talk(&handle, server, socket).await));
}

async fn talk(handle: &NetHandle, server: IpEndpoint, socket: tcp::Config) -> Result<(), Error> {
    let local = IpListenEndpoint { addr: None, port: LOCAL_PORT };
    let stream = handle.tcp_connect(socket, server, local).await?;
    let (mut rx, mut tx) = stream.split();

    let line = b"hello\n";
    let mut frame = handle.tx_frame().await;
    frame.as_mut()[..line.len()].copy_from_slice(line);
    frame.truncate(line.len());
    let frame = frame.freeze();
    tx.room(line.len()).await?;
    tx.try_send(frame).map_err(|(_frame, error)| error)?;

    loop {
        match rx.recv().await {
            Ok(frame) => print!("{}", String::from_utf8_lossy(frame.payload())),
            Err(Error::Finished) => break,
            Err(error) => return Err(error),
        }
    }

    tx.shutdown();
    tx.delivered().await;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let (Some(control), Some(server)) = (args.next(), args.next()) else {
        return Err("usage: tcp_client_junction CONTROL_SOCKET HOST:PORT".into());
    };
    let Some(SocketAddr::V4(server)) = server.to_socket_addrs()?.find(SocketAddr::is_ipv4) else {
        return Err("server has no IPv4 address".into());
    };

    let socket = tcp::Config { rx_capacity: BUFFER, tx_capacity: BUFFER, mss_cap: None };
    let session = Session::connect(Path::new(&control), MAX_SOCKETS)
        .map_err(|error| format!("connect {control}: {error}"))?;
    // The daemon's MTU: its frame capacity plus 28 bytes of IP and UDP headers.
    let mtu = session.frame_capacity() as usize + 28;
    let mut net = Net::remote(session, RemoteConfig { tx_frames: tcp::tx_mbuf_budget(&socket, mtu) });

    let clock = Clock { start: WallInstant::now() };
    let mut rt = Runtime::new(clock.ns());
    let done = Rc::new(Cell::new(None));
    let server = IpEndpoint::new((*server.ip()).into(), server.port());
    rt.spawner()
        .spawn(hello(net.handle(), server, socket, Rc::clone(&done)))
        .map_err(|error| format!("spawn: {:?}", error.reason))?;

    let result = loop {
        if let Some(result) = done.take() {
            break result;
        }
        if clock.start.elapsed() > LIMIT {
            return Err(format!("not finished after {LIMIT:?}").into());
        }
        let (now, tick) = clock.sample();
        let moved = switch::net::round(&mut net, &mut rt, now, tick)
            .map_err(|error| format!("round: {error:?}"))?;
        if !moved {
            std::thread::sleep(IDLE_SLEEP);
        }
    };

    rt.shutdown();
    let mut session = net.into_session().map_err(|_net| "a socket or handle outlived the task")?;
    let deadline = WallInstant::now() + Duration::from_secs(10);
    while !session.closed() {
        // Until the end of the session can be sent, `finish` fails: poll and try again.
        let _ = session.finish();
        session.poll(|_, _| {}, |_, _, _| {}).map_err(|error| format!("finish session: {error}"))?;
        if WallInstant::now() >= deadline {
            return Err("session finish timed out".into());
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    println!("session finished");
    result.map_err(|error| format!("connection: {error}").into())
}
