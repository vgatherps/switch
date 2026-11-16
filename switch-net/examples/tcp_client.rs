//! Connects to a TCP server from a switch task over the local backend, sends one line,
//! prints what the server sends back, and shuts down once the server closes its side.
//!
//! ```text
//! cargo build -p switch-net --example tcp_client
//! taskset -c 3 target/debug/examples/tcp_client tap.json 192.0.2.1:7000
//! ```
//!
//! `switch_open::open` needs a process allowed on exactly one CPU, hence `taskset`.
//! To try it without a NIC (Linux with `CAP_NET_ADMIN` and `/dev/net/tun`, or the
//! Docker image), use `tap.json` next to this file, start a kernel-side server, start
//! the client, and give the tap device its kernel address once the client created it:
//!
//! ```text
//! python3 -c 'import socket; s = socket.create_server(("0.0.0.0", 7000)); c, _ = s.accept(); c.recv(100); c.sendall(b"tick\n"); c.close()' &
//! taskset -c 3 target/debug/examples/tcp_client tap.json 192.0.2.1:7000 &
//! until ip link show cdnic0 >/dev/null 2>&1; do sleep 0.1; done
//! ip addr add 192.0.2.1/24 dev cdnic0 && ip link set cdnic0 up
//! wait
//! ```

use std::cell::Cell;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::Path;
use std::rc::Rc;
use std::time::{Duration, Instant as WallInstant};

use dpdk_smoltcp::DEFAULT_NEIGHBOR_CAPACITY;
use switch::Runtime;
use switch_net::{tcp, Error, Instant, IpEndpoint, IpListenEndpoint, Net, NetConfig, NetHandle};
use switch_netconf::Config;
use switch_open::{open, Sizing};

const BUFFER: usize = 64 * 1024;
const INGRESS_BUDGET: usize = 32;
/// The longest sleep after a round that moved nothing: the kernel peer shares the CPU.
const IDLE_SLEEP: Duration = Duration::from_millis(1);
const LIMIT: Duration = Duration::from_secs(30);

// ANCHOR: clock
/// Monotonic time since `start`: Stack time in microseconds and runtime ticks in nanoseconds.
struct Clock {
    start: WallInstant,
}

impl Clock {
    fn now(&self) -> Instant {
        let micros = i64::try_from(self.start.elapsed().as_micros())
            .expect("elapsed wall time fits i64 microseconds");
        Instant::from_micros(micros)
    }

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
// ANCHOR_END: clock

// ANCHOR: task
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

    // ANCHOR: send
    let line = b"hello\n";
    let mut frame = handle.tx_frame().await;
    frame.as_mut()[..line.len()].copy_from_slice(line);
    frame.truncate(line.len());
    let frame = frame.freeze();
    tx.room(line.len()).await?;
    tx.try_send(frame).map_err(|(_frame, error)| error)?;
    // ANCHOR_END: send

    // ANCHOR: recv
    loop {
        match rx.recv().await {
            Ok(frame) => print!("{}", String::from_utf8_lossy(frame.payload())),
            Err(Error::Finished) => break,
            Err(error) => return Err(error),
        }
    }
    // ANCHOR_END: recv

    tx.shutdown();
    tx.delivered().await;
    Ok(())
}
// ANCHOR_END: task

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ANCHOR: config
    let mut args = std::env::args().skip(1);
    let (Some(config), Some(server)) = (args.next(), args.next()) else {
        return Err("usage: tcp_client CONFIG HOST:PORT".into());
    };
    let config = Config::load(Path::new(&config))?;
    // Name lookup goes through the kernel and blocks, so do it before open.
    let Some(SocketAddr::V4(server)) = server.to_socket_addrs()?.find(SocketAddr::is_ipv4) else {
        return Err("server has no IPv4 address".into());
    };
    // ANCHOR_END: config

    // ANCHOR: setup
    let socket = tcp::Config {
        rx_capacity: BUFFER,
        tx_capacity: BUFFER,
        mss_cap: Some(config.path_mtu(*server.ip()) - 40),
    };
    let sizing = Sizing {
        frame_share: tcp::tx_mbuf_budget(&socket, usize::from(config.mtu)),
        max_sockets: 1,
        max_udp_sockets: 0,
        rx_staging: 32,
        tx_pending: 32,
        neighbor_capacity: DEFAULT_NEIGHBOR_CAPACITY,
    };

    // Every turn and every poll gets its time from this one clock.
    let clock = Clock { start: WallInstant::now() };
    // SAFETY: EAL creates the device named in the config, this thread owns it and every
    // packet, and EAL (`_eal` below) lives until the Port is closed.
    let opened = unsafe { open(&config, &sizing, None, &mut || clock.now()) }?;
    println!("{}", opened.report);
    let _eal = opened.eal;

    let mut net = Net::new(opened.stack, NetConfig { ingress_budget: INGRESS_BUDGET });
    let mut rt = Runtime::new(clock.ns());
    let done = Rc::new(Cell::new(None));
    let server = IpEndpoint::new((*server.ip()).into(), server.port());
    rt.spawner()
        .spawn(hello(net.handle(), server, socket, Rc::clone(&done)))
        .map_err(|error| format!("spawn: {:?}", error.reason))?;
    // ANCHOR_END: setup

    // ANCHOR: drive
    let result = loop {
        if let Some(result) = done.take() {
            break result;
        }
        if clock.start.elapsed() > LIMIT {
            return Err(format!("not finished after {LIMIT:?}").into());
        }
        let (now, tick) = clock.sample();
        let moved = switch_net::round(&mut net, &mut rt, now, tick)
            .map_err(|error| format!("round: {error:?}"))?;
        if !moved {
            std::thread::sleep(IDLE_SLEEP);
        }
    };
    // ANCHOR_END: drive

    // ANCHOR: teardown
    rt.shutdown();
    let stack = net.into_stack().map_err(|_net| "a socket or handle outlived the task")?;
    stack.into_port().try_close().map_err(|(_port, error)| format!("port close: {error}"))?;
    // ANCHOR_END: teardown
    println!("port closed");
    result.map_err(|error| format!("connection: {error}").into())
}
