//! Echoes TCP bytes over the local backend on a Linux tap device.
//!
//! Build from the repository root, then run as root on an allowed CPU (3 below):
//!
//! ```text
//! cargo build -p switch-net --example tcp_echo_server
//! taskset -c 3 target/debug/examples/tcp_echo_server switch-net/examples/tap.json 7000 &
//! server=$!
//! until ip link show cdnic0 >/dev/null 2>&1; do kill -0 "$server" || exit 1; sleep 0.1; done
//! ip addr add 192.0.2.1/24 dev cdnic0 && ip link set cdnic0 up
//! python3 -c 'import socket; s = socket.create_connection(("192.0.2.2", 7000), timeout=10); data = b"hello\n" * 2000; s.sendall(data); s.shutdown(socket.SHUT_WR); echoed = s.makefile("rb").read(); assert echoed == data, (len(echoed), len(data)); print("echoed", len(echoed), "bytes"); s.close()'
//! kill "$server"
//! wait "$server"
//! ```
//!
//! This needs `CAP_NET_ADMIN` and `/dev/net/tun`, as provided by the Docker image.
//! `switch_open::open` requires the calling thread to be allowed on exactly one CPU.
//! The server runs until stopped or a network, runtime or task-spawn error occurs.

use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;
use std::time::{Duration, Instant as WallInstant};

use dpdk_smoltcp::DEFAULT_NEIGHBOR_CAPACITY;
use switch::{Runtime, Spawner};
use switch_net::{tcp, Error, Instant, IpListenEndpoint, Net, NetConfig, NetHandle, TcpStream};
use switch_netconf::Config;
use switch_open::{open, Sizing};

const BUFFER: usize = 64 * 1024;
// One slot is for the next listener; the others are for active connections.
const MAX_SOCKETS: usize = 16;
const IDLE_SLEEP: Duration = Duration::from_millis(1);

type Failure = Rc<RefCell<Option<String>>>;

// ANCHOR: accept_loop
#[switch::task]
async fn accept_loop(
    spawner: Spawner,
    handle: NetHandle,
    socket: tcp::Config,
    port: u16,
    failure: Failure,
) {
    let local = IpListenEndpoint { addr: None, port };
    let mut accepting = handle.tcp_accept(socket, local);
    loop {
        let stream = match accepting.await {
            Ok(stream) => stream,
            Err(error) => {
                *failure.borrow_mut() = Some(format!("accept: {error}"));
                return;
            }
        };
        // Each accept is one listening socket. Arm the next before serving this one.
        accepting = handle.tcp_accept(socket, local);
        if let Err(error) = spawner.spawn(serve(handle.clone(), stream, Rc::clone(&failure))) {
            *failure.borrow_mut() = Some(format!("spawn echo: {:?}", error.reason));
            return;
        }
    }
}
// ANCHOR_END: accept_loop

#[switch::task]
async fn serve(handle: NetHandle, stream: TcpStream, failure: Failure) {
    if let Err(error) = echo(&handle, stream).await {
        *failure.borrow_mut() = Some(format!("echo: {error}"));
    }
}

// ANCHOR: echo
async fn echo(handle: &NetHandle, stream: TcpStream) -> Result<(), Error> {
    let (mut rx, mut tx) = stream.split();
    let max_payload = tx.max_segment_payload().ok_or(Error::Closed)?;
    loop {
        let received = match rx.recv().await {
            Ok(received) => received,
            Err(Error::Finished) => break,
            Err(error) => return Err(error),
        };
        let mut rest = received.payload();
        while !rest.is_empty() {
            let mut frame = handle.tx_frame().await;
            let len = rest.len().min(max_payload).min(frame.as_mut().len());
            frame.as_mut()[..len].copy_from_slice(&rest[..len]);
            frame.truncate(len);
            tx.room(len).await?;
            tx.try_send(frame.freeze()).map_err(|(_frame, error)| error)?;
            rest = &rest[len..];
        }
    }
    tx.shutdown();
    tx.delivered().await;
    Ok(())
}
// ANCHOR_END: echo

struct Clock {
    start: WallInstant,
}

impl Clock {
    fn ns(&self) -> u64 {
        u64::try_from(self.start.elapsed().as_nanos()).expect("elapsed time fits u64 nanoseconds")
    }

    fn sample(&self) -> (Instant, u64) {
        let nanos = self.ns();
        let micros = i64::try_from(nanos / 1_000).expect("elapsed time fits i64 microseconds");
        (Instant::from_micros(micros), nanos)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let (Some(config), Some(port)) = (args.next(), args.next()) else {
        return Err("usage: tcp_echo_server CONFIG PORT".into());
    };
    let config = Config::load(Path::new(&config))?;
    let port: u16 = port.parse()?;
    let socket = tcp::Config {
        rx_capacity: BUFFER,
        tx_capacity: BUFFER,
        mss_cap: Some(config.mtu - 40),
    };
    let sizing = Sizing {
        frame_share: MAX_SOCKETS * tcp::tx_mbuf_budget(&socket, usize::from(config.mtu)),
        max_sockets: MAX_SOCKETS,
        max_udp_sockets: 0,
        rx_staging: 32,
        tx_pending: 32,
        neighbor_capacity: DEFAULT_NEIGHBOR_CAPACITY,
    };
    let clock = Clock { start: WallInstant::now() };
    // SAFETY: EAL creates this thread's device and packets, and remains alive until
    // the Port has been closed.
    let opened = unsafe { open(&config, &sizing, None, &mut || clock.sample().0) }?;
    println!("{}", opened.report);
    let _eal = opened.eal;
    let mut net = Net::new(opened.stack, NetConfig { ingress_budget: 32 });
    let mut rt = Runtime::new(clock.ns());
    let failure = Rc::new(RefCell::new(None));
    let spawner = rt.spawner();
    spawner
        .spawn(accept_loop(spawner.clone(), net.handle(), socket, port, Rc::clone(&failure)))
        .map_err(|error| format!("spawn accept: {:?}", error.reason))?;
    let result = loop {
        if let Some(error) = failure.borrow_mut().take() {
            break Err(error);
        }
        let (now, tick) = clock.sample();
        match switch_net::round(&mut net, &mut rt, now, tick) {
            Ok(false) => std::thread::sleep(IDLE_SLEEP),
            Ok(true) => {}
            Err(error) => break Err(format!("round: {error:?}")),
        }
    };
    rt.shutdown();
    let stack = net.into_stack().map_err(|_net| "a socket or handle outlived the tasks")?;
    stack.into_port().try_close().map_err(|(_port, error)| format!("port close: {error}"))?;
    result.map_err(Into::into)
}
