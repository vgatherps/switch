//! Echo WebSocket messages. Connect with any WebSocket client to `ws://ADDR:PORT/`.
//!
//! ```text
//! cargo build -p switch-examples --features ws --example ws_echo_server
//! taskset -c 0 target/debug/examples/ws_echo_server examples/tap.json 7000
//! ```
//!
//! For a tap, assign `192.0.2.1/24` to `cdnic0` and bring it up after the server
//! creates it; the server's address is `192.0.2.2`. The server runs until stopped.

use std::path::Path;
use std::time::{Duration, Instant as WallInstant};

use switch::net::{Instant, IpListenEndpoint, Net, NetConfig, NetHandle, TcpStream, tcp};
use switch::nic::{Config, DEFAULT_NEIGHBOR_CAPACITY, Sizing, open};
use switch::ws::Limits;
use switch::{Runtime, Spawner};

// One listening socket plus at most 15 active connections.
const MAX_SOCKETS: usize = 16;
const BUFFER: usize = 64 * 1024;

#[switch::task]
async fn accept(spawner: Spawner, handle: NetHandle, socket: tcp::Config, port: u16) {
    let local = IpListenEndpoint { addr: None, port };
    let mut accepting = handle.tcp_accept(socket, local);
    loop {
        let stream = accepting.await.expect("accept TCP");
        accepting = handle.tcp_accept(socket, local);
        spawner.spawn(echo(handle.clone(), stream)).unwrap();
    }
}

#[switch::task]
async fn echo(handle: NetHandle, stream: TcpStream) {
    let limits = Limits {
        max_message: 64 * 1024,
        max_send: 64 * 1024,
    };
    let mut ws = switch::ws::net::accept_ws(&handle, stream, limits)
        .await
        .expect("upgrade WebSocket");
    while let Some((message, mut sender)) = ws.recv_with_sender().await.expect("receive WebSocket")
    {
        sender
            .send(message.kind(), message.bytes())
            .await
            .expect("echo WebSocket");
    }
    println!("WebSocket closed");
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let (Some(config), Some(port)) = (args.next(), args.next()) else {
        return Err("usage: ws_echo_server CONFIG PORT".into());
    };
    let config = Config::load(Path::new(&config))?;
    let port = port.parse()?;
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
    let start = WallInstant::now();
    // SAFETY: this thread owns the device and its packets; EAL outlives the port.
    let opened = unsafe {
        open(&config, &sizing, None, &mut || {
            Instant::from_micros(i64::try_from(start.elapsed().as_micros()).unwrap())
        })
    }?;
    println!("{}", opened.report);
    let _eal = opened.eal;
    let mut net = Net::new(opened.stack, NetConfig { ingress_budget: 32 });
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    spawner
        .spawn(accept(spawner.clone(), net.handle(), socket, port))
        .unwrap();
    loop {
        let tick = u64::try_from(start.elapsed().as_nanos())?;
        let now = Instant::from_micros(i64::try_from(tick / 1_000)?);
        if !switch::net::round(&mut net, &mut runtime, now, tick)
            .map_err(|error| format!("round: {error:?}"))?
        {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}
