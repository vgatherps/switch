//! Echo each UDP datagram to its sender. Run from the repository root:
//!
//! ```text
//! cargo build -p switch-examples --features net --example udp_echo
//! taskset -c 0 target/debug/examples/udp_echo examples/tap.json 7000 &
//! server=$!
//! until ip link show cdnic0 >/dev/null 2>&1; do kill -0 "$server" || exit 1; sleep 0.1; done
//! ip addr add 192.0.2.1/24 dev cdnic0 && ip link set cdnic0 up
//! python3 -c 'import socket; s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM); s.settimeout(5); s.sendto(b"hello",("192.0.2.2",7000)); data,_=s.recvfrom(1500); assert data==b"hello"; print("echo:",data.decode())'
//! kill "$server"
//! ```
//!
//! The peer is a Python UDP socket on the kernel side of the tap.

use std::path::Path;
use std::time::{Duration, Instant as WallInstant};

use switch::Runtime;
use switch::net::{Error, Instant, Net, NetConfig, NetHandle, UdpConfig};
use switch::nic::{Config, DEFAULT_NEIGHBOR_CAPACITY, Sizing, open};

#[switch::task]
async fn echo(handle: NetHandle, port: u16) {
    let socket = handle
        .udp_bind(UdpConfig {
            port,
            local_addr: None,
            rx_queue: 32,
            tx_checksum: true,
            broadcast: false,
        })
        .await
        .expect("bind UDP");
    let (mut rx, mut tx) = socket.split();
    loop {
        let received = rx.recv().await;
        let mut frame = handle.tx_frame().await;
        let len = received.payload().len();
        frame.as_mut()[..len].copy_from_slice(received.payload());
        frame.truncate(len);
        let mut frame = frame.freeze();
        loop {
            match tx.send(Some(received.src()), frame) {
                Ok(()) => break,
                Err((returned, Error::Full | Error::NeighborPending)) => {
                    frame = returned;
                    switch::yield_now().await;
                }
                Err((_, error)) => panic!("send UDP: {error}"),
            }
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let (Some(config), Some(port)) = (args.next(), args.next()) else {
        return Err("usage: udp_echo CONFIG PORT".into());
    };
    let config = Config::load(Path::new(&config))?;
    let port = port.parse()?;
    let sizing = Sizing {
        frame_share: 32,
        max_sockets: 1,
        max_udp_sockets: 1,
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
    runtime.spawner().spawn(echo(net.handle(), port)).unwrap();
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
