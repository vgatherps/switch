//! A Net on a DPDK net_tap Port, for Docker tests and drivers against kernel-side peers
//! (feature `testkit`). Zhida is 192.0.2.2/24; the kernel netdev gets 192.0.2.1/24, where the
//! peer listens.

use std::cell::RefCell;
use std::ffi::CString;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
use std::panic::resume_unwind;
use std::process::Command;
use std::rc::Rc;
use std::thread;
use std::time::Duration;

use dpdk_smoltcp::smoltcp::wire::{EthernetAddress, Ipv4Cidr};
use dpdk_smoltcp::{DEFAULT_NEIGHBOR_CAPACITY, Offloads, Port, PortConfig, StackConfig, frame_reserve, rx_reserve};
use switch::Runtime;
use zhida_buffer::PoolConfig;
use zhida_dpdk_sys::RTE_PKTMBUF_HEADROOM;
use zhida_test_kit::{tap::TapVdev, with_eal};

use crate::{Instant, IpEndpoint, IpListenEndpoint, Net, NetConfig, NetHandle, Stack, TcpStream, boxed, tcp};

pub const ZHIDA_ADDR: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 2);
pub const KERNEL_ADDR: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 1);
const MTU: u16 = 1500;
const INGRESS_BUDGET: usize = 32;
/// The client's local port in [`client_server`].
const LOCAL_PORT: u16 = 49152;
/// A [`client_server`] run, and every peer socket read and write.
const LIMIT: Duration = Duration::from_secs(60);
/// The loops sleep this long each round so a peer thread on the same CPU runs.
pub const ROUND_SLEEP: Duration = Duration::from_micros(50);

pub struct TapConfig {
    /// Short and unique among live taps: the vdev is `net_tap_{name}`, the netdev
    /// `cdtap_{name}`, the pools `{name}_rx`, `{name}_tx`, `{name}_frame`.
    pub name: &'static str,
    /// Every socket's config; the pools hold `max_sockets` of them.
    pub socket: tcp::Config,
    pub max_sockets: usize,
    pub random_seed: u64,
}

/// Time since the Net was made.
pub struct Clock(std::time::Instant);

impl Clock {
    /// For `Net::turn`.
    pub fn now(&self) -> Instant {
        Instant::from_micros(i64::try_from(self.0.elapsed().as_micros()).unwrap())
    }

    /// Milliseconds, for `Runtime::poll`.
    pub fn ms(&self) -> u64 {
        u64::try_from(self.0.elapsed().as_millis()).unwrap()
    }

    pub fn elapsed(&self) -> Duration {
        self.0.elapsed()
    }
}

/// Runs `body` on a Net over a fresh net_tap Port. `body` must drop every handle and socket
/// it made; then the Net gives its Stack back, the Port closes with every buffer returned and
/// the vdev is removed.
pub fn with_tap_net<T>(config: &TapConfig, body: impl FnOnce(&mut Net, &Clock) -> T) -> T {
    let name = |format: &str| CString::new(format.replace("{}", config.name)).unwrap();
    let (vdev, iface) = (name("net_tap_{}"), name("cdtap_{}"));
    let pools = [name("{}_rx"), name("{}_tx"), name("{}_frame")];
    with_eal(|eal| {
        let tap = TapVdev::new(&vdev, &iface);
        // SAFETY: net_tap was just created and this EAL thread alone configures and uses its
        // Port and packets. The offload-free net_tap profile returns single-segment RX buffers
        // and frees TX buffers synchronously. The Port closes before the vdev is removed, and
        // EAL outlives both.
        let port = unsafe { Port::open(eal, port_config(tap.port_id(), config, &pools)) }.expect("open net_tap Port");
        add_address(&iface, KERNEL_ADDR, 24);
        let stack_config = StackConfig {
            max_udp_sockets: 0,
            ethernet: EthernetAddress([0x02, 0, 0, 0, 0, 0x01]),
            ipv4: Ipv4Cidr::new(ZHIDA_ADDR, 24),
            gateway: None,
            paths: &[],
            owned_ipv4: &[],
            random_seed: config.random_seed,
            neighbor_capacity: DEFAULT_NEIGHBOR_CAPACITY,
            max_sockets: config.max_sockets,
            rx_staging: 32,
            tx_pending: 32,
        };
        let clock = Clock(std::time::Instant::now());
        let stack = Stack::new(port, stack_config, clock.now()).map_err(|(_, e)| e).expect("create Zhida Stack");
        let mut net = Net::new(stack, NetConfig { ingress_budget: INGRESS_BUDGET });
        let out = body(&mut net, &clock);
        let stack = net.into_stack().ok().expect("a handle or socket outlived the body");
        stack.into_port().try_close().expect("close the Port with every buffer returned");
        tap.close();
        out
    })
}

fn port_config<'a>(port_id: u16, config: &TapConfig, [rx, tx, frame]: &'a [CString; 3]) -> PortConfig<'a> {
    let pool = |name, count: usize| PoolConfig {
        name,
        count: u32::try_from(count).unwrap(),
        cache_size: 8,
        data_room_size: RTE_PKTMBUF_HEADROOM as u16 + 2048,
        socket_id: -1,
    };
    let sockets = |budget: usize| budget * config.max_sockets;
    PortConfig {
        port_id,
        mtu: MTU,
        rx_descriptors: 16,
        tx_descriptors: 16,
        rx_burst: 32,
        tx_burst: 32,
        queue_socket: -1,
        rx_pool: pool(rx.as_c_str(), rx_reserve(16, 32, 8) + sockets(tcp::rx_mbuf_budget(&config.socket, MTU.into()))),
        tx_pool: pool(tx.as_c_str(), 127),
        frame_pool: pool(frame.as_c_str(), frame_reserve(16, 32) + sockets(tcp::tx_mbuf_budget(&config.socket, MTU.into()))),
        offloads: Offloads::default(),
    }
}

fn add_address(iface: &CString, addr: Ipv4Addr, prefix: u8) {
    let cidr = format!("{addr}/{prefix}");
    let output = Command::new("ip")
        .args(["address", "add", &cidr, "dev", iface.to_str().unwrap()])
        .output()
        .expect("run ip address add");
    assert!(output.status.success(), "ip address add {cidr}: {}", String::from_utf8_lossy(&output.stderr));
}

/// One client against one kernel peer: the peer, on a thread, runs `serve` on the connection
/// it accepts on 192.0.2.1:`port` (after `listener_setup` on its listener); `client` runs as
/// a switch task on the Net's handle and the connected stream, and hands back what it holds.
/// Turns the Net, calling `each_round` each time, until both are done; what the client holds
/// is dropped only then, so what the peer saw came from the client, not from its sockets
/// closing. Returns what `serve` returned.
pub fn client_server<T: Send + 'static, H: 'static, F: Future<Output = H> + 'static>(
    tap: &TapConfig,
    port: u16,
    listener_setup: impl FnOnce(&TcpListener),
    serve: impl FnOnce(std::net::TcpStream) -> T + Send + 'static,
    client: impl FnOnce(NetHandle, TcpStream) -> F + 'static,
    mut each_round: impl FnMut(),
) -> T {
    with_tap_net(tap, |net, clock| {
        let listener = TcpListener::bind(SocketAddrV4::new(KERNEL_ADDR, port)).expect("bind the peer");
        listener_setup(&listener);
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept the client");
            stream.set_read_timeout(Some(LIMIT)).unwrap();
            stream.set_write_timeout(Some(LIMIT)).unwrap();
            serve(stream)
        });
        let mut rt = Runtime::new(0);
        let handle = net.handle();
        let held: Rc<RefCell<Option<H>>> = Rc::default();
        let task_held = held.clone();
        let socket = tap.socket;
        let task = async move {
            let remote = IpEndpoint::new(KERNEL_ADDR.into(), port);
            let local = IpListenEndpoint { addr: None, port: LOCAL_PORT };
            let stream = handle.tcp_connect(socket, remote, local).expect("open TCP").await.expect("TCP connect");
            *task_held.borrow_mut() = Some(client(handle, stream).await);
        };
        rt.spawner().spawn(boxed(Box::pin(task))).expect("spawn the client");
        let client_done = || held.borrow().is_some();
        while !(client_done() && server.is_finished()) {
            assert!(clock.elapsed() < LIMIT, "client done {}, peer done {}", client_done(), server.is_finished());
            net.turn(clock.now()).expect("turn the Net");
            rt.poll(clock.ms()).expect("poll the client");
            each_round();
            thread::sleep(ROUND_SLEEP);
        }
        let saw = match server.join() {
            Ok(saw) => saw,
            Err(panic) => resume_unwind(panic),
        };
        drop(held.borrow_mut().take());
        rt.shutdown();
        saw
    })
}
