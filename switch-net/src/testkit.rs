//! A Net on a DPDK net_tap Port, for Docker tests and drivers against kernel-side peers
//! (feature `testkit`). Switch is 192.0.2.2/24; the kernel netdev gets 192.0.2.1/24, where the
//! peer listens.

use std::cell::RefCell;
use std::ffi::CString;
use std::future::Future;
use std::io::ErrorKind;
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
use std::panic::resume_unwind;
use std::process::Command;
use std::rc::Rc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use dpdk_smoltcp::smoltcp::wire::{EthernetAddress, Ipv4Cidr};
use dpdk_smoltcp::{DEFAULT_NEIGHBOR_CAPACITY, Offloads, Port, PortConfig, StackConfig, frame_reserve, rx_reserve};
use switch::Runtime;
use switch_buffer::PoolConfig;
use switch_dpdk_sys::RTE_PKTMBUF_HEADROOM;
use switch_test_kit::{tap::TapVdev, with_eal};

use crate::{Instant, IpEndpoint, IpListenEndpoint, Net, NetConfig, NetHandle, Stack, TcpStream, tcp};

mod host;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Backend {
    Conduit,
    Junction,
}

/// A client Net plus the optional junction host driven on the same test thread.
pub struct TapNet {
    net: Net,
    host: Option<host::Host>,
}

impl TapNet {
    pub fn handle(&self) -> NetHandle {
        self.net.handle()
    }

    pub fn poll_at(&mut self) -> Option<Instant> {
        self.net.poll_at()
    }

    pub fn tcp_reset_by_peer(&self) -> bool {
        self.net.tcp_reset_by_peer()
    }

    pub fn receive(&mut self, now: Instant) -> Result<crate::Turn, crate::TurnError> {
        if let Some(host) = &mut self.host {
            host.iteration(now);
        }
        self.net.receive(now)
    }

    pub fn send(&mut self) -> Result<crate::Sent, crate::TurnError> {
        self.net.send()
    }
}

pub const SWITCH_ADDR: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 2);
pub const KERNEL_ADDR: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 1);
const MTU: u16 = 1500;
const INGRESS_BUDGET: usize = 32;
/// A [`client_server`] run, and every peer socket read and write.
const LIMIT: Duration = Duration::from_secs(60);
/// The loops sleep this long each round so a peer thread on the same CPU runs.
pub const ROUND_SLEEP: Duration = Duration::from_micros(50);

pub struct TapConfig {
    /// Short and unique among live taps: the vdev is `net_tap_{name}`, the netdev
    /// `cdtap_{name}`, the pools `{name}_rx`, `{name}_tx`, `{name}_frame`.
    pub name: &'static str,
    /// Socket config used by the test; pools are shared.
    pub socket: tcp::Config,
    pub max_sockets: usize,
    pub random_seed: u64,
}

/// Time since the Net was made.
pub struct Clock(std::time::Instant);

impl Clock {
    /// For `Net::receive`.
    pub fn now(&self) -> Instant {
        Instant::from_micros(i64::try_from(self.0.elapsed().as_micros()).unwrap())
    }

    /// Monotonic nanoseconds, for `Runtime::poll` and relative sleeps.
    pub fn ns(&self) -> u64 {
        u64::try_from(self.0.elapsed().as_nanos()).unwrap()
    }

    pub fn elapsed(&self) -> Duration {
        self.0.elapsed()
    }
}

/// Runs `body` on a Net over a fresh net_tap Port, locally or through junction.
/// `body` must drop every handle, socket and frame it made; then the host gives its Stack
/// back, the Port closes with every buffer returned and the vdev is removed.
pub fn with_tap_net<T>(backend: Backend, config: &TapConfig, body: impl FnOnce(&mut TapNet, &Clock) -> T) -> T {
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
            ipv4: Ipv4Cidr::new(SWITCH_ADDR, 24),
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
        let stack = Stack::new(port, stack_config, clock.now()).map_err(|(_, e)| e).expect("create switch Stack");
        let (net, host) = match backend {
            Backend::Conduit => (Net::new(stack, NetConfig { ingress_budget: INGRESS_BUDGET }), None),
            Backend::Junction => {
                let (host, session) = host::Host::new(stack, clock.now(), INGRESS_BUDGET, config.max_sockets);
                let tx_frames = tcp::tx_mbuf_budget(&config.socket, MTU.into());
                (Net::remote(session, crate::RemoteConfig { tx_frames }), Some(host))
            }
        };
        let mut tap_net = TapNet { net, host };
        let out = body(&mut tap_net, &clock);
        let TapNet { mut net, host } = tap_net;
        let stack = match host {
            None => net.into_stack().ok().expect("a handle or socket outlived the body"),
            Some(host) => {
                net.assert_remote_frames_returned();
                drop(net.into_session().ok().expect("a handle or socket outlived the body"));
                host.finish(clock.now())
            }
        };
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
    // Size the shared RX pool for one receive-window's worth of full-MTU
    // segments plus 64 packets; short segments and retained frames use more.
    let share = |frames: usize| frames + 64;
    PortConfig {
        port_id,
        mtu: MTU,
        rx_descriptors: 16,
        tx_descriptors: 16,
        rx_burst: 32,
        tx_burst: 32,
        queue_socket: -1,
        rx_pool: pool(rx.as_c_str(), rx_reserve(16, 32, 32, 8) + share(config.socket.rx_capacity.div_ceil(usize::from(MTU) - 40))),
        tx_pool: pool(tx.as_c_str(), 127),
        frame_pool: pool(frame.as_c_str(), frame_reserve(16, 32) + share(tcp::tx_mbuf_budget(&config.socket, MTU.into()))),
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

impl Net {
    /// Drains internally queued RX and checks that no remote frame outlived the body.
    pub fn assert_remote_frames_returned(&mut self) {
        if let crate::Inner::Remote(remote) = &mut *self.shared.borrow_mut() {
            remote.assert_frames_returned();
        }
    }

    /// Test inspection: whether the peer reset any TCP socket open on this Net.
    pub fn tcp_reset_by_peer(&self) -> bool {
        let inner = self.shared.borrow();
        let crate::Inner::Local(inner) = &*inner else {
            let crate::Inner::Remote(remote) = &*inner else { unreachable!() };
            return remote.reset_by_peer();
        };
        inner.entries.iter().any(|entry| match &entry.socket {
            Some(crate::Socket::Tcp(handle, _)) => inner.stack.tcp(handle).reset_by_peer(),
            _ => false,
        })
    }
}

/// A local port no earlier connection of this process used. The kernel keeps a test's socket
/// after the test (in FIN-WAIT-1 or LAST-ACK when the Net stopped turning before the close
/// finished); a new connection on the same address pair would collide with it and never
/// connect.
pub fn next_local_port() -> u16 {
    static NEXT: AtomicU16 = AtomicU16::new(49152);
    let port = NEXT.fetch_add(1, Ordering::Relaxed);
    assert!(port >= 49152, "local ports used up");
    port
}

/// The kernel side of one test connection: listens on 192.0.2.1:`port` and accepts in the
/// test's own loop, so a test that fails before its client connects drops the listener as it
/// unwinds, instead of leaving a thread blocked in `accept` that holds the port for the
/// tests after it.
pub struct KernelPeer<T> {
    listener: Option<TcpListener>,
    serve: Option<Box<dyn FnOnce(std::net::TcpStream) -> T + Send>>,
    server: Option<JoinHandle<T>>,
}

impl<T: Send + 'static> KernelPeer<T> {
    /// `serve` runs on its own thread on the accepted connection (reads and writes time out
    /// after a minute).
    pub fn listen(
        port: u16,
        listener_setup: impl FnOnce(&TcpListener),
        serve: impl FnOnce(std::net::TcpStream) -> T + Send + 'static,
    ) -> Self {
        let listener = TcpListener::bind(SocketAddrV4::new(KERNEL_ADDR, port)).expect("bind the peer");
        listener_setup(&listener);
        listener.set_nonblocking(true).expect("non-blocking listener");
        KernelPeer { listener: Some(listener), serve: Some(Box::new(serve)), server: None }
    }

    /// Accepts the client if it connected: starts `serve` and closes the listener.
    pub fn accept(&mut self) {
        let Some(listener) = &self.listener else { return };
        let stream = match listener.accept() {
            Ok((stream, _)) => stream,
            Err(e) if e.kind() == ErrorKind::WouldBlock => return,
            Err(e) => panic!("accept the client: {e}"),
        };
        self.listener = None;
        stream.set_nonblocking(false).unwrap();
        stream.set_read_timeout(Some(LIMIT)).unwrap();
        stream.set_write_timeout(Some(LIMIT)).unwrap();
        let serve = self.serve.take().expect("serve runs once");
        self.server = Some(thread::spawn(move || serve(stream)));
    }

    /// `serve` ran to the end.
    pub fn is_finished(&self) -> bool {
        self.server.as_ref().is_some_and(JoinHandle::is_finished)
    }

    /// What `serve` returned, once it returns; its panic resumes here. Panics if the client
    /// never connected.
    pub fn join(mut self) -> T {
        self.accept();
        match self.server.take().expect("the client never connected").join() {
            Ok(out) => out,
            Err(panic) => resume_unwind(panic),
        }
    }
}

/// Connects to the kernel peer on `port` from a fresh local port, runs `client` on the stream,
/// and puts what it hands back in `held`.
#[switch::task]
async fn connect_client<H: 'static, F: Future<Output = H> + 'static, C: FnOnce(NetHandle, TcpStream) -> F + 'static>(
    handle: NetHandle,
    socket: tcp::Config,
    port: u16,
    client: C,
    held: Rc<RefCell<Option<H>>>,
) {
    let remote = IpEndpoint::new(KERNEL_ADDR.into(), port);
    let local = IpListenEndpoint { addr: None, port: next_local_port() };
    let stream = handle.tcp_connect(socket, remote, local).await.expect("TCP connect");
    *held.borrow_mut() = Some(client(handle, stream).await);
}

/// One client against one kernel peer: the peer, on a thread, runs `serve` on the connection
/// it accepts on 192.0.2.1:`port` (after `listener_setup` on its listener); `client` runs as
/// a switch task on the Net's handle and the connected stream (from a fresh local port), and
/// hands back what it holds.
/// Turns the Net, calling `each_round` each time, until both are done; what the client holds
/// is dropped only then, so what the peer saw came from the client, not from its sockets
/// closing. Returns what `serve` returned.
pub fn client_server<T: Send + 'static, H: 'static, F: Future<Output = H> + 'static>(
    backend: Backend,
    tap: &TapConfig,
    port: u16,
    listener_setup: impl FnOnce(&TcpListener),
    serve: impl FnOnce(std::net::TcpStream) -> T + Send + 'static,
    client: impl FnOnce(NetHandle, TcpStream) -> F + 'static,
    mut each_round: impl FnMut(),
) -> T {
    with_tap_net(backend, tap, |net, clock| {
        let mut peer = KernelPeer::listen(port, listener_setup, serve);
        let mut rt = Runtime::new(0);
        let handle = net.handle();
        let held: Rc<RefCell<Option<H>>> = Rc::default();
        let task = connect_client(handle, tap.socket, port, client, held.clone());
        rt.spawner().spawn(task).expect("spawn the client");
        let client_done = || held.borrow().is_some();
        while !(client_done() && peer.is_finished()) {
            assert!(clock.elapsed() < LIMIT, "client done {}, peer done {}", client_done(), peer.is_finished());
            peer.accept();
            net.receive(clock.now()).expect("receive");
            rt.poll(clock.ns()).expect("poll the client");
            net.send().expect("send");
            each_round();
            thread::sleep(ROUND_SLEEP);
        }
        let saw = peer.join();
        drop(held.borrow_mut().take());
        rt.shutdown();
        saw
    })
}
