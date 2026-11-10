//! Direct in-process junction host wiring shared by socket and tap tests.

use std::os::unix::fs::DirBuilderExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use ceph_fifo::Fifo;
use ceph_ring::{Config, Reader};
use dpdk_smoltcp::Stack;
use dpdk_smoltcp::smoltcp::time::Instant;
use junction::host::{ControlChannels, ControlConfig, HostControl};
use junction::session::Session;
use junctiond::network::{Network, NetworkConfig};

pub struct Host {
    pub network: Network,
    control: JoinHandle<std::io::Result<()>>,
    directory: PathBuf,
}

impl Host {
    pub fn new(stack: Stack, now: Instant, ingress_budget: usize, max_sockets: usize) -> (Self, Session) {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let directory = std::env::temp_dir().join(format!(
            "switchnet-host-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::DirBuilder::new().mode(0o700).create(&directory).expect("create host directory");
        let socket_path = directory.join("control.sock");
        let frame_capacity = 1500 - 28;
        let max_record = frame_capacity + 24;
        let reader = Reader::create(Config {
            ring_size: 1024 * 1024,
            max_record,
            dir: directory.clone(),
        }).expect("create ingress ring");
        let (to_network, operations) = mpsc::channel();
        let (events, from_network) = mpsc::channel();
        let (to_fifo, fifo_control) = mpsc::channel();
        let (retire, retired) = mpsc::channel();
        let max_sockets = u16::try_from(max_sockets).expect("socket count fits protocol");
        let control = HostControl::bind(ControlConfig {
            socket_path: socket_path.clone(),
            ingress: reader.name().to_owned(),
            rx_dir: directory.clone(),
            rx_size: 256 * 1024,
            max_record: u32::try_from(max_record).unwrap(),
            frame_capacity: u32::try_from(frame_capacity).unwrap(),
            max_sessions: 1,
            max_sockets,
            poll_ms: 1,
            hello_timeout: Duration::from_secs(5),
        }, ControlChannels { to_network, to_fifo, from_network, retire: retired }).expect("bind host control");
        let control = thread::spawn(move || control.run());
        let fifo = Fifo::new(reader, 1, fifo_control, retire);
        let network = Network::new(stack, fifo, 1, NetworkConfig {
            ingress_budget,
            control_batch: 64,
            fifo_batch: 64,
        }, operations, events);
        // Welcome is produced by HostControl itself; no Network turn is needed to connect.
        let session = Session::connect(&socket_path, max_sockets).expect("connect to host");
        let mut host = Self { network, control, directory };
        host.iteration(now);
        (host, session)
    }

    pub fn iteration(&mut self, now: Instant) -> bool {
        self.network.iteration(now).expect("turn host Network").moved()
    }

    /// Call only after dropping the remote Net's session and every retained frame.
    pub fn finish(mut self, now: Instant) -> Stack {
        self.iteration(now);
        let stack = self.network.into_stack();
        self.control.join().expect("host control thread").expect("host control shutdown");
        std::fs::remove_file(self.directory.join("control.sock")).expect("remove control socket");
        std::fs::remove_dir(self.directory).expect("remove host directory");
        stack
    }
}
