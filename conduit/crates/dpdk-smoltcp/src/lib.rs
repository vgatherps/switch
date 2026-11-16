//! A TCP/UDP stack for one DPDK NIC port, driven from one thread.
//!
//! This crate runs a TCP/IP stack (a vendored, modified smoltcp, re-exported as [`smoltcp`]) over a NIC that DPDK drives from userspace. Received data reaches your code as references into the NIC's own packet buffers, and data you send can be written in place into buffers the NIC sends from. `switch-net` builds async sockets on it, and the junction daemon runs one for many applications. Use this crate directly when you want a synchronous loop with no async runtime, or are building your own layer on top.
//!
//! [`Eal::init`] starts DPDK once per process. [`Port::open`] configures one NIC port with three pools of packet buffers (RX, TX and frame). [`Stack::new`] builds the stack over that port. Sockets are named by [`TcpHandle`] and [`UdpHandle`], which belong to the Stack that made them. The Stack reads no clock and never waits: your loop calls [`Stack::turn`] (or [`Stack::receive`], then your work, then [`Stack::send`]) with the current time in microseconds, and each call does the work possible now. The `Eal`, `Port`, `Stack`, handles and every frame stay on the thread that started EAL. Most programs open the Stack with `switch_open::open` from a network config file instead of calling `Eal::init` and `Port::open` themselves.
//!
//! ```rust,no_run
//! use std::net::SocketAddrV4;
//! use std::time::Instant as WallInstant;
//!
//! use dpdk_smoltcp::smoltcp::socket::tcp::{self, State};
//! use dpdk_smoltcp::smoltcp::time::Instant;
//! use dpdk_smoltcp::Stack;
//!
//! /// Connect, send one line, print what comes back, close when the server closes.
//! /// `stack` was created at `Instant::ZERO` just after `start` was taken.
//! fn talk(stack: &mut Stack, start: WallInstant, server: SocketAddrV4) -> Result<(), Box<dyn std::error::Error>> {
//!     let clock = || Instant::from_micros(i64::try_from(start.elapsed().as_micros()).unwrap());
//!     let handle = stack.add_tcp(tcp::Config { rx_capacity: 65536, tx_capacity: 65536, mss_cap: None })?;
//!     // There is no ephemeral port allocator; the local port must be nonzero.
//!     stack.connect(&handle, server, 40000)?;
//!     let line = b"hello\n";
//!     // `send_slice` queues what fits, possibly nothing; keep going until the line is queued.
//!     let mut queued = 0;
//!     loop {
//!         stack.receive(clock(), 32)?;
//!         let socket = stack.tcp_mut(&handle);
//!         while socket.may_recv() {
//!             let Some(frame) = socket.recv()? else { break };
//!             println!("{:?}", frame.payload());
//!         }
//!         if queued < line.len() && socket.may_send() {
//!             queued += socket.send_slice(&line[queued..])?;
//!         }
//!         if socket.state() == State::CloseWait {
//!             stack.close(&handle);
//!         }
//!         if stack.tcp(&handle).state() == State::Closed {
//!             return Ok(());
//!         }
//!         stack.send()?;
//!     }
//! }
//! ```
//!
//! See the switch book, part "The Stack without async", starting at chapter "Programming the Stack directly".

#[cfg(doctest)]
mod api_guards {
    //! `make test-stack` builds `switch-buffer` with default features as a dependency
    //! of this crate (`switch-tap-echo` also depends on it that way), so these doctests check
    //! that the test-only EAL and pool helpers are not exported. They assume a
    //! per-package run (`cargo test -p dpdk-smoltcp`): testing several packages at
    //! once, `switch-buffer`'s included, unifies the `testing` feature that its self
    //! dev-dependency enables, and these `compile_fail` tests then fail.
    //!
    //! ```compile_fail,E0432
    //! use switch_buffer::ensure_eal_init;
    //! ```
    //! ```compile_fail,E0432
    //! use switch_buffer::acquire_test_pool;
    //! ```
    //! ```compile_fail,E0432
    //! use switch_buffer::TestPoolGuard;
    //! ```
    //! ```compile_fail,E0599
    //! let _ = switch_buffer::PoolManager::new;
    //! ```
    //! ```compile_fail,E0599
    //! let _ = switch_buffer::PoolManager::new_unchecked;
    //! ```
    //! The production API resolves through the same paths:
    //! ```
    //! use switch_buffer::{Mbuf, PoolConfig, PoolCreateError, PoolManager};
    //! let _ = PoolManager::new_production;
    //! ```
    //! Applications cannot disable TX frame-pool wakes:
    //! ```compile_fail,E0624
    //! fn disable_wake(stack: &mut dpdk_smoltcp::Stack, h: &dpdk_smoltcp::TcpHandle) {
    //!     stack.tcp_mut(h).clear_frame_wake();
    //! }
    //! ```
    //! Applications get TCP sockets from `Stack::add_tcp(tcp::Config)`, and no public API hands out
    //! an owned socket, so the MSS cap comes from the config and an admitted socket cannot be
    //! overwritten with a new one. Swapping it with a socket held by another `TcpSet` or Stack is
    //! unsupported:
    //! ```compile_fail,E0624
    //! use dpdk_smoltcp::smoltcp::socket::tcp;
    //! let _ = tcp::Socket::new;
    //! ```
    //! ```compile_fail,E0624
    //! use dpdk_smoltcp::smoltcp::socket::tcp;
    //! let _ = tcp::Socket::set_mss_cap;
    //! ```
    //! ```compile_fail,E0624
    //! fn unadmitted(set: &mut dpdk_smoltcp::smoltcp::iface::TcpSet<'static>,
    //!               socket: dpdk_smoltcp::smoltcp::socket::tcp::Socket<'static>) {
    //!     set.add(socket);
    //! }
    //! ```
    //! Removing a socket from any `TcpSet` drops it, so no admitted socket can be taken out and
    //! written over another one through `Stack::tcp_mut`:
    //! ```compile_fail,E0308
    //! use dpdk_smoltcp::smoltcp::{iface::{SocketHandle, TcpSet}, socket::tcp};
    //! fn removed(set: &mut TcpSet<'static>, h: SocketHandle) -> tcp::Socket<'static> {
    //!     set.remove(h).0
    //! }
    //! ```
    //! ```compile_fail,E0624
    //! use dpdk_smoltcp::smoltcp::{iface::{SocketHandle, TcpSet}, socket::tcp};
    //! fn taken(set: &mut TcpSet<'static>, h: SocketHandle) -> tcp::Socket<'static> {
    //!     set.take(h).0
    //! }
    //! ```
}

mod device;
mod eal;
mod handle;
mod port;
mod rx_pool;
mod stack;

pub use eal::{Eal, EalError};
pub use handle::{TcpHandle, UdpHandle};
pub use port::{
    device_info, port_by_name, port_resets, read_clock, DeviceInfo, Offloads, Packet, Port,
    PortConfig, PortError, PortOpenError, RxBurst, TxPacket,
};
pub use rx_pool::{frame_reserve, rx_reserve, tx_reserve};
pub use smoltcp;
pub use smoltcp::phy::HeldFrames;
pub use smoltcp::socket::tcp::TcpFrame;
pub use smoltcp::iface::{
    NoRoute, ReadySide, UdpConfig, UdpConnectError, UdpDatagram, UdpSendError, UdpSocket,
    UdpSocketStats,
};
pub use stack::{
    os_random_seed, AddTcpError, AddUdpError, Sent, Stack, StackConfig, StackError, TcpReadyKey,
    Turn, UdpReadyKey, UdpSendFrameError, DEFAULT_NEIGHBOR_CAPACITY,
};
