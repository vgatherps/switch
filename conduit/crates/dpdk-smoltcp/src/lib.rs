//! A synchronous TCP/UDP stack for one DPDK NIC port, IPv4 over Ethernet, driven from one
//! thread with no async runtime.
//!
//! [`Stack`] is the stack. Its methods are the reference for programs that drive it
//! directly: the drive calls ([`Stack::receive`], [`Stack::send`], [`Stack::turn`]), time,
//! sockets and readiness tokens. Applications usually get a `Stack` from `switch::nic::open`
//! and hand it to `switch::net`; [`Eal::init`], [`Port::open`] and [`Stack::new`] build one
//! by hand. The TCP/IP code is a vendored, modified smoltcp, re-exported as [`smoltcp`].

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
    //! The production API resolves through the same paths:
    //! ```
    //! use switch_buffer::{Mbuf, PoolConfig, PoolCreateError, PoolManager};
    //! let _ = PoolManager::new_production;
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
    //! ```compile_fail,E0609
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
    //! `Eal`, `TcpHandle` and `UdpHandle` stay on the thread that made them:
    //! ```compile_fail
    //! use dpdk_smoltcp::Eal;
    //! fn requires_send<T: Send>() {}
    //! requires_send::<Eal>();
    //! ```
    //! ```compile_fail
    //! use dpdk_smoltcp::Eal;
    //! fn requires_sync<T: Sync>() {}
    //! requires_sync::<Eal>();
    //! ```
    //! ```compile_fail,E0277
    //! use dpdk_smoltcp::TcpHandle;
    //! fn needs_send<T: Send>(_: T) {}
    //! fn f(h: TcpHandle) { needs_send(h); }
    //! ```
    //! `remove_tcp` and `remove_udp` consume the handle, and handles are not `Clone`:
    //! ```compile_fail,E0382
    //! use dpdk_smoltcp::{Stack, TcpHandle};
    //! fn f(stack: &mut Stack, h: TcpHandle) {
    //!     stack.remove_tcp(h);
    //!     stack.tcp(&h);
    //! }
    //! ```
    //! ```compile_fail,E0382
    //! use dpdk_smoltcp::{Stack, TcpHandle};
    //! fn f(stack: &mut Stack, h: TcpHandle) {
    //!     stack.remove_tcp(h);
    //!     stack.remove_tcp(h);
    //! }
    //! ```
    //! ```compile_fail,E0599
    //! use dpdk_smoltcp::TcpHandle;
    //! fn f(h: TcpHandle) { h.clone(); }
    //! ```
    //! ```no_run
    //! use dpdk_smoltcp::{Stack, TcpHandle};
    //! fn f(stack: &mut Stack, h: TcpHandle) {
    //!     let state = stack.tcp(&h).state();
    //!     stack.remove_tcp(h);
    //! }
    //! ```
    //! ```compile_fail,E0382
    //! use dpdk_smoltcp::{Stack, UdpHandle};
    //! fn f(stack: &mut Stack, h: UdpHandle) {
    //!     stack.remove_udp(h);
    //!     stack.udp(&h);
    //! }
    //! ```
    //! ```compile_fail,E0382
    //! use dpdk_smoltcp::{Stack, UdpHandle};
    //! fn f(stack: &mut Stack, h: UdpHandle) {
    //!     stack.remove_udp(h);
    //!     stack.remove_udp(h);
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
