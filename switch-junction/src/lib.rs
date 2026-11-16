//! Client side of the junction daemon: connect to a running `switch-junctiond` and use TCP and UDP sockets on the NIC it owns, with data moving through shared memory. This crate does not use DPDK. Most applications use it through switch-net, by passing a connected `Session` to `switch_net::Net::remote`; the same tasks then run unchanged against the daemon.
//!
//! A [`session::Session`] is one application's connection to the daemon. `Session::connect` takes the path of the daemon's `control.sock` and the number of socket slots wanted. Opening a socket (`connect_tcp`, `listen_tcp`, `bind_udp`) returns a [`control::SocketId`] at once; the daemon answers later with an [`control::Event`]: `Connected` or `Bound` when the socket is ready, `Rejected` when the open failed. The program must wait for that answer before sending on the socket. `poll` delivers events and then received packets, each packet as an [`rx::PacketMeta`] and an [`rx::OwnedFrame`] that points into the session's receive ring. Ring space returns once the frame is dropped and a later poll reclaims it. A `Session` belongs to the thread that connected it. End it with `finish()` and keep polling until `closed()`; dropping it without finishing resets every socket it still had.
//!
//! ```rust,no_run
//! use std::net::{Ipv4Addr, SocketAddrV4};
//! use std::path::Path;
//! use switch_junction::control::{Event, ListenEndpoint, UdpConfig};
//! use switch_junction::session::Session;
//!
//! let mut session = Session::connect(Path::new("/run/switch-junctiond/control.sock"), 8)?;
//! let id = session.bind_udp(UdpConfig {
//!     local: ListenEndpoint { addr: None, port: 30000 },
//!     rx_queue: 8,
//!     tx_checksum: true,
//!     broadcast: false,
//! })?;
//!
//! // Wait for the daemon's answer before sending on the socket.
//! let mut answer = None;
//! while answer.is_none() {
//!     session.poll_events(|socket, event| {
//!         if socket == id {
//!             match event {
//!                 Event::Bound => answer = Some(Ok(())),
//!                 Event::Rejected(code) => answer = Some(Err(code)),
//!                 _ => {}
//!             }
//!         }
//!     })?;
//! }
//! if let Some(Err(code)) = answer {
//!     return Err(format!("bind rejected: {code:?}").into());
//! }
//! session.send_udp(id, SocketAddrV4::new(Ipv4Addr::new(10, 77, 0, 1), 30001), b"hello")?;
//!
//! // Print one datagram, then leave cleanly.
//! let mut received = false;
//! while !received {
//!     session.poll(|_, _| {}, |_, meta, frame| {
//!         println!("{:?}:{} sent {:?}", meta.src_addr, meta.src_port, &*frame);
//!         received = true;
//!     })?;
//! }
//! session.close(id, false);
//! while !session.closed() {
//!     let _ = session.finish();
//!     session.poll(|_, _| {}, |_, _, _| {})?;
//! }
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! The `control` and `send` modules define the wire format, for people writing another client; `host` is the daemon's side of the control socket, public so the daemon and in-process test fixtures can use it.
//!
//! See the switch book, chapter "Sharing a NIC with the junction daemon", and "Using a Session directly" for this API.

pub mod control;
pub mod host;
pub mod rx;
pub mod send;
pub mod session;

#[cfg(doctest)]
mod api_guards {
    //! A session cannot move to another thread:
    //!
    //! ```compile_fail,E0277
    //! fn require_send<T: Send>() {}
    //! require_send::<switch_junction::session::Session>();
    //! ```
}
