//! WebSocket (RFC 6455) over switch-net TCP: clients, plain (ws://) or through switch-tls's TLS 1.3 (wss://), and a plain ws:// server: feature `switch-net`, module [`switch_net`]. Received messages borrow the frame bytes in place; only messages spanning frames or fragments are copied, into one reusable scratch buffer. A sent message that fits one frame is written straight into the frame buffer, behind the header; a longer one is copied once into a reusable send buffer, then out a frame at a time. Over wss these are the plaintext buffers: switch-tls copies each sealed record's plaintext once more for rustls to encrypt it into the frame. For plain WebSocket connections, `Ws::recv_with_sender` permits sending directly from a borrowed received message without an intermediate payload buffer.
//!
//! `switch_net::connect` (wss://), `connect_ws` (ws://) and `accept_ws` (ws:// server) take a connected or accepted `TcpStream` and return a [`switch_net::Wss`] or [`switch_net::Ws`]. Both have `recv`, `send`, `close`, `linger` and `peer_close`, and are used from one switch task. There is no background task: queued output (the rest of a long message, a pong, a close echo) goes out during the next `send`, `close`, `recv` or `linger`, so a program that only sends must still call `recv`. A [`Message`] borrows the connection until it is dropped. `send` waits for room; the waits that depend on the peer (connect, `recv` during a close, `close`, `linger`) should be bounded with `switch_net::within`. No extensions or subprotocols are offered or accepted.
//!
//! ```rust,no_run
//! # #[cfg(feature = "switch-net")]
//! # mod example {
//! use std::sync::Arc;
//! use rustls::ClientConfig;
//! use switch_net::{IpEndpoint, IpListenEndpoint, NetHandle, tcp};
//! use switch_ws::{Kind, Limits};
//!
//! async fn hello(handle: &NetHandle, config: Arc<ClientConfig>, server: IpEndpoint, socket: tcp::Config) -> Result<(), switch_ws::switch_net::Error> {
//!     let local = IpListenEndpoint { addr: None, port: 49152 };
//!     let stream = handle.tcp_connect(socket, server, local).await.expect("tcp connect");
//!     let mut wss = switch_ws::switch_net::connect(handle, stream, config, "example.com", 443, "/ws", Limits::default()).await?;
//!     wss.send(Kind::Text, b"hello").await?;
//!     if let Some(message) = wss.recv().await? {
//!         println!("{:?} {} bytes", message.kind(), message.bytes().len());
//!     }
//!     wss.close(1000, "").await?;
//!     while wss.recv().await?.is_some() {}
//!     Ok(())
//! }
//! # }
//! ```
//!
//! Without the feature only the protocol core builds, crate-private and exercised by its unit tests (it needs no DPDK, so they run on any host and under Miri).
//!
//! Book: See the switch book, part "TLS and WebSocket", starting at "TLS and WebSocket over switch-net".
#![cfg_attr(not(feature = "switch-net"), allow(dead_code))]

mod lower;
mod mask;
mod raw;
mod rx;
mod session;
mod tx;
mod upgrade;
mod utf8;

#[cfg(feature = "switch-net")]
pub mod switch_net;

#[cfg(test)]
mod tests;

pub use rx::{Kind, Limits, Message, ProtocolError};
pub use upgrade::UpgradeError;
