//! WebSocket (RFC 6455) over switch-net TCP: a `wss://` client (TLS 1.3 through switch-tls), a
//! plain `ws://` client and a plain `ws://` server, in module [`switch_net`] behind feature
//! `switch-net`.
//!
//! `switch_net::connect` (wss://), `connect_ws` (ws://) and `accept_ws` (ws:// server) take a
//! connected or accepted `TcpStream` and return a [`switch_net::Wss`] or [`switch_net::Ws`].
//! Both have `recv`, `send`, `close`, `linger` and `peer_close`, and are used from one switch
//! task. A [`Message`] borrows the connection until it is dropped. No extensions or
//! subprotocols are offered or accepted.
//!
//! There is no background task: the connection works only while a call is running. A pong, a
//! close echo, or the rest of a message whose `send` was cancelled after the message was
//! accepted goes out during the next `send`, `close`, `recv` or `linger`, so a program that only
//! sends must still call `recv`. `send` returns once the whole message is handed to TCP. The
//! waits that depend on the peer (connect, `close`, `recv` during a close, `linger`) last as
//! long as the peer stays silent; bound them with `switch_net::within`.
//!
//! ```rust,no_run
//! # #[cfg(feature = "switch-net")]
//! # mod example {
//! use std::error::Error;
//! use std::sync::Arc;
//! use std::time::Duration;
//! use rustls::ClientConfig;
//! use switch_runtime::Spawner;
//! use switch_net::{IpEndpoint, IpListenEndpoint, NetHandle, tcp, within};
//! use switch_ws::{Kind, Limits};
//!
//! async fn hello(
//!     handle: &NetHandle,
//!     spawner: &Spawner,
//!     config: Arc<ClientConfig>,
//!     server: IpEndpoint,
//!     socket: tcp::Config,
//! ) -> Result<(), Box<dyn Error>> {
//!     let limit = Duration::from_secs(10);
//!     let connecting = async {
//!         let local = IpListenEndpoint { addr: None, port: 49152 };
//!         let stream = handle.tcp_connect(socket, server, local).await.map_err(|e| format!("{e:?}"))?;
//!         let wss = switch_ws::switch_net::connect(handle, stream, config, "example.com", 443, "/ws", Limits::default())
//!             .await?;
//!         Ok::<_, Box<dyn Error>>(wss)
//!     };
//!     let mut wss = within(spawner, limit, connecting).await.ok_or("connect timed out")??;
//!     wss.send(Kind::Text, b"hello").await?;
//!     if let Some(message) = wss.recv().await? {
//!         println!("{:?} {} bytes", message.kind(), message.bytes().len());
//!     }
//!     let closing = async {
//!         wss.close(1000, "").await?;
//!         while wss.recv().await?.is_some() {}
//!         Ok::<_, switch_ws::switch_net::Error>(())
//!     };
//!     within(spawner, limit, closing).await.ok_or("close timed out")??;
//!     Ok(())
//! }
//! # }
//! ```
//!
//! Copies: received messages borrow the frame bytes in place; only messages spanning frames or
//! fragments are copied, into one reusable scratch buffer. A sent message that fits one frame is
//! written straight into the frame, behind the header; a longer one is copied once into a
//! reusable send buffer, then out a frame at a time. Over wss, switch-tls copies each record's
//! plaintext once more for rustls to encrypt it into the frame. On a `Ws`,
//! `Ws::recv_with_sender` sends straight from a borrowed received message.
//!
//! Book: See the switch book, part "TLS and WebSocket", starting at "TLS and WebSocket over
//! switch-net".
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
