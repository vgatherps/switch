//! WebSocket (RFC 6455) over TCP: a `wss://` client (over TLS 1.3), a `ws://` client and a
//! `ws://` server, in module [`net`]. Applications reach it as `switch::ws` (feature `ws`).
//!
//! The root holds what all three share: the received [`Message`] and its [`Kind`], the size
//! [`Limits`], and the failure reasons [`ProtocolError`] and [`UpgradeError`]. No extension or
//! subprotocol is offered or accepted.
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
pub mod net;

#[cfg(test)]
mod tests;

pub use rx::{Kind, Limits, Message, ProtocolError};
pub use upgrade::UpgradeError;
