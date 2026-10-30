//! WebSocket (RFC 6455) over switchnet TCP: clients, plain (ws://) or through sheath's TLS 1.3
//! (wss://), and a plain ws:// server: feature `switchnet`, module [`switchnet`]. Received messages borrow the frame
//! bytes in place; only messages spanning frames or fragments are copied, into one reusable
//! scratch buffer. Sent payloads are written straight into the frame buffer, behind the header.
//!
//! Without the feature only the protocol core builds, crate-private and exercised by its unit
//! tests (it needs no DPDK, so they run on any host and under Miri).
#![cfg_attr(not(feature = "switchnet"), allow(dead_code))]

mod lower;
mod raw;
mod rx;
mod session;
mod tx;
mod upgrade;
mod utf8;

#[cfg(feature = "switchnet")]
pub mod switchnet;

#[cfg(test)]
mod tests;

pub use rx::{Kind, Message, ProtocolError};
pub use upgrade::UpgradeError;
