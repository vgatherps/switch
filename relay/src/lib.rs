//! WebSocket (RFC 6455) over switchnet TCP: clients, plain (ws://) or through sheath's TLS 1.3
//! (wss://), and a plain ws:// server: feature `switchnet`, module [`switchnet`]. Received messages borrow the frame
//! bytes in place; only messages spanning frames or fragments are copied, into one reusable
//! scratch buffer. A sent message that fits one frame is written straight into the frame
//! buffer, behind the header; a longer one is copied once into a reusable send buffer, then
//! out a frame at a time. Over wss these are the plaintext buffers: sheath copies each sealed
//! record's plaintext once more for rustls to encrypt it into the frame.
//! Masked payloads are XORed in eight-byte chunks, including across receive pieces.
//! For plain WebSocket connections, `Ws::recv_with_sender` permits sending directly from a
//! borrowed received message without an intermediate payload buffer.
//!
//! Without the feature only the protocol core builds, crate-private and exercised by its unit
//! tests (it needs no DPDK, so they run on any host and under Miri).
#![cfg_attr(not(feature = "switchnet"), allow(dead_code))]

mod lower;
mod mask;
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

pub use rx::{Kind, Limits, Message, ProtocolError};
pub use upgrade::UpgradeError;
