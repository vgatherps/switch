//! The message format of sockperf 3.10, a kernel-socket latency tool, so `switch-nic
//! sockperf-server` can answer unmodified `sockperf` clients. It has no dependencies and does not
//! use DPDK. Everything is in [`sockperf`].
//!
//! ```rust
//! use switch_sockperf_wire::sockperf::{reply, validate_udp, Framer, MAX_MESSAGE};
//!
//! let mut message = vec![0u8; 64];
//! message[8..10].copy_from_slice(&3u16.to_be_bytes());
//! message[10..14].copy_from_slice(&64u32.to_be_bytes());
//! assert!(validate_udp(&message).is_ok());
//!
//! let mut framer = Framer::new(MAX_MESSAGE);
//! let mut replies = 0;
//! framer
//!     .push(&message, |m| {
//!         if reply(m) {
//!             replies += 1;
//!         }
//!     })
//!     .unwrap();
//! assert_eq!(replies, 1);
//! ```
//!
//! The server is described in `docs/operations/switch-nic.md` (section `sockperf-server`).
/// Validating, answering and framing sockperf messages.
pub mod sockperf;
