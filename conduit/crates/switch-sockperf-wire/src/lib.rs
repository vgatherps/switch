//! Message framing for the sockperf 3.10 wire format, used by `switch-nic sockperf-server` to answer kernel `sockperf` clients. It has no dependencies and does not use DPDK.
//!
//! A sockperf message starts with a [`sockperf::HEADER`] of 14 bytes; bytes 10 to 13 hold the whole message's length, big-endian, and bytes 8 and 9 hold flags. [`sockperf::validate_udp`] accepts a datagram that is exactly one message. [`sockperf::reply`] turns a validated message into its reply in place and returns `true` when the flags ask for one (`flags & 7 == 3`); otherwise it changes nothing. [`sockperf::Framer`] cuts a TCP byte stream into messages: `push` calls a closure for each complete message in order and keeps only the trailing partial one, and returns [`sockperf::Malformed`] for a length below `HEADER` or above the limit it was created with. [`sockperf::MAX_MESSAGE`] is 65507, the largest UDP payload.
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
//! See the switch book, chapter "switch-nic: checking a NIC and serving load".
pub mod sockperf;
