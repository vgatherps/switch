//! Owned receive records: TCP byte-stream chunks and whole UDP datagrams.
//!
//! Retaining an `OwnedFrame` or `ImmutableFrame` prevents reuse of its ring storage.
//! Drop frames and continue polling the session to reclaim that storage for the host.

pub use switch_frame::{ImmutableFrame, OwnedFrame};
pub use slice_ringbuffer::PacketMeta;

/// TCP payload record.
pub const TCP: u8 = 1;
/// UDP datagram record.
pub const UDP: u8 = 2;
