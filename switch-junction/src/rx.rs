//! What a session hands over for each received packet: a [`PacketMeta`] and an
//! [`OwnedFrame`] that points into the session's receive ring.
//!
//! A TCP connection's data arrives as a series of frames in byte order, which need not match
//! the sender's writes; a UDP datagram arrives whole, as one frame. Ring space comes back
//! only after the frame is dropped and a later poll reclaims it
//! ([`Session`](crate::session::Session#received-packets-and-ring-space)).

pub use switch_frame::{ImmutableFrame, OwnedFrame};
pub use slice_ringbuffer::PacketMeta;

/// [`PacketMeta::kind`] of a TCP data chunk.
pub const TCP: u8 = 1;
/// [`PacketMeta::kind`] of a UDP datagram.
pub const UDP: u8 = 2;
