//! The messages between cephalopod's consumer (`ceph-fifo`) and its control thread
//! (`ceph-control`), and the stream-id layout they share. A program wiring
//! `ceph_control::Listener` to `ceph_fifo::Fifo` creates one `std::sync::mpsc` channel of
//! [`Control`] and one of [`Retire`]. The daemon and its client use the same types.

/// A stream id as carried in every record: the slot index in the high 32 bits, the unique id
/// in the low 32 bits.
pub fn pack(slot: u32, uid: u32) -> u64 {
    (slot as u64) << 32 | uid as u64
}

/// The slot index of a stream id.
pub fn slot(stream: u64) -> u32 {
    (stream >> 32) as u32
}

/// The unique id of a stream id.
pub fn uid(stream: u64) -> u32 {
    stream as u32
}

/// Control thread to consumer: a stream opened or closed. Records of a closed stream that
/// were not delivered yet are discarded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Control {
    Open { slot: u32, uid: u32 },
    Close { slot: u32, uid: u32 },
}

/// Consumer to control thread: a stream ended. The control thread closes the producer's
/// connection and frees the slot in both cases.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retire {
    /// The consumer dropped the stream because of a gap it could not fill.
    Kicked { slot: u32, uid: u32 },
    /// The consumer delivered the stream's end record.
    Finished { slot: u32, uid: u32 },
}
