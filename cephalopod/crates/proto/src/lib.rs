//! Definitions shared by cephalopod's consumer (`ceph-fifo`) and its control thread (`ceph-control`).
//!
//! [`Control`] goes from the control thread to the consumer when a stream opens or closes; [`Retire`] goes back when the consumer has delivered a stream's end record or kicked the stream. A program wiring `ceph_control::Listener` to `ceph_fifo::Fifo` creates one `std::sync::mpsc` channel of each. Stream ids keep the slot in the high 32 bits and the unique id in the low 32 bits; [`pack`], [`slot`] and [`uid`] convert between them. switch-junction and switch-junctiond also use these types.
//!
//! See the switch book, chapter "cephalopod: many writers, one reader".

/// A stream id as carried in every record: the slot index in the high 32 bits, the unique id
/// in the low 32 bits.
pub fn pack(slot: u32, uid: u32) -> u64 {
    (slot as u64) << 32 | uid as u64
}

pub fn slot(stream: u64) -> u32 {
    (stream >> 32) as u32
}

pub fn uid(stream: u64) -> u32 {
    stream as u32
}

/// Control thread to polling thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Control {
    Open { slot: u32, uid: u32 },
    Close { slot: u32, uid: u32 },
}

/// Polling thread to control thread. The control thread closes the socket and frees the slot in
/// both cases.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retire {
    Kicked { slot: u32, uid: u32 },
    Finished { slot: u32, uid: u32 },
}
