use alloc::rc::Rc;
use netframe::OwnedFrame;

use crate::phy::RxShared;
use super::rx_store::RxStore;

/// An owned in-order TCP frame from one received segment.
#[derive(Debug)]
pub struct TcpFrame {
    pub(super) frame: OwnedFrame,
    pub(super) rx_timestamp: u64,
}

impl TcpFrame {
    pub fn payload(&self) -> &[u8] {
        &self.frame
    }

    pub fn payload_mut(&mut self) -> &mut [u8] {
        &mut self.frame
    }

    /// Raw NIC stamp of the segment that created this frame; 0 means none.
    pub fn rx_timestamp(&self) -> u64 {
        self.rx_timestamp
    }

    /// Transfer the payload storage; socket accounting lasts through its final clone.
    pub fn into_frame(self) -> OwnedFrame {
        self.frame
    }
}

/// Receive storage for a TCP socket.
#[derive(Debug)]
pub struct RxBuffer {
    pub(super) store: RxStore,
    pub(super) capacity: usize,
    pub(super) shared: Rc<RxShared>,
}

impl RxBuffer {
    /// Set the receive window ceiling and its scale.
    pub fn new(capacity: usize) -> RxBuffer {
        RxBuffer {
            store: RxStore::new(),
            capacity,
            shared: RxShared::new(),
        }
    }
}
