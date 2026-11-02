use crate::phy::RxBuf;
use super::rx_store::RxStore;

/// An owned in-order TCP frame from one received segment.
#[derive(Debug)]
pub struct TcpFrame(pub(super) RxBuf);

impl TcpFrame {
    pub fn payload(&self) -> &[u8] {
        self.0.as_slice()
    }

    pub fn payload_mut(&mut self) -> &mut [u8] {
        self.0.as_mut_slice()
    }

    /// Raw NIC stamp of the segment that created this frame; 0 means none.
    pub fn rx_timestamp(&self) -> u64 {
        self.0.rx_timestamp()
    }
}

/// Receive storage for a TCP socket.
#[derive(Debug)]
pub struct RxBuffer {
    pub(super) store: RxStore,
    pub(super) capacity: usize,
}

impl RxBuffer {
    /// Set the receive window ceiling and its scale.
    pub fn new(capacity: usize) -> RxBuffer {
        RxBuffer {
            store: RxStore::new(),
            capacity,
        }
    }
}
