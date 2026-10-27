use alloc::collections::VecDeque;
use alloc::vec;
use alloc::vec::Vec;

use crate::phy::{self, ChecksumCapabilities, Device, DeviceCapabilities, Medium};
use crate::time::Instant;

/// A loopback device.
#[derive(Debug)]
pub struct Loopback {
    pub(crate) queue: VecDeque<Vec<u8>>,
    medium: Medium,
}

#[allow(clippy::new_without_default)]
impl Loopback {
    /// Creates a loopback device.
    ///
    /// Every packet transmitted through this device will be received through it
    /// in FIFO order.
    pub fn new(medium: Medium) -> Loopback {
        Loopback {
            queue: VecDeque::new(),
            medium,
        }
    }
}

impl Device for Loopback {
    type RxToken<'a> = RxToken;
    type TxToken<'a> = TxToken<'a>;
    type FrameToken<'a> = FrameToken<'a>;

    fn capabilities(&self) -> DeviceCapabilities {
        DeviceCapabilities {
            max_transmission_unit: 65535,
            medium: self.medium,
            checksum: ChecksumCapabilities::ignored(),
            ..DeviceCapabilities::default()
        }
    }

    fn receive(&mut self, _timestamp: Instant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        self.queue.pop_front().map(move |buffer| {
            let rx = RxToken { buffer };
            let tx = TxToken {
                queue: &mut self.queue,
            };
            (rx, tx)
        })
    }

    fn transmit(&mut self, _timestamp: Instant) -> Option<Self::TxToken<'_>> {
        Some(TxToken {
            queue: &mut self.queue,
        })
    }

    fn transmit_frame(&mut self, _timestamp: Instant) -> Option<Self::FrameToken<'_>> {
        Some(FrameToken {
            queue: &mut self.queue,
        })
    }
}

#[doc(hidden)]
pub struct RxToken {
    buffer: Vec<u8>,
}

impl phy::RxToken for RxToken {
    fn into_buf(self) -> phy::RxBuf {
        phy::RxBuf::from_vec(self.buffer)
    }
}

#[doc(hidden)]
#[derive(Debug)]
pub struct TxToken<'a> {
    queue: &'a mut VecDeque<Vec<u8>>,
}

impl<'a> phy::TxToken for TxToken<'a> {
    fn consume<R, F>(self, len: usize, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        let mut buffer = vec![0; len];
        let result = f(&mut buffer);
        self.queue.push_back(buffer);
        result
    }
}

#[doc(hidden)]
#[derive(Debug)]
pub struct FrameToken<'a> {
    queue: &'a mut VecDeque<Vec<u8>>,
}

impl phy::FrameToken for FrameToken<'_> {
    fn consume(self, header: phy::TxHeader, frame: phy::TxBuf) -> Result<(), phy::TxBuf> {
        let buffer = phy::frame_bytes(header, &frame);
        self.queue.push_back(buffer);
        Ok(())
    }

    fn consume_retain(
        self,
        header: phy::TxHeader,
        frame: phy::TxBuf,
    ) -> Result<phy::TxBuf, phy::TxBuf> {
        let buffer = phy::frame_bytes(header, &frame);
        self.queue.push_back(buffer);
        Ok(frame)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::phy::FrameToken as _;

    #[test]
    fn transmit_frame_copies_headers_and_payload() {
        let mut device = Loopback::new(Medium::Ip);
        let frame = crate::tests::tx_test_buf(&[3, 4]);
        let mut header = phy::TxHeader::new(2, None);
        header.bytes_mut().copy_from_slice(&[1, 2]);
        let frame = device
            .transmit_frame(Instant::ZERO)
            .unwrap()
            .consume_retain(header, frame)
            .ok()
            .unwrap();
        assert_eq!(device.queue.pop_front().unwrap(), &[1, 2, 3, 4]);
        assert!(frame.unique());
    }
}
