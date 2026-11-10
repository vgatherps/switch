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
    source: phy::TxSource,
}

#[allow(clippy::new_without_default)]
impl Loopback {
    /// Creates a loopback device.
    ///
    /// Every packet transmitted through this device will be received through it
    /// in FIFO order.
    pub fn new(medium: Medium, source: phy::TxSource) -> Loopback {
        Loopback {
            queue: VecDeque::new(),
            medium,
            source,
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
            origin: self.source.origin(),
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
    origin: *mut (),
}

impl phy::FrameToken for FrameToken<'_> {
    fn consume(self, frame: phy::ImmutableFrame, header_len: usize, _: Option<phy::TxChecksum>) {
        assert_eq!(frame.ctx(), self.origin, "foreign TX frame producer");
        #[allow(unsafe_code)]
        self.queue.push_back(unsafe { crate::phy::frame_packet(&frame, header_len) }.to_vec());
    }

    fn consume_retain(
        self,
        frame: phy::ImmutableFrame,
        header_len: usize,
        _: Option<phy::TxChecksum>,
    ) -> phy::ImmutableFrame {
        assert_eq!(frame.ctx(), self.origin, "foreign TX frame producer");
        #[allow(unsafe_code)]
        self.queue.push_back(unsafe { crate::phy::frame_packet(&frame, header_len) }.to_vec());
        frame
    }
}

#[cfg(test)]
#[allow(unsafe_code)]
mod tests {
    use super::*;
    use crate::phy::FrameToken as _;

    #[test]
    fn transmit_frame_copies_headers_and_payload() {
        let source = phy::TxSource::new(phy::TxPool::new(2));
        let mut device = Loopback::new(Medium::Ethernet, source.clone());
        let mut frame = source.alloc().unwrap();
        frame.copy_from_slice(&[3, 4]);
        // SAFETY: the source allocated this frame at its original start.
        unsafe { phy::frame_headroom(&mut frame, 2) }.0.copy_from_slice(&[1, 2]);
        let frame = frame.freeze();
        let frame = device
            .transmit_frame(Instant::ZERO)
            .unwrap()
            .consume_retain(frame, 2, None);
        assert_eq!(device.queue.pop_front().unwrap(), &[1, 2, 3, 4]);
        assert_eq!(crate::phy::frame_refcount(&frame), 1);
    }

    #[test]
    #[should_panic(expected = "foreign TX frame producer")]
    fn transmit_frame_rejects_foreign_producer() {
        let source = phy::TxSource::new(phy::TxPool::new(2));
        let mut device = Loopback::new(Medium::Ethernet, source);
        let foreign = phy::TxPool::new(2);
        let frame = phy::alloc_frame(&foreign).unwrap().freeze();
        device.transmit_frame(Instant::ZERO).unwrap().consume(frame, 0, None);
    }

    #[test]
    #[should_panic(expected = "foreign TX frame producer")]
    fn transmit_frame_retain_rejects_foreign_producer() {
        let source = phy::TxSource::new(phy::TxPool::new(2));
        let mut device = Loopback::new(Medium::Ethernet, source);
        let foreign = phy::TxPool::new(2);
        let frame = phy::alloc_frame(&foreign).unwrap().freeze();
        device.transmit_frame(Instant::ZERO).unwrap().consume_retain(frame, 0, None);
    }
}
