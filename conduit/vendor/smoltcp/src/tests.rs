use std::collections::VecDeque;

use crate::iface::*;
use crate::phy::{self, Device, DeviceCapabilities, Medium};
use crate::time::Instant;
use crate::wire::*;

pub(crate) fn setup<'a>(medium: Medium) -> (Interface, TcpSet<'a>, TestingDevice) {
    let mut device = TestingDevice::new(medium);
    let iface = setup_with_device(medium, &mut device);
    (iface, TcpSet::new(vec![]), device)
}

fn setup_with_device(medium: Medium, device: &mut impl Device) -> Interface {
    let config = Config::new(match medium {
        Medium::Ethernet => {
            HardwareAddress::Ethernet(EthernetAddress([0x02, 0x02, 0x02, 0x02, 0x02, 0x02]))
        }
        #[cfg(feature = "medium-ip")]
        Medium::Ip => HardwareAddress::Ip,
    });

    let mut iface = Interface::new(config, device, Instant::ZERO);

    {
        iface.update_ip_addrs(|ip_addrs| {
            ip_addrs
                .push(IpCidr::new(IpAddress::v4(192, 168, 1, 1), 24))
                .unwrap();
            ip_addrs
                .push(IpCidr::new(IpAddress::v4(127, 0, 0, 1), 8))
                .unwrap();
        });
    }

    iface
}

/// A testing device.
#[derive(Debug)]
pub struct TestingDevice {
    pub(crate) tx_queue: VecDeque<Vec<u8>>,
    pub(crate) rx_queue: VecDeque<Vec<u8>>,
    max_transmission_unit: usize,
    medium: Medium,
}

#[allow(clippy::new_without_default)]
impl TestingDevice {
    /// Creates a testing device.
    ///
    /// Every packet transmitted through this device will be received through it
    /// in FIFO order.
    pub fn new(medium: Medium) -> Self {
        TestingDevice {
            tx_queue: VecDeque::new(),
            rx_queue: VecDeque::new(),
            max_transmission_unit: match medium {
                Medium::Ethernet => 1514,
                #[cfg(feature = "medium-ip")]
                Medium::Ip => 1500,
            },
            medium,
        }
    }
}

impl Device for TestingDevice {
    type RxToken<'a> = RxToken;
    type TxToken<'a> = TxToken<'a>;
    type FrameToken<'a> = FrameToken<'a>;

    fn capabilities(&self) -> DeviceCapabilities {
        DeviceCapabilities {
            medium: self.medium,
            max_transmission_unit: self.max_transmission_unit,
            ..DeviceCapabilities::default()
        }
    }

    fn receive(&mut self, _timestamp: Instant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        self.rx_queue.pop_front().map(move |buffer| {
            let rx = RxToken { buffer };
            let tx = TxToken {
                queue: &mut self.tx_queue,
            };
            (rx, tx)
        })
    }

    fn transmit(&mut self, _timestamp: Instant) -> Option<Self::TxToken<'_>> {
        Some(TxToken {
            queue: &mut self.tx_queue,
        })
    }

    fn transmit_frame(&mut self, _timestamp: Instant) -> Option<Self::FrameToken<'_>> {
        Some(FrameToken {
            queue: &mut self.tx_queue,
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
    fn consume_shared<R, F>(self, frame: &phy::TxBuf, header_len: usize, f: F) -> R
    where
        F: FnOnce(&mut [u8], &[u8]) -> R,
    {
        let (result, buffer) = phy::copy_with_headers(frame, header_len, f);
        self.queue.push_back(buffer);
        result
    }

    fn consume_owned<R, F>(self, frame: phy::TxBuf, header_len: usize, f: F) -> R
    where
        F: FnOnce(&mut [u8], &[u8]) -> R,
    {
        let (result, buffer) = phy::copy_with_headers(&frame, header_len, f);
        self.queue.push_back(buffer);
        result
    }
}

pub(crate) fn tx_test_buf(bytes: &[u8]) -> phy::TxBuf {
    let alloc = phy::test_owner::CountingAlloc::new(bytes.len().max(1), 64);
    let mut buf = phy::TxAlloc::alloc(alloc.as_ref()).unwrap();
    buf.room_mut()[..bytes.len()].copy_from_slice(bytes);
    buf.set_len(bytes.len());
    buf
}

#[test]
fn testing_device_frame_token_writes_before_publish() {
    use crate::phy::FrameToken as _;

    let mut device = TestingDevice::new(Medium::Ip);
    let frame = tx_test_buf(&[3, 4]);
    let token = device.transmit_frame(Instant::ZERO).unwrap();
    let result = token.consume_shared(&frame, 2, |headers, payload| {
        assert_eq!(payload, &[3, 4]);
        headers.copy_from_slice(&[1, 2]);
        7
    });
    assert_eq!(result, 7);
    assert_eq!(device.tx_queue.pop_front().unwrap(), &[1, 2, 3, 4]);
    assert!(frame.unique());
}

#[test]
fn testing_device_owned_frame_releases_after_publish() {
    use crate::phy::FrameToken as _;

    let alloc = phy::test_owner::CountingAlloc::new(2, 64);
    let mut frame = phy::TxAlloc::alloc(alloc.as_ref()).unwrap();
    frame.room_mut()[..2].copy_from_slice(&[3, 4]);
    frame.set_len(2);
    let mut device = TestingDevice::new(Medium::Ip);
    device
        .transmit_frame(Instant::ZERO)
        .unwrap()
        .consume_owned(frame, 2, |headers, _| headers.copy_from_slice(&[1, 2]));
    assert_eq!(device.tx_queue.pop_front().unwrap(), &[1, 2, 3, 4]);
    assert_eq!(alloc.live(), 0);
}

#[test]
fn testing_device_shared_panic_does_not_publish() {
    use crate::phy::FrameToken as _;
    use std::panic::{catch_unwind, AssertUnwindSafe};

    let mut device = TestingDevice::new(Medium::Ip);
    let frame = tx_test_buf(&[3, 4]);
    let result = catch_unwind(AssertUnwindSafe(|| {
        device
            .transmit_frame(Instant::ZERO)
            .unwrap()
            .consume_shared(&frame, 2, |headers, _| {
                headers.copy_from_slice(&[1, 2]);
                panic!("stop before publish");
            });
    }));
    assert!(result.is_err());
    assert!(device.tx_queue.is_empty());
    assert!(frame.unique());
}

#[test]
fn testing_device_owned_panic_releases_without_publish() {
    use crate::phy::FrameToken as _;
    use std::panic::{catch_unwind, AssertUnwindSafe};

    let alloc = phy::test_owner::CountingAlloc::new(2, 64);
    let mut frame = phy::TxAlloc::alloc(alloc.as_ref()).unwrap();
    frame.room_mut()[..2].copy_from_slice(&[3, 4]);
    frame.set_len(2);
    let mut device = TestingDevice::new(Medium::Ip);
    let result = catch_unwind(AssertUnwindSafe(|| {
        device
            .transmit_frame(Instant::ZERO)
            .unwrap()
            .consume_owned(frame, 2, |headers, _| {
                headers.copy_from_slice(&[1, 2]);
                panic!("stop before publish");
            });
    }));
    assert!(result.is_err());
    assert!(device.tx_queue.is_empty());
    assert_eq!(alloc.live(), 0);
}
