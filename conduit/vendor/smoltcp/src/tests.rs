use std::collections::VecDeque;

use crate::iface::*;
use crate::phy::{self, Device, DeviceCapabilities, Medium};
use crate::time::Instant;
use crate::wire::*;

/// MAC of `setup`'s Ethernet interface.
pub(crate) const HARDWARE: EthernetAddress = EthernetAddress([0x02; 6]);
/// MAC of the test peer: the source of `wrap_eth` frames and the destination `strip_eth` expects.
pub(crate) const PEER_HARDWARE: EthernetAddress = EthernetAddress([2, 0, 0, 0, 0, 1]);
/// The test peer's address, on `setup`'s 192.168.1.1/24 subnet.
pub(crate) const PEER: Ipv4Address = Ipv4Address::new(192, 168, 1, 2);

pub(crate) fn setup<'a>(medium: Medium) -> (Interface, TcpSet<'a>, TestingDevice) {
    let mut device = TestingDevice::new(medium);
    let iface = setup_with_device(medium, &mut device);
    (iface, TcpSet::new(2048), device)
}
fn setup_with_device(medium: Medium, device: &mut impl Device) -> Interface {
    let config = Config::new(match medium {
        Medium::Ethernet => HardwareAddress::Ethernet(HARDWARE),
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

/// `setup(Medium::Ethernet)` with `PEER` resolved at `PEER_HARDWARE` for the whole test, so
/// egress to the peer never ARPs. `update_ip_addrs` flushes that entry.
pub(crate) fn setup_eth<'a>() -> (Interface, TcpSet<'a>, TestingDevice) {
    let (mut iface, sockets, device) = setup(Medium::Ethernet);
    iface.resolve_neighbor(PEER, PEER_HARDWARE);
    (iface, sockets, device)
}

/// The other end of `setup_eth`: `PEER`/24 at `PEER_HARDWARE`, with 192.168.1.1 resolved at
/// `HARDWARE` for the whole test.
pub(crate) fn setup_eth_peer<'a>() -> (Interface, TcpSet<'a>, TestingDevice) {
    let (mut iface, sockets, device) = setup(Medium::Ethernet);
    iface.set_hardware_addr(PEER_HARDWARE.into());
    iface.update_ip_addrs(|addrs| {
        addrs.clear();
        addrs.push(IpCidr::new(PEER.into(), 24)).unwrap();
    });
    iface.resolve_neighbor(Ipv4Address::new(192, 168, 1, 1), HARDWARE);
    (iface, sockets, device)
}

/// IPv4 packet `ip` in an Ethernet frame from `PEER_HARDWARE` to `HARDWARE`.
pub(crate) fn wrap_eth(ip: &[u8]) -> Vec<u8> {
    let mut frame = vec![0; EthernetFrame::<&[u8]>::buffer_len(ip.len())];
    let mut ethernet = EthernetFrame::new_unchecked(&mut frame);
    ethernet.set_src_addr(PEER_HARDWARE);
    ethernet.set_dst_addr(HARDWARE);
    ethernet.set_ethertype(EthernetProtocol::Ipv4);
    ethernet.payload_mut().copy_from_slice(ip);
    frame
}

/// IPv4 packet of `frame`, which must be an Ethernet frame from `HARDWARE` to `PEER_HARDWARE`.
pub(crate) fn strip_eth(frame: &[u8]) -> &[u8] {
    let ethernet = EthernetFrame::new_checked(frame).unwrap();
    assert_eq!(
        (ethernet.src_addr(), ethernet.dst_addr(), ethernet.ethertype()),
        (HARDWARE, PEER_HARDWARE, EthernetProtocol::Ipv4)
    );
    &frame[EthernetFrame::<&[u8]>::header_len()..]
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
    fn consume(self, frame: phy::TxBuf, header_len: usize, _: Option<phy::TxChecksum>) {
        self.queue.push_back(frame.packet(header_len).to_vec());
    }

    fn consume_retain(
        self,
        frame: phy::TxBuf,
        header_len: usize,
        _: Option<phy::TxChecksum>,
    ) -> phy::TxBuf {
        self.queue.push_back(frame.packet(header_len).to_vec());
        frame
    }
}

pub(crate) fn tx_test_buf(bytes: &[u8]) -> phy::TxBuf {
    let mut buf = phy::TxPool::new(bytes.len().max(1)).alloc().unwrap();
    buf.room_mut()[..bytes.len()].copy_from_slice(bytes);
    buf.set_len(bytes.len());
    buf
}

pub(crate) fn tx_test_source(room: usize) -> phy::TxSource {
    phy::TxSource::new(phy::TxPool::new(room))
}

#[test]
fn testing_device_frame_token_writes_before_publish() {
    use crate::phy::FrameToken as _;

    let mut device = TestingDevice::new(Medium::Ip);
    let mut frame = tx_test_buf(&[3, 4]);
    frame.headroom_mut(2).0.copy_from_slice(&[1, 2]);
    assert!(device.tx_queue.is_empty());
    let token = device.transmit_frame(Instant::ZERO).unwrap();
    let frame = token.consume_retain(frame, 2, None);
    assert_eq!(device.tx_queue.pop_front().unwrap(), &[1, 2, 3, 4]);
    assert!(frame.unique());
}

#[test]
fn testing_device_owned_frame_releases_after_publish() {
    use crate::phy::FrameToken as _;

    let alloc = phy::TxPool::new(2);
    let mut frame = alloc.alloc().unwrap();
    frame.room_mut()[..2].copy_from_slice(&[3, 4]);
    frame.set_len(2);
    frame.headroom_mut(2).0.copy_from_slice(&[1, 2]);
    let mut device = TestingDevice::new(Medium::Ip);
    device
        .transmit_frame(Instant::ZERO)
        .unwrap()
        .consume(frame, 2, None);
    assert_eq!(device.tx_queue.pop_front().unwrap(), &[1, 2, 3, 4]);
    assert_eq!(alloc.live(), 0);
}
