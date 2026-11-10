use super::*;
use crate::iface::header_template::UdpTemplate;
use crate::iface::udp_set::PendingUdp;
use crate::iface::{UdpHandle, UdpSendError, UdpSocket};
use crate::phy::{ChecksumCapabilities, ImmutableFrame, OwnedFrame};

impl Interface {
    /// The destination of one UDP send of `len` payload bytes, checked before any device work.
    /// Errors come in this order: `NoDestination`, `PeerMismatch`, `Unaddressable`, `TooLarge`.
    /// `Unaddressable` includes our own addresses, the owned set too. The set is not searched for
    /// the destination of the socket's header template: a template exists only for a destination
    /// that passed here, and the set is fixed.
    ///
    /// Panics if `handle` is stale.
    fn udp_destination(
        &self,
        udp: &UdpSet,
        handle: UdpHandle,
        to: Option<IpEndpoint>,
        len: usize,
    ) -> Result<IpEndpoint, UdpSendError> {
        let socket = udp.get(handle);
        let to = match (to, socket.peer()) {
            (None, None) => return Err(UdpSendError::NoDestination),
            (Some(to), Some(peer)) if to != peer => return Err(UdpSendError::PeerMismatch),
            (Some(to), _) | (None, Some(to)) => to,
        };
        let IpAddress::Ipv4(dst_addr) = to.addr;
        if dst_addr.is_unspecified()
            || dst_addr.is_multicast()
            || self.inner.has_interface_addr(to.addr)
            || self.inner.owns_ipv4(dst_addr)
            || to.port == 0
            || (self.inner.is_broadcast(&to.addr) && !socket.config().broadcast)
        {
            return Err(UdpSendError::Unaddressable);
        }
        if len
            > self
                .inner
                .path_ip_mtu(&to.addr)
                .checked_sub(28)
                .expect("IP MTU is smaller than IPv4 and UDP headers")
        {
            return Err(UdpSendError::TooLarge);
        }
        Ok(to)
    }

    /// Copy `payload` into a frame from the UDP set's frame source and send it as one datagram
    /// from `handle` to `to` (`None` for the socket's peer). Errors are the destination errors
    /// (`NoDestination`, `PeerMismatch`, `Unaddressable`, `TooLarge`), then `WouldBlock`,
    /// `NoRoute` and `NeighborPending` in `udp_send_frame`'s order; an empty frame source is
    /// `WouldBlock` before the neighbor lookup.
    ///
    /// Panics if `handle` is stale.
    pub fn udp_send(
        &mut self,
        device: &mut (impl Device + ?Sized),
        udp: &mut UdpSet,
        handle: UdpHandle,
        to: Option<IpEndpoint>,
        payload: &[u8],
    ) -> Result<(), UdpSendError> {
        let to = self.udp_destination(udp, handle, to, payload.len())?;
        let frame = udp_copy(udp, payload)?;
        self.udp_send_unique(device, udp, handle, to, frame)
            .map_err(|(_, error)| error)
    }

    /// Send `frame` as one UDP datagram from `handle` to `to` (`None` for the socket's peer).
    /// A unique frame is published in place: its headers go in front of its payload and its
    /// only reference moves to the device. A shared frame's payload is copied into a frame from
    /// the UDP set's frame source, as `udp_send` does; on success this reference is dropped.
    /// Errors are the destination errors (`NoDestination`, `PeerMismatch`, `Unaddressable`,
    /// `TooLarge`), then `WouldBlock`, `NoRoute` and `NeighborPending`, each with the frame back.
    /// The neighbor lookup runs first and takes a TX token only to send an ARP request; the
    /// publish slot is taken after it, so a full device reports `WouldBlock` only for a resolved
    /// neighbor. A shared frame with an empty frame source is `WouldBlock` before the lookup.
    ///
    /// Panics if `handle` is stale.
    pub fn udp_send_frame(
        &mut self,
        device: &mut (impl Device + ?Sized),
        udp: &mut UdpSet,
        handle: UdpHandle,
        to: Option<IpEndpoint>,
        frame: ImmutableFrame,
    ) -> Result<(), (ImmutableFrame, UdpSendError)> {
        assert_eq!(frame.ctx(), udp.source.origin(), "foreign TX frame producer");
        let to = match self.udp_destination(udp, handle, to, frame.len()) {
            Ok(to) => to,
            Err(error) => return Err((frame, error)),
        };
        match frame.try_own() {
            Ok(unique) => {
                // SAFETY: the entry check identifies this UDP set's producer.
                #[allow(unsafe_code)]
                let at_start = unsafe { crate::phy::frame_at_start(&unique) };
                if at_start {
                    self.udp_send_unique(device, udp, handle, to, unique)
                        .map_err(|(unique, error)| (unique.freeze(), error))
                } else {
                    self.udp_send_copy(device, udp, handle, to, unique.freeze())
                }
            }
            Err(shared) => self.udp_send_copy(device, udp, handle, to, shared),
        }
    }

    /// Copy a shared or off-start frame; errors give the original frame back.
    /// Out of line so the unique path stays small.
    #[cold]
    #[inline(never)]
    fn udp_send_copy(
        &mut self,
        device: &mut (impl Device + ?Sized),
        udp: &mut UdpSet,
        handle: UdpHandle,
        to: IpEndpoint,
        shared: ImmutableFrame,
    ) -> Result<(), (ImmutableFrame, UdpSendError)> {
        let copy = match udp_copy(udp, &shared) {
            Ok(copy) => copy,
            Err(error) => return Err((shared, error)),
        };
        self.udp_send_unique(device, udp, handle, to, copy)
            .map_err(|(_, error)| (shared, error))
    }

    /// Publish the unique `frame` as one UDP datagram to `to`; errors give it back.
    #[inline(always)]
    fn udp_send_unique(
        &mut self,
        device: &mut (impl Device + ?Sized),
        udp: &mut UdpSet,
        handle: UdpHandle,
        to: IpEndpoint,
        mut frame: OwnedFrame,
    ) -> Result<(), (OwnedFrame, UdpSendError)> {
        let IpAddress::Ipv4(dst_addr) = to.addr;
        let len = frame.len();
        let socket = udp.get_mut(handle);
        let now = self.inner.now;
        self.inner.neighbor_cache.expire(now);
        let src_mac = self.inner.hardware_addr.ethernet_or_panic();
        let (ip_repr, udp_repr, checksum_caps) = self.inner.udp_reprs(socket, to, dst_addr, len);
        if let Some(ip_repr) = &ip_repr
            && let Some(template) = &socket.template
            && template.fits(
                self.inner.neighbor_cache.changes(),
                src_mac,
                ip_repr,
                &udp_repr,
            )
        {
            let Some(token) = device.transmit_frame(now) else {
                return Err((frame, UdpSendError::WouldBlock));
            };
            let tx_checksum = self.inner.tx_checksum(&IpRepr::Ipv4(*ip_repr), 0);
            // SAFETY: a unique frame from this UDP set's producer, at its original start.
            #[allow(unsafe_code)]
            let (headers, payload) = unsafe { crate::phy::frame_headroom(&mut frame, UDP_FRAME_HEADER_LEN) };
            template.emit_header(headers, ip_repr, payload, &checksum_caps);
            token.consume(frame.freeze(), UDP_FRAME_HEADER_LEN, tx_checksum);
            self.inner.dispatch_neighbor_probe(device);
            return Ok(());
        }
        let mut next_hop = None;
        let hardware_addr = match self.inner.find_hardware_addr(&to.addr, Some(&mut next_hop)) {
            Ok(HardwareLookup::Found(hardware_addr)) => hardware_addr,
            Ok(HardwareLookup::Arp(addr)) => {
                let Some(token) = device.transmit(now) else {
                    return Err((frame, UdpSendError::WouldBlock));
                };
                let error = match self.inner.send_arp_request(token, addr) {
                    Ok(()) => UdpSendError::NeighborPending,
                    Err(error) => udp_dispatch_error(error),
                };
                return Err((frame, error));
            }
            Err(error) => return Err((frame, udp_dispatch_error(error))),
        };
        let Some(ip_repr) = ip_repr else {
            return Err((frame, UdpSendError::NoRoute));
        };
        let Some(token) = device.transmit_frame(now) else {
            return Err((frame, UdpSendError::WouldBlock));
        };
        let tx_checksum = self.inner.tx_checksum(&IpRepr::Ipv4(ip_repr), 0);
        // SAFETY: a unique frame from this UDP set's producer, at its original start.
        #[allow(unsafe_code)]
        let (headers, payload) = unsafe { crate::phy::frame_headroom(&mut frame, UDP_FRAME_HEADER_LEN) };
        emit_udp_header(
            headers,
            src_mac,
            hardware_addr.ethernet_or_panic(),
            &ip_repr,
            &udp_repr,
            payload,
            &checksum_caps,
        );
        token.consume(frame.freeze(), UDP_FRAME_HEADER_LEN, tx_checksum);
        if let Some((changes, mac)) = next_hop {
            socket.template = Some(UdpTemplate::new(
                src_mac,
                mac,
                &ip_repr,
                udp_repr.src_port,
                udp_repr.dst_port,
                changes,
            ));
        }
        self.inner.dispatch_neighbor_probe(device);
        Ok(())
    }
}

/// A unique frame from `udp`'s frame source holding `payload`. `WouldBlock` when the source is
/// empty.
#[inline]
fn udp_copy(udp: &UdpSet, payload: &[u8]) -> Result<OwnedFrame, UdpSendError> {
    let mut frame = udp.source.alloc().ok_or(UdpSendError::WouldBlock)?;
    frame.as_mut()[..payload.len()].copy_from_slice(payload);
    frame.truncate(payload.len());
    Ok(frame)
}


impl InterfaceInner {
    /// One send's IPv4 header (`None` without a source address for `dst_addr`), UDP ports and
    /// checksum capabilities, the UDP checksum on only if the socket and device both ask for it.
    /// The source is the socket's bound address, or for an all-IP socket the interface's
    /// address for `dst_addr`.
    #[inline]
    fn udp_reprs(
        &self,
        socket: &UdpSocket,
        to: IpEndpoint,
        dst_addr: Ipv4Address,
        len: usize,
    ) -> (Option<Ipv4Repr>, UdpRepr, ChecksumCapabilities) {
        let udp_repr = UdpRepr {
            src_port: socket.config().port,
            dst_port: to.port,
        };
        let src_addr = match socket.config().local_addr {
            Some(addr) => Some(addr),
            None => self.get_source_address_ipv4(&dst_addr),
        };
        let ip_repr = src_addr.map(|src_addr| Ipv4Repr {
            src_addr,
            dst_addr,
            next_header: IpProtocol::Udp,
            payload_len: UDP_HEADER_LEN
                .checked_add(len)
                .expect("UDP length overflow"),
            hop_limit: 64,
        });
        let mut checksum_caps = self.caps.checksum.clone();
        checksum_caps.udp = if socket.config().tx_checksum && self.caps.checksum.udp.tx() {
            crate::phy::Checksum::Both
        } else {
            crate::phy::Checksum::None
        };
        (ip_repr, udp_repr, checksum_caps)
    }

    /// `dst_ours` is `process_ipv4`'s `has_ip_addr` answer for the destination, so an owned
    /// address is searched once per datagram.
    pub(super) fn process_udp<'frame>(
        &mut self,
        udp: Option<&mut UdpSet>,
        meta: PacketMeta,
        ipv4_repr: Ipv4Repr,
        dst_ours: bool,
        ip_payload: &'frame [u8],
        payload_offset: usize,
    ) -> Option<Packet<'frame>> {
        let udp = udp?;
        assert!(udp.pending_rx_attach.is_none());
        let Ok(packet) = UdpPacket::new_checked(ip_payload) else {
            return None;
        };
        if !meta.l4_checksum_verified
            && packet.checksum() != 0
            && !packet.verify_checksum(&ipv4_repr.src_addr.into(), &ipv4_repr.dst_addr.into())
        {
            return None;
        }
        let broadcast = self.is_broadcast_v4(ipv4_repr.dst_addr);
        if !broadcast
            && (!dst_ours || !ipv4_repr.dst_addr.x_is_unicast())
        {
            return None;
        }
        let slot = udp.lookup(ipv4_repr.dst_addr, packet.dst_port(), broadcast)?;
        let src = IpEndpoint::new(ipv4_repr.src_addr.into(), packet.src_port());
        let socket = udp.socket_at_mut(slot);
        if socket.peer().is_some_and(|peer| src != peer) {
            return None;
        }
        if socket.at_limit() {
            socket.stats.rx_dropped_full = socket
                .stats
                .rx_dropped_full
                .checked_add(1)
                .expect("UDP full drop count overflow");
            return None;
        }
        let start = payload_offset
            .checked_add(UDP_HEADER_LEN)
            .expect("UDP payload offset overflow");
        let end = payload_offset
            .checked_add(packet.len() as usize)
            .expect("UDP payload end overflow");
        udp.pending_rx_attach = Some((
            slot,
            PendingUdp {
                src,
                dst_addr: ipv4_repr.dst_addr,
                payload: start..end,
            },
        ));
        None
    }
}

/// Ethernet, IPv4 and UDP headers in front of a UDP frame's payload.
const UDP_FRAME_HEADER_LEN: usize = 42;

fn udp_dispatch_error(error: DispatchError) -> UdpSendError {
    match error {
        DispatchError::NoRoute => UdpSendError::NoRoute,
        DispatchError::NeighborPending => UdpSendError::NeighborPending,
    }
}

/// The generic emitters' header bytes for a datagram whose payload is not behind them: the
/// UDP checksum covers the header words written here and `payload` where it lies. `bytes` is
/// 42 long and `ip_repr.payload_len` is the UDP length of `payload` and fits u16.
fn emit_udp_header(
    bytes: &mut [u8],
    src_mac: EthernetAddress,
    dst_mac: EthernetAddress,
    ip_repr: &Ipv4Repr,
    udp_repr: &UdpRepr,
    payload: &[u8],
    checksum_caps: &ChecksumCapabilities,
) {
    debug_assert_eq!(ip_repr.payload_len, UDP_HEADER_LEN + payload.len());
    let udp_len = ip_repr.payload_len as u16;
    let mut frame = EthernetFrame::new_unchecked(&mut bytes[..]);
    frame.set_src_addr(src_mac);
    frame.set_dst_addr(dst_mac);
    frame.set_ethertype(EthernetProtocol::Ipv4);
    ip_repr.emit(&mut Ipv4Packet::new_unchecked(&mut bytes[14..34]), checksum_caps);
    let mut packet = UdpPacket::new_unchecked(&mut bytes[34..]);
    packet.set_src_port(udp_repr.src_port);
    packet.set_dst_port(udp_repr.dst_port);
    packet.set_len(udp_len);
    packet.set_checksum(0);
    if checksum_caps.udp.tx() {
        let sum = !checksum::combine(&[
            checksum::pseudo_header_v4(
                &ip_repr.src_addr,
                &ip_repr.dst_addr,
                IpProtocol::Udp,
                u32::from(udp_len),
            ),
            checksum::data(packet.as_ref()),
            checksum::data(payload),
        ]);
        packet.set_checksum(if sum == 0 { 0xffff } else { sum });
    }
}
