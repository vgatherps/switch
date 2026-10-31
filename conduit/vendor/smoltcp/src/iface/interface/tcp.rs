use super::*;

use crate::iface::header_template::TcpTemplate;
use crate::socket::tcp::Socket;

impl InterfaceInner {
    pub(super) fn tcp_frame_template_ready(
        &self,
        ip: &IpRepr,
        tcp: &TcpRepr,
        next_hop: &Option<(u64, EthernetAddress)>,
        template: &Option<crate::iface::header_template::TcpTemplate>,
    ) -> bool {
        let IpRepr::Ipv4(ip4) = ip;
        self.caps.medium == Medium::Ethernet
            && TcpTemplate::is_eligible(tcp)
            && next_hop.is_some_and(|(generation, _)| generation == self.neighbor_cache.changes())
            && template.as_ref().is_some_and(|template| {
                template.fits(
                    self.neighbor_cache.changes(),
                    self.hardware_addr.ethernet_or_panic(),
                    ip4,
                    tcp,
                )
            })
    }

    pub(super) fn prepare_tcp_frame<Tx: TxToken>(
        &mut self,
        ip: &IpRepr,
        tcp: &TcpRepr,
        next_hop: &mut Option<(u64, EthernetAddress)>,
        template: &mut Option<crate::iface::header_template::TcpTemplate>,
        reserve: impl FnOnce() -> Option<Tx>,
    ) -> Result<(), EgressError> {
        let IpRepr::Ipv4(ip4) = ip;
        assert_eq!(self.caps.medium, Medium::Ethernet);
        assert!(TcpTemplate::is_eligible(tcp));
        let changes = self.neighbor_cache.changes();
        let src_mac = self.hardware_addr.ethernet_or_panic();
        let mac = match self
            .find_hardware_addr(&ip.dst_addr(), Some(next_hop))
            .map_err(|_| EgressError::Dispatch)?
        {
            HardwareLookup::Found(mac) => mac,
            HardwareLookup::Arp(dst_addr) => {
                let tx = reserve().ok_or(EgressError::Exhausted)?;
                self.send_arp_request(tx, dst_addr)
                    .map_err(|_| EgressError::Dispatch)?;
                return Err(EgressError::Dispatch);
            }
        };
        *template = Some(TcpTemplate::new(
            src_mac,
            mac.ethernet_or_panic(),
            ip4,
            tcp.src_port,
            tcp.dst_port,
            changes,
        ));
        Ok(())
    }

    pub(super) fn dispatch_tcp_frame<Frame: PhyFrameToken>(
        &mut self,
        frame_token: Frame,
        ip: IpRepr,
        tcp: TcpRepr,
        mut frame: UniqueTxBuf,
        template: &crate::iface::header_template::TcpTemplate,
    ) -> TxBuf {
        let IpRepr::Ipv4(ip4) = ip;
        // dispatch puts options only on segments without payload, and sends no timestamps.
        debug_assert_eq!(tcp.header_len(), TCP_HEADER_LEN, "TCP frame header carries options");
        let header_len = 14 + ip4.buffer_len() + TCP_HEADER_LEN;
        let tx_checksum = self.tx_checksum(&ip, TCP_HEADER_LEN);
        let (headers, payload) = frame.headroom_mut(header_len);
        template.emit_header(headers, &ip4, &tcp, payload, &self.caps.checksum);
        frame_token.consume_retain(frame.into_buf(), header_len, tx_checksum)
    }

    pub(super) fn dispatch_tcp<Tx: TxToken>(
        &mut self,
        mut tx: Tx,
        mut meta: PacketMeta,
        ip: IpRepr,
        tcp: TcpRepr,
        next_hop: &mut Option<(u64, EthernetAddress)>,
        template: &mut Option<crate::iface::header_template::TcpTemplate>,
    ) -> Result<(), DispatchError> {
        use crate::iface::header_template::TcpTemplate;

        #[allow(irrefutable_let_patterns)]
        if let IpRepr::Ipv4(ip4) = ip
            && self.caps.medium == Medium::Ethernet
            && TcpTemplate::is_eligible(&tcp)
        {
            assert!(!ip4.dst_addr.is_unspecified());
            let changes = self.neighbor_cache.changes();
            let src_mac = self.hardware_addr.ethernet_or_panic();
            if !next_hop.is_some_and(|(generation, _)| generation == changes)
                || !template.as_ref().is_some_and(|template| {
                    template.fits(changes, src_mac, &ip4, &tcp)
                })
            {
                let (mac, resolved_tx) = self.lookup_hardware_addr(
                    tx,
                    &ip4.dst_addr.into(),
                    Some(next_hop),
                )?;
                tx = resolved_tx;
                *template = Some(TcpTemplate::new(
                    src_mac,
                    mac.ethernet_or_panic(),
                    &ip4,
                    tcp.src_port,
                    tcp.dst_port,
                    self.neighbor_cache.changes(),
                ));
            }
            if 20 + ip4.payload_len > self.caps.ip_mtu() {
                return Ok(());
            }
            meta.tx_checksum = self.tx_checksum(&ip, tcp.header_len());
            tx.set_meta(meta);
            tx.consume(14 + 20 + ip4.payload_len, |frame| {
                template.as_ref().unwrap().emit(frame, &ip4, &tcp, &self.caps.checksum)
            });
            return Ok(());
        }

        self.dispatch_ip(
            tx,
            meta,
            Packet::new(ip.clone(), IpPayload::Tcp(tcp)),
            Some(next_hop),
        )?;
        #[allow(irrefutable_let_patterns)]
        if let IpRepr::Ipv4(ip4) = ip
            && self.caps.medium == Medium::Ethernet
            && let Some((changes, mac)) = *next_hop
            && changes == self.neighbor_cache.changes()
        {
            *template = Some(TcpTemplate::new(
                self.hardware_addr.ethernet_or_panic(),
                mac,
                &ip4,
                tcp.src_port,
                tcp.dst_port,
                changes,
            ));
        }
        Ok(())
    }

    /// The TCP segment in `ip_payload`, or `None` for one this stack drops before demux: an
    /// unspecified address (RFC 1122 §3.2.1.3; a socket with an unspecified peer would panic on
    /// egress), a malformed header, a zero port or a bad checksum.
    #[inline]
    fn tcp_segment<'frame>(
        &self,
        meta: PacketMeta,
        src_addr: IpAddress,
        dst_addr: IpAddress,
        ip_payload: &'frame [u8],
    ) -> Option<TcpRepr<'frame>> {
        if src_addr.is_unspecified() || dst_addr.is_unspecified() {
            return None;
        }
        let tcp_packet = check!(TcpPacket::new_checked(ip_payload));
        if tcp_packet.src_port() == 0 || tcp_packet.dst_port() == 0 {
            return None;
        }
        if !meta.l4_checksum_verified && self.caps.checksum.tcp.rx()
            && !tcp_packet.verify_checksum(&src_addr, &dst_addr)
        {
            return None;
        }
        let mut checksum_caps = self.caps.checksum.clone();
        checksum_caps.tcp = crate::phy::Checksum::None;
        Some(check!(TcpRepr::parse(
            &tcp_packet,
            &src_addr,
            &dst_addr,
            &checksum_caps
        )))
    }

    /// Demultiplex one TCP segment flow-first: an exact (4-tuple) flow takes it without
    /// checking the destination, since a flow's local address is ours by construction
    /// (`Socket::connect` refuses another, and a listener only accepts SYNs to our addresses).
    /// On a miss the destination must be ours, else the segment is dropped without a reply;
    /// only then do listeners and the RST reply apply. Received segments never touch the
    /// neighbor cache.
    /// Inlined into its one caller, `process_ipv4`: left out of line, each TCP segment paid
    /// about 25 instructions more for the call (callgrind, established round).
    #[inline(always)]
    pub(crate) fn process_tcp<'frame>(
        &mut self,
        sockets: &mut TcpSet,
        meta: PacketMeta,
        ip_repr: IpRepr,
        ip_payload: &'frame [u8],
    ) -> Option<Packet<'frame>> {
        assert!(sockets.pending_rx_attach.is_none());
        assert!(sockets.pending_tcp_reply.is_none());

        let (src_addr, dst_addr) = (ip_repr.src_addr(), ip_repr.dst_addr());
        let tcp_repr = self.tcp_segment(meta, src_addr, dst_addr, ip_payload)?;

        let syn_like = tcp_repr.ack_number.is_none() && tcp_repr.control != TcpControl::Rst;
        let dst = IpEndpoint::new(dst_addr, tcp_repr.dst_port);
        let src = IpEndpoint::new(src_addr, tcp_repr.src_port);

        #[cfg(feature = "diagnostics")]
        let linear = {
            // The lowest listener bound to the destination address, else the lowest all-IP one.
            let mut listeners = [None, None];
            for index in (0..sockets.slot_count()).rev() {
                if let Some(item) = sockets.item_at_mut(index)
                    && item.socket.is_listening()
                    && item.socket.accepts(self, &ip_repr, &tcp_repr)
                {
                    listeners[usize::from(item.socket.listen_endpoint().addr.is_none())] = Some(index);
                }
            }
            let listener = listeners[0].or(listeners[1]);
            (0..sockets.slot_count())
                .find(|&index| {
                    sockets.item_at_mut(index).is_some_and(|item| {
                        !item.socket.is_listening()
                            && item.socket.accepts(self, &ip_repr, &tcp_repr)
                            && !(listener.is_some()
                                && tcp_repr.control == TcpControl::Syn
                                && item.socket.time_wait_accepts_syn(&tcp_repr))
                    })
                })
                .or(listener)
        };

        let mut winner = sockets.tcp_exact_lookup(dst, src, syn_like);
        if winner.is_none() {
            if !self.has_ip_addr(dst_addr) {
                net_trace!("Rejecting TCP segment to {}", dst_addr);
                return None;
            }
            if syn_like {
                winner = sockets.tcp_listener_lookup(dst);
            }
        }
        // Only a plain SYN retires an acceptable TIME-WAIT tuple for a listener; a SYN+FIN goes
        // to the TIME-WAIT socket, which ACKs it, listener or not.
        if tcp_repr.control == TcpControl::Syn
            && tcp_repr.ack_number.is_none()
            && let Some(index) = winner
            && let Some(listener) = sockets.tcp_listener_lookup(dst)
        {
            let socket = &mut sockets
                .item_at_mut(index)
                .expect("TCP flow index contains an empty slot")
                .socket;
            if socket.time_wait_accepts_syn(&tcp_repr) {
                socket.finish_time_wait();
                sockets.update_identity(index);
                sockets.check_reapable(index);
                sockets.mark_dirty(index);
                winner = Some(listener);
            }
        }

        #[cfg(feature = "diagnostics")]
        {
            assert_eq!(
                linear, winner,
                "TCP segment {src_addr}:{} -> {dst_addr}:{} {tcp_repr:?}: linear slot {linear:?}, flow index slot {winner:?}",
                tcp_repr.src_port, tcp_repr.dst_port,
            );
        }

        if let Some(index) = winner {
            let socket = &mut sockets
                .item_at_mut(index)
                .expect("TCP flow index contains an empty slot")
                .socket;
            debug_assert!(
                socket.accepts(self, &ip_repr, &tcp_repr),
                "flow index out of date for slot {index}"
            );
            let before = socket.state();
            let (reply, pending) = socket.process(self, &ip_repr, &tcp_repr);
            let state_changed = socket.state() != before;
            sockets.update_identity(index);
            if state_changed {
                sockets.check_reapable(index);
            }
            sockets.mark_dirty(index);
            if let Some(pending) = pending {
                sockets.pending_rx_attach = Some((index, pending));
            }
            #[cfg(feature = "diagnostics")]
            sockets.check_flow_index();
            return reply.map(|(ip, tcp)| {
                if self.caps.medium == Medium::Ethernet && ip.version() == IpVersion::Ipv4 {
                    sockets.pending_tcp_reply = Some(index);
                }
                Packet::new(ip, IpPayload::Tcp(tcp))
            });
        }

        if tcp_repr.control == TcpControl::Rst
            || ip_repr.dst_addr().is_unspecified()
            || ip_repr.src_addr().is_unspecified()
        {
            // Never reply to a TCP RST packet with another TCP RST packet.
            // Never send a TCP RST packet with unspecified addresses.
            None
        } else {
            // The packet wasn't handled by a socket, send a TCP RST packet.
            let (ip, tcp) = Socket::rst_reply(&ip_repr, &tcp_repr);
            Some(Packet::new(ip, IpPayload::Tcp(tcp)))
        }
    }
}
