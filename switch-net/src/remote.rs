//! Transport for sharing a card through the daemon. `Session` owns socket slots and generations.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::net::SocketAddrV4;
use std::num::NonZeroU16;
use std::ptr::NonNull;
use std::rc::Rc;
use std::task::{Context, Poll};

use switch_junction::control::{self, Event, RejectCode, SocketId};
use switch_junction::rx::PacketMeta;
use switch_junction::session::{Lost, OpenError, SendError, Session};
use dpdk_smoltcp::smoltcp::wire::IpAddress;

use crate::{Datagram, Error, ImmutableFrame, Interest, IpEndpoint, IpListenEndpoint, OwnedFrame,
    RemoteConfig, Sent, StoredWaker, TcpFrame, Turn, UdpConfig, UdpSocketStats, tcp};

struct Entry {
    id: SocketId,
    halves: u8,
    parked: [Option<(StoredWaker, Interest)>; 2],
    opened: Option<Result<Option<usize>, Error>>,
    packets: VecDeque<(PacketMeta, OwnedFrame)>,
    fin: bool,
    closed: bool,
    reset: bool,
    shut: bool,
    delivered: bool,
}
impl Entry {
    fn new(id: SocketId) -> Self {
        Self {
            id, halves: 0, parked: [None; 2], opened: None,
            packets: VecDeque::new(), fin: false, closed: false, reset: false,
            shut: false, delivered: false,
        }
    }
}


pub(super) struct Remote {
    pub session: Session,
    entries: Vec<Entry>,
    by_slot: Vec<Option<u32>>,
    free: Vec<u32>,
    pool: Rc<FramePool>,
    frame_waiters: VecDeque<StoredWaker>,
    lost: bool,
    full: bool,
    sent: usize,
}

impl Remote {
    pub fn new(session: Session, config: RemoteConfig) -> Self {
        let slots = session.max_sockets();
        let pool = FramePool::new(config.tx_frames, session.frame_capacity());
        Self {
            session,
            entries: (0..slots).map(|_| Entry::new(SocketId { slot: 0, generation: 0 })).collect(),
            by_slot: vec![None; slots],
            free: (0..slots as u32).rev().collect(),
            pool,
            frame_waiters: VecDeque::with_capacity(slots),
            lost: false,
            full: false,
            sent: 0,
        }
    }

    fn insert(&mut self, id: SocketId) -> u32 {
        let index = self.free.pop().unwrap_or_else(|| {
            self.entries.push(Entry::new(id));
            (self.entries.len() - 1) as u32
        });
        self.by_slot[id.slot as usize] = Some(index);
        let entry = &mut self.entries[index as usize];
        let packets = std::mem::take(&mut entry.packets);
        *entry = Entry::new(id);
        entry.packets = packets;
        entry.halves = 2;
        index
    }

    fn entry(&self, index: u32) -> &Entry {
        &self.entries[index as usize]
    }

    fn entry_mut(&mut self, index: u32) -> &mut Entry {
        &mut self.entries[index as usize]
    }

    pub fn connect(&mut self, config: tcp::Config, remote: IpEndpoint, local: IpListenEndpoint) -> Result<u32, Error> {
        if self.lost { return Err(Error::HostLost); }
        let id = self.session.connect_tcp(tcp_config(config)?, endpoint(remote), listen(local)).map_err(open_error)?;
        Ok(self.insert(id))
    }

    pub fn accept(&mut self, config: tcp::Config, local: IpListenEndpoint) -> Result<u32, Error> {
        if self.lost { return Err(Error::HostLost); }
        let id = self.session.listen_tcp(tcp_config(config)?, listen(local)).map_err(open_error)?;
        Ok(self.insert(id))
    }

    pub fn bind(&mut self, config: UdpConfig) -> Result<u32, Error> {
        if self.lost { return Err(Error::HostLost); }
        let config = control::UdpConfig {
            local: control::ListenEndpoint { addr: config.local_addr, port: config.port },
            rx_queue: config.rx_queue.try_into().map_err(|_| Error::InvalidConfig)?,
            tx_checksum: config.tx_checksum,
            broadcast: config.broadcast,
        };
        let id = self.session.bind_udp(config).map_err(open_error)?;
        Ok(self.insert(id))
    }

    pub fn opened(&self, index: u32, accepting: bool) -> Option<Result<Option<usize>, Error>> {
        if self.lost { return Some(Err(Error::HostLost)); }
        let entry = self.entry(index);
        if let Some(Err(error)) = entry.opened { return Some(Err(error)); }
        if entry.closed {
            Some(if accepting { Ok(None) } else {
                Err(if entry.reset { Error::Refused } else { Error::Closed })
            })
        } else { entry.opened }
    }

    pub fn bound(&self, index: u32) -> Option<Result<(), Error>> {
        self.opened(index, false).map(|result| result.map(|_| ()))
    }

    pub fn park(&mut self, index: u32, interest: Interest, cx: &Context<'_>) {
        self.entry_mut(index).parked[interest.side()] = Some((StoredWaker::new(cx), interest));
    }

    pub fn unpark(&mut self, index: u32, side: usize) {
        self.entry_mut(index).parked[side] = None;
    }

    pub fn release(&mut self, index: u32, side: usize) {
        let entry = self.entry_mut(index);
        entry.parked[side] = None;
        entry.halves -= 1;
        if entry.halves == 0 {
            self.drain_packets();
            let entry = &self.entries[index as usize];
            self.free.push(index);
            let registered = self.by_slot[entry.id.slot as usize] == Some(index);
            if registered {
                self.by_slot[entry.id.slot as usize] = None;
            }
            if !self.lost && registered {
                // Data published after this drain can still race the host applying Close.
                self.session.close(entry.id, !entry.packets.is_empty());
            }
            self.entries[index as usize].packets.clear();
        }
    }

    /// Sends `AbortTcp`, which the daemon applies at once, and unregisters the socket, so
    /// `release` sends no `Close`. A rejected open has nothing to abort.
    pub fn abort(&mut self, index: u32) {
        let id = self.entry(index).id;
        if self.by_slot[id.slot as usize] != Some(index) {
            return;
        }
        self.by_slot[id.slot as usize] = None;
        if !self.lost {
            self.session.abort(id);
        }
    }

    fn drain_packets(&mut self) {
        let entries = &mut self.entries;
        let by_slot = &self.by_slot;
        self.session.poll_packets(|id, meta, frame| { route_packet(entries, by_slot, id, meta, frame); });
    }

    pub fn receive(&mut self) -> Result<Turn, Lost> {
        let mut events = 0;
        let mut packets = 0;
        let entries = RefCell::new(&mut self.entries);
        let by_slot = RefCell::new(&mut self.by_slot);
        let result = if self.lost { Err(Lost::Disconnected) } else { self.session.poll(
            |id, event| {
                events += 1;
                let mut entries = entries.borrow_mut();
                let mut by_slot = by_slot.borrow_mut();
                let Some(index) = by_slot[id.slot as usize] else { return };
                let entry = &mut entries[index as usize];
                if entry.id != id { return; }
                match event {
                    Event::Connected { mss } => entry.opened = Some(Ok(Some(usize::from(mss)))),
                    Event::Bound => entry.opened = Some(Ok(None)),
                    Event::Rejected(code) => {
                        entry.opened = Some(Err(reject(code)));
                        entry.closed = true;
                        by_slot[id.slot as usize] = None;
                    }
                    Event::PeerFin => entry.fin = true,
                    Event::Delivered => entry.delivered = true,
                    Event::Closed { reset_by_peer } => {
                        entry.closed = true;
                        entry.reset |= reset_by_peer;
                    }
                    Event::Freed => {}
                }
            },
            |id, meta, frame| {
                packets += usize::from(route_packet(&mut entries.borrow_mut(), &by_slot.borrow(), id, meta, frame));
            },
        ) };
        self.lost |= result.is_err();
        self.full = false;
        for entry in self.entries.iter_mut().filter(|entry| entry.halves != 0) {
            for side in 0..2 {
                let Some((waker, interest)) = entry.parked[side] else { continue };
                let ready = match interest {
                    Interest::Connect | Interest::Accept | Interest::Bind => self.lost || entry.closed || entry.opened.is_some(),
                    Interest::TcpRecv => self.lost || !entry.packets.is_empty() || entry.fin || entry.closed,
                    Interest::UdpRecv => !entry.packets.is_empty(),
                    Interest::TcpRoom(_) => true,
                    Interest::TcpDelivered => self.lost || entry.delivered || entry.closed,
                };
                if ready {
                    entry.parked[side] = None;
                    waker.wake();
                }
            }
        }
        let free = self.pool.free.borrow().len().min(self.frame_waiters.len());
        for waker in self.frame_waiters.drain(..free) { waker.wake(); }
        result.map(|()| Turn { rx_received: packets, rx_rejected: 0, rx_processed: events + packets,
            rx_staged: 0, tx_accepted: 0, tx_pending: 0 })
    }

    pub fn send(&mut self) -> Result<Sent, Lost> {
        if self.lost { return Err(Lost::Disconnected); }
        Ok(Sent { tx_accepted: std::mem::take(&mut self.sent), tx_pending: 0 })
    }

    pub fn recv(&mut self, index: u32) -> Option<Result<TcpFrame, Error>> {
        let lost = self.lost;
        let entry = self.entry_mut(index);
        if let Some((meta, frame)) = entry.packets.pop_front() {
            return Some(Ok(TcpFrame { frame, rx_timestamp: meta.rx_timestamp }));
        }
        if lost || entry.reset { Some(Err(Error::InvalidState)) }
        else if entry.fin { Some(Err(Error::Finished)) }
        else if entry.closed { Some(Err(Error::InvalidState)) } else { None }
    }

    pub fn recv_udp(&mut self, index: u32) -> Option<Datagram> {
        self.entry_mut(index).packets.pop_front().map(|(meta, frame)| Datagram {
            frame, src: IpEndpoint::new(crate::Ipv4Address::from(meta.src_addr).into(), meta.src_port),
            dst_addr: crate::Ipv4Address::from(meta.dst_addr), rx_timestamp: meta.rx_timestamp,
        })
    }

    pub fn send_capacity(&self) -> usize { self.session.max_record() - 16 }

    pub fn room(&self, index: u32, len: usize) -> Option<Result<(), Error>> {
        if len > self.send_capacity() { Some(Err(Error::TooLarge)) }
        else if self.lost || self.entry(index).closed || self.entry(index).shut { Some(Err(Error::InvalidState)) }
        else if self.full { None } else { Some(Ok(())) }
    }

    pub fn send_tcp(&mut self, index: u32, frame: ImmutableFrame) -> Result<(), (ImmutableFrame, Error)> {
        if frame.ctx() != Rc::as_ptr(&self.pool).cast_mut().cast() { return Err((frame, Error::Foreign)); }
        let entry = self.entry(index);
        if self.lost || entry.closed || entry.shut { return Err((frame, Error::InvalidState)); }
        if frame.is_empty() { return Err((frame, Error::Empty)); }
        let result = self.session.send_tcp(entry.id, &frame);
        self.sent(result, frame)
    }

    pub fn send_udp(&mut self, index: u32, to: Option<IpEndpoint>, frame: ImmutableFrame) -> Result<(), (ImmutableFrame, Error)> {
        if frame.ctx() != Rc::as_ptr(&self.pool).cast_mut().cast() { return Err((frame, Error::Foreign)); }
        let entry = self.entry(index);
        if self.lost || entry.closed { return Err((frame, Error::InvalidState)); }
        let Some(to) = to else { return Err((frame, Error::NoDestination)); };
        let result = self.session.send_udp(entry.id, endpoint(to), &frame);
        self.sent(result, frame)
    }

    fn sent(&mut self, result: Result<(), SendError>, frame: ImmutableFrame) -> Result<(), (ImmutableFrame, Error)> {
        match result {
            Ok(()) => { self.sent += 1; Ok(()) }
            Err(error) => {
                let error = match error {
                    SendError::Full => { self.full = true; Error::Full }
                    SendError::TooLarge => Error::TooLarge,
                    SendError::NotOpen => Error::InvalidState,
                };
                Err((frame, error))
            }
        }
    }

    pub fn shutdown(&mut self, index: u32) {
        let entry = self.entry_mut(index);
        if entry.shut || entry.closed { return; }
        entry.shut = true;
        let id = entry.id;
        if !self.lost { self.session.shutdown(id); }
    }

    pub fn delivered(&self, index: u32) -> Option<()> {
        (self.lost || self.entry(index).closed || self.entry(index).delivered).then_some(())
    }

    pub fn stats(&self, _index: u32) -> UdpSocketStats { UdpSocketStats::default() }

    #[cfg(feature = "testkit")]
    pub fn reset_by_peer(&self) -> bool {
        self.entries.iter().any(|entry| entry.halves != 0 && entry.reset)
    }

    #[cfg(feature = "testkit")]
    pub fn assert_frames_returned(&mut self) {
        self.session.poll_packets(|_, _, _| {});
        for entry in &mut self.entries {
            entry.packets.clear();
        }
        assert_eq!(self.pool.free.borrow().len(), self.pool.count, "remote TX frames outlived the body");
        assert_eq!(self.session.outstanding_rx_frames(), 0, "remote RX frames outlived the body");
    }

    #[cfg(feature = "testkit")]
    pub fn outstanding_rx_frames(&mut self) -> usize {
        self.drain_packets();
        self.session.outstanding_rx_frames()
    }

    pub fn tx_frame(&mut self, cx: &Context<'_>) -> Poll<OwnedFrame> {
        if let Some(frame) = self.pool.take() { return Poll::Ready(frame); }
        let waker = StoredWaker::new(cx);
        if !self.frame_waiters.contains(&waker) { self.frame_waiters.push_back(waker); }
        Poll::Pending
    }
}

fn route_packet(entries: &mut [Entry], by_slot: &[Option<u32>], id: SocketId, meta: PacketMeta, frame: OwnedFrame) -> bool {
    let Some(index) = by_slot[id.slot as usize] else { return false };
    let entry = &mut entries[index as usize];
    if entry.id != id { return false; }
    entry.packets.push_back((meta, frame));
    true
}

fn tcp_config(config: tcp::Config) -> Result<control::TcpConfig, Error> {
    if config.mss_cap == Some(0) { return Err(Error::InvalidConfig); }
    Ok(control::TcpConfig {
        rx_capacity: config.rx_capacity.try_into().map_err(|_| Error::InvalidConfig)?,
        tx_capacity: config.tx_capacity.try_into().map_err(|_| Error::InvalidConfig)?,
        mss_cap: config.mss_cap.and_then(NonZeroU16::new),
    })
}

fn endpoint(endpoint: IpEndpoint) -> SocketAddrV4 {
    let IpAddress::Ipv4(addr) = endpoint.addr;
    SocketAddrV4::new(addr, endpoint.port)
}

fn listen(endpoint: IpListenEndpoint) -> control::ListenEndpoint {
    control::ListenEndpoint {
        addr: endpoint.addr.map(|addr| { let IpAddress::Ipv4(addr) = addr; addr }),
        port: endpoint.port,
    }
}

fn open_error(error: OpenError) -> Error {
    match error { OpenError::NoFreeSlot => Error::SocketsFull, OpenError::Config(_) => Error::InvalidConfig }
}

fn reject(code: RejectCode) -> Error {
    match code {
        RejectCode::TcpSocketsFull | RejectCode::UdpSocketsFull => Error::SocketsFull,
        RejectCode::ConnectAddressInUse | RejectCode::UdpPortInUse => Error::PortInUse,
        RejectCode::ConnectUnaddressable | RejectCode::ListenUnaddressable | RejectCode::UdpUnaddressable => Error::Unaddressable,
        RejectCode::ConnectInvalidState | RejectCode::ListenInvalidState => Error::InvalidState,
    }
}

struct FramePool {
    free: RefCell<Vec<Box<[u16]>>>,
    capacity: u32,
    #[cfg(feature = "testkit")]
    count: usize,
}

impl FramePool {
    fn new(count: usize, capacity: u32) -> Rc<Self> {
        assert!(capacity <= u32::MAX - 2, "frame capacity exceeds switch-frame's count offset range");
        let words = 1 + (capacity as usize).div_ceil(2);
        Rc::new(Self {
            free: RefCell::new((0..count).map(|_| vec![0; words].into_boxed_slice()).collect()),
            capacity,
            #[cfg(feature = "testkit")]
            count,
        })
    }

    fn take(self: &Rc<Self>) -> Option<OwnedFrame> {
        let mut buffer = self.free.borrow_mut().pop()?;
        buffer[0] = 1;
        let rc = NonNull::new(Box::into_raw(buffer).cast::<u16>()).expect("nonempty buffer");
        // SAFETY: one uniquely owned allocation contains the count then capacity initialized
        // payload bytes. The frame retains the pool, and final release returns this allocation.
        Some(unsafe { OwnedFrame::from_raw_parts(rc.cast::<u8>().add(2), self.capacity, rc,
            release_buffer, Rc::into_raw(self.clone()).cast_mut().cast()) })
    }
}

unsafe extern "C" fn release_buffer(rc: NonNull<u16>, ctx: *mut ()) {
    // SAFETY: take transfers one Rc and one Box to the frame. This runs once on its last drop.
    let pool = unsafe { Rc::from_raw(ctx.cast::<FramePool>()) };
    let words = 1 + (pool.capacity as usize).div_ceil(2);
    let buffer = unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(rc.as_ptr(), words)) };
    pool.free.borrow_mut().push(buffer);
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mutation reject_wrong_error: map any rejection to another socket error.
    #[test]
    fn every_host_rejection_maps_to_the_local_socket_error() {
        for (code, expected) in [
            (RejectCode::TcpSocketsFull, Error::SocketsFull),
            (RejectCode::ConnectInvalidState, Error::InvalidState),
            (RejectCode::ConnectUnaddressable, Error::Unaddressable),
            (RejectCode::ConnectAddressInUse, Error::PortInUse),
            (RejectCode::ListenInvalidState, Error::InvalidState),
            (RejectCode::ListenUnaddressable, Error::Unaddressable),
            (RejectCode::UdpSocketsFull, Error::SocketsFull),
            (RejectCode::UdpPortInUse, Error::PortInUse),
            (RejectCode::UdpUnaddressable, Error::Unaddressable),
        ] {
            assert_eq!(reject(code), expected, "{code:?}");
        }
    }
}
