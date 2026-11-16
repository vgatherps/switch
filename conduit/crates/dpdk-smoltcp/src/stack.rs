use std::fmt;
use std::io::Read;

use smoltcp::iface::{
    Config, DropQueue, Interface, NoRoute, OwnedIpv4, PathMtu, PathMtus, PollIngressSingleResult,
    PollResult, ReadySide,
    SocketHandle, TcpSet, UdpAddError, UdpConfig, UdpSendError, UdpSet,
    UdpSocket,
};
use smoltcp::phy::{ImmutableFrame, OwnedFrame};
use smoltcp::socket::tcp;
use smoltcp::time::{Duration, Instant};
use smoltcp::wire::{
    EthernetAddress, IpAddress, IpCidr, IpEndpoint, IpListenEndpoint, Ipv4Address, Ipv4Cidr,
};

use crate::device::Device;
use crate::handle::{TcpHandle, UdpHandle};
use crate::port::{Port, PortError};
use crate::rx_pool::{frame_reserve, rx_reserve, tx_reserve};

/// How often a turn runs DPDK's timers (`rte_timer_manage`): ENA's watchdog is a 1 s timer.
const SERVICE_PERIOD: Duration = Duration::from_secs(1);

/// Default number of on-link neighbors the ARP cache holds.
pub const DEFAULT_NEIGHBOR_CAPACITY: usize = 256;

/// Read the interface PRNG seed from OS entropy.
pub fn os_random_seed() -> std::io::Result<u64> {
    let mut bytes = [0; 8];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

/// What [`Stack::new`] builds. A rule broken here fails `new` with
/// [`StackError::InvalidConfig`] naming it.
#[derive(Debug, Clone, Copy)]
pub struct StackConfig<'a> {
    /// The MAC address the network stack sends from. Unicast and nonzero; normally the device's own,
    /// `EthernetAddress(device_info(&eal, port_id)?.mac)`.
    pub ethernet: EthernetAddress,
    /// The interface address and its connected subnet.
    pub ipv4: Ipv4Cidr,
    /// Default IPv4 route for destinations outside `ipv4`'s subnet; None
    /// reaches on-link peers only. Must be a unicast address inside that
    /// subnet other than our own, its network address and its broadcast
    /// address (a /31 has neither, RFC 3021).
    pub gateway: Option<Ipv4Address>,
    /// Per-destination path MTUs. Longest prefix wins. The interface subnet
    /// gets an implicit card-MTU rule unless an explicit same-prefix rule replaces it.
    /// Results are clamped to the card's IP MTU; unmatched destinations use that MTU.
    /// Empty paths give every destination the card's IP MTU.
    /// TCP connects cap MSS at path MTU minus 40. Copied at `Stack::new`.
    pub paths: &'a [PathMtu],
    /// Addresses the network stack owns besides `ipv4`'s address: single addresses
    /// (/32) and prefixes, inside or outside the subnet. It answers ARP for
    /// each and accepts IPv4 traffic to each; routing, the subnet broadcast
    /// and the default source stay with `ipv4`. Duplicates and overlaps are
    /// merged. Every address must be unicast, and none may be the gateway or
    /// the subnet's broadcast address; a prefix must not have host bits set.
    /// Copied at `Stack::new`.
    pub owned_ipv4: &'a [Ipv4Cidr],
    /// Interface PRNG seed, including the TCP ISN secret. Production callers
    /// use [`os_random_seed`]; tests and library benchmarks may fix it.
    pub random_seed: u64,
    /// How many on-link neighbors the ARP cache holds.
    /// [`DEFAULT_NEIGHBOR_CAPACITY`] is 256. When the cache is full, a new
    /// neighbor evicts the one that expires first.
    pub neighbor_capacity: usize,
    /// TCP socket slots. Must be positive.
    pub max_sockets: usize,
    /// How many UDP sockets may be added. Zero disables UDP sockets.
    pub max_udp_sockets: usize,
    /// Received packets held between the RX burst and processing. Must be positive. Each
    /// `receive` or `turn` processes at most its `ingress_budget`; the rest wait here.
    pub rx_staging: usize,
    /// Capacity of the network stack's pending TX frame queue: frames built but not yet accepted by
    /// the card. Must be positive. While it is full, UDP sends return `WouldBlock` and
    /// `send` can only flush.
    pub tx_pending: usize,
}

/// Why [`Stack::new`] or a drive call failed.
///
/// `new` returns the `Port` with the error, so it can be closed. `Display` prints the
/// `Debug` form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StackError {
    /// From `new`: a [`StackConfig`] rule failed, the start time is before zero, the card's
    /// IP MTU is from 68 to 87, or a path MTU is below 88 (the smallest MTU that carries a
    /// 48-byte TCP segment).
    InvalidConfig(&'static str),
    /// From `new`: the card's IP MTU is below 68.
    IpMtuBelowIpv4Minimum { mtu: u16 },
    /// From `new`: the TX pool holds fewer than `tx_reserve(TX descriptors, tx_pending)` mbufs.
    TxPoolTooSmall { count: usize, required: usize },
    /// From `new`: the frame pool holds fewer than `frame_reserve(TX descriptors, tx_pending)` mbufs.
    TxFramePoolTooSmall { count: usize, required: usize },
    /// From `new`: the RX pool holds fewer than `rx_reserve(RX descriptors, rx_burst, rx_staging, cache size)` mbufs.
    RxPoolTooSmall { count: usize, required: usize },
    /// From `receive` or `turn`: `now` is earlier than the last time. Nothing was done; the
    /// network stack stays usable.
    TimeWentBackwards { last: Instant, now: Instant },
    /// From a drive call: the card's driver failed, or the card could not be recovered after a
    /// device reset. Later calls fail too: [`Stack::into_port`], then
    /// [`Port::try_close`](crate::Port::try_close).
    Port(PortError),
}

impl fmt::Display for StackError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for StackError {}

/// `add_tcp` refused the socket; each variant returns the config it was given.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddTcpError {
    /// Every socket slot is in use (`StackConfig::max_sockets`).
    SocketsFull(tcp::Config),
}

impl fmt::Display for AddTcpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SocketsFull(_) => write!(f, "stack has no free socket slot"),
        }
    }
}

impl std::error::Error for AddTcpError {}

/// Why [`Stack::add_udp`] refused a socket. Checked in this order: `InvalidConfig`,
/// `Unaddressable`, `PortInUse`, `Full`. Nothing changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddUdpError {
    /// Every one of the `max_udp_sockets` slots is taken.
    Full,
    /// That address (or all addresses) and port are already bound.
    PortInUse,
    /// The port or `rx_queue` is zero.
    InvalidConfig(&'static str),
    /// `UdpConfig::local_addr` is not an address this network stack owns.
    Unaddressable,
}

/// `Stack::udp_send_frame` refused the frame; it comes back with the error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UdpSendFrameError {
    Udp(UdpSendError),
    /// The frame is not from this network stack's frame pool.
    Foreign,
}

/// A TCP socket's readiness key from [`Stack::tcp_ready_key`], checked once against its handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpReadyKey(SocketHandle);

/// A UDP socket's readiness key from [`Stack::udp_ready_key`], checked once against its handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UdpReadyKey(smoltcp::iface::UdpHandle);

/// Counters from one receive step or combined network turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Turn {
    /// Packets this call's RX burst accepted into staging. Rejected packets are counted only
    /// in `rx_rejected`.
    pub rx_received: usize,
    /// Packets this turn's RX burst released: unsupported, or a bad IPv4
    /// header checksum reported by the card (see `RxBurst::rejected`).
    pub rx_rejected: usize,
    /// Staged packets handed to smoltcp this turn.
    pub rx_processed: usize,
    /// Packets still staged after ingress: the budget ran out. Receiving never
    /// waits for TX room; a reply that finds none is dropped.
    pub rx_staged: usize,
    /// Packets the card accepted across this call's flushes.
    pub tx_accepted: usize,
    /// Packets still pending after this call.
    pub tx_pending: usize,
}

impl Turn {
    /// Whether this turn received, processed or transmitted anything; a turn that did not and
    /// left nothing staged or pending is idle.
    pub fn moved(&self) -> bool {
        self.rx_received + self.rx_rejected + self.rx_processed + self.tx_accepted != 0
    }
}

/// Counters from one network send or flush.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sent {
    /// Packets the card accepted in the final flush.
    pub tx_accepted: usize,
    /// Packets still pending after it.
    pub tx_pending: usize,
}

/// A TCP/UDP network stack over one owned card ([`Port`]), driven by the caller's loop.
///
/// The network stack, its sockets, handles, and every frame and datagram it hands out stay on the
/// thread that started DPDK. It reads no clock and never waits: each call does the work
/// possible now and returns.
///
/// # The drive loop
///
/// Nothing happens between calls. Each pass of the loop does, in order:
///
/// 1. [`receive`](Self::receive)`(now, ingress_budget)`: take in arriving packets and process
///    up to `ingress_budget` of them; replies built while receiving (resets, immediate ACKs,
///    ARP replies) are flushed to the card.
/// 2. The application reads and writes its sockets.
/// 3. [`send`](Self::send)`()`: build what the sockets want to send, including what the
///    application just wrote, and flush.
///
/// [`turn`](Self::turn)`(now, ingress_budget)` does both steps in one call; what the
/// application then writes leaves on the next pass. Everything egress builds (data,
/// SYN-ACKs, retransmissions, delayed ACKs, window updates) leaves only in `send` or
/// `turn`, so keep the application step between `receive` and `send` short.
/// [`flush`](Self::flush) only hands pending packets to the card.
///
/// After a call, `rx_staged > 0` means the ingress budget ran out: call again promptly.
/// `tx_pending > 0` means the driver did not take everything: call `flush` until nothing is
/// pending, then `send`.
///
/// # Time
///
/// `new`, `receive` and `turn` take the current time as a [`smoltcp::time::Instant`], in
/// microseconds. The start time must be zero or more; after it, time must never go back
/// (equal times are allowed), or the call returns [`StackError::TimeWentBackwards`] having
/// done nothing. `send` and `flush` reuse the last time. Use one clock for every call, for
/// example microseconds elapsed since a `std::time::Instant` taken just before `new`, not
/// `smoltcp::time::Instant::now()`, which reads the wall clock.
///
/// # Sleeping
///
/// A loop on a pinned CPU calls again at once. A loop that sleeps when a call was idle
/// ([`Turn::moved`] false) must cap the sleep: [`poll_at`](Self::poll_at) covers socket
/// timers only, not packets arriving at the card, staged or pending packets, or ARP probes.
/// The repository's loops sleep `min(poll_at - now, 1 ms)`, and 1 ms when it is `None`.
///
/// # TCP behaviour
///
/// | | |
/// |---|---|
/// | Retransmission timeout | 300 ms until the round-trip time is measured; then the smoothed round-trip time plus the larger of four times its variation and 50 ms. Each timeout doubles it, up to 60 s. |
/// | Delayed ACK | 10 ms; `set_ack_delay(None)` turns it off, `ack_now()` sends an owed ACK at the next egress |
/// | TIME-WAIT | 10 s |
/// | Keepalive | none; an idle connection sends nothing |
/// | Nagle's algorithm | none; queued data may leave at the next egress |
/// | Congestion control | Cubic, initial window 1 MiB (`set_initial_window`) |
/// | TCP timestamps option | never negotiated |
/// | Out-of-order data | at most 1024 frames per socket past its first gap; further out-of-order segments are refused and counted in `rx_refused_segments()` |
///
/// When the RX pool falls below one RX ring refill plus one burst, the sockets holding the
/// most out-of-order frames release them, farthest past the gap first, until the pool
/// recovers; peers retransmit what was released. In-order data is never released.
///
/// A device reset requested by the card's driver (ENA's health watchdog, run among DPDK's
/// timers once a second) is recovered at the start of the next `receive` or `turn`; see
/// [`turn`](Self::turn). [`port_resets`](crate::port_resets) counts recoveries.
///
/// # Panics
///
/// `turn`, `receive` and `send` panic if `Port::alloc_tx` does: its TX done
/// cleanup failing with an error other than `-ENOTSUP`. With
/// `Offloads::tx_tcp_checksum` or `Offloads::tx_udp_checksum` on, these and
/// `flush` also panic when a queued frame fails TX checksum preparation (see
/// [`Offloads::tx_tcp_checksum`](crate::Offloads::tx_tcp_checksum) and
/// [`Offloads::tx_udp_checksum`](crate::Offloads::tx_udp_checksum)).
///
/// A panic unwinding out of any of them leaves the network stack unusable: drop it or
/// call `into_port`, nothing else.
///
/// Every method taking a handle panics on a handle from another network stack. Other documented
/// panics: [`add_tcp`](Self::add_tcp) with `rx_capacity` over 1 GiB or `mss_cap: Some(0)`;
/// `tcp::tx_mbuf_budget` with an IP MTU below 68 or `mss_cap: Some(0)`;
/// `set_initial_window(0)`, `set_ack_delay` over 60 s and `set_hop_limit(Some(0))` on a
/// socket; an out-of-range readiness key. Debug builds also reject stale readiness keys;
/// in release builds a stale in-range key can arm the wrong socket (see [`arm_tcp`](Self::arm_tcp)).
pub struct Stack {
    iface: Interface,
    /// Exactly `max_sockets` slots, never grown.
    sockets: TcpSet<'static>,
    udp: UdpSet,
    dropped: DropQueue,
    dropped_tcp: Vec<SocketHandle>,
    dropped_udp: Vec<smoltcp::iface::UdpHandle>,
    max_sockets: usize,
    max_udp_sockets: usize,
    ip_mtu: u16,
    /// Holds every staged and pending packet; its `Port` drops after them.
    device: Device,
    now: Instant,
    /// The first turn at or after this time runs DPDK's timers.
    next_service: Instant,
    /// The last RX pool reading was below the low mark: every turn reads it again, received
    /// packets or not, until a reading is not.
    rx_pool_low: bool,
}

impl Stack {
    /// Validate `config`, then build the interface at time `now` with one IPv4
    /// address. Invalid config, or `now` before `Instant::ZERO`, returns the
    /// [`Port`] unchanged with `InvalidConfig`.
    /// IP MTU below 68 returns the `Port` with `IpMtuBelowIpv4Minimum`.
    /// A TX pool below `tx_reserve` returns the `Port` with `TxPoolTooSmall`.
    /// A frame pool below `frame_reserve` returns the `Port` with
    /// `TxFramePoolTooSmall`: packets the card holds and pending packets can
    /// then never take the last frame. An RX pool below `rx_reserve` returns
    /// the `Port` with `RxPoolTooSmall`: with the RX descriptors posted and the
    /// staging queue full, the pool could not stay above its low mark.
    /// The negotiated TX descriptor count bounds sent mbufs held by card drivers.
    /// net_ring ignores that count and keeps sent mbufs on its attached ring
    /// instead: open it with TX descriptors equal to the TX ring size.
    ///
    /// smoltcp encodes "poll now" as `Instant::ZERO`, which `poll_at`
    /// normalization relies on; the monotonic rule of `turn` and `receive`
    /// keeps every later time at or above this start. Times near `i64::MAX`
    /// microseconds overflow upstream timer arithmetic.
    // Setup path; the error size does not matter.
    #[allow(clippy::result_large_err)]
    pub fn new(port: Port, config: StackConfig<'_>, now: Instant) -> Result<Self, (Port, StackError)> {
        if let Err(reason) = validate(&config) {
            return Err((port, StackError::InvalidConfig(reason)));
        }
        if now < Instant::ZERO {
            return Err((port, StackError::InvalidConfig("start time must be >= 0")));
        }
        let mtu = port
            .frame_capacity()
            .checked_sub(14)
            .expect("Ethernet header fits");
        if mtu < 68 {
            return Err((port, StackError::IpMtuBelowIpv4Minimum { mtu }));
        }
        let paths = match PathMtus::new(config.paths, config.ipv4, mtu) {
            Ok(paths) => paths,
            Err(reason) => return Err((port, StackError::InvalidConfig(reason))),
        };
        let owned = match OwnedIpv4::new(config.owned_ipv4, config.ipv4, config.gateway) {
            Ok(owned) => owned,
            Err(reason) => return Err((port, StackError::InvalidConfig(reason))),
        };
        let count = port.tx_pool_count();
        let required = tx_reserve(port.descriptor_counts().1, config.tx_pending);
        if count < required {
            return Err((port, StackError::TxPoolTooSmall { count, required }));
        }
        let frame_count = port.frame_pool().count();
        let frame_required = frame_reserve(port.descriptor_counts().1, config.tx_pending);
        if frame_count < frame_required {
            return Err((
                port,
                StackError::TxFramePoolTooSmall {
                    count: frame_count,
                    required: frame_required,
                },
            ));
        }
        let rx_count = port.rx_pool_count();
        let rx_required = rx_reserve(
            port.descriptor_counts().0,
            port.rx_burst_size(),
            config.rx_staging,
            port.rx_pool_cache_size(),
        );
        if rx_count < rx_required {
            return Err((
                port,
                StackError::RxPoolTooSmall {
                    count: rx_count,
                    required: rx_required,
                },
            ));
        }
        let mut device = Device::new(port, config.rx_staging, config.tx_pending);
        let mut iface_config = Config::new(config.ethernet.into());
        iface_config.random_seed = config.random_seed;
        iface_config.neighbor_capacity = config.neighbor_capacity;
        iface_config.owned_ipv4 = owned;
        let mut iface = Interface::new(iface_config, &mut device, now);
        iface.update_ip_addrs(|addrs| {
            // The address list starts empty and holds at least one entry.
            addrs.push(IpCidr::Ipv4(config.ipv4)).unwrap();
        });
        iface.set_ipv4_gateway(config.gateway);
        iface.set_path_mtus(paths);
        Ok(Self {
            iface,
            sockets: TcpSet::new(config.max_sockets),
            udp: UdpSet::new(config.max_udp_sockets, device.frame_source()),
            dropped: DropQueue::with_capacity(config.max_sockets, config.max_udp_sockets),
            dropped_tcp: Vec::with_capacity(config.max_sockets),
            dropped_udp: Vec::with_capacity(config.max_udp_sockets),
            max_sockets: config.max_sockets,
            max_udp_sockets: config.max_udp_sockets,
            ip_mtu: mtu,
            device,
            now,
            next_service: now,
            rx_pool_low: false,
        })
    }

    /// One bounded turn at `now`: DPDK's timers once a second, device recovery if the driver
    /// asked for a reset, flush pending, one RX burst, maintenance, up to
    /// `ingress_budget` received packets, egress passes until one reports no
    /// socket progress or queues no frame, flush.
    ///
    /// Once a second (the first `turn` or `receive` at or after a second since the last) the
    /// turn calls `rte_timer_manage`, which runs DPDK timers due on this lcore: ENA's
    /// health watchdog, which raises `RTE_ETH_EVENT_INTR_RESET` on a device fault. A card
    /// with that event pending is reset, configured and started again before the RX burst:
    /// frames the device held return to their pools, staged and pending frames stay, sockets
    /// and neighbors are untouched, and TCP retransmits what the device dropped.
    /// A failed recovery returns `StackError::Port` and leaves the card not ready, so later
    /// turns fail too: close the network stack.
    ///
    /// `now` earlier than the previous `new`/`turn`/`receive`
    /// time returns
    /// `TimeWentBackwards` without doing any work; equal times are allowed, so
    /// times stay >= 0.
    ///
    /// `receive` then `send` runs the same steps plus one flush between
    /// ingress and egress when frames are pending. Without TX backpressure both
    /// put the same frames on the wire with the same combined counters. When
    /// the pending queue or the TX ring is full, that flush can let more frames
    /// through in the same loop iteration: `tx_accepted` and `tx_pending` differ.
    ///
    /// Panics as described in [Panics](Stack#panics).
    pub fn turn(&mut self, now: Instant, ingress_budget: usize) -> Result<Turn, StackError> {
        let mut turn = self.ingress(now, ingress_budget)?;
        turn.tx_accepted += self.egress()?;
        turn.tx_pending = self.device.pending();
        Ok(turn)
    }

    /// The first half of a turn, so the application can reply in the same
    /// loop iteration: `receive`, then read and write sockets, then `send`.
    /// At `now`: flush pending, one RX burst, maintenance, up to
    /// `ingress_budget` received packets, then flush again if frames are
    /// pending. Frames smoltcp built while processing received packets (RST,
    /// immediate and challenge ACKs, ARP replies) are thus offered to the card
    /// before the application step. Egress does not run: what sockets want to
    /// send, including what the application writes, waits for `send`.
    ///
    /// Time rules as `turn`. The extra flush when frames are pending is the
    /// one difference from `turn`, which runs egress before its final flush:
    /// under TX backpressure (pending queue or TX ring full) `receive` then
    /// `send` can let more frames through in this loop iteration than `turn`,
    /// with different `tx_accepted` and `tx_pending`. Ingress never waits for TX
    /// room: a reply built while receiving that finds none is dropped.
    ///
    /// Panics as described in [Panics](Stack#panics).
    #[inline]
    pub fn receive(&mut self, now: Instant, ingress_budget: usize) -> Result<Turn, StackError> {
        let mut turn = self.ingress(now, ingress_budget)?;
        if turn.tx_pending != 0 {
            turn.tx_accepted += self.device.flush().map_err(StackError::Port)?;
            turn.tx_pending = self.device.pending();
        }
        Ok(turn)
    }

    /// The second half of a turn: egress passes until one reports no socket
    /// progress or queues no frame, then flush. Whatever the application
    /// wrote to sockets since `receive` leaves here. Frames smoltcp builds in
    /// egress (SYN-ACK, retransmissions, expired delayed ACKs, window updates)
    /// also leave only here, after the application step: keep
    /// that step short, or connection setup and retransmissions wait for it.
    ///
    /// Runs at the time of the last `new`, `turn` or `receive`.
    /// Needs no preceding `receive` and may repeat; with nothing to send it
    /// sends nothing.
    ///
    /// Panics as described in [Panics](Stack#panics).
    #[inline]
    pub fn send(&mut self) -> Result<Sent, StackError> {
        self.tidy();
        let tx_accepted = self.egress()?;
        Ok(Sent {
            tx_accepted,
            tx_pending: self.device.pending(),
        })
    }

    /// Push every pending frame to the card, burst after burst while it accepts
    /// everything offered. The unaccepted suffix stays pending in order;
    /// `tx_accepted` counts accepted frames and `tx_pending` counts that suffix.
    /// Retry on the caller's bounded cadence when frames remain pending.
    /// Builds no frames and does not change time; time rules are in [`Self::turn`].
    ///
    /// TX checksum preparation happens here; panics as described in
    /// [Panics](Stack#panics), except that `flush` does not use `Port::alloc_tx`.
    pub fn flush(&mut self) -> Result<Sent, StackError> {
        let tx_accepted = self.device.flush().map_err(StackError::Port)?;
        Ok(Sent {
            tx_accepted,
            tx_pending: self.device.pending(),
        })
    }

    /// Resolve the configured gateway's Ethernet address. If it is not cached,
    /// queue one ARP request when the gateway is not rate-limited and TX
    /// capacity is available. The request leaves on the next flush.
    /// `Err(NoRoute)` when `StackConfig::gateway` is `None`.
    pub fn resolve_gateway(&mut self) -> Result<Option<EthernetAddress>, NoRoute> {
        self.iface.resolve_gateway(&mut self.device)
    }

    /// Exclusive storage covering this network stack's full payload room: `len()` is the IP
    /// MTU − 28, the largest UDP payload. `None` when the pool is empty.
    /// Fill the payload, `truncate` to its length, then `freeze` to send.
    /// Dropping the frame returns it to the pool.
    #[inline]
    pub fn tx_frame(&mut self) -> Option<OwnedFrame> {
        smoltcp::phy::alloc_frame(self.device.frame_pool())
    }

    /// Frames this lcore can allocate from the frame pool now: the pool's ring plus this lcore's cache.
    pub fn free_frames(&self) -> usize {
        self.device.frame_pool().free_count()
    }

    fn set_now(&mut self, now: Instant) -> Result<(), StackError> {
        if now < self.now {
            return Err(StackError::TimeWentBackwards {
                last: self.now,
                now,
            });
        }
        self.now = now;
        Ok(())
    }

    /// Time check, DPDK timers once a second and recovery of a reset card (see `turn`),
    /// tidying, flush, one RX burst, maintenance, then ingress. After ingress of a burst that
    /// returned packets, and on every turn after a reading below it, the RX pool is checked
    /// against the low mark (one RX ring refill plus one burst). While it is below, the TCP
    /// sockets holding the most out-of-order frames release them, farthest from their in-order
    /// data first, until the pool is back at the mark (`TcpSet::prune_out_of_order`), the
    /// frames this turn's ingress retained included.
    fn ingress(&mut self, now: Instant, ingress_budget: usize) -> Result<Turn, StackError> {
        self.set_now(now)?;
        if now >= self.next_service {
            // SAFETY: the Port's owner thread, an EAL lcore.
            unsafe { switch_dpdk_sys::rte_timer_manage() };
            self.next_service = now + SERVICE_PERIOD;
        }
        if self.device.port_mut().reset_pending() {
            self.device.port_mut().recover().map_err(StackError::Port)?;
        }
        self.tidy();
        let tx_accepted = self.device.flush().map_err(StackError::Port)?;
        let burst = self.device.rx_burst().map_err(StackError::Port)?;
        let received = burst.accepted + burst.rejected;
        self.iface.poll_maintenance(now);

        let mut rx_processed = 0;
        while rx_processed < ingress_budget {
            match self.iface.poll_ingress_single_udp(
                now,
                &mut self.device,
                &mut self.sockets,
                &mut self.udp,
            ) {
                PollIngressSingleResult::None => break,
                PollIngressSingleResult::PacketProcessed
                | PollIngressSingleResult::SocketStateChanged => {}
            }
            rx_processed += 1;
        }
        if received != 0 || self.rx_pool_low {
            let shortfall = self.device.rx_pool_shortfall();
            self.rx_pool_low = shortfall != 0;
            if shortfall != 0 {
                self.sockets.prune_out_of_order(shortfall);
            }
        }

        Ok(Turn {
            rx_received: burst.accepted,
            rx_rejected: burst.rejected,
            rx_processed,
            rx_staged: self.device.staged(),
            tx_accepted,
            tx_pending: self.device.pending(),
        })
    }

    /// Egress passes at the stored time, then flush; returns the frames the
    /// card accepted.
    fn egress(&mut self) -> Result<usize, StackError> {
        // Every pass that continues the loop queued a frame into the
        // fixed-capacity pending queue, so at most capacity + 1 passes run. A
        // pass can report a change without queueing anything, and a socket may
        // then want the same send again at this time.
        // Stop after a send when the next pass would do nothing.
        loop {
            let pending = self.device.pending();
            if self
                .iface
                .poll_egress(self.now, &mut self.device, &mut self.sockets)
                == PollResult::None
            {
                break;
            }
            if self.device.pending() == pending {
                break;
            }
            if self.iface.egress_idle(&self.sockets) {
                break;
            }
        }
        self.device.flush().map_err(StackError::Port)
    }

    /// `StackConfig::max_sockets`.
    pub fn max_sockets(&self) -> usize {
        self.max_sockets
    }

    /// `StackConfig::max_udp_sockets`.
    pub fn max_udp_sockets(&self) -> usize {
        self.max_udp_sockets
    }

    /// The card's `PortConfig::tx_burst`: the most frames one flush burst offers the card.
    pub fn tx_burst(&self) -> usize {
        self.device.tx_burst_size()
    }

    /// When sockets next want a turn, evaluated at the latest time given
    /// to `new`, `turn` or `receive`; a deadline already past is reported as
    /// that time, as is a socket with a segment ready to send. `None`: no
    /// socket deadline. Socket timers live in a millisecond timer wheel: the
    /// result is the next wheel boundary or ready/due socket, never later than
    /// the earliest socket deadline rounded up to a whole millisecond, and may
    /// be earlier (a turn at a boundary can find nothing due).
    ///
    /// A non-empty DropQueue, or a dropped socket that reached CLOSED, reports
    /// the current time so the next turn processes or frees it.
    /// It does not cover frames staged or pending in the network stack. When the last
    /// `turn` or `receive` reported `rx_staged > 0`, call it again promptly:
    /// the ingress budget ran out. When a call reported `tx_pending > 0`, the next
    /// call progresses only once the card accepts frames or frees TX packets: retry
    /// on the caller's own bounded cadence, independent of this deadline. Pending frames leave through `flush`, or the flush inside
    /// `turn`, `receive` or `send`. When a call reported `tx_pending > 0`, call
    /// `flush` until nothing is pending, then `send`: `send` runs egress before
    /// its flush, so a full pending queue lets it only flush. An ordinary TCP
    /// sender refused by the full queue stays due now and is served first on
    /// retry; a refused dropped socket's zero-window probe is rescheduled.
    /// An ARP probe owed for a stale neighbor is not covered by this deadline.
    /// After freeing capacity, `send` or `turn` retries that probe in egress.
    pub fn poll_at(&mut self) -> Option<Instant> {
        if !self.dropped.is_empty() || self.sockets.has_reapable() {
            return Some(self.now);
        }
        self.iface
            .poll_at(self.now, &mut self.sockets)
            .map(|at| at.max(self.now))
    }

    /// Build a TCP socket from `config` and add it. Returns the config when all
    /// `max_sockets` slots are occupied. `rx_capacity` is the advertised receive window
    /// target, not a limit on received data. Contiguous payload is accepted even beyond its
    /// advertised edge; out-of-order data remains window-bounded. RX data uses the shared pool.
    /// Queued data uses the shared frame pool and is limited by `config.tx_capacity`
    /// bytes, including unacknowledged data. With the RX pool empty the driver
    /// drops arrivals and the peer retransmits; with the frame pool empty
    /// `send_slice` queues nothing until frames return.
    /// Connections never use TCP timestamps.
    /// A dropped connection holds its slot until it closes; `SocketsFull` is
    /// returned until then. If it closes during a call, the next tidying call
    /// frees the slot.
    ///
    /// # Panics
    /// If `config.rx_capacity` exceeds 1 GiB or `config.mss_cap` is `Some(0)`.
    pub fn add_tcp(&mut self, config: tcp::Config) -> Result<TcpHandle, AddTcpError> {
        self.tidy();
        // Upstream `add` pushes a new slot when none is free.
        if self.sockets.len() == self.max_sockets {
            return Err(AddTcpError::SocketsFull(config));
        }
        let handle = self.sockets.add_admitted(
            &config,
            usize::from(self.ip_mtu),
            self.device.frame_source(),
        );
        Ok(TcpHandle {
            raw: handle,
            queue: self.dropped.clone(),
        })
    }

    /// Bind a local UDP port on one owned address (`local_addr`) or on all of them (`None`);
    /// each (address or all-IP, port) at most once. A datagram goes to the socket bound to its
    /// destination, else to the port's all-IP socket; broadcasts only to the all-IP one. At
    /// most `rx_queue` datagrams are queued or held by the application; their mbufs come
    /// from the RX pool every socket shares. A refused socket changes nothing.
    pub fn add_udp(&mut self, config: UdpConfig) -> Result<UdpHandle, AddUdpError> {
        self.tidy();
        let handle = self.udp.add(&self.iface, config).map_err(|error| match error {
            UdpAddError::Full => AddUdpError::Full,
            UdpAddError::PortInUse => AddUdpError::PortInUse,
            UdpAddError::InvalidConfig(reason) => AddUdpError::InvalidConfig(reason),
            UdpAddError::Unaddressable => AddUdpError::Unaddressable,
        })?;
        Ok(UdpHandle {
            raw: handle,
            queue: self.dropped.clone(),
        })
    }

    /// Read-only access to a UDP socket.
    pub fn udp(&self, handle: &UdpHandle) -> &UdpSocket {
        self.udp.get(self.udp_raw(handle))
    }

    /// Change a UDP socket.
    /// Replacing the socket through this reference is unsupported.
    pub fn udp_mut(&mut self, handle: &UdpHandle) -> &mut UdpSocket {
        let raw = self.udp_raw(handle);
        self.udp.get_mut(raw)
    }

    /// Copy one datagram into the shared pending FIFO at this network stack's current
    /// time, in call order with TCP frames; for payloads the caller already
    /// holds. To write a payload in place, fill a `tx_frame` and use
    /// `udp_send_frame`. `to` may be omitted for a connected socket. The packet
    /// leaves on `flush`, or the flush inside `turn`, `receive` or `send`.
    ///
    /// The payload is copied into a frame from this network stack's frame pool, then sent as
    /// `udp_send_frame` sends a unique frame. Errors come in this order: the destination
    /// and size checks (`NoDestination`, `PeerMismatch`, `Unaddressable`, `TooLarge`),
    /// `WouldBlock` for an empty frame pool, the neighbor lookup (`NoRoute`,
    /// `NeighborPending`), then `WouldBlock` for a full pending queue: flush, then retry.
    /// Flush at least every `StackConfig::tx_pending` sends. A full pending queue does
    /// not block ingress; replies that find no room are dropped.
    /// A send loop must call `receive` or `turn` to take ARP replies and advance time.
    ///
    /// | Error | Meaning |
    /// |---|---|
    /// | `NoDestination` | `to` is `None` and the socket has no peer. |
    /// | `PeerMismatch` | `to` differs from the connected peer. |
    /// | `Unaddressable` | The destination is unspecified, multicast, one of our own addresses, port 0, or the broadcast address without `UdpConfig::broadcast`. |
    /// | `TooLarge` | The payload is longer than the destination's path MTU − 28. Nothing is fragmented. |
    /// | `WouldBlock` (frame pool) | The frame pool is empty; retry once frames return. |
    /// | `NoRoute` | The destination is off the subnet with no gateway, or there is no source address for it. |
    /// | `NeighborPending` | The next hop's MAC is not known yet. An ARP request was queued and this datagram dropped: keep driving the loop, then send again. |
    /// | `WouldBlock` (pending queue) | The pending queue is full: flush, then retry. |
    ///
    /// A learned neighbor stays usable while the network stack refreshes it, so sends through a
    /// resolved gateway do not hit `NeighborPending` unless more than `neighbor_capacity`
    /// neighbors evicted it.
    pub fn udp_send(
        &mut self,
        handle: &UdpHandle,
        to: Option<IpEndpoint>,
        payload: &[u8],
    ) -> Result<(), UdpSendError> {
        let raw = self.udp_raw(handle);
        self.iface
            .udp_send(&mut self.device, &mut self.udp, raw, to, payload)
    }

    /// Send `frame` as one datagram; `to` may be omitted for a connected
    /// socket. On success the frame belongs to the network stack. A unique frame at its original payload
    /// start goes out in place: its headers are written in front of its payload and its mbuf
    /// joins the shared pending FIFO in call order, freed after TX completion.
    /// A shared frame or one sliced to a later start is copied into a new frame-pool frame
    /// as `udp_send` does, and this reference is dropped. A zero-length frame is an empty
    /// datagram.
    ///
    /// Every error returns the frame untouched. `Foreign`: the frame is not from this
    /// network stack's frame pool. Then `udp_send`'s errors in its order; only a unique frame at its
    /// original payload start avoids a copy and cannot report an empty frame pool.
    /// For other frames, `WouldBlock` can mean the frame pool is empty. Retry after capacity
    /// returns.
    #[inline]
    pub fn udp_send_frame(
        &mut self,
        handle: &UdpHandle,
        to: Option<IpEndpoint>,
        frame: ImmutableFrame,
    ) -> Result<(), (ImmutableFrame, UdpSendFrameError)> {
        if frame.ctx() != self.device.frame_pool().mempool().cast() {
            return Err((frame, UdpSendFrameError::Foreign));
        }
        let raw = self.udp_raw(handle);
        self.iface
            .udp_send_frame(&mut self.device, &mut self.udp, raw, to, frame)
            .map_err(|(frame, error)| (frame, UdpSendFrameError::Udp(error)))
    }

    /// Free queued datagrams. Datagrams the application still holds keep their RX
    /// mbufs until dropped.
    pub fn remove_udp(&mut self, handle: UdpHandle) {
        self.udp_raw(&handle);
        self.remove_udp_raw(handle.into_raw());
    }

    fn remove_udp_raw(&mut self, handle: smoltcp::iface::UdpHandle) {
        self.udp.remove(handle);
    }

    /// Read-only access to a socket, including retransmits, loss_events and congestion_window. Marks nothing.
    pub fn tcp(&self, handle: &TcpHandle) -> &tcp::Socket<'static> {
        self.sockets.get(self.tcp_raw(handle))
    }

    /// Change a socket's data or settings and schedule a readiness check before the
    /// next `poll_at` call or egress pass. Connect, listen, close and abort go through
    /// the network stack's methods of those names. No public API hands out an owned socket, so
    /// this reference cannot be overwritten with a new one; swapping it with a
    /// socket held by another `TcpSet` or network stack is unsupported.
    pub fn tcp_mut(&mut self, handle: &TcpHandle) -> &mut tcp::Socket<'static> {
        let raw = self.tcp_raw(handle);
        self.sockets.get_mut(raw)
    }

    /// Queue `frame` on the socket by value (`tcp::Socket::send_frame`); it
    /// leaves in a later egress (`send` or `turn`). A frame that fits whole into the
    /// unsent tail frame within one segment is copied there and freed at once; any
    /// other becomes its own send-queue entry, held until the peer ACKs its last byte,
    /// and one longer than the segment payload is cut at send. Errors return the frame
    /// untouched: `Foreign` when it is not from this network stack's TX-frame pool,
    /// `InvalidState` unless the socket may send, `Empty` for a zero-length
    /// frame, and `Full` when it exceeds the socket's free bytes.
    pub fn tcp_send_frame(
        &mut self,
        handle: &TcpHandle,
        frame: ImmutableFrame,
    ) -> Result<(), (ImmutableFrame, tcp::SendFrameError)> {
        self.tcp_mut(handle).send_frame(frame)
    }

    /// Start connecting: queue a SYN to `remote` from `local` (a port, or an address and
    /// port). It leaves at the next egress; watch `tcp(&handle).state()` for the handshake.
    /// There is no automatic port choice, so the local port must be nonzero.
    ///
    /// A failed connect leaves the socket unchanged:
    ///
    /// | `tcp::ConnectError` | Meaning |
    /// |---|---|
    /// | `InvalidState` | The socket is not closed. |
    /// | `Unaddressable` | A zero port on either side, an unspecified remote, a local address the network stack does not own, or no source address toward `remote`. |
    /// | `AddressInUse` | Another live socket, one in TIME-WAIT included, holds the same addresses and ports. |
    pub fn connect<T: Into<IpEndpoint>, U: Into<IpListenEndpoint>>(
        &mut self,
        handle: &TcpHandle,
        remote: T,
        local: U,
    ) -> Result<(), tcp::ConnectError> {
        let raw = self.tcp_raw(handle);
        self.sockets.connect(raw, self.iface.context(), remote, local)
    }

    /// Wait for one incoming connection on `local` (a port for every owned address, or an
    /// address and port). There is no accept queue: the socket itself becomes the
    /// connection. To accept several at once, listen on the same port with several sockets;
    /// to accept again, call `listen` once the socket is `Closed`.
    ///
    /// A failed listen leaves the socket unchanged. `ListenError::Unaddressable`: port 0,
    /// or an address that is neither `StackConfig::ipv4`'s nor in `owned_ipv4`.
    /// `ListenError::InvalidState`: the socket is open and not already listening on that
    /// endpoint (listening again on the same endpoint is `Ok`).
    pub fn listen<T: Into<IpListenEndpoint>>(
        &mut self,
        handle: &TcpHandle,
        local: T,
    ) -> Result<(), tcp::ListenError> {
        let raw = self.tcp_raw(handle);
        let local = local.into();
        if local.addr.is_some_and(|addr| !self.iface.has_ip_addr(addr)) {
            return Err(tcp::ListenError::Unaddressable);
        }
        self.sockets.listen(raw, local)
    }

    /// `tcp::Socket::close`: send FIN after the queued data, or, in LISTEN and
    /// SYN-SENT, close at once; in SYN-RECEIVED, reset.
    ///
    /// The ways to end a connection:
    ///
    /// | Call | On the wire | Unread data | Slot |
    /// |---|---|---|---|
    /// | `close` | FIN after the queued data | stays readable | kept; drive until `Closed`, then reuse or remove |
    /// | [`abort`](Self::abort) | one reset at the next egress, for an open connection | discarded | kept, `Closed` at once |
    /// | [`remove_tcp`](Self::remove_tcp) | nothing | discarded | freed at once |
    /// | drop the [`TcpHandle`] | reset if unread in-order data remains, else FIN; nothing from `SynSent` | discarded | held until the connection has closed |
    ///
    /// A dropped handle is processed at the start of the next `turn`, `receive`, `send`,
    /// `add_tcp` or `add_udp`. Its connection then ends on its own: after TIME-WAIT (10 s),
    /// after 60 s in FIN-WAIT-2 with no FIN from the peer, or after 8 retransmission
    /// backoffs if the peer stops answering. Until then its slot counts against
    /// `max_sockets`. None of these calls cancels packets already in the pending queue.
    pub fn close(&mut self, handle: &TcpHandle) {
        let raw = self.tcp_raw(handle);
        self.sockets.close(raw);
    }

    /// `tcp::Socket::abort`: close at once; an open connection sends one RST.
    pub fn abort(&mut self, handle: &TcpHandle) {
        let raw = self.tcp_raw(handle);
        self.sockets.abort(raw);
    }

    /// Remove the socket immediately, freeing its slot and releasing its queued
    /// TX frames (the card and pending queue keep their own references to frames
    /// already sent).
    /// Discards unread received bytes and sends nothing on the wire.
    /// Frames the application still holds keep their RX mbufs until dropped.
    pub fn remove_tcp(&mut self, handle: TcpHandle) {
        self.tcp_raw(&handle);
        self.tidy();
        self.remove_tcp_raw(handle.into_raw());
    }

    fn remove_tcp_raw(&mut self, handle: SocketHandle) {
        self.sockets.remove(handle);
    }

    /// The socket's readiness key, checked against `handle` as `tcp` does: it belongs to this
    /// network stack and names a live socket. Arming then runs on every park without repeating that
    /// check. The key is valid for this network stack until the socket is removed; take it once per
    /// socket and keep it beside the handle.
    pub fn tcp_ready_key(&self, handle: &TcpHandle) -> TcpReadyKey {
        let raw = self.tcp_raw(handle);
        self.sockets.get(raw);
        TcpReadyKey(raw)
    }

    /// Arm `token` for `side` of the socket, replacing a token armed there. `ReadySide::Rx`
    /// is for code waiting to receive, `ReadySide::Tx` for code waiting to send. The next
    /// change that can make the socket readable, writable or change its state fires it: a
    /// received segment, `tcp_mut` (also through `tcp_send_frame`, `connect`, `listen`), or a
    /// state change in egress (close timers, TIME-WAIT end). A fired token is disarmed and
    /// returned once by [`Self::pop_fired`]; changes that do not alter readiness may fire it
    /// too, so check the socket's real state and arm again. Sending queued data, frames
    /// returning to the pool and idle calls fire nothing. `remove_tcp` disarms both halves,
    /// and so does the network stack when it processes a dropped handle; `disarm_tcp` stops a token
    /// at once.
    ///
    /// Because `tcp_mut` fires the socket's tokens, arm after reading and writing the
    /// socket, or disarm before and arm after: otherwise the application's own `recv` fires
    /// the token it just armed.
    ///
    /// Only debug builds check that `key` still names a live socket; a key used after its
    /// socket was removed, or on another network stack, misdirects the token to whatever socket holds
    /// that slot, where it fires spuriously, and panics if the slot does not exist.
    pub fn arm_tcp(&mut self, key: TcpReadyKey, side: ReadySide, token: u32) {
        self.sockets.arm(key.0, side, token);
    }

    /// Disarm `side` of the socket. `true` when a token was armed; `false` when none was, or
    /// it fired and `pop_fired` has not returned it yet. Key checks as in [`Self::arm_tcp`].
    pub fn disarm_tcp(&mut self, key: TcpReadyKey, side: ReadySide) -> bool {
        self.sockets.disarm(key.0, side)
    }

    /// [`Self::tcp_ready_key`] for a UDP socket.
    pub fn udp_ready_key(&self, handle: &UdpHandle) -> UdpReadyKey {
        let raw = self.udp_raw(handle);
        self.udp.get(raw);
        UdpReadyKey(raw)
    }

    /// Arm `token` for the UDP socket's receive queue, replacing a token armed there; the next
    /// queued datagram fires it as in [`Self::arm_tcp`]. `remove_udp`, or the network stack processing
    /// a dropped handle, disarms it. Key checks as in [`Self::arm_tcp`].
    pub fn arm_udp(&mut self, key: UdpReadyKey, token: u32) {
        self.udp.arm_rx(key.0, token);
    }

    /// [`Self::disarm_tcp`] for the UDP socket's receive queue.
    pub fn disarm_udp(&mut self, key: UdpReadyKey) -> bool {
        self.udp.disarm_rx(key.0)
    }

    /// One fired token, in no particular order; `None` once every fired token was returned.
    /// The lists are sized at `new` for every token armed once: re-arming a token before it
    /// was returned can grow them.
    #[inline]
    pub fn pop_fired(&mut self) -> Option<u32> {
        self.sockets.pop_fired().or_else(|| self.udp.pop_fired())
    }

    fn tcp_raw(&self, handle: &TcpHandle) -> SocketHandle {
        assert!(
            handle.queue.same(&self.dropped),
            "TCP handle belongs to another Stack"
        );
        handle.raw
    }

    fn udp_raw(&self, handle: &UdpHandle) -> smoltcp::iface::UdpHandle {
        assert!(
            handle.queue.same(&self.dropped),
            "UDP handle belongs to another Stack"
        );
        handle.raw
    }

    /// Process dropped handles and reapable sockets; returns at once when there are none.
    #[inline]
    fn tidy(&mut self) {
        if self.dropped.is_empty() && !self.sockets.has_reapable() {
            return;
        }
        self.free_dropped_sockets();
    }

    /// Close dropped TCP sockets, remove dropped UDP sockets, then remove TCP sockets that
    /// reached CLOSED after their handle was dropped.
    #[cold]
    #[inline(never)]
    fn free_dropped_sockets(&mut self) {
        self.dropped
            .drain_into(&mut self.dropped_tcp, &mut self.dropped_udp);
        for &raw in &self.dropped_tcp {
            self.sockets.orphan(raw, self.now);
        }
        self.dropped_tcp.clear();
        for index in 0..self.dropped_udp.len() {
            self.remove_udp_raw(self.dropped_udp[index]);
        }
        self.dropped_udp.clear();
        self.sockets.take_reapable(|raw| self.dropped_tcp.push(raw));
        for index in 0..self.dropped_tcp.len() {
            self.remove_tcp_raw(self.dropped_tcp[index]);
        }
        self.dropped_tcp.clear();
    }

    /// Mark the card as if its driver raised `RTE_ETH_EVENT_INTR_RESET`: the next `turn` or
    /// `receive` recovers it.
    #[cfg(feature = "testing")]
    #[doc(hidden)]
    pub fn raise_reset(&mut self) {
        self.device.port_mut().raise_reset();
    }

    /// Drop sockets, interface and every staged and pending packet, then
    /// return the [`Port`] for `Port::try_close`.
    pub fn into_port(self) -> Port {
        let Self {
            iface,
            sockets,
            udp,
            device,
            ..
        } = self;
        drop((iface, sockets, udp));
        device.into_port()
    }
}

fn validate(config: &StackConfig<'_>) -> Result<(), &'static str> {
    if !config.ethernet.is_unicast() {
        return Err("Ethernet address must be unicast");
    }
    if config.ethernet == EthernetAddress([0; 6]) {
        return Err("Ethernet address must be nonzero");
    }
    if !IpAddress::Ipv4(config.ipv4.address()).is_unicast() {
        return Err("IPv4 address must be unicast");
    }
    if let Some(gateway) = config.gateway {
        if !IpAddress::Ipv4(gateway).is_unicast() {
            return Err("gateway must be unicast");
        }
        if !config.ipv4.contains_addr(&gateway) {
            return Err("gateway must be inside the IPv4 subnet");
        }
        if gateway == config.ipv4.address() {
            return Err("gateway must not be our IPv4 address");
        }
        // /31 and /32 have neither (RFC 3021); smoltcp's broadcast() is None.
        if let Some(broadcast) = config.ipv4.broadcast() {
            if gateway == config.ipv4.network().address() {
                return Err("gateway must not be the subnet's network address");
            }
            if gateway == broadcast {
                return Err("gateway must not be the subnet's broadcast address");
            }
        }
    }
    if config.max_sockets == 0 {
        return Err("max_sockets must be positive");
    }
    if config.neighbor_capacity == 0 {
        return Err("neighbor_capacity must be > 0");
    }
    if config.rx_staging == 0 {
        return Err("rx_staging must be positive");
    }
    if config.tx_pending == 0 {
        return Err("tx_pending must be positive");
    }
    Ok(())
}
