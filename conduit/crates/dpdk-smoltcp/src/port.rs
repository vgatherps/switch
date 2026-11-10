use std::collections::VecDeque;
use std::ffi::{c_int, c_void, CStr, CString};
use std::fmt;
use std::mem::ManuallyDrop;
use std::ptr::{null_mut, NonNull};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};

use smoltcp::phy::{RxBuf, TxSource};
use zhida_buffer::{FramePool, Mbuf, PoolConfig, PoolCreateError};
use zhida_dpdk_sys::*;

use crate::eal::{Eal, PoolResources};
use crate::rx_pool::rx_low_mark;

/// Static properties and MAC address reported by a DPDK ethdev.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceInfo {
    pub driver_name: String,
    pub rx_capa: u64,
    pub tx_capa: u64,
    pub min_mtu: u16,
    pub max_mtu: u16,
    pub max_rx_pktlen: u32,
    pub mac: [u8; 6],
}

impl DeviceInfo {
    /// Offload bits this device advertises: rx_checksum needs the IPv4, UDP and TCP RX checksum bits.
    pub fn advertised(&self) -> Offloads {
        Offloads {
            rx_checksum: (self.rx_capa
                & (RX_OFFLOAD_IPV4_CKSUM | RX_OFFLOAD_UDP_CKSUM | RX_OFFLOAD_TCP_CKSUM))
                == (RX_OFFLOAD_IPV4_CKSUM | RX_OFFLOAD_UDP_CKSUM | RX_OFFLOAD_TCP_CKSUM),
            rx_timestamp: self.rx_capa & RX_OFFLOAD_TIMESTAMP != 0,
            tx_tcp_checksum: self.tx_capa & TX_OFFLOAD_TCP_CKSUM != 0,
            tx_udp_checksum: self.tx_capa & TX_OFFLOAD_UDP_CKSUM != 0,
        }
    }
}

/// Return DPDK's current static properties and MAC address for `port_id`.
pub fn device_info(_eal: &Eal, port_id: u16) -> Result<DeviceInfo, PortError> {
    // SAFETY: EAL initialized this ethdev and `info` is writable for the call.
    let mut info: rte_eth_dev_info = unsafe { std::mem::zeroed() };
    // SAFETY: EAL initialized this ethdev and `info` is writable for the call.
    unsafe {
        checked(
            "rte_eth_dev_info_get",
            rte_eth_dev_info_get(port_id, &mut info),
        )?
    };
    let driver_name = if info.driver_name.is_null() {
        "<null>".into()
    } else {
        // SAFETY: DPDK returned a live NUL-terminated driver name.
        unsafe { CStr::from_ptr(info.driver_name) }
            .to_string_lossy()
            .into_owned()
    };
    // SAFETY: EAL initialized this ethdev and `mac` is writable for the call.
    let mut mac: rte_ether_addr = unsafe { std::mem::zeroed() };
    // SAFETY: EAL initialized this ethdev and `mac` is writable for the call.
    unsafe {
        checked(
            "rte_eth_macaddr_get",
            rte_eth_macaddr_get(port_id, &mut mac),
        )?
    };
    Ok(DeviceInfo {
        driver_name,
        rx_capa: info.rx_offload_capa,
        tx_capa: info.tx_offload_capa,
        min_mtu: info.min_mtu,
        max_mtu: info.max_mtu,
        max_rx_pktlen: info.max_rx_pktlen,
        mac: mac.addr_bytes,
    })
}

/// Return the DPDK port ID currently registered under `name`.
pub fn port_by_name(_eal: &Eal, name: &str) -> Option<u16> {
    let name = CString::new(name).ok()?;
    let mut port_id = 0;
    // SAFETY: `name` is NUL-terminated and `port_id` is writable for the call.
    (unsafe { rte_eth_dev_get_port_by_name(name.as_ptr(), &mut port_id) } == 0).then_some(port_id)
}

#[derive(Debug, Clone, Copy)]
pub struct PortConfig<'a> {
    pub port_id: u16,
    pub mtu: u16,
    pub rx_descriptors: u16,
    pub tx_descriptors: u16,
    /// Maximum packets requested from the PMD per `rx_burst` call.
    pub rx_burst: u16,
    /// Maximum packets offered to the PMD per `tx_burst` call.
    pub tx_burst: u16,
    pub queue_socket: i32,
    pub rx_pool: PoolConfig<'a>,
    pub tx_pool: PoolConfig<'a>,
    /// The Stack's TX frame pool (`Stack::tx_frame`). Its data room must hold
    /// the frame like `tx_pool`'s; a frame's payload starts 42 bytes after the
    /// mbuf headroom and holds up to IP MTU − 28 bytes. `Stack::new` refuses
    /// fewer than `frame_reserve(TX descriptors, tx_pending)` mbufs.
    pub frame_pool: PoolConfig<'a>,
    pub offloads: Offloads,
}

/// NIC offloads `Port::open` requests from the PMD. The default requests none.
///
/// RX checksum verification drops BAD verdicts and lets smoltcp skip checks
/// with GOOD verdicts. TX TCP and UDP checksum offloads are requested per IPv4 frame;
/// IPv4 header checksums are still computed in software.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Offloads {
    /// RX verification of IPv4 header, TCP and UDP checksums
    /// (`RTE_ETH_RX_OFFLOAD_IPV4_CKSUM | RTE_ETH_RX_OFFLOAD_UDP_CKSUM |
    /// RTE_ETH_RX_OFFLOAD_TCP_CKSUM`). IPv4-header-BAD frames are dropped and
    /// counted. L4-BAD frames reach smoltcp for software verification.
    pub rx_checksum: bool,
    /// Requests `RTE_ETH_RX_OFFLOAD_TIMESTAMP` (bit 14). Each flagged packet
    /// reports the PMD's raw u64 unchanged: ENA uses ns on the Amazon Time Sync
    /// PHC clock; net_af_packet uses ns from the kernel socket stamp; net_pcap
    /// writes µs but does not advertise the offload. An unflagged packet reports
    /// 0. Zhida converts nothing.
    pub rx_timestamp: bool,
    /// TX TCP checksum computation (`RTE_ETH_TX_OFFLOAD_TCP_CKSUM`).
    ///
    /// With this bit on, the stack supplies TCP checksum metadata when emitting
    /// an IPv4/TCP frame. The Device sets `RTE_MBUF_F_TX_IPV4 | RTE_MBUF_F_TX_TCP_CKSUM`
    /// and L2/L3/L4 header lengths from that metadata. At the first flush, before
    /// any TX burst, `eth_tx_prepare` prepares the queued range in contiguous
    /// parts of at most `u16::MAX` frames. A PMD refusal panics with the port,
    /// frame index in that range and rte_errno. `Port::tx_burst` itself sets no
    /// per-frame offload metadata.
    ///
    /// net_tap (a test PMD) computes this checksum in software by allocating a
    /// header copy per frame from the frame's own TX pool: a TX pool with no
    /// free mbuf beyond the pending frames makes it accept nothing, and a failed
    /// kernel write leaks the copy and leaves the pending frame stripped of its
    /// headers. Use it on net_tap only in tests with TX pool slack that fail on
    /// any TX error.
    pub tx_tcp_checksum: bool,
    /// TX UDP checksum computation (`RTE_ETH_TX_OFFLOAD_UDP_CKSUM`).
    ///
    /// With this bit on, the stack supplies UDP checksum metadata when emitting
    /// an IPv4/UDP frame. The Device sets `RTE_MBUF_F_TX_IPV4 | RTE_MBUF_F_TX_UDP_CKSUM`
    /// and L2/L3/L4 header lengths of 14 / the emitted IPv4 header length / 8.
    /// The first flush prepares the queued range as described for `tx_tcp_checksum`.
    /// `Port::tx_burst` itself sets no per-frame offload metadata.
    /// The NIC computes every UDP checksum the Stack sends on this port, so
    /// `UdpConfig::tx_checksum` has no effect: false still gets a computed checksum.
    /// The same net_tap caveat described on `tx_tcp_checksum` applies here.
    pub tx_udp_checksum: bool,
}

// rte_ethdev.h defines these with the function-like RTE_BIT64 macro, which
// bindgen does not expand.
const RX_OFFLOAD_IPV4_CKSUM: u64 = 1 << 1;
const RX_OFFLOAD_UDP_CKSUM: u64 = 1 << 2;
const RX_OFFLOAD_TCP_CKSUM: u64 = 1 << 3;
const RX_OFFLOAD_TIMESTAMP: u64 = 1 << 14;
const TX_OFFLOAD_UDP_CKSUM: u64 = 1 << 2;
const TX_OFFLOAD_TCP_CKSUM: u64 = 1 << 3;
/// RTE_ETH_TX_OFFLOAD_MULTI_SEGS (rte_ethdev.h:1643). The ENA PMD forces it into
/// txmode at configure (ena_ethdev.c:2645). It only lets the PMD accept chained
/// TX mbufs; Port transmits single-segment mbufs, so it is the one forced bit
/// readback tolerates.
const TX_OFFLOAD_MULTI_SEGS: u64 = 1 << 15;

impl Offloads {
    fn rx_flags(self) -> u64 {
        let checksum = if self.rx_checksum {
            RX_OFFLOAD_IPV4_CKSUM | RX_OFFLOAD_UDP_CKSUM | RX_OFFLOAD_TCP_CKSUM
        } else {
            0
        };
        let timestamp = if self.rx_timestamp {
            RX_OFFLOAD_TIMESTAMP
        } else {
            0
        };
        checksum | timestamp
    }

    fn tx_flags(self) -> u64 {
        let tcp = if self.tx_tcp_checksum {
            TX_OFFLOAD_TCP_CKSUM
        } else {
            0
        };
        let udp = if self.tx_udp_checksum {
            TX_OFFLOAD_UDP_CKSUM
        } else {
            0
        };
        tcp | udp
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortError {
    OwnerMismatch {
        expected: u32,
        actual: u32,
    },
    InvalidSettings(&'static str),
    Pool {
        direction: &'static str,
        error: PoolCreateError,
    },
    Dpdk {
        operation: &'static str,
        code: i32,
    },
    OutstandingBuffers {
        rx: u32,
        tx: u32,
        frame: u32,
    },
    /// Setup did not complete or cleanup has begun; no packet transfers.
    NotReady,
}

impl fmt::Display for PortError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for PortError {}

#[derive(Debug)]
pub struct PortOpenError {
    pub cause: PortError,
    pub rollback_error: Option<PortError>,
    pub retained: Option<Port>,
}

impl From<PortError> for PortOpenError {
    fn from(cause: PortError) -> Self {
        Self {
            cause,
            rollback_error: None,
            retained: None,
        }
    }
}

impl fmt::Display for PortOpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "port setup failed: {}", self.cause)?;
        if let Some(error) = self.rollback_error {
            write!(f, "; rollback failed: {error}")?;
        }
        Ok(())
    }
}

impl std::error::Error for PortOpenError {}

/// Outcome of one `Port::rx_burst`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RxBurst {
    /// Packets appended to the caller's queue, in arrival order.
    pub accepted: usize,
    /// Packets released by standard DPDK free: unsupported, or (with the RX
    /// checksum profile on) a bad IPv4 header checksum reported by the NIC.
    pub rejected: usize,
}

/// Restricted packet owner: one direct, single-segment mbuf with `refcnt == 1`,
/// no external buffer and `pkt_len == data_len`, from a Zhida production pool.
/// Dropping it returns the mbuf to its pool with the single-segment free; the
/// wrapped `Mbuf`'s chain-walking free never runs.
#[repr(transparent)]
pub struct Packet(ManuallyDrop<Mbuf>);

const _: () = {
    assert!(std::mem::size_of::<Packet>() == std::mem::size_of::<*mut rte_mbuf>());
    assert!(std::mem::align_of::<Packet>() == std::mem::align_of::<*mut rte_mbuf>());
};

impl Packet {
    /// # Safety
    /// `mbuf` meets the restricted invariant above.
    unsafe fn from_restricted(mbuf: Mbuf) -> Self {
        Self(ManuallyDrop::new(mbuf))
    }

    pub fn data(&self) -> &[u8] {
        self.0.data()
    }

    pub fn data_mut(&mut self) -> &mut [u8] {
        // SAFETY: the restricted invariant makes this the only owner of
        // direct, unshared bytes.
        unsafe { self.0.data_mut_force() }
    }

    pub(crate) fn ol_flags(&self) -> u64 {
        // SAFETY: this Packet owns a live mbuf exclusively.
        unsafe { (*self.0.as_ptr()).ol_flags }
    }

    pub(crate) fn as_ptr(&self) -> *mut rte_mbuf {
        self.0.as_ptr()
    }

    /// # Safety
    /// `offset` must be a registered u64 dynamic field, initialized in this mbuf.
    pub(crate) unsafe fn dynfield_u64(&self, offset: usize) -> u64 {
        // SAFETY: the caller supplies an initialized, registered u64 field in
        // this live mbuf; registration guarantees its bounds and u64 alignment.
        unsafe { self.as_ptr().cast::<u8>().add(offset).cast::<u64>().read() }
    }

    pub(crate) fn into_rx_buf(self) -> RxBuf {
        let len = usize::from(self.0.len());
        let room = usize::from(self.0.tailroom());
        // SAFETY: Packet owns this live mbuf. The pointer comes from buf_addr, not
        // from a data_len slice, so it is valid for the tailroom as well.
        let data = NonNull::new(unsafe { (*self.0.as_ptr()).data_ptr() }).unwrap();
        let owner = NonNull::new(self.0.as_ptr()).unwrap().cast();
        std::mem::forget(self);
        // SAFETY: the restricted invariant (direct, single-segment, refcnt 1,
        // no external buffer) gives Packet exclusive ownership of the bytes and
        // tailroom. Forgetting Packet transfers its storage to RxBuf, whose
        // callback returns it to the pool once on the owner thread.
        unsafe { RxBuf::from_raw_parts(owner, release_packet, data, len, room) }
    }

    /// Shrink to exactly `len` bytes: `data_len == pkt_len == len`, still one
    /// segment. Panics, changing nothing, if `len` exceeds the current length.
    pub(crate) fn truncate(&mut self, len: usize) {
        let current = usize::from(self.0.len());
        assert!(len <= current, "truncate to {len} bytes exceeds packet length {current}");
        // SAFETY: the restricted invariant (one direct segment, refcnt 1,
        // pkt_len == data_len) makes the head the only and last segment, owned
        // only by this Packet; the assert keeps the new length within it.
        unsafe {
            let head = &mut *self.0.as_ptr();
            head.set_data_len(len as u16);
            head.set_pkt_len(len as u32);
        }
    }
}

impl Drop for Packet {
    fn drop(&mut self) {
        let owner = NonNull::new(self.0.as_ptr()).unwrap().cast();
        // SAFETY: this Packet owns a live mbuf with the restricted invariant.
        unsafe { release_packet(owner) };
    }
}

/// One single-segment TX mbuf the Device has queued for the PMD, from the TX
/// pool or the frame pool. A frame-pool mbuf may also be held by a TCP send
/// queue (refcount 2). Dropping it releases this one reference with the
/// refcount-aware single-segment free; `tx_burst` hands accepted ones to the PMD.
#[repr(transparent)]
pub struct TxPacket(NonNull<rte_mbuf>);

const _: () = {
    assert!(std::mem::size_of::<TxPacket>() == std::mem::size_of::<*mut rte_mbuf>());
    assert!(std::mem::align_of::<TxPacket>() == std::mem::align_of::<*mut rte_mbuf>());
};

impl TxPacket {
    /// # Safety
    /// The caller transfers one reference to `raw`, a live direct,
    /// single-segment mbuf from a production or frame pool of this owner thread.
    pub(crate) unsafe fn from_raw(raw: NonNull<rte_mbuf>) -> Self {
        Self(raw)
    }

    pub(crate) fn as_ptr(&self) -> *mut rte_mbuf {
        self.0.as_ptr()
    }

    /// The first segment's bytes. Read-only: a frame-pool packet may be shared.
    pub fn data(&self) -> &[u8] {
        // SAFETY: this reference keeps the mbuf live; bytes are only read here.
        unsafe {
            let m = &*self.as_ptr();
            std::slice::from_raw_parts(m.data_ptr(), usize::from(m.data_len()))
        }
    }
}

impl From<Packet> for TxPacket {
    fn from(packet: Packet) -> Self {
        let packet = ManuallyDrop::new(packet);
        // SAFETY: a Packet always holds a live mbuf, so the pointer is non-null.
        // The restricted invariant is a direct, single-segment mbuf with one
        // reference, which Packet gives up here (its drop never runs).
        unsafe { Self::from_raw(NonNull::new_unchecked(packet.as_ptr())) }
    }
}

impl Drop for TxPacket {
    fn drop(&mut self) {
        // SAFETY: this value owns one reference to a direct, single-segment
        // mbuf whose pool's cache slot is set; it returns with its last reference.
        unsafe { zhida_pktmbuf_free_seg(self.as_ptr()) };
    }
}

unsafe fn release_packet(owner: NonNull<()>) {
    let raw = owner.cast::<rte_mbuf>().as_ptr();
    debug_assert!({
        // SAFETY: the caller owns this live mbuf with the restricted invariant.
        let m = unsafe { &*raw };
        m.refcnt() == 1
            && m.nb_segs() == 1
            && m.next().is_null()
            && m.ol_flags & (RTE_MBUF_F_INDIRECT | RTE_MBUF_F_EXTERNAL) == 0
    });
    // SAFETY: the restricted invariant is the minimal free's precondition
    // (refcnt 1, one segment, no next, direct, no external buffer); the
    // mbuf's pool is a production pool with its cache slot set, on this
    // owner thread. TX preparation changes only offload metadata and bytes.
    unsafe { zhida_pktmbuf_free_minimal(raw) };
}

#[derive(Clone, Copy)]
pub(crate) struct RxTimestampField {
    pub(crate) offset: usize,
    pub(crate) flag: u64,
}

fn register_rx_timestamp() -> Result<RxTimestampField, PortError> {
    let mut offset = 0;
    let mut flag = 0;
    // SAFETY: EAL is initialized on the owner thread; both output pointers
    // refer to writable values for the duration of registration.
    let code = unsafe { rte_mbuf_dyn_rx_timestamp_register(&mut offset, &mut flag) };
    if code == -1 {
        // SAFETY: read this thread's rte_errno immediately after registration failed.
        checked("rte_mbuf_dyn_rx_timestamp_register", unsafe { -zhida_errno() })?;
    }
    Ok(RxTimestampField {
        // Successful registration returns a nonnegative mbuf byte offset.
        offset: offset as usize,
        flag,
    })
}

/// Device resets `Port::recover` completed in this process.
static PORT_RESETS: AtomicU64 = AtomicU64::new(0);

/// Device resets completed in this process (`Stack::turn` recovers a Port whose PMD raised
/// `RTE_ETH_EVENT_INTR_RESET`). A NIC clock read across a reset may not continue the clock.
pub fn port_resets() -> u64 {
    PORT_RESETS.load(Ordering::Relaxed)
}

/// Held by `read_clock`, and by `Port::recover` and Port close around the device reset and
/// close: ENA's reset and close free its PHC, which `rte_eth_read_clock` reads unlocked.
static CLOCK: Mutex<()> = Mutex::new(());

fn clock_lock() -> MutexGuard<'static, ()> {
    // Guards no data: a panic while holding it leaves nothing inconsistent.
    CLOCK.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The port's NIC clock (ENA: the PHC, ns) through `rte_eth_read_clock`, or its negative
/// errno. Any thread; never concurrent with a device reset or close of a Port (it takes the
/// lock they hold). The receive and send paths never take that lock.
pub fn read_clock(port_id: u16) -> Result<u64, i32> {
    let _clock = clock_lock();
    let mut clock = 0;
    // SAFETY: `clock` is writable; an invalid port returns -ENODEV, and the lock excludes the
    // reset and close that free the device's clock state.
    match unsafe { rte_eth_read_clock(port_id, &mut clock) } {
        0 => Ok(clock),
        error => Err(error),
    }
}

/// `RTE_ETH_EVENT_INTR_RESET` callback: only records the event; `Stack::turn` recovers.
unsafe extern "C" fn on_reset(
    _port_id: u16,
    _event: rte_eth_event_type,
    flag: *mut c_void,
    _ret_param: *mut c_void,
) -> c_int {
    // SAFETY: `flag` is the Port's boxed AtomicBool, registered until before the box drops.
    unsafe { &*flag.cast::<AtomicBool>() }.store(true, Ordering::Relaxed);
    0
}

/// What `Port::setup` asks of the ethdev, at open and again at each `recover`.
#[derive(Clone, Copy)]
struct QueueSetup {
    mtu: u16,
    offloads: Offloads,
    rx_descriptors: u16,
    tx_descriptors: u16,
    queue_socket: i32,
}

impl From<&PortConfig<'_>> for QueueSetup {
    fn from(config: &PortConfig<'_>) -> Self {
        Self {
            mtu: config.mtu,
            offloads: config.offloads,
            rx_descriptors: config.rx_descriptors,
            tx_descriptors: config.tx_descriptors,
            queue_socket: config.queue_socket,
        }
    }
}

/// Null slots after a TX burst's packet pointers. ENA's `eth_ena_xmit_pkts` prefetches
/// `tx_pkts[i + 4]` (masked by its ring size, not the burst) after each packet it sends
/// (DPDK 25.11.3 drivers/net/ena/ena_ethdev.c:3438), so a full burst of n reads up to index
/// n + 3. `tx_scratch` holds `tx_burst + TX_LOOKAHEAD` pointers, the tail always null.
const TX_LOOKAHEAD: usize = 4;

/// One exclusively controlled ethdev and its separate RX/TX pool backing.
pub struct Port {
    port_id: u16,
    owner_lcore: u32,
    mtu: u16,
    rx_checksum: bool,
    rx_timestamp: Option<RxTimestampField>,
    tx_tcp_checksum: bool,
    tx_udp_checksum: bool,
    /// TX data room after headroom: the largest `alloc_tx` length.
    tx_room: u16,
    descriptors: (u16, u16),
    /// Whether the RX pool is low; see `rx_pool_low`.
    rx_pressure: RxPressure,
    rx: ManuallyDrop<PoolResources>,
    tx: ManuallyDrop<PoolResources>,
    /// This Port's clone of the frame pool; other clones may outlive the Port.
    frame: ManuallyDrop<Rc<FramePool>>,
    /// The frame pool's mempool (every frame's `ImmutableFrame::ctx`), kept here so the publish
    /// check reads one field.
    frame_origin: NonNull<()>,
    rx_scratch: Box<[*mut rte_mbuf]>,
    tx_scratch: Box<[*mut rte_mbuf]>,
    queue_setup: QueueSetup,
    /// Set by `on_reset`; its address is the callback's argument while `reset_registered`, so
    /// it is freed only after unregistering (and leaked if that never succeeded).
    reset: ManuallyDrop<Box<AtomicBool>>,
    reset_registered: bool,
    ready: bool,
    stopped: bool,
    closed: bool,
    close_error: Option<PortError>,
    released: bool,
    cleanup_attempted: bool,
}

impl fmt::Debug for Port {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Port")
            .field("port_id", &self.port_id)
            .field("descriptors", &self.descriptors)
            .field("ready", &self.ready)
            .field("stopped", &self.stopped)
            .field("closed", &self.closed)
            .field("close_error", &self.close_error)
            .finish_non_exhaustive()
    }
}

impl Port {
    /// Configure and start one queue pair with exactly the requested offloads.
    /// A requested offload the PMD does not support fails before pool
    /// acquisition; a configuration that does not read back exactly the
    /// requested set fails setup.
    ///
    /// Before both pools are acquired, errors leave the ethdev with the caller.
    /// After acquisition, setup owns the ethdev and rolls it back on failure.
    /// Failed rollback returns its actual retained owner in `PortOpenError`.
    ///
    /// # Safety
    /// The ethdev must be newly created, never configured or started, and under
    /// exclusive control. EAL, packet operations and cleanup stay on its owner
    /// thread. Preserve the production pools' direct/unique/single-segment
    /// contract and the frame pool's (`PoolManager::new_shared`).
    /// Caller-owned vdevs and attached rings must outlive device close;
    /// their queued packets must return before pool destruction. The PMD must
    /// honor the requested packet profile and discharge its buffers/DMA on close.
    /// The PMD, and for net_ring whoever writes the attached RX ring, must supply
    /// only valid DPDK mbufs with valid links/refcounts, each transferring one reference.
    /// TX acceptance must report at most the offered count and take the front of the
    /// offered array.
    /// EAL and retained backing survive until all obligations are discharged.
    // Setup path; the error size does not matter.
    #[allow(clippy::result_large_err)]
    pub unsafe fn open(eal: &Eal, config: PortConfig<'_>) -> Result<Self, PortOpenError> {
        check_owner(eal.owner_lcore_id())?;
        if config.rx_descriptors == 0 || config.tx_descriptors == 0 {
            return Err(PortError::InvalidSettings("descriptor counts must be positive").into());
        }
        if config.rx_burst == 0 || config.tx_burst == 0 {
            return Err(PortError::InvalidSettings("burst sizes must be positive").into());
        }
        let mut info: rte_eth_dev_info = std::mem::zeroed();
        checked(
            "rte_eth_dev_info_get",
            rte_eth_dev_info_get(config.port_id, &mut info),
        )?;
        if info.max_rx_queues == 0 || info.max_tx_queues == 0 {
            return Err(PortError::InvalidSettings("one RX and TX queue are required").into());
        }
        if config.mtu < info.min_mtu || config.mtu > info.max_mtu {
            return Err(PortError::InvalidSettings("MTU is outside device limits").into());
        }
        let rx_offloads = config.offloads.rx_flags() & !RX_OFFLOAD_TIMESTAMP;
        if info.rx_offload_capa & rx_offloads != rx_offloads {
            return Err(PortError::InvalidSettings(
                "PMD lacks the requested RX IPv4/TCP/UDP checksum offload",
            )
            .into());
        }
        if config.offloads.tx_tcp_checksum && info.tx_offload_capa & TX_OFFLOAD_TCP_CKSUM == 0 {
            return Err(PortError::InvalidSettings(
                "PMD lacks the requested TX TCP checksum offload",
            )
            .into());
        }
        if config.offloads.tx_udp_checksum && info.tx_offload_capa & TX_OFFLOAD_UDP_CKSUM == 0 {
            return Err(PortError::InvalidSettings(
                "PMD lacks the requested TX UDP checksum offload",
            )
            .into());
        }
        if config.offloads.rx_timestamp && info.rx_offload_capa & RX_OFFLOAD_TIMESTAMP == 0 {
            return Err(PortError::InvalidSettings(
                "PMD lacks the requested RX timestamp offload",
            )
            .into());
        }
        let rx_timestamp = if config.offloads.rx_timestamp {
            Some(register_rx_timestamp()?)
        } else {
            None
        };
        let frame = u64::from(config.mtu) + 14;
        let headroom = u64::from(RTE_PKTMBUF_HEADROOM);
        let required_rx_room = (u64::from(config.mtu) + 26).div_ceil(1024) * 1024;
        if u64::from(config.rx_pool.data_room_size) < headroom + required_rx_room {
            return Err(PortError::InvalidSettings(
                "RX usable room must be at least ceil1024(MTU + 26) for single-segment RX",
            )
            .into());
        }
        if u64::from(config.rx_pool.data_room_size)
            < headroom + frame.max(u64::from(info.min_rx_bufsize))
            || u64::from(config.tx_pool.data_room_size) < headroom + frame
        {
            return Err(PortError::InvalidSettings("pool room cannot hold the frame").into());
        }
        if u64::from(config.frame_pool.data_room_size) < headroom + frame {
            return Err(PortError::InvalidSettings("frame pool room cannot hold the frame").into());
        }
        let rx = PoolResources::new(config.rx_pool).map_err(|error| PortError::Pool {
            direction: "RX",
            error,
        })?;
        let tx = PoolResources::new(config.tx_pool).map_err(|error| PortError::Pool {
            direction: "TX",
            error,
        })?;
        // The largest UDP payload; `data_room_size` >= headroom + frame was checked above.
        let frame_room = (config.mtu + 14).saturating_sub(42);
        let frame_pool =
            FramePool::new(config.frame_pool, frame_room).map_err(|error| PortError::Pool {
                direction: "frame",
                error,
            })?;
        let mut port = Self {
            port_id: config.port_id,
            owner_lcore: eal.owner_lcore_id(),
            mtu: config.mtu,
            rx_checksum: config.offloads.rx_checksum,
            rx_timestamp,
            tx_tcp_checksum: config.offloads.tx_tcp_checksum,
            tx_udp_checksum: config.offloads.tx_udp_checksum,
            // `data_room_size` >= headroom + frame was checked above.
            tx_room: config.tx_pool.data_room_size - RTE_PKTMBUF_HEADROOM as u16,
            descriptors: (config.rx_descriptors, config.tx_descriptors),
            rx_pressure: RxPressure::new(config.rx_descriptors, config.rx_burst),
            rx: ManuallyDrop::new(rx),
            tx: ManuallyDrop::new(tx),
            frame_origin: NonNull::new(frame_pool.mempool()).expect("a frame pool has a mempool").cast(),
            frame: ManuallyDrop::new(Rc::new(frame_pool)),
            rx_scratch: vec![null_mut(); usize::from(config.rx_burst)].into_boxed_slice(),
            tx_scratch: vec![null_mut(); usize::from(config.tx_burst) + TX_LOOKAHEAD]
                .into_boxed_slice(),
            queue_setup: QueueSetup::from(&config),
            reset: ManuallyDrop::new(Box::new(AtomicBool::new(false))),
            reset_registered: false,
            ready: false,
            stopped: false,
            closed: false,
            close_error: None,
            released: false,
            cleanup_attempted: false,
        };
        if let Err(cause) = port.register_reset().and_then(|()| port.setup()) {
            port.cleanup_attempted = true;
            return Err(match port.cleanup() {
                Ok(()) => cause.into(),
                Err(error) => PortOpenError {
                    cause,
                    rollback_error: Some(error),
                    retained: Some(port),
                },
            });
        }
        port.ready = true;
        Ok(port)
    }

    fn register_reset(&mut self) -> Result<(), PortError> {
        let flag: *const AtomicBool = &**self.reset;
        // SAFETY: the callback only stores to `flag`, which stays allocated until after
        // `cleanup` unregisters it (or forever).
        checked("rte_eth_dev_callback_register", unsafe {
            rte_eth_dev_callback_register(
                self.port_id,
                rte_eth_event_type_RTE_ETH_EVENT_INTR_RESET,
                Some(on_reset),
                flag.cast_mut().cast(),
            )
        })?;
        self.reset_registered = true;
        Ok(())
    }

    unsafe fn setup(&mut self) -> Result<(), PortError> {
        let config = self.queue_setup;
        let mut conf: rte_eth_conf = std::mem::zeroed();
        conf.rxmode.mtu = u32::from(config.mtu);
        conf.rxmode.offloads = config.offloads.rx_flags();
        conf.txmode.offloads = config.offloads.tx_flags();
        checked(
            "rte_eth_dev_configure",
            rte_eth_dev_configure(self.port_id, 1, 1, &conf),
        )?;
        checked(
            "rte_eth_dev_conf_get",
            rte_eth_dev_conf_get(self.port_id, &mut conf),
        )?;
        check_configuration(config.mtu, config.offloads, &conf)?;
        let mut info: rte_eth_dev_info = std::mem::zeroed();
        checked(
            "rte_eth_dev_info_get",
            rte_eth_dev_info_get(self.port_id, &mut info),
        )?;
        let (mut rx_desc, mut tx_desc) = (config.rx_descriptors, config.tx_descriptors);
        checked(
            "rte_eth_dev_adjust_nb_rx_tx_desc",
            rte_eth_dev_adjust_nb_rx_tx_desc(self.port_id, &mut rx_desc, &mut tx_desc),
        )?;
        for (count, lim) in [(rx_desc, info.rx_desc_lim), (tx_desc, info.tx_desc_lim)] {
            if count == 0
                || lim.nb_align == 0
                || count < lim.nb_min
                || count > lim.nb_max
                || count % lim.nb_align != 0
            {
                return Err(PortError::InvalidSettings(
                    "invalid adjusted descriptor count",
                ));
            }
        }
        self.descriptors = (rx_desc, tx_desc);
        self.rx_pressure = RxPressure::new(rx_desc, self.rx_burst_size());
        let mut rxconf = info.default_rxconf;
        rxconf.offloads = 0;
        rxconf.rx_deferred_start = 0;
        rxconf.rx_nseg = 0;
        rxconf.rx_seg = null_mut();
        rxconf.rx_nmempool = 0;
        rxconf.rx_mempools = null_mut();
        rxconf.share_group = 0;
        rxconf.share_qid = 0;
        rxconf.reserved_64s = [0; 2];
        rxconf.reserved_ptrs = [null_mut(); 2];
        checked(
            "rte_eth_rx_queue_setup",
            rte_eth_rx_queue_setup(
                self.port_id,
                0,
                rx_desc,
                config.queue_socket as u32,
                &rxconf,
                self.rx.pool().pool(),
            ),
        )?;
        let mut rxq_info: rte_eth_rxq_info = std::mem::zeroed();
        let code = rx_queue_info_get(self.port_id, 0, &mut rxq_info);
        if code != -(ENOTSUP as i32) {
            checked("rte_eth_rx_queue_info_get", code)?;
            if rxq_info.scattered_rx != 0 {
                return Err(PortError::InvalidSettings(
                    "PMD enabled scattered RX; only single-segment RX is supported",
                ));
            }
        }
        let mut txconf = info.default_txconf;
        txconf.offloads = 0;
        txconf.tx_deferred_start = 0;
        txconf.reserved_64s = [0; 2];
        txconf.reserved_ptrs = [null_mut(); 2];
        #[cfg(not(test))]
        let tx_queue = 0;
        #[cfg(test)]
        let tx_queue = tests::tx_queue();
        checked(
            "rte_eth_tx_queue_setup",
            rte_eth_tx_queue_setup(
                self.port_id,
                tx_queue,
                tx_desc,
                config.queue_socket as u32,
                &txconf,
            ),
        )?;
        checked("rte_eth_dev_start", start(self.port_id))
    }

    /// Whether the PMD raised `RTE_ETH_EVENT_INTR_RESET` since the last `recover`.
    #[inline]
    pub(crate) fn reset_pending(&self) -> bool {
        self.reset.load(Ordering::Relaxed)
    }

    /// Sets the flag `RTE_ETH_EVENT_INTR_RESET` sets.
    #[cfg(feature = "testing")]
    pub(crate) fn raise_reset(&self) {
        self.reset.store(true, Ordering::Relaxed);
    }

    /// After `RTE_ETH_EVENT_INTR_RESET`: reset the device (`rte_eth_dev_reset`, or
    /// `rte_eth_dev_stop` on a PMD without reset), then configure, set up both queues and
    /// start again as `open` did, holding the clock lock (`read_clock`) throughout. Packets
    /// the PMD held are back in their pools: posted RX buffers and unsent TX frames. Staged
    /// and pending packets are the caller's and stay valid.
    ///
    /// Fails, leaving the Port not ready for cleanup, if a step fails or the negotiated
    /// descriptor counts change (the Stack sized its pools from them).
    pub(crate) fn recover(&mut self) -> Result<(), PortError> {
        self.ready = false;
        check_owner(self.owner_lcore)?;
        // Cleared first: an event raised from here on asks for another recovery.
        self.reset.store(false, Ordering::Relaxed);
        let _clock = clock_lock();
        // SAFETY: the owner thread, between bursts; the ethdev stays open.
        let code = unsafe { rte_eth_dev_reset(self.port_id) };
        if code == -(ENOTSUP as i32) {
            checked("rte_eth_dev_stop", unsafe { stop(self.port_id) })?;
        } else {
            checked("rte_eth_dev_reset", code)?;
        }
        let descriptors = self.descriptors;
        // SAFETY: the ethdev is stopped and stays under this Port's exclusive control.
        unsafe { self.setup() }?;
        if self.descriptors != descriptors {
            return Err(PortError::InvalidSettings(
                "device reset changed the negotiated descriptor counts",
            ));
        }
        self.ready = true;
        PORT_RESETS.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Stop/close and release backing, or return the same owner for cleanup.
    /// A released-ID close failure permanently retains backing resources.
    // Teardown path; the error size does not matter.
    #[allow(clippy::result_large_err)]
    pub fn try_close(mut self) -> Result<(), (Self, PortError)> {
        self.cleanup_attempted = true;
        match self.cleanup() {
            Ok(()) => Ok(()),
            Err(error) => Err((self, error)),
        }
    }

    /// Descriptor counts negotiated for the RX and TX queues.
    pub fn descriptor_counts(&self) -> (u16, u16) {
        self.descriptors
    }

    pub(crate) fn rx_pool_count(&self) -> usize {
        // SAFETY: this Port owns the live RX pool; its object count is fixed.
        usize::try_from(unsafe { (*self.rx.pool().pool()).size })
            .expect("RX pool count exceeds usize")
    }

    pub(crate) fn rx_pool_cache_size(&self) -> u32 {
        // SAFETY: this Port owns the live RX pool; its cache size is fixed.
        unsafe { (*self.rx.pool().pool()).cache_size }
    }

    /// Packets requested from the PMD per RX burst at most: `PortConfig::rx_burst`.
    pub(crate) fn rx_burst_size(&self) -> u16 {
        u16::try_from(self.rx_scratch.len()).expect("rx_burst is a u16")
    }

    /// After an RX burst, whether the RX mbufs this lcore can allocate (the RX pool's backing
    /// store plus this lcore's cache, which the PMD refills from) fall short of the low mark
    /// (`rx_low_mark`). A zero `received` count forces a fresh reading; otherwise the last reading
    /// may already rule out a low pool (`RxPressure`).
    #[inline]
    pub(crate) fn rx_pool_low(&mut self, received: usize) -> bool {
        if received != 0 && self.rx_pressure.covers(received) {
            return false;
        }
        self.read_rx_pool_low()
    }

    #[cold]
    #[inline(never)]
    fn read_rx_pool_low(&mut self) -> bool {
        self.rx_pressure.read(self.rx.pool().free_count() as usize)
    }

    pub(crate) fn tx_pool_count(&self) -> usize {
        // SAFETY: this Port owns the live TX pool; its object count is fixed.
        usize::try_from(unsafe { (*self.tx.pool().pool()).size })
            .expect("TX pool count exceeds usize")
    }

    pub(crate) fn frame_pool(&self) -> &FramePool {
        &self.frame
    }

    /// The frame pool's mempool: every frame's `ImmutableFrame::ctx`.
    pub(crate) fn frame_origin(&self) -> NonNull<()> {
        self.frame_origin
    }

    /// An allocator handle over the frame pool for one TCP socket. Each handle
    /// keeps the pool's backing alive, so a socket outliving this Port still
    /// releases its frames into a live pool.
    pub(crate) fn frame_source(&self) -> TxSource {
        TxSource::new(Rc::clone(&self.frame))
    }

    pub(crate) fn rx_checksum_enabled(&self) -> bool {
        self.rx_checksum
    }

    pub(crate) fn rx_timestamp_field(&self) -> Option<RxTimestampField> {
        self.rx_timestamp
    }

    pub(crate) fn port_id(&self) -> u16 {
        self.port_id
    }

    pub(crate) fn tx_tcp_checksum(&self) -> bool {
        self.tx_tcp_checksum
    }

    pub(crate) fn tx_udp_checksum(&self) -> bool {
        self.tx_udp_checksum
    }

    /// Bench and test code uses this. It turns on the TX UDP checksum profile
    /// without the PMD advertising it; on a PMD without the offload, frames
    /// leave with UDP checksum 0.
    #[cfg(any(test, feature = "bench-offload"))]
    #[doc(hidden)]
    pub fn force_tx_udp_checksum(&mut self) {
        self.tx_udp_checksum = true;
    }

    /// Bench and test code uses this. It turns on the TX TCP checksum profile
    /// without the PMD advertising it; on a PMD without the offload, frames
    /// leave with TCP checksum 0.
    #[cfg(any(test, feature = "bench-offload"))]
    #[doc(hidden)]
    pub fn force_tx_tcp_checksum(&mut self) {
        self.tx_tcp_checksum = true;
    }

    /// Largest Ethernet frame without FCS: IP MTU + 14. `open` validated that
    /// the TX data room holds it, so this always fits `alloc_tx`.
    pub(crate) fn frame_capacity(&self) -> u16 {
        self.mtu + 14
    }

    /// Configured `tx_burst`: the most packets one `tx_burst` call offers.
    pub(crate) fn tx_burst_size(&self) -> usize {
        self.tx_scratch.len() - TX_LOOKAHEAD
    }

    /// Allocate a TX packet with `len` data bytes; `None` if `len` exceeds the
    /// room after headroom, or if the TX pool is exhausted and stays so after
    /// one `rte_eth_tx_done_cleanup` of TX queue 0. The cleanup lets a NIC PMD
    /// return sent mbufs it still holds: some (e.g. ixgbe) reclaim completed
    /// descriptors only inside TX burst, and `tx_burst` with nothing pending
    /// does not call the PMD. `-ENOTSUP` returns `None`. memif copy TX frees
    /// accepted mbufs, but some NIC PMDs report `-ENOTSUP` while
    /// holding sent mbufs (DPDK 25.11.3 ixgbe vector TX), so a TX pool such a
    /// NIC can hold entirely may stall; Stack::new refuses a pool too small
    /// for the negotiated TX descriptor count and its reservations. That
    /// descriptor bound holds for NIC PMDs. Test PMDs such as net_ring queue
    /// sent mbufs into an attached ring, ignore the descriptor count and are
    /// bounded by the ring's capacity instead. An empty TX pool also blocks
    /// receive, because Device::receive takes a TX reservation first.
    /// A Port that is not ready (retained after a failed open or
    /// a refused close) allocates from its pool but never calls the cleanup:
    /// its queue may not exist.
    ///
    /// Panics, with port id, queue and code, if the cleanup of a ready Port
    /// fails with any other error: queue 0 was set up at open, so that is a
    /// broken invariant.
    pub fn alloc_tx(&self, len: u16) -> Option<Packet> {
        if len > self.tx_room {
            return None;
        }
        // SAFETY (both allocations): a fresh production-pool mbuf: reset to one
        // direct segment, refcnt 1, `pkt_len == data_len == len`.
        if let Some(mbuf) = self.tx.pool().alloc_with_len(len) {
            return Some(unsafe { Packet::from_restricted(mbuf) });
        }
        if !self.ready {
            return None;
        }
        // free_cnt 0: reclaim every completed descriptor.
        let code = unsafe { tx_done_cleanup(self.port_id, 0, 0) };
        if code == -(ENOTSUP as i32) {
            return None;
        }
        assert!(
            code >= 0,
            "rte_eth_tx_done_cleanup(port {}, queue 0) failed: {code}",
            self.port_id
        );
        let mbuf = self.tx.pool().alloc_with_len(len)?;
        Some(unsafe { Packet::from_restricted(mbuf) })
    }

    /// Receive from RX queue 0 into the spare capacity of `staged`, so `staged`
    /// never reallocates.
    ///
    /// The PMD request is min(spare capacity of `staged`, configured `rx_burst`);
    /// with no spare capacity the PMD is not called.
    /// Some vector PMDs floor requests to a multiple of their per-loop count
    /// (e.g. 4) and return nothing below it, so callers keeping small spare
    /// capacity may see short or empty bursts; the hardware burst contract is not
    /// yet settled.
    ///
    /// Imports only mbufs from this port's RX pool that are direct, single-
    /// segment, `refcnt == 1`, without external buffer and with valid lengths,
    /// appending them in arrival order. Every other packet is released with
    /// standard DPDK free before any Rust owner exists and is counted.
    pub fn rx_burst(&mut self, staged: &mut VecDeque<Packet>) -> Result<RxBurst, PortError> {
        if !self.ready {
            return Err(PortError::NotReady);
        }
        let count = (staged.capacity() - staged.len()).min(self.rx_scratch.len());
        if count == 0 {
            return Ok(RxBurst { accepted: 0, rejected: 0 });
        }
        // `count` fits: the scratch length came from a u16.
        let received =
            unsafe { eth_rx_burst(self.port_id, 0, self.rx_scratch.as_mut_ptr(), count as u16) };
        let pool = self.rx.pool().pool();
        let mut rejected = 0;
        for &raw in &self.rx_scratch[..usize::from(received)] {
            if self.rx_checksum {
                let flags = unsafe { (*raw).ol_flags };
                let ip_bad = flags & RTE_MBUF_F_RX_IP_CKSUM_MASK == RTE_MBUF_F_RX_IP_CKSUM_BAD;
                if ip_bad {
                    unsafe { dpdk_raw_pktmbuf_free(raw) };
                    rejected += 1;
                    continue;
                }
            }
            if importable(unsafe { &*raw }, pool) {
                // Within the spare capacity measured above: no reallocation.
                staged.push_back(unsafe {
                    Packet::from_restricted(Mbuf::from_raw(NonNull::new_unchecked(raw)))
                });
            } else {
                unsafe { dpdk_raw_pktmbuf_free(raw) };
                rejected += 1;
            }
        }
        Ok(RxBurst {
            accepted: usize::from(received) - rejected,
            rejected,
        })
    }

    /// Offer up to `tx_burst` packets from the front of `pending` to TX queue 0.
    ///
    /// Returns the accepted count. The PMD owns that prefix, which leaves
    /// `pending`; the unaccepted suffix stays in `pending` in its original order.
    /// An empty `pending` returns 0 without calling the PMD: memif's copy-mode
    /// TX in DPDK 25.11.3 reads the first packet pointer even for an empty burst.
    pub fn tx_burst(&mut self, pending: &mut VecDeque<TxPacket>) -> Result<usize, PortError> {
        if !self.ready {
            return Err(PortError::NotReady);
        }
        if pending.is_empty() {
            return Ok(0);
        }
        let count = pending.len().min(self.tx_burst_size());
        for (slot, packet) in self.tx_scratch[..count].iter_mut().zip(pending.iter()) {
            *slot = packet.as_ptr();
        }
        // `count` fits: it is at most the configured u16 `tx_burst`.
        let accepted =
            unsafe { eth_tx_burst(self.port_id, 0, self.tx_scratch.as_mut_ptr(), count as u16) };
        // The PMD may already have freed the accepted prefix. Disarm those owners
        // by moving their pointer values out of `pending`: no mbuf access, no
        // allocation, nothing that can unwind.
        // A PMD reporting more than offered breaks its contract; never forget
        // packets it was not offered.
        let disarmed = usize::from(accepted).min(count);
        for packet in pending.drain(..disarmed) {
            std::mem::forget(packet);
        }
        Ok(disarmed)
    }

    fn cleanup(&mut self) -> Result<(), PortError> {
        self.ready = false;
        check_owner(self.owner_lcore)?;
        if self.released {
            return Ok(());
        }
        if let Some(error) = self.close_error {
            return Err(error);
        }
        if !self.closed {
            if self.reset_registered {
                let flag: *const AtomicBool = &**self.reset;
                // SAFETY: the registration `register_reset` made with these arguments.
                checked("rte_eth_dev_callback_unregister", unsafe {
                    rte_eth_dev_callback_unregister(
                        self.port_id,
                        rte_eth_event_type_RTE_ETH_EVENT_INTR_RESET,
                        Some(on_reset),
                        flag.cast_mut().cast(),
                    )
                })?;
                self.reset_registered = false;
            }
            if !self.stopped {
                checked("rte_eth_dev_stop", unsafe { stop(self.port_id) })?;
                self.stopped = true;
            }
            let _clock = clock_lock();
            let result = unsafe { close(self.port_id) };
            self.closed = unsafe { rte_eth_dev_is_valid_port(self.port_id) } == 0;
            if let Err(error) = checked("rte_eth_dev_close", result) {
                if self.closed {
                    self.close_error = Some(error);
                }
                return Err(error);
            }
        }
        let (rx, tx, frame) = (
            self.rx.pool().in_use_count(),
            self.tx.pool().in_use_count(),
            self.frame.in_use_count(),
        );
        if rx != 0 || tx != 0 || frame != 0 {
            return Err(PortError::OutstandingBuffers { rx, tx, frame });
        }
        self.released = true;
        unsafe {
            ManuallyDrop::drop(&mut self.rx);
            ManuallyDrop::drop(&mut self.tx);
            ManuallyDrop::drop(&mut self.frame);
        }
        Ok(())
    }
}

impl Drop for Port {
    fn drop(&mut self) {
        if !self.cleanup_attempted {
            self.cleanup_attempted = true;
            if let Err(error) = self.cleanup() {
                use std::io::Write;
                let _ = writeln!(std::io::stderr(), "Zhida retained port backing: {error}");
            }
        } else if !self.released {
            use std::io::Write;
            let _ = writeln!(
                std::io::stderr(),
                "Zhida retained port backing after refused cleanup"
            );
        }
        if !self.reset_registered {
            // SAFETY: no callback registration refers to the flag any more; dropped once.
            unsafe { ManuallyDrop::drop(&mut self.reset) };
        }
    }
}

fn check_owner(expected: u32) -> Result<(), PortError> {
    let actual = unsafe { zhida_lcore_id() };
    if actual != expected {
        return Err(PortError::OwnerMismatch { expected, actual });
    }
    Ok(())
}

fn check_configuration(mtu: u16, offloads: Offloads, conf: &rte_eth_conf) -> Result<(), PortError> {
    if conf.rxmode.mtu != u32::from(mtu) {
        return Err(PortError::InvalidSettings("PMD changed the requested MTU"));
    }
    for (requested, actual, tolerated) in [
        (offloads.rx_flags(), conf.rxmode.offloads, 0),
        (offloads.tx_flags(), conf.txmode.offloads, TX_OFFLOAD_MULTI_SEGS),
    ] {
        if actual & !(requested | tolerated) != 0 {
            return Err(PortError::InvalidSettings("PMD enabled unrequested offloads"));
        }
        if requested & !actual != 0 {
            return Err(PortError::InvalidSettings("PMD dropped requested offloads"));
        }
    }
    Ok(())
}

/// The most RX mbufs a driver holds posted for receive: max(RX descriptors, 32). A NIC posts
/// at most one per descriptor; memif copy mode posts none between receives.
fn rx_posted(rx_descriptors: u16) -> usize {
    usize::from(rx_descriptors.max(32))
}

/// Whether the RX pool is below its low mark, reading its free count only when needed.
///
/// Only the driver takes RX mbufs from the pool, inside its RX burst: buffers it posts, at most
/// `rx_posted`, and one per packet it returns. So the free count can fall by at most `posted`
/// plus the packets received since a reading. A reading of `free` therefore rules out "below
/// the mark" for the next `free - low_mark - posted` received packets, and no reading is needed
/// until they are used up: the answer is the one an exact reading would give.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RxPressure {
    low_mark: usize,
    posted: usize,
    /// Packets that may still be received before the pool could fall below the low mark.
    unchecked: usize,
}

impl RxPressure {
    fn new(rx_descriptors: u16, rx_burst: u16) -> Self {
        Self { low_mark: rx_low_mark(rx_descriptors, rx_burst), posted: rx_posted(rx_descriptors), unchecked: 0 }
    }

    /// Count `received` packets against the last reading; `true` while it still rules out low.
    #[inline]
    fn covers(&mut self, received: usize) -> bool {
        if received <= self.unchecked {
            self.unchecked -= received;
            true
        } else {
            false
        }
    }

    /// Take a reading of `free` mbufs: whether the pool is low now.
    fn read(&mut self, free: usize) -> bool {
        self.unchecked = free.saturating_sub(self.low_mark + self.posted);
        free < self.low_mark
    }
}

fn importable(m: &rte_mbuf, rx_pool: *mut rte_mempool) -> bool {
    m.pool == rx_pool
        && m.ol_flags & (RTE_MBUF_F_INDIRECT | RTE_MBUF_F_EXTERNAL) == 0
        && m.nb_segs() == 1
        && m.next().is_null()
        && m.refcnt() == 1
        && u32::from(m.data_off()) + u32::from(m.data_len()) <= u32::from(m.buf_len())
        && m.pkt_len() == u32::from(m.data_len())
}

fn checked(operation: &'static str, code: i32) -> Result<(), PortError> {
    if code != 0 {
        return Err(PortError::Dpdk { operation, code });
    }
    Ok(())
}

#[cfg(test)]
pub(crate) use tests::eth_tx_prepare;
#[cfg(test)]
use tests::{close, eth_rx_burst, eth_tx_burst, rx_queue_info_get, start, stop, tx_done_cleanup};
#[cfg(not(test))]
pub(crate) use zhida_dpdk_sys::dpdk_raw_eth_tx_prepare as eth_tx_prepare;
#[cfg(not(test))]
use zhida_dpdk_sys::{
    dpdk_raw_eth_rx_burst as eth_rx_burst, dpdk_raw_eth_tx_burst as eth_tx_burst,
    rte_eth_dev_close as close, rte_eth_dev_start as start, rte_eth_dev_stop as stop,
    rte_eth_rx_queue_info_get as rx_queue_info_get, rte_eth_tx_done_cleanup as tx_done_cleanup,
};
#[cfg(test)]
#[path = "port_tests.rs"]
pub(crate) mod tests;
