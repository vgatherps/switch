// Heads up! Before working on this file you should read, at least,
// the parts of RFC 1122 that discuss ARP.

use core::cell::Cell;

use crate::time::{Duration, Instant};
use crate::wire::{HardwareAddress, IpAddress, Ipv4Address};

/// A cached neighbor.
///
/// A neighbor mapping translates from a protocol address to a hardware address,
/// and contains the timestamp past which the mapping should be discarded.
#[derive(Debug, Clone)]
pub struct Neighbor {
    hardware_addr: HardwareAddress,
    expires_at: Instant,
    silent_until: Instant,
    stale_until: Cell<Option<Instant>>,
}

/// An answer to a neighbor cache lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Answer {
    /// The neighbor address is in the cache and not expired.
    Found(HardwareAddress),
    Stale {
        hardware_addr: HardwareAddress,
        probe: bool,
    },
    /// The neighbor address is not in the cache, or has expired.
    NotFound,
    /// The neighbor address is not in the cache, or has expired,
    /// and a lookup has been made recently.
    RateLimited,
}

impl Answer {
    /// Returns whether a valid address was found.
    pub(crate) fn found(&self) -> bool {
        match self {
            Answer::Found(_) => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
enum Entry {
    Resolved(Neighbor),
    Pending { silent_until: Instant },
}

impl Entry {
    fn expires_at(&self) -> Instant {
        match self {
            Self::Resolved(neighbor) => neighbor.expires_at,
            Self::Pending { silent_until } => *silent_until,
        }
    }
}

/// A neighbor cache holds up to `capacity` neighbors. When full, a new neighbor
/// evicts the entry that expires first; among entries expiring at the same instant,
/// the lowest address goes.
#[derive(Debug)]
pub struct Cache {
    /// Entries by IPv4 address as an integer (`key`). Sized like `SlotMap` (iface/mod.rs), for
    /// twice `capacity`, so the remove and insert of an eviction never force it to grow.
    storage: std::collections::HashMap<u32, Entry, rustc_hash::FxBuildHasher>,
    capacity: usize,
    changes: u64,
    next_expiry: Option<Instant>,
}

impl Cache {
    /// Minimum delay between discovery requests, in milliseconds.
    pub(crate) const SILENT_TIME: Duration = Duration::from_millis(1_000);

    pub(crate) const STALE_TIME: Duration = Duration::from_micros(
        Self::SILENT_TIME
            .total_micros()
            .checked_mul(3)
            .expect("neighbor stale duration overflow"),
    );

    /// Neighbor entry lifetime, in milliseconds.
    pub(crate) const ENTRY_LIFETIME: Duration = Duration::from_millis(60_000);

    /// Create a cache holding up to `capacity` neighbors.
    ///
    /// # Panics
    /// Panics if `capacity` is 0.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity != 0, "Neighbor capacity must be nonzero.");
        Self {
            storage: std::collections::HashMap::with_capacity_and_hasher(
                capacity.checked_mul(2).expect("neighbor capacity overflow"),
                rustc_hash::FxBuildHasher,
            ),
            capacity,
            changes: 0,
            next_expiry: None,
        }
    }

    /// The entry that expires first; among those, the lowest address (an integer key orders like
    /// the address). Scans the whole cache, so only a full cache calls it.
    fn earliest(&self) -> u32 {
        *self
            .storage
            .iter()
            .min_by_key(|&(key, entry)| (entry.expires_at(), *key))
            .expect("empty neighbor cache storage")
            .0
    }

    fn pending_eviction_is_silent(&self, timestamp: Instant) -> bool {
        self.storage.len() == self.capacity
            && matches!(
                self.storage[&self.earliest()],
                Entry::Pending { silent_until } if timestamp < silent_until
            )
    }

    fn evict_earliest(&mut self) -> (Ipv4Address, Entry) {
        let key = self.earliest();
        let removed = self.storage.remove(&key).expect("earliest neighbor is missing");
        if matches!(removed, Entry::Resolved(_)) {
            self.bump();
        }
        (Ipv4Address::from(key), removed)
    }

    #[inline(always)]
    pub fn reset_expiry_if_existing(
        &mut self,
        protocol_addr: IpAddress,
        source_hardware_addr: HardwareAddress,
        timestamp: Instant,
    ) {
        if let Some(Entry::Resolved(neighbor)) = self.storage.get_mut(&key(&protocol_addr)) {
            if source_hardware_addr == neighbor.hardware_addr {
                let expired = timestamp >= neighbor.expires_at;
                neighbor.expires_at = timestamp + Self::ENTRY_LIFETIME;
                neighbor.stale_until.set(None);
                {
                    self.next_expiry = Some(
                        self.next_expiry
                            .map_or(neighbor.expires_at, |old| old.min(neighbor.expires_at)),
                    );
                    if expired {
                        self.bump();
                    }
                }
            }
        }
    }

    pub fn update_existing(
        &mut self,
        protocol_addr: IpAddress,
        hardware_addr: HardwareAddress,
        timestamp: Instant,
    ) {
        if let Some(entry) = self.storage.get(&key(&protocol_addr)) {
            if matches!(
                entry,
                Entry::Resolved(neighbor) if neighbor.hardware_addr == hardware_addr
            ) {
                return;
            }
            self.fill(protocol_addr, hardware_addr, timestamp);
        }
    }

    /// Learn `protocol_addr` at `hardware_addr`. A refill of a live entry with the same MAC only
    /// moves its expiry: templates and cached next hops stay valid, so `changes` is not bumped.
    /// A new entry, a changed MAC, or a refill after expiry bumps it.
    pub fn fill(
        &mut self,
        protocol_addr: IpAddress,
        hardware_addr: HardwareAddress,
        timestamp: Instant,
    ) {
        debug_assert!(protocol_addr.is_unicast());
        debug_assert!(hardware_addr.is_unicast());

        let expires_at = timestamp + Self::ENTRY_LIFETIME;
        if let Some(entry) = self.storage.get_mut(&key(&protocol_addr))
            && let Entry::Resolved(neighbor) = entry
            && neighbor.hardware_addr == hardware_addr
            && timestamp < neighbor.expires_at
        {
            *entry = Entry::Resolved(Neighbor {
                silent_until: expires_at,
                stale_until: Cell::new(None),
                expires_at,
                hardware_addr,
            });
            self.next_expiry = Some(
                self.next_expiry
                    .map_or(expires_at, |old| old.min(expires_at)),
            );
            return;
        }
        self.fill_with_expiration(protocol_addr, hardware_addr, expires_at);
    }

    pub fn fill_with_expiration(
        &mut self,
        protocol_addr: IpAddress,
        hardware_addr: HardwareAddress,
        expires_at: Instant,
    ) {
        debug_assert!(protocol_addr.is_unicast());
        debug_assert!(hardware_addr.is_unicast());

        {
            self.bump();
            self.next_expiry = Some(
                self.next_expiry
                    .map_or(expires_at, |old| old.min(expires_at)),
            );
        }
        let neighbor = Entry::Resolved(Neighbor {
            silent_until: expires_at,
            stale_until: Cell::new(None),
            expires_at,
            hardware_addr,
        });
        let key = key(&protocol_addr);
        match self.storage.get_mut(&key) {
            Some(entry) => {
                let old_neighbor = core::mem::replace(entry, neighbor);
                if let Entry::Resolved(old_neighbor) = old_neighbor
                    && old_neighbor.hardware_addr != hardware_addr
                {
                    net_trace!(
                        "replaced {} => {} (was {})",
                        protocol_addr,
                        hardware_addr,
                        old_neighbor.hardware_addr
                    );
                }
                return;
            }
            None => {}
        }
        if self.storage.len() == self.capacity {
            let (old_protocol_addr, _old_neighbor) = self.evict_earliest();
            net_trace!(
                "filled {} => {} (evicted {} => {:?})",
                protocol_addr,
                hardware_addr,
                old_protocol_addr,
                _old_neighbor
            );
        } else {
            net_trace!("filled {} => {} (was empty)", protocol_addr, hardware_addr);
        }
        self.storage.insert(key, neighbor);
    }

    pub(crate) fn lookup(&self, protocol_addr: &IpAddress, timestamp: Instant) -> Answer {
        assert!(protocol_addr.is_unicast());

        if let Some(entry) = self.storage.get(&key(protocol_addr)) {
            match entry {
                Entry::Resolved(neighbor) => {
                    if timestamp < neighbor.expires_at {
                        return Answer::Found(neighbor.hardware_addr);
                    }
                    let stale_until = neighbor.stale_until.get().unwrap_or_else(|| {
                        let stale_until = Instant::from_micros(
                            timestamp
                                .total_micros()
                                .checked_add(Self::STALE_TIME.total_micros() as i64)
                                .expect("neighbor stale expiry overflow"),
                        );
                        neighbor.stale_until.set(Some(stale_until));
                        stale_until
                    });
                    if timestamp < stale_until {
                        return Answer::Stale {
                            hardware_addr: neighbor.hardware_addr,
                            probe: timestamp >= neighbor.silent_until,
                        };
                    }
                    if timestamp < neighbor.silent_until {
                        return Answer::RateLimited;
                    }
                }
                Entry::Pending { silent_until } if timestamp < *silent_until => {
                    return Answer::RateLimited;
                }
                _ => (),
            }
            return Answer::NotFound;
        }
        if self.pending_eviction_is_silent(timestamp) {
            return Answer::RateLimited;
        }
        Answer::NotFound
    }

    pub(crate) fn limit_rate(&mut self, protocol_addr: IpAddress, timestamp: Instant) {
        debug_assert!(protocol_addr.is_unicast());

        let silent_until = Instant::from_micros(
            timestamp
                .total_micros()
                .checked_add(Self::SILENT_TIME.total_micros() as i64)
                .expect("neighbor silence overflow"),
        );
        let key = key(&protocol_addr);
        match self.storage.get_mut(&key) {
            Some(entry) => {
                if let Entry::Resolved(neighbor) = entry {
                    neighbor.silent_until = silent_until;
                    return;
                }
                *entry = Entry::Pending { silent_until };
            }
            None => {
                if self.pending_eviction_is_silent(timestamp) {
                    return;
                }
                if self.storage.len() == self.capacity {
                    self.evict_earliest();
                }
                self.storage.insert(key, Entry::Pending { silent_until });
            }
        }
    }

    pub(crate) fn flush(&mut self) {
        self.storage.clear();
        {
            self.bump();
            self.next_expiry = None;
        }
    }

    pub(crate) fn bump(&mut self) {
        self.changes = self
            .changes
            .checked_add(1)
            .expect("neighbor changes overflow");
    }

    pub(crate) fn changes(&self) -> u64 {
        self.changes
    }

    pub(crate) fn expire(&mut self, timestamp: Instant) {
        let Some(old) = self.next_expiry else {
            return;
        };
        if timestamp < old {
            return;
        }
        let mut expired = false;
        let mut next_expiry: Option<Instant> = None;
        for entry in self.storage.values() {
            let Entry::Resolved(neighbor) = entry else {
                continue;
            };
            if neighbor.expires_at > timestamp {
                next_expiry = Some(
                    next_expiry.map_or(neighbor.expires_at, |old| old.min(neighbor.expires_at)),
                );
            } else if neighbor.expires_at >= old {
                expired = true;
            }
        }
        if expired {
            self.bump();
        }
        self.next_expiry = next_expiry;
    }
}

/// `addr` as the cache's map key.
#[inline]
fn key(addr: &IpAddress) -> u32 {
    let IpAddress::Ipv4(addr) = addr;
    u32::from(*addr)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::wire::ipv4::test::{MOCK_IP_ADDR_1, MOCK_IP_ADDR_2, MOCK_IP_ADDR_3, MOCK_IP_ADDR_4};

    use crate::wire::EthernetAddress;

    const HADDR_A: HardwareAddress = HardwareAddress::Ethernet(EthernetAddress([0, 0, 0, 0, 0, 1]));
    const HADDR_B: HardwareAddress = HardwareAddress::Ethernet(EthernetAddress([0, 0, 0, 0, 0, 2]));
    const HADDR_C: HardwareAddress = HardwareAddress::Ethernet(EthernetAddress([0, 0, 0, 0, 0, 3]));
    const HADDR_D: HardwareAddress = HardwareAddress::Ethernet(EthernetAddress([0, 0, 0, 0, 0, 4]));

    #[test]
    fn update_existing_absent() {
        let mut cache = Cache::new(3);

        cache.update_existing(MOCK_IP_ADDR_1.into(), HADDR_B, Instant::ZERO);
        assert_eq!(
            cache.lookup(&MOCK_IP_ADDR_1.into(), Instant::ZERO),
            Answer::NotFound
        );
        {
            assert_eq!(cache.storage.len(), 0);
            assert_eq!(cache.changes(), 0);
        }
    }

    #[test]
    fn update_existing_pending() {
        let mut cache = Cache::new(3);
        cache.limit_rate(MOCK_IP_ADDR_1.into(), Instant::ZERO);
        assert_eq!(
            cache.lookup(&MOCK_IP_ADDR_1.into(), Instant::ZERO),
            Answer::RateLimited
        );
        let changes = cache.changes();
        let now = Instant::from_millis(100);
        cache.update_existing(MOCK_IP_ADDR_1.into(), HADDR_B, now);
        assert_eq!(
            cache.lookup(&MOCK_IP_ADDR_1.into(), now),
            Answer::Found(HADDR_B)
        );
        assert_eq!(cache.changes(), changes + 1);
        let Entry::Resolved(neighbor) = &cache.storage[&key(&MOCK_IP_ADDR_1.into())] else {
            panic!("neighbor remains pending");
        };
        assert_eq!(neighbor.expires_at, now + Cache::ENTRY_LIFETIME);
        assert_eq!(neighbor.silent_until, neighbor.expires_at);
        assert_eq!(cache.storage.len(), 1);
    }

    #[test]
    fn update_existing_different_mac() {
        let mut cache = Cache::new(3);

        cache.fill(MOCK_IP_ADDR_1.into(), HADDR_A, Instant::ZERO);
        let changes = cache.changes();
        let now = Instant::from_millis(100);
        cache.update_existing(MOCK_IP_ADDR_1.into(), HADDR_B, now);
        assert_eq!(
            cache.lookup(&MOCK_IP_ADDR_1.into(), now),
            Answer::Found(HADDR_B)
        );
        assert_eq!(
            cache.lookup(&MOCK_IP_ADDR_1.into(), Instant::ZERO + Cache::ENTRY_LIFETIME),
            Answer::Found(HADDR_B)
        );
        assert_eq!(cache.changes(), changes + 1);
    }

    #[test]
    fn update_existing_same_mac() {
        let mut cache = Cache::new(3);

        cache.fill(MOCK_IP_ADDR_1.into(), HADDR_A, Instant::ZERO);
        let changes = cache.changes();
        let now = Instant::from_millis(10_000);
        cache.update_existing(MOCK_IP_ADDR_1.into(), HADDR_A, now);
        assert_eq!(
            cache.lookup(&MOCK_IP_ADDR_1.into(), now),
            Answer::Found(HADDR_A)
        );
        {
            assert_eq!(cache.changes(), changes);
            let Entry::Resolved(neighbor) = &cache.storage[&key(&MOCK_IP_ADDR_1.into())] else {
                panic!("resolved neighbor became pending");
            };
            assert_eq!(neighbor.expires_at, Instant::ZERO + Cache::ENTRY_LIFETIME);
            assert_eq!(neighbor.silent_until, neighbor.expires_at);
        }
    }

    #[test]
    fn test_fill() {
        let mut cache = Cache::new(3);

        assert!(
            !cache
                .lookup(&MOCK_IP_ADDR_1.into(), Instant::from_millis(0))
                .found()
        );
        assert!(
            !cache
                .lookup(&MOCK_IP_ADDR_2.into(), Instant::from_millis(0))
                .found()
        );

        cache.fill(MOCK_IP_ADDR_1.into(), HADDR_A, Instant::from_millis(0));
        assert_eq!(
            cache.lookup(&MOCK_IP_ADDR_1.into(), Instant::from_millis(0)),
            Answer::Found(HADDR_A)
        );
        assert!(
            !cache
                .lookup(&MOCK_IP_ADDR_2.into(), Instant::from_millis(0))
                .found()
        );
        assert!(
            !cache
                .lookup(
                    &MOCK_IP_ADDR_1.into(),
                    Instant::from_millis(0) + Cache::ENTRY_LIFETIME * 2
                )
                .found(),
        );

        cache.fill(MOCK_IP_ADDR_1.into(), HADDR_A, Instant::from_millis(0));
        assert!(
            !cache
                .lookup(&MOCK_IP_ADDR_2.into(), Instant::from_millis(0))
                .found()
        );
    }

    #[test]
    fn test_expire() {
        let mut cache = Cache::new(3);

        cache.fill(MOCK_IP_ADDR_1.into(), HADDR_A, Instant::from_millis(0));
        assert_eq!(
            cache.lookup(&MOCK_IP_ADDR_1.into(), Instant::from_millis(0)),
            Answer::Found(HADDR_A)
        );
        assert!(
            !cache
                .lookup(
                    &MOCK_IP_ADDR_1.into(),
                    Instant::from_millis(0) + Cache::ENTRY_LIFETIME * 2
                )
                .found(),
        );
    }

    #[test]
    fn test_replace() {
        let mut cache = Cache::new(3);

        cache.fill(MOCK_IP_ADDR_1.into(), HADDR_A, Instant::from_millis(0));
        assert_eq!(
            cache.lookup(&MOCK_IP_ADDR_1.into(), Instant::from_millis(0)),
            Answer::Found(HADDR_A)
        );
        cache.fill(MOCK_IP_ADDR_1.into(), HADDR_B, Instant::from_millis(0));
        assert_eq!(
            cache.lookup(&MOCK_IP_ADDR_1.into(), Instant::from_millis(0)),
            Answer::Found(HADDR_B)
        );
    }

    #[test]
    fn test_evict() {
        let mut cache = Cache::new(3);

        cache.fill(MOCK_IP_ADDR_1.into(), HADDR_A, Instant::from_millis(100));
        cache.fill(MOCK_IP_ADDR_2.into(), HADDR_B, Instant::from_millis(50));
        cache.fill(MOCK_IP_ADDR_3.into(), HADDR_C, Instant::from_millis(200));
        assert_eq!(
            cache.lookup(&MOCK_IP_ADDR_2.into(), Instant::from_millis(1000)),
            Answer::Found(HADDR_B)
        );
        assert!(
            !cache
                .lookup(&MOCK_IP_ADDR_4.into(), Instant::from_millis(1000))
                .found()
        );

        cache.fill(MOCK_IP_ADDR_4.into(), HADDR_D, Instant::from_millis(300));
        assert!(
            !cache
                .lookup(&MOCK_IP_ADDR_2.into(), Instant::from_millis(1000))
                .found()
        );
        assert_eq!(
            cache.lookup(&MOCK_IP_ADDR_4.into(), Instant::from_millis(1000)),
            Answer::Found(HADDR_D)
        );
    }

    #[test]
    fn test_hush() {
        let mut cache = Cache::new(3);

        assert_eq!(
            cache.lookup(&MOCK_IP_ADDR_1.into(), Instant::from_millis(0)),
            Answer::NotFound
        );

        cache.limit_rate(MOCK_IP_ADDR_1.into(), Instant::from_millis(0));
        assert_eq!(
            cache.lookup(&MOCK_IP_ADDR_1.into(), Instant::from_millis(100)),
            Answer::RateLimited
        );
        assert_eq!(
            cache.lookup(&MOCK_IP_ADDR_1.into(), Instant::from_millis(2000)),
            Answer::NotFound
        );
    }

    #[test]
    fn test_flush() {
        let mut cache = Cache::new(3);

        cache.fill(MOCK_IP_ADDR_1.into(), HADDR_A, Instant::from_millis(0));
        assert_eq!(
            cache.lookup(&MOCK_IP_ADDR_1.into(), Instant::from_millis(0)),
            Answer::Found(HADDR_A)
        );
        assert!(
            !cache
                .lookup(&MOCK_IP_ADDR_2.into(), Instant::from_millis(0))
                .found()
        );

        cache.flush();
        assert!(
            !cache
                .lookup(&MOCK_IP_ADDR_1.into(), Instant::from_millis(0))
                .found()
        );
        assert!(
            !cache
                .lookup(&MOCK_IP_ADDR_1.into(), Instant::from_millis(0))
                .found()
        );
    }

    mod alloc_tests {
        use super::*;

        fn address(last: u8) -> IpAddress {
            return crate::wire::Ipv4Address::new(192, 0, 2, last).into();
        }

        #[test]
        fn idle_stale_window_starts_at_lookup() {
            let mut cache = Cache::new(1);
            cache.fill(address(1), HADDR_A, Instant::ZERO);
            cache.expire(Instant::from_secs(60));
            cache.expire(Instant::from_secs(70));
            for millis in [70_000, 72_999] {
                assert_eq!(
                    cache.lookup(&address(1), Instant::from_millis(millis)),
                    Answer::Stale {
                        hardware_addr: HADDR_A,
                        probe: true,
                    }
                );
            }
            assert_eq!(
                cache.lookup(&address(1), Instant::from_micros(73_000_001)),
                Answer::NotFound
            );
        }

        #[test]
        fn idle_stale_window_refresh() {
            for refill in [false, true] {
                let mut cache = Cache::new(1);
                cache.fill(address(1), HADDR_A, Instant::ZERO);
                let now = Instant::from_secs(70);
                assert_eq!(
                    cache.lookup(&address(1), now),
                    Answer::Stale {
                        hardware_addr: HADDR_A,
                        probe: true,
                    }
                );
                cache.limit_rate(address(1), now);
                let refresh = Instant::from_millis(70_500);
                if refill {
                    cache.fill(address(1), HADDR_A, refresh);
                } else {
                    cache.reset_expiry_if_existing(address(1), HADDR_A, refresh);
                }
                assert_eq!(cache.lookup(&address(1), refresh), Answer::Found(HADDR_A));
                assert_eq!(
                    cache.lookup(&address(1), Instant::from_millis(130_499)),
                    Answer::Found(HADDR_A)
                );
                assert_eq!(
                    cache.lookup(&address(1), Instant::from_secs(140)),
                    Answer::Stale {
                        hardware_addr: HADDR_A,
                        probe: true,
                    }
                );
                assert_eq!(
                    cache.lookup(&address(1), Instant::from_secs(143)),
                    Answer::NotFound
                );
            }
        }

        #[test]
        fn stale_window_answers() {
            let mut cache = Cache::new(1);
            cache.fill(address(1), HADDR_A, Instant::ZERO);
            let fresh = cache.lookup(&address(1), Instant::from_millis(59_999));
            assert_eq!(fresh, Answer::Found(HADDR_A));
            assert!(fresh.found());
            for millis in [60_000, 60_001, 62_999] {
                let stale = cache.lookup(&address(1), Instant::from_millis(millis));
                assert_eq!(
                    stale,
                    Answer::Stale {
                        hardware_addr: HADDR_A,
                        probe: true
                    }
                );
                assert!(!stale.found());
            }
            assert_eq!(Cache::STALE_TIME, Duration::from_millis(3_000));
        }

        #[test]
        fn stale_probe_once_per_silence() {
            let mut cache = Cache::new(2);
            cache.fill(address(1), HADDR_A, Instant::ZERO);
            cache.fill(address(2), HADDR_B, Instant::ZERO);
            cache.expire(Instant::from_millis(60_000));
            let changes = cache.changes();
            for millis in [60_000, 61_000, 62_000] {
                let now = Instant::from_millis(millis);
                assert_eq!(
                    cache.lookup(&address(1), now),
                    Answer::Stale {
                        hardware_addr: HADDR_A,
                        probe: true,
                    }
                );
                cache.limit_rate(address(1), now);
                for offset in [0, 500, 999] {
                    assert_eq!(
                        cache.lookup(&address(1), Instant::from_millis(millis + offset)),
                        Answer::Stale {
                            hardware_addr: HADDR_A,
                            probe: false
                        }
                    );
                }
                assert_eq!(
                    cache.lookup(&address(2), now),
                    Answer::Stale {
                        hardware_addr: HADDR_B,
                        probe: true,
                    }
                );
                assert_eq!(cache.changes(), changes);
                assert_eq!(cache.storage.len(), 2);
                assert_eq!(cache.next_expiry, None);
            }
        }

        #[test]
        fn fill_makes_stale_fresh() {
            let mut cache = Cache::new(1);
            cache.fill(address(1), HADDR_A, Instant::ZERO);
            cache.limit_rate(address(1), Instant::from_millis(60_000));
            assert_eq!(
                cache.lookup(&address(1), Instant::from_millis(60_500)),
                Answer::Stale {
                    hardware_addr: HADDR_A,
                    probe: false
                }
            );
            cache.fill(address(1), HADDR_B, Instant::from_millis(60_500));
            assert_eq!(
                cache.lookup(&address(1), Instant::from_millis(60_500)),
                Answer::Found(HADDR_B)
            );
            assert_eq!(
                cache.lookup(&address(1), Instant::from_millis(120_499)),
                Answer::Found(HADDR_B)
            );
            assert_eq!(
                cache.lookup(&address(1), Instant::from_millis(120_500)),
                Answer::Stale {
                    hardware_addr: HADDR_B,
                    probe: true
                }
            );
        }

        #[test]
        fn after_stale_time_missing() {
            let mut cache = Cache::new(1);
            cache.fill(address(1), HADDR_A, Instant::ZERO);
            assert_eq!(
                cache.lookup(&address(1), Instant::from_millis(60_000)),
                Answer::Stale {
                    hardware_addr: HADDR_A,
                    probe: true,
                }
            );
            cache.limit_rate(address(1), Instant::from_millis(62_999));
            for millis in [63_000, 63_500, 64_000] {
                let answer = cache.lookup(&address(1), Instant::from_millis(millis));
                assert_eq!(
                    answer,
                    if millis < 63_999 {
                        Answer::RateLimited
                    } else {
                        Answer::NotFound
                    }
                );
                assert!(!answer.found());
            }
        }

        #[test]
        fn reset_expiry_refreshes_stale() {
            let mut cache = Cache::new(1);
            cache.fill(address(1), HADDR_A, Instant::ZERO);
            let now = Instant::from_millis(60_500);
            cache.limit_rate(address(1), now);
            cache.reset_expiry_if_existing(address(1), HADDR_B, now);
            assert_eq!(
                cache.lookup(&address(1), now),
                Answer::Stale {
                    hardware_addr: HADDR_A,
                    probe: false
                }
            );
            let changes = cache.changes();
            cache.reset_expiry_if_existing(address(1), HADDR_A, now);
            assert_eq!(cache.lookup(&address(1), now), Answer::Found(HADDR_A));
            assert_eq!(
                cache.lookup(&address(1), Instant::from_millis(120_499)),
                Answer::Found(HADDR_A)
            );
            assert_eq!(
                cache.lookup(&address(1), Instant::from_millis(120_500)),
                Answer::Stale {
                    hardware_addr: HADDR_A,
                    probe: true
                }
            );
            assert_eq!(cache.changes(), changes + 1);
        }

        #[test]
        fn silence_is_per_address() {
            let mut cache = Cache::new(3);
            cache.limit_rate(address(1), Instant::ZERO);
            let now = Instant::from_millis(100);
            assert_eq!(cache.lookup(&address(1), now), Answer::RateLimited);
            assert_eq!(cache.lookup(&address(2), now), Answer::NotFound);
            cache.limit_rate(address(2), now);
            assert_eq!(cache.lookup(&address(1), now), Answer::RateLimited);
            assert_eq!(cache.lookup(&address(2), now), Answer::RateLimited);
            assert_eq!(cache.lookup(&address(3), now), Answer::NotFound);
        }

        #[test]
        fn pending_entry_expires_after_silent_time() {
            let mut cache = Cache::new(1);
            cache.limit_rate(address(1), Instant::ZERO);
            assert_eq!(
                cache.lookup(&address(1), Instant::from_millis(999)),
                Answer::RateLimited
            );
            assert_eq!(
                cache.lookup(&address(1), Instant::from_millis(1_000)),
                Answer::NotFound
            );
            cache.limit_rate(address(1), Instant::from_millis(1_000));
            assert_eq!(
                cache.lookup(&address(1), Instant::from_millis(1_999)),
                Answer::RateLimited
            );
            assert_eq!(
                cache.lookup(&address(1), Instant::from_millis(2_000)),
                Answer::NotFound
            );
            assert_eq!(cache.storage.len(), 1);
        }

        #[test]
        fn fill_resolves_pending() {
            let mut cache = Cache::new(2);
            cache.limit_rate(address(1), Instant::ZERO);
            cache.limit_rate(address(2), Instant::ZERO);
            cache.reset_expiry_if_existing(address(1), HADDR_A, Instant::ZERO);
            assert_eq!(
                cache.lookup(&address(1), Instant::ZERO),
                Answer::RateLimited
            );
            assert_eq!(cache.next_expiry, None);
            cache.fill(address(1), HADDR_A, Instant::from_millis(100));
            assert_eq!(
                cache.lookup(&address(1), Instant::from_millis(100)),
                Answer::Found(HADDR_A)
            );
            assert_eq!(cache.changes(), 1);
            assert_eq!(cache.next_expiry, Some(Instant::from_millis(60_100)));
            cache.fill_with_expiration(address(2), HADDR_B, Instant::from_millis(2_000));
            assert_eq!(
                cache.lookup(&address(2), Instant::from_millis(100)),
                Answer::Found(HADDR_B)
            );
            assert_eq!(cache.changes(), 2);
            assert_eq!(cache.next_expiry, Some(Instant::from_millis(2_000)));
            cache.limit_rate(address(1), Instant::from_millis(200));
            assert_eq!(
                cache.lookup(&address(1), Instant::from_millis(200)),
                Answer::Found(HADDR_A)
            );
            cache.limit_rate(address(2), Instant::from_millis(2_000));
            assert_eq!(
                cache.lookup(&address(2), Instant::from_millis(2_000)),
                Answer::Stale {
                    hardware_addr: HADDR_B,
                    probe: false
                }
            );
            assert_eq!(
                cache.lookup(&address(2), Instant::from_millis(5_000)),
                Answer::NotFound
            );
            assert_eq!(cache.changes(), 2);
            assert_eq!(cache.storage.len(), 2);
        }

        #[test]
        fn pending_eviction_of_resolved_bumps_changes() {
            let mut cache = Cache::new(1);
            cache.fill(address(1), HADDR_A, Instant::ZERO);
            let changes = cache.changes();
            cache.limit_rate(address(2), Instant::from_millis(2));
            assert_eq!(cache.changes(), changes + 1);
            assert!(!cache.storage.contains_key(&key(&address(1))));
            assert_eq!(
                cache.lookup(&address(2), Instant::from_millis(2)),
                Answer::RateLimited
            );
            assert_eq!(cache.storage.len(), 1);
            cache.expire(Instant::from_millis(60_000));
            assert_eq!(cache.changes(), changes + 1);
            assert_eq!(cache.next_expiry, None);
        }

        #[test]
        fn silence_after_stale_window() {
            let mut cache = Cache::new(1);
            cache.fill(address(1), HADDR_A, Instant::ZERO);
            assert_eq!(
                cache.lookup(&address(1), Instant::from_millis(60_000)),
                Answer::Stale {
                    hardware_addr: HADDR_A,
                    probe: true,
                }
            );
            cache.limit_rate(address(1), Instant::from_millis(62_999));
            assert_eq!(
                cache.lookup(&address(1), Instant::from_millis(62_999)),
                Answer::Stale {
                    hardware_addr: HADDR_A,
                    probe: false
                }
            );
            for millis in [63_000, 63_500, 63_998] {
                assert_eq!(
                    cache.lookup(&address(1), Instant::from_millis(millis)),
                    Answer::RateLimited
                );
            }
            assert_eq!(
                cache.lookup(&address(1), Instant::from_millis(63_999)),
                Answer::NotFound
            );
            cache.limit_rate(address(1), Instant::from_millis(63_999));
            assert_eq!(
                cache.lookup(&address(1), Instant::from_millis(64_998)),
                Answer::RateLimited
            );
            assert_eq!(
                cache.lookup(&address(1), Instant::from_millis(64_999)),
                Answer::NotFound
            );
        }

        #[test]
        fn full_cache_of_silent_pending_answers_rate_limited() {
            let mut cache = Cache::new(1);
            cache.limit_rate(address(1), Instant::ZERO);
            for millis in [0, 100, 999] {
                let now = Instant::from_millis(millis);
                assert_eq!(cache.lookup(&address(2), now), Answer::RateLimited);
                cache.limit_rate(address(2), now);
                assert_eq!(cache.lookup(&address(1), now), Answer::RateLimited);
                assert!(!cache.storage.contains_key(&key(&address(2))));
                assert_eq!(cache.storage.len(), 1);
                assert_eq!(cache.changes(), 0);
            }
            let now = Instant::from_millis(1_000);
            assert_eq!(cache.lookup(&address(2), now), Answer::NotFound);
            cache.limit_rate(address(2), now);
            assert!(!cache.storage.contains_key(&key(&address(1))));
            assert_eq!(cache.lookup(&address(2), now), Answer::RateLimited);
            assert_eq!(cache.storage.len(), 1);
            assert_eq!(cache.changes(), 0);
        }

        #[test]
        fn pending_never_evicts_resolved_that_expires_later() {
            let mut cache = Cache::new(2);
            cache.fill(address(1), HADDR_A, Instant::ZERO);
            cache.limit_rate(address(2), Instant::ZERO);
            cache.limit_rate(address(3), Instant::from_millis(100));
            let now = Instant::from_millis(100);
            assert_eq!(cache.lookup(&address(1), now), Answer::Found(HADDR_A));
            assert_eq!(cache.lookup(&address(2), now), Answer::RateLimited);
            assert_eq!(cache.lookup(&address(3), now), Answer::RateLimited);
            cache.fill(address(4), HADDR_D, now);
            assert_eq!(cache.lookup(&address(1), now), Answer::Found(HADDR_A));
            assert_eq!(cache.lookup(&address(3), now), Answer::NotFound);
            assert_eq!(cache.lookup(&address(4), now), Answer::Found(HADDR_D));
            assert_eq!(cache.storage.len(), 2);
        }

        #[test]
        fn pending_does_not_bump_changes() {
            let mut cache = Cache::new(2);
            cache.limit_rate(address(1), Instant::ZERO);
            cache.limit_rate(address(1), Instant::from_millis(100));
            assert_eq!(cache.changes(), 0);
            assert_eq!(cache.next_expiry, None);
            cache.expire(Instant::from_millis(100));
            assert_eq!(cache.changes(), 0);
            assert_eq!(cache.next_expiry, None);
            cache.fill_with_expiration(address(2), HADDR_B, Instant::from_millis(500));
            cache.limit_rate(address(3), Instant::from_millis(200));
            assert_eq!(cache.changes(), 2);
            assert_eq!(cache.next_expiry, Some(Instant::from_millis(500)));
            cache.expire(Instant::from_millis(500));
            assert_eq!(cache.changes(), 2);
            assert_eq!(cache.next_expiry, None);
            cache.fill_with_expiration(address(2), HADDR_B, Instant::from_millis(2_000));
            cache.expire(Instant::from_millis(1_200));
            assert_eq!(cache.changes(), 3);
            assert_eq!(cache.next_expiry, Some(Instant::from_millis(2_000)));
            cache.expire(Instant::from_millis(2_000));
            assert_eq!(cache.changes(), 4);
            assert_eq!(cache.next_expiry, None);
        }

        #[test]
        fn len_exact_with_pending() {
            let mut cache = Cache::new(3);
            let colliding: Vec<IpAddress> = (1..=4).map(address).collect();
            for (index, address) in colliding[..3].iter().enumerate() {
                cache.limit_rate(*address, Instant::from_millis(index as i64));
                assert_eq!(cache.storage.len(), index + 1);
            }
            cache.limit_rate(colliding[1], Instant::from_millis(100));
            cache.fill(colliding[2], HADDR_C, Instant::ZERO);
            assert_eq!(cache.storage.len(), 3);
            cache.lookup(&colliding[2], Instant::ZERO);
            cache.limit_rate(colliding[3], Instant::from_millis(200));
            assert_eq!(cache.storage.len(), 3);
            let now = Instant::from_millis(200);
            assert_eq!(cache.lookup(&colliding[0], now), Answer::RateLimited);
            assert_eq!(cache.lookup(&colliding[1], now), Answer::RateLimited);
            assert_eq!(cache.lookup(&colliding[2], now), Answer::Found(HADDR_C));
            assert_eq!(cache.lookup(&colliding[3], now), Answer::RateLimited);
            cache.expire(Instant::from_millis(1_200));
            assert_eq!(
                cache.lookup(&colliding[1], Instant::from_millis(1_200)),
                Answer::NotFound
            );
            assert_eq!(cache.storage.len(), 3);
            cache.flush();
            assert_eq!(cache.storage.len(), 0);
            assert_eq!(cache.next_expiry, None);
            for address in &colliding {
                assert_eq!(cache.lookup(address, Instant::ZERO), Answer::NotFound);
            }
        }

        #[test]
        fn changes_track_fill_flush_and_expiry() {
            let mut cache = Cache::new(1);
            assert_eq!(cache.changes(), 0);
            cache.fill(address(1), HADDR_A, Instant::ZERO);
            assert_eq!(cache.changes(), 1);
            cache.fill(address(1), HADDR_B, Instant::ZERO);
            assert_eq!(cache.changes(), 2);
            cache.fill(address(2), HADDR_C, Instant::ZERO);
            assert_eq!(cache.changes(), 4);
            cache.expire(Instant::from_millis(59_999));
            assert_eq!(cache.changes(), 4);
            cache.expire(Instant::from_millis(60_000));
            assert_eq!(cache.changes(), 5);
            cache.expire(Instant::from_millis(60_000));
            cache.expire(Instant::from_millis(120_000));
            assert_eq!(cache.changes(), 5);
            cache.flush();
            assert_eq!(cache.changes(), 6);
            cache.expire(Instant::from_millis(180_000));
            assert_eq!(cache.changes(), 6);
        }

        #[test]
        fn expiry_counts_each_scan_once_and_skips_old_entries() {
            let mut cache = Cache::new(3);
            cache.fill(address(1), HADDR_A, Instant::ZERO);
            cache.fill(address(2), HADDR_B, Instant::ZERO);
            cache.fill(address(3), HADDR_C, Instant::from_millis(10_000));
            cache.expire(Instant::from_millis(60_000));
            assert_eq!(cache.changes(), 4);
            cache.expire(Instant::from_millis(69_999));
            assert_eq!(cache.changes(), 4);
            cache.expire(Instant::from_millis(70_000));
            assert_eq!(cache.changes(), 5);
            cache.expire(Instant::from_millis(140_000));
            assert_eq!(cache.changes(), 5);
        }

        #[test]
        fn refresh_reschedules_without_bumping() {
            let mut cache = Cache::new(1);
            cache.fill(address(1), HADDR_A, Instant::ZERO);
            cache.reset_expiry_if_existing(address(1), HADDR_B, Instant::from_millis(1_000));
            assert_eq!(cache.changes(), 1);
            cache.reset_expiry_if_existing(address(1), HADDR_A, Instant::from_millis(10_000));
            assert_eq!(cache.changes(), 1);
            cache.expire(Instant::from_millis(60_000));
            assert_eq!(cache.changes(), 1);
            assert_eq!(cache.next_expiry, Some(Instant::from_millis(70_000)));
            cache.expire(Instant::from_millis(70_000));
            assert_eq!(cache.changes(), 2);
        }

        #[test]
        fn refresh_revives_counted_entry_and_can_shorten_expiry() {
            let mut cache = Cache::new(1);
            cache.fill(address(1), HADDR_A, Instant::ZERO);
            cache.expire(Instant::from_millis(60_000));
            assert_eq!(cache.changes(), 2);
            cache.reset_expiry_if_existing(address(1), HADDR_A, Instant::from_millis(61_000));
            assert_eq!(cache.changes(), 3);
            assert_eq!(cache.next_expiry, Some(Instant::from_millis(121_000)));
            cache.expire(Instant::from_millis(121_000));
            assert_eq!(cache.changes(), 4);
            cache.fill_with_expiration(address(1), HADDR_A, Instant::from_millis(300_000));
            cache.expire(Instant::from_millis(122_000));
            cache.reset_expiry_if_existing(address(1), HADDR_A, Instant::from_millis(123_000));
            assert_eq!(cache.changes(), 5);
            assert_eq!(cache.next_expiry, Some(Instant::from_millis(183_000)));
            cache.expire(Instant::from_millis(183_000));
            assert_eq!(cache.changes(), 6);
            cache.expire(Instant::from_millis(400_000));
            assert_eq!(cache.changes(), 6);
        }

        #[test]
        fn maximum_instant_expiry_is_counted_once() {
            let mut cache = Cache::new(1);
            let maximum = Instant::from_micros(i64::MAX);
            cache.expire(maximum);
            assert_eq!(cache.changes(), 0);
            cache.fill_with_expiration(address(1), HADDR_A, maximum);
            assert_eq!(cache.changes(), 1);
            assert_eq!(cache.next_expiry, Some(maximum));
            cache.expire(maximum);
            assert_eq!(cache.changes(), 2);
            assert_eq!(cache.next_expiry, None);
            cache.expire(maximum);
            assert_eq!(cache.changes(), 2);
        }

        #[test]
        fn eviction_tie_goes_to_lowest_address() {
            let mut cache = Cache::new(3);
            for last in (1..=3).rev() {
                cache.fill(address(last), HADDR_A, Instant::ZERO);
            }
            cache.fill(address(4), HADDR_D, Instant::ZERO);

            assert_eq!(cache.lookup(&address(1), Instant::ZERO), Answer::NotFound);
            for last in 2..=3 {
                assert_eq!(
                    cache.lookup(&address(last), Instant::ZERO),
                    Answer::Found(HADDR_A)
                );
            }
            assert_eq!(
                cache.lookup(&address(4), Instant::ZERO),
                Answer::Found(HADDR_D)
            );
        }

        #[test]
        fn fill_existing_when_full_updates_in_place() {
            let mut cache = Cache::new(3);
            for last in 1..=3 {
                cache.fill(address(last), HADDR_A, Instant::ZERO);
            }
            cache.fill(address(1), HADDR_D, Instant::from_millis(1_000));

            assert!(cache.storage.contains_key(&key(&address(1))));
            assert_eq!(cache.storage.len(), 3);
            assert_eq!(
                cache.lookup(&address(1), Instant::ZERO),
                Answer::Found(HADDR_D)
            );
            for last in 2..=3 {
                assert_eq!(
                    cache.lookup(&address(last), Instant::ZERO),
                    Answer::Found(HADDR_A)
                );
            }
            assert_eq!(
                cache.lookup(&address(1), Instant::from_millis(60_000)),
                Answer::Found(HADDR_D)
            );
            assert_eq!(
                cache.lookup(&address(1), Instant::from_millis(61_000)),
                Answer::Stale {
                    hardware_addr: HADDR_D,
                    probe: true
                }
            );
            assert_eq!(
                cache.lookup(&address(1), Instant::from_millis(64_000)),
                Answer::NotFound
            );
        }

        #[test]
        fn refresh_extends_expiry() {
            let mut cache = Cache::new(3);
            cache.fill(address(1), HADDR_A, Instant::ZERO);
            cache.reset_expiry_if_existing(address(1), HADDR_B, Instant::from_millis(10_000));
            assert_eq!(
                cache.lookup(&address(1), Instant::from_millis(60_000)),
                Answer::Stale {
                    hardware_addr: HADDR_A,
                    probe: true
                }
            );
            assert_eq!(
                cache.lookup(&address(1), Instant::from_millis(63_000)),
                Answer::NotFound
            );
            assert_eq!(cache.storage.len(), 1);

            cache.reset_expiry_if_existing(address(1), HADDR_A, Instant::from_millis(60_000));
            assert_eq!(
                cache.lookup(&address(1), Instant::from_millis(119_999)),
                Answer::Found(HADDR_A)
            );
            assert_eq!(
                cache.lookup(&address(1), Instant::from_millis(120_000)),
                Answer::Stale {
                    hardware_addr: HADDR_A,
                    probe: true
                }
            );
            assert_eq!(
                cache.lookup(&address(1), Instant::from_millis(123_000)),
                Answer::NotFound
            );

            cache.reset_expiry_if_existing(address(2), HADDR_A, Instant::from_millis(60_000));
            assert_eq!(
                cache.lookup(&address(2), Instant::from_millis(60_000)),
                Answer::NotFound
            );
            assert_eq!(cache.storage.len(), 1);
        }

        #[test]
        fn mac_change_replaces_hardware_address() {
            let mut cache = Cache::new(3);
            cache.fill_with_expiration(address(1), HADDR_A, Instant::from_millis(1_000));
            cache.fill_with_expiration(address(1), HADDR_B, Instant::from_millis(2_000));

            assert_eq!(
                cache.lookup(&address(1), Instant::from_millis(1_000)),
                Answer::Found(HADDR_B)
            );
            assert_eq!(
                cache.lookup(&address(1), Instant::from_millis(2_000)),
                Answer::Stale {
                    hardware_addr: HADDR_B,
                    probe: true
                }
            );
            assert_eq!(
                cache.lookup(&address(1), Instant::from_millis(5_000)),
                Answer::NotFound
            );
            assert_eq!(cache.storage.len(), 1);
        }

        /// Eviction removes the earliest entry; every remaining address is still found.
        #[test]
        fn eviction_keeps_remaining_entries_findable() {
            let mut cache = Cache::new(3);
            for last in 1..=3 {
                cache.fill(address(last), HADDR_A, Instant::from_millis(i64::from(last)));
            }
            cache.fill(address(4), HADDR_D, Instant::from_millis(4));
            assert_eq!(cache.lookup(&address(1), Instant::from_millis(10)), Answer::NotFound);
            for last in 2..=3 {
                assert_eq!(
                    cache.lookup(&address(last), Instant::from_millis(10)),
                    Answer::Found(HADDR_A)
                );
            }
            assert_eq!(
                cache.lookup(&address(4), Instant::from_millis(10)),
                Answer::Found(HADDR_D)
            );
        }

        /// A same-MAC refill of a live entry moves its expiry without bumping `changes`; a
        /// changed MAC, or a same-MAC refill once the entry has expired, bumps it.
        #[test]
        fn same_mac_refill_bumps_changes_only_after_expiry() {
            let mut cache = Cache::new(2);
            cache.fill(address(1), HADDR_A, Instant::from_millis(0));
            let changes = cache.changes();
            cache.fill(address(1), HADDR_A, Instant::from_millis(30_000));
            assert_eq!(cache.changes(), changes);
            assert_eq!(
                cache.lookup(&address(1), Instant::from_millis(89_999)),
                Answer::Found(HADDR_A)
            );
            cache.fill(address(1), HADDR_B, Instant::from_millis(70_000));
            assert_eq!(cache.changes(), changes + 1);
            cache.fill(address(1), HADDR_B, Instant::from_millis(130_000));
            assert_eq!(cache.changes(), changes + 2);
            assert_eq!(
                cache.lookup(&address(1), Instant::from_millis(189_999)),
                Answer::Found(HADDR_B)
            );
        }
    }
}
