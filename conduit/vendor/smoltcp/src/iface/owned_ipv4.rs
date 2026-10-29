use alloc::vec::Vec;
use crate::wire::{Ipv4Address, Ipv4Cidr};

/// IPv4 addresses the interface owns besides its own address: single addresses (/32) and
/// prefixes, kept as sorted, disjoint, merged inclusive `[start, end]` ranges. Fixed for the
/// interface's life; looked up by binary search.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OwnedIpv4 {
    ranges: Vec<(u32, u32)>,
}

const MULTICAST: (u32, u32) = (0xe000_0000, 0xefff_ffff);

impl OwnedIpv4 {
    /// Build the set for an interface at `subnet` with default route `gateway`. Duplicate,
    /// overlapping and adjacent entries are merged. Rejects a prefix with host bits set, and an
    /// entry that holds a non-unicast address (0.0.0.0, multicast, 255.255.255.255), the gateway
    /// or `subnet`'s broadcast address.
    pub fn new(
        cidrs: &[Ipv4Cidr],
        subnet: Ipv4Cidr,
        gateway: Option<Ipv4Address>,
    ) -> Result<Self, &'static str> {
        let mut ranges = Vec::with_capacity(cidrs.len());
        for cidr in cidrs {
            if *cidr != cidr.network() {
                return Err("owned IPv4 prefix has host bits set");
            }
            let start = u32::from(cidr.address());
            let end = start | (u32::MAX.checked_shr(u32::from(cidr.prefix_len())).unwrap_or(0));
            let holds = |addr: u32| start <= addr && addr <= end;
            if holds(0) || holds(u32::MAX) || (start <= MULTICAST.1 && MULTICAST.0 <= end) {
                return Err("owned IPv4 addresses must be unicast");
            }
            if gateway.is_some_and(|gateway| holds(u32::from(gateway))) {
                return Err("owned IPv4 addresses must not include the gateway");
            }
            if subnet.broadcast().is_some_and(|broadcast| holds(u32::from(broadcast))) {
                return Err("owned IPv4 addresses must not include the subnet's broadcast address");
            }
            ranges.push((start, end));
        }
        ranges.sort_unstable();
        let mut merged: Vec<(u32, u32)> = Vec::with_capacity(ranges.len());
        for (start, end) in ranges {
            match merged.last_mut() {
                Some(last) if start <= last.1.saturating_add(1) => last.1 = last.1.max(end),
                _ => merged.push((start, end)),
            }
        }
        merged.shrink_to_fit();
        Ok(Self { ranges: merged })
    }

    /// Whether `addr` is in the set.
    #[inline]
    pub(crate) fn contains(&self, addr: Ipv4Address) -> bool {
        let addr = u32::from(addr);
        let after = self.ranges.partition_point(|&(start, _)| start <= addr);
        after != 0 && self.ranges[after - 1].1 >= addr
    }

    #[cfg(test)]
    pub(crate) fn ranges(&self) -> &[(u32, u32)] {
        &self.ranges
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SUBNET: Ipv4Cidr = Ipv4Cidr::new(Ipv4Address::new(10, 0, 0, 10), 24);
    const GATEWAY: Option<Ipv4Address> = Some(Ipv4Address::new(10, 0, 0, 1));

    fn cidr(text: &str) -> Ipv4Cidr {
        text.parse().unwrap()
    }

    fn owned(cidrs: &[&str]) -> OwnedIpv4 {
        let cidrs: Vec<_> = cidrs.iter().map(|text| cidr(text)).collect();
        OwnedIpv4::new(&cidrs, SUBNET, GATEWAY).unwrap()
    }

    fn addr(text: &str) -> Ipv4Address {
        text.parse().unwrap()
    }

    #[test]
    fn empty_set_holds_nothing() {
        let set = owned(&[]);
        for text in ["0.0.0.0", "10.0.0.10", "10.0.0.11", "255.255.255.254"] {
            assert!(!set.contains(addr(text)));
        }
    }

    /// Each range holds its first and last address and nothing next to it; the hole between two
    /// ranges holds nothing.
    #[test]
    fn membership_at_boundaries_and_holes() {
        let set = owned(&["10.0.1.16/28", "10.0.1.40/32", "172.16.0.0/16"]);
        for text in ["10.0.1.16", "10.0.1.20", "10.0.1.31", "10.0.1.40", "172.16.0.0", "172.16.255.255"] {
            assert!(set.contains(addr(text)), "{text}");
        }
        for text in ["10.0.1.15", "10.0.1.32", "10.0.1.39", "10.0.1.41", "172.15.255.255", "172.17.0.0", "1.1.1.1"] {
            assert!(!set.contains(addr(text)), "{text}");
        }
    }

    /// Duplicates, overlaps, a range inside another and adjacent ranges merge; input order does
    /// not matter.
    #[test]
    fn duplicates_overlaps_and_adjacent_ranges_merge() {
        let set = owned(&[
            "10.0.2.5/32",
            "10.0.2.0/29",
            "10.0.2.5/32",
            "10.0.2.8/29",
            "10.0.3.0/24",
            "10.0.3.128/25",
            "10.0.9.9/32",
        ]);
        let range = |a: &str, b: &str| (u32::from(addr(a)), u32::from(addr(b)));
        assert_eq!(
            set.ranges(),
            [range("10.0.2.0", "10.0.2.15"), range("10.0.3.0", "10.0.3.255"), range("10.0.9.9", "10.0.9.9")]
        );
    }

    /// Single addresses with every other address skipped: each is a member, each skipped one is
    /// not, at every set size the plan names.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn single_addresses_at_every_size() {
        for count in [1u32, 50, 500, 1000, 4096] {
            let base = u32::from(addr("100.64.0.0"));
            let cidrs: Vec<_> = (0..count)
                .map(|index| Ipv4Cidr::new(Ipv4Address::from(base + 2 * index), 32))
                .rev()
                .collect();
            let set = OwnedIpv4::new(&cidrs, SUBNET, GATEWAY).unwrap();
            assert_eq!(set.ranges().len(), count as usize);
            for index in 0..count {
                assert!(set.contains(Ipv4Address::from(base + 2 * index)));
                assert!(!set.contains(Ipv4Address::from(base + 2 * index + 1)));
            }
            assert!(!set.contains(Ipv4Address::from(base - 1)));
        }
    }

    #[test]
    fn validation_rejects_non_unicast_gateway_broadcast_and_host_bits() {
        // Each input breaks exactly one rule: not unicast, holds the gateway, holds the subnet's
        // broadcast address, or has host bits set.
        for text in [
            "0.0.0.0/32",
            "0.0.0.0/8",
            "224.0.0.1/32",
            "239.255.255.255/32",
            "192.0.0.0/2",
            "255.255.255.255/32",
            "10.0.0.1/32",
            "10.0.0.0/28",
            "10.0.0.255/32",
            "10.0.0.240/28",
            "10.0.1.5/24",
        ] {
            assert!(OwnedIpv4::new(&[cidr(text)], SUBNET, GATEWAY).is_err(), "{text}");
        }
        // Next to the forbidden addresses is fine, and a /31 or /32 subnet has no broadcast.
        for text in ["10.0.0.2/32", "10.0.0.254/32", "223.255.255.255/32", "240.0.0.0/32"] {
            assert!(OwnedIpv4::new(&[cidr(text)], SUBNET, GATEWAY).is_ok(), "{text}");
        }
        let point = Ipv4Cidr::new(addr("10.0.0.10"), 32);
        assert!(OwnedIpv4::new(&[cidr("10.0.0.255/32")], point, None).is_ok());
    }
}
