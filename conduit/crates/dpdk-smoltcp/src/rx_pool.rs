/// RX mbufs reserved for the driver, the low mark, the staging queue and one lcore's cache.
/// The count is max(rx_descriptors, 32) + `rx_low_mark(rx_descriptors, rx_burst)` +
/// rx_staging + floor(3 * rx_cache_size / 2).
/// A NIC keeps its RX descriptors posted (ENA refills once min(ring / 8, 256) are empty);
/// memif copy mode needs 32 mbufs before it can receive. With those posted and staging full,
/// the pool must still hold the low mark free: below it the TCP sockets holding the most
/// out-of-order frames release them. A DPDK cache can hold up to its flush threshold of 1.5 times its
/// configured size. `Stack::new` refuses a smaller RX pool. Size the RX pool as this plus the
/// received mbufs all sockets together may hold (unread data, out-of-order segments, frames
/// the application holds). Smaller segments can require more mbufs for the same byte volume.
///
/// Panics if the reserve exceeds usize.
pub fn rx_reserve(rx_descriptors: u16, rx_burst: u16, rx_staging: usize, rx_cache_size: u32) -> usize {
    let cache = usize::try_from(rx_cache_size)
        .expect("RX cache size exceeds usize")
        .checked_mul(3)
        .expect("RX cache flush threshold exceeds usize")
        / 2;
    usize::from(rx_descriptors.max(32))
        .checked_add(rx_low_mark(rx_descriptors, rx_burst))
        .and_then(|count| count.checked_add(rx_staging))
        .and_then(|count| count.checked_add(cache))
        .expect("RX reserve exceeds usize")
}

/// One refill of the whole RX ring plus one RX burst: max(RX descriptors, 32) + `rx_burst`.
/// The Port reads the RX pool as low while fewer mbufs are free. DPDK 25.11.3 PMDs refill in
/// chunks up to that size, all or nothing: memif copy mode allocates 32 per receive whatever
/// the burst and receives nothing without them; ixgbe's vector RX rearms 64; ENA
/// bulk-allocates every free descriptor at once, and while that fails still returns completed
/// packets until the buffers already posted run out, then receives nothing. net_tap takes one
/// per packet and net_ring none.
pub(crate) fn rx_low_mark(rx_descriptors: u16, rx_burst: u16) -> usize {
    usize::from(rx_descriptors.max(32)) + usize::from(rx_burst)
}

/// TX mbufs reserved for a NIC PMD and the Device.
/// The count is tx_descriptors + tx_pending.
/// Use the negotiated TX descriptor count from Port::descriptor_counts().1:
/// a NIC PMD may keep that many sent mbufs until it reclaims them. The Device
/// holds at most tx_pending more: built frames, the live reservation and the
/// spare share those slots. The pool cache needs no slack because this lcore
/// is the only allocator.
///
/// Panics if the reserve exceeds usize.
pub fn tx_reserve(tx_descriptors: u16, tx_pending: usize) -> usize {
    usize::from(tx_descriptors)
        .checked_add(tx_pending)
        .expect("TX reserve exceeds usize")
}

/// Frame-pool mbufs reserved for sent packets: tx_descriptors + tx_pending + 1.
/// Each packet the PMD or the pending queue holds keeps at most one frame-pool
/// mbuf that no socket or application counts (a frame the socket already
/// released at ACK, or a sent UDP frame); the one extra frame means those
/// packets can never take the last frame. Size the frame pool as this plus the
/// frames all sockets and the application together may hold (queued or
/// unacknowledged TCP data, frames from `Stack::tx_frame`); it does not grow
/// with the socket count. `tcp::tx_mbuf_budget` estimates a socket's needs
/// for full-sized segments; smaller writes can use more frames.
///
/// Panics if the reserve exceeds usize.
pub fn frame_reserve(tx_descriptors: u16, tx_pending: usize) -> usize {
    tx_reserve(tx_descriptors, tx_pending)
        .checked_add(1)
        .expect("frame reserve exceeds usize")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_reserve_covers_one_mbuf_per_held_packet_plus_one() {
        assert_eq!(frame_reserve(16, 8), 16 + 8 + 1);
        assert_eq!(frame_reserve(1024, 32), 1024 + 32 + 1);
        assert_eq!(frame_reserve(0, 0), 1);
    }

    #[test]
    #[should_panic(expected = "frame reserve exceeds usize")]
    fn frame_reserve_refuses_overflow() {
        frame_reserve(0, usize::MAX);
    }

    #[test]
    fn tx_reserve_covers_descriptors_and_pending() {
        assert_eq!(tx_reserve(1024, 32), 1024 + 32);
        assert_eq!(tx_reserve(64, 32), 96);
        assert_eq!(tx_reserve(16, 8), 24);
        assert_eq!(tx_reserve(16, 1), 17);
        assert_eq!(tx_reserve(16, 0), 16);
        assert_eq!(tx_reserve(0, 0), 0);
    }

    #[test]
    #[should_panic(expected = "TX reserve exceeds usize")]
    fn tx_reserve_refuses_pending_overflow() {
        tx_reserve(1, usize::MAX);
    }

    #[test]
    fn tx_reserve_accepts_pending_at_usize_limit() {
        assert_eq!(tx_reserve(0, usize::MAX), usize::MAX);
        assert_eq!(tx_reserve(1, usize::MAX - 1), usize::MAX);
    }

    #[test]
    fn rx_low_mark_covers_a_full_ring_refill_plus_a_burst() {
        // The kit's net_ring (16 descriptors, burst 8), memif with a short burst, an ENA ring.
        assert_eq!(rx_low_mark(16, 8), 32 + 8);
        assert_eq!(rx_low_mark(64, 8), 64 + 8);
        assert_eq!(rx_low_mark(1024, 32), 1024 + 32);
    }

    #[test]
    fn rx_reserve_covers_posted_low_mark_staging_and_cache() {
        assert_eq!(rx_reserve(1024, 32, 32, 64), 1024 + (1024 + 32) + 32 + 96);
        assert_eq!(rx_reserve(64, 32, 32, 8), 64 + (64 + 32) + 32 + 12);
        assert_eq!(rx_reserve(16, 8, 32, 8), 32 + (32 + 8) + 32 + 12);
        assert_eq!(rx_reserve(16, 1, 0, 1), 32 + 33 + 1);
    }

    /// An ENA-shaped port as netconf-open sizes it (nic-runner's feed and the netconf-open
    /// examples): 1024 RX descriptors, rx_burst 32, mempool cache 8, staging 32, RX pool =
    /// reserve + share. ENA keeps its ring posted (1023 mbufs: it refills once min(ring / 8,
    /// 256) descriptors are empty), staging is full and the sockets hold their whole share, so
    /// what is free is the reserve minus those. The Port reads the pool as low while free <
    /// `rx_low_mark`; it must not, or the Stack prunes out-of-order frames on every turn
    /// although the sockets hold no more than the share.
    #[test]
    fn reserve_is_not_low_with_the_ring_posted_and_staging_full() {
        let (descriptors, burst, staging, cache) = (1024, 32, 32, 8);
        let free = rx_reserve(descriptors, burst, staging, cache) - (usize::from(descriptors) - 1) - staging;
        let mark = rx_low_mark(descriptors, burst);
        assert!(free >= mark, "low: free {free} < mark {mark}");
    }
}
