/// RX mbufs reserved for the driver, staging queue and one lcore's cache.
/// The count is max(rx_descriptors, 32) + rx_staging + floor(3 * rx_cache_size / 2).
/// A NIC keeps its RX descriptors filled; memif copy mode needs 32 mbufs
/// before it can receive. Staged packets still use mbufs. A DPDK cache can
/// hold up to its flush threshold of 1.5 times its configured size.
/// `Stack::new` refuses a smaller RX pool. Size the RX pool as this plus the
/// received mbufs all sockets together may hold (unread data, out-of-order
/// segments, frames the application holds); it does not grow with the
/// socket count. Each TCP socket holds at most `tcp::rx_mbuf_budget` of them.
///
/// Panics if the reserve exceeds usize.
pub fn rx_reserve(rx_descriptors: u16, rx_staging: usize, rx_cache_size: u32) -> usize {
    let cache = usize::try_from(rx_cache_size)
        .expect("RX cache size exceeds usize")
        .checked_mul(3)
        .expect("RX cache flush threshold exceeds usize")
        / 2;
    usize::from(rx_descriptors)
        .max(32)
        .checked_add(rx_staging)
        .and_then(|count| count.checked_add(cache))
        .expect("RX reserve exceeds usize")
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
/// with the socket count. Each TCP socket queues at most `tcp::tx_mbuf_budget`.
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
    fn rx_reserve_covers_descriptors_staging_and_cache() {
        assert_eq!(rx_reserve(1024, 32, 64), 1024 + 32 + 96);
        assert_eq!(rx_reserve(64, 32, 8), 64 + 32 + 12);
        assert_eq!(rx_reserve(16, 32, 8), 32 + 32 + 12);
        assert_eq!(rx_reserve(16, 0, 1), 33);
    }
}
