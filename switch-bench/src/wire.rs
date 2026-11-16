//! The bench message: a fixed header, then padding up to the message size. The echo side
//! returns the bytes unchanged.

/// Bytes of the header: step u32, sender u32, seq u64, scheduled send time u64 (ns since the
/// generator's origin), little endian.
pub const HEADER: usize = 24;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub step: u32,
    pub sender: u32,
    pub seq: u64,
    pub sched_ns: u64,
}

impl Header {
    /// Writes the header at the start of `buf` (at least `HEADER` bytes); the rest is left as is.
    pub fn write(&self, buf: &mut [u8]) {
        buf[0..4].copy_from_slice(&self.step.to_le_bytes());
        buf[4..8].copy_from_slice(&self.sender.to_le_bytes());
        buf[8..16].copy_from_slice(&self.seq.to_le_bytes());
        buf[16..24].copy_from_slice(&self.sched_ns.to_le_bytes());
    }

    /// The header of `buf`; `None` if it is shorter than `HEADER`.
    pub fn read(buf: &[u8]) -> Option<Header> {
        let buf = buf.get(..HEADER)?;
        let u32_at = |i: usize| u32::from_le_bytes(buf[i..i + 4].try_into().unwrap());
        let u64_at = |i: usize| u64::from_le_bytes(buf[i..i + 8].try_into().unwrap());
        Some(Header {
            step: u32_at(0),
            sender: u32_at(4),
            seq: u64_at(8),
            sched_ns: u64_at(16),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_round_trips_and_short_buffers_are_refused() {
        let header = Header {
            step: 7,
            sender: 3,
            seq: u64::MAX - 1,
            sched_ns: 123_456_789_012,
        };
        let mut buf = [0xaa; 64];
        header.write(&mut buf);
        assert_eq!(Header::read(&buf), Some(header));
        assert_eq!(buf[HEADER], 0xaa, "padding untouched");
        assert_eq!(Header::read(&buf[..HEADER - 1]), None);
    }
}
