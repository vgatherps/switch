use std::fmt;
use std::sync::OnceLock;
use std::time::Instant;

use serde::{Deserialize, Serialize};

pub const MAGIC: u32 = 0x4b49_4f54;
pub const VERSION: u16 = 1;
pub const HEADER_LEN: usize = 32;
pub const DEFAULT_PAYLOAD_BYTES: usize = 256;
pub const DEFAULT_MAX_PAYLOAD_BYTES: usize = 1 << 20;
const DECODER_COMPACT_AFTER_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrameHeader {
    pub magic: u32,
    pub version: u16,
    pub flags: u16,
    pub seq: u64,
    pub send_time_ns: u64,
    pub payload_len: u32,
    pub stream_id: u32,
}

impl FrameHeader {
    pub fn new(seq: u64, send_time_ns: u64, payload_len: u32, stream_id: u32) -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            flags: 0,
            seq,
            send_time_ns,
            payload_len,
            stream_id,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub header: FrameHeader,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameError {
    BadMagic(u32),
    BadVersion(u16),
    PayloadTooLarge {
        payload_len: u32,
        max_payload_len: u32,
    },
    PayloadLenMismatch {
        header_len: u32,
        actual_len: usize,
    },
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadMagic(got) => write!(f, "bad frame magic: {got:#x}"),
            Self::BadVersion(got) => write!(f, "bad frame version: {got}"),
            Self::PayloadTooLarge {
                payload_len,
                max_payload_len,
            } => write!(
                f,
                "frame payload too large: {payload_len} > {max_payload_len}"
            ),
            Self::PayloadLenMismatch {
                header_len,
                actual_len,
            } => write!(
                f,
                "frame payload length mismatch: header={header_len} actual={actual_len}"
            ),
        }
    }
}

impl std::error::Error for FrameError {}

pub fn encode_frame(frame: &Frame, out: &mut Vec<u8>) -> Result<(), FrameError> {
    encode_frame_parts(frame.header, &frame.payload, out)
}

pub fn encode_frame_parts(
    header: FrameHeader,
    payload: &[u8],
    out: &mut Vec<u8>,
) -> Result<(), FrameError> {
    if header.payload_len as usize != payload.len() {
        return Err(FrameError::PayloadLenMismatch {
            header_len: header.payload_len,
            actual_len: payload.len(),
        });
    }

    out.reserve(HEADER_LEN + payload.len());
    out.extend_from_slice(&header.magic.to_be_bytes());
    out.extend_from_slice(&header.version.to_be_bytes());
    out.extend_from_slice(&header.flags.to_be_bytes());
    out.extend_from_slice(&header.seq.to_be_bytes());
    out.extend_from_slice(&header.send_time_ns.to_be_bytes());
    out.extend_from_slice(&header.payload_len.to_be_bytes());
    out.extend_from_slice(&header.stream_id.to_be_bytes());
    out.extend_from_slice(payload);
    Ok(())
}

#[derive(Debug)]
pub struct FrameDecoder {
    buf: Vec<u8>,
    start: usize,
    max_payload_len: u32,
}

impl Default for FrameDecoder {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_PAYLOAD_BYTES)
    }
}

impl FrameDecoder {
    pub fn new(max_payload_len: usize) -> Self {
        Self {
            buf: Vec::new(),
            start: 0,
            max_payload_len: max_payload_len.min(u32::MAX as usize) as u32,
        }
    }

    pub fn push(&mut self, bytes: &[u8]) {
        self.compact_consumed();
        self.buf.extend_from_slice(bytes);
    }

    pub fn buffered_len(&self) -> usize {
        self.buf.len().saturating_sub(self.start)
    }

    pub fn next_frame(&mut self) -> Result<Option<Frame>, FrameError> {
        if self.buffered_len() < HEADER_LEN {
            return Ok(None);
        }

        let header_end = self.start + HEADER_LEN;
        let header = parse_header(&self.buf[self.start..header_end])?;
        if header.payload_len > self.max_payload_len {
            return Err(FrameError::PayloadTooLarge {
                payload_len: header.payload_len,
                max_payload_len: self.max_payload_len,
            });
        }

        let frame_len = HEADER_LEN + header.payload_len as usize;
        let frame_end = self.start + frame_len;
        if self.buf.len() < frame_end {
            return Ok(None);
        }

        let payload = self.buf[header_end..frame_end].to_vec();
        self.start = frame_end;
        self.compact_consumed();
        Ok(Some(Frame { header, payload }))
    }

    fn compact_consumed(&mut self) {
        if self.start == 0 {
            return;
        }
        if self.start == self.buf.len() {
            self.buf.clear();
            self.start = 0;
        } else if self.start >= DECODER_COMPACT_AFTER_BYTES {
            self.buf.drain(..self.start);
            self.start = 0;
        }
    }
}

fn parse_header(bytes: &[u8]) -> Result<FrameHeader, FrameError> {
    debug_assert_eq!(bytes.len(), HEADER_LEN);

    let magic = u32::from_be_bytes(bytes[0..4].try_into().expect("header magic"));
    if magic != MAGIC {
        return Err(FrameError::BadMagic(magic));
    }

    let version = u16::from_be_bytes(bytes[4..6].try_into().expect("header version"));
    if version != VERSION {
        return Err(FrameError::BadVersion(version));
    }

    Ok(FrameHeader {
        magic,
        version,
        flags: u16::from_be_bytes(bytes[6..8].try_into().expect("header flags")),
        seq: u64::from_be_bytes(bytes[8..16].try_into().expect("header seq")),
        send_time_ns: u64::from_be_bytes(bytes[16..24].try_into().expect("header time")),
        payload_len: u32::from_be_bytes(bytes[24..28].try_into().expect("header payload")),
        stream_id: u32::from_be_bytes(bytes[28..32].try_into().expect("header stream")),
    })
}

pub fn monotonic_ns() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_nanos() as u64
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunConfig {
    pub address: String,
    pub connections: usize,
    pub rate_total: f64,
    pub duration_secs: u64,
    pub payload_bytes: usize,
    pub max_pending_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VictimSummary {
    pub listen: String,
    pub elapsed_secs: f64,
    pub connections_accepted: u64,
    pub connections_closed: u64,
    pub frames: u64,
    pub bytes: u64,
    pub read_calls: u64,
    pub write_calls: u64,
    pub readiness_events: u64,
    pub parse_errors: u64,
    pub connection_errors: u64,
    pub pending_limit_errors: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SenderSummary {
    pub target: String,
    pub connections: usize,
    pub payload_bytes: usize,
    pub max_pending_bytes: usize,
    pub offered_rate_total: f64,
    pub elapsed_secs: f64,
    pub active_elapsed_secs: f64,
    pub total_elapsed_secs: f64,
    pub expected_sent: f64,
    pub offered_ratio: f64,
    pub sent: u64,
    pub received: u64,
    pub lost: u64,
    pub achieved_rate: f64,
    pub read_calls: u64,
    pub write_calls: u64,
    pub write_bytes: u64,
    pub readiness_events: u64,
    pub connection_errors: u64,
    pub validation_errors: u64,
    pub pending_limit_errors: u64,
    pub rtt_p50_ns: Option<u64>,
    pub rtt_p90_ns: Option<u64>,
    pub rtt_p99_ns: Option<u64>,
    pub rtt_p999_ns: Option<u64>,
    pub rtt_p9999_ns: Option<u64>,
    pub rtt_max_ns: Option<u64>,
}

pub fn percentile(sorted_values: &[u64], percentile: f64) -> Option<u64> {
    if sorted_values.is_empty() {
        return None;
    }

    let rank = (percentile / 100.0) * (sorted_values.len().saturating_sub(1) as f64);
    Some(sorted_values[rank.ceil() as usize])
}

#[derive(Debug, Clone)]
pub struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    pub fn new(seed: u64) -> Self {
        Self { state: seed.max(1) }
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    pub fn next_f64_open01(&mut self) -> f64 {
        let value = self.next_u64() >> 11;
        ((value as f64) + 1.0) / ((1_u64 << 53) as f64 + 1.0)
    }
}

pub fn poisson_gap_ns(rate_per_sec: f64, rng: &mut XorShift64) -> u64 {
    let u = rng.next_f64_open01();
    let seconds = -u.ln() / rate_per_sec;
    ((seconds * 1_000_000_000.0).ceil() as u64).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(seq: u64, payload: &[u8]) -> Frame {
        Frame {
            header: FrameHeader::new(seq, 123, payload.len() as u32, 7),
            payload: payload.to_vec(),
        }
    }

    #[test]
    fn roundtrip_one_frame() {
        let input = frame(1, b"hello");
        let mut encoded = Vec::new();
        encode_frame(&input, &mut encoded).unwrap();

        let mut decoder = FrameDecoder::default();
        decoder.push(&encoded);
        assert_eq!(decoder.next_frame().unwrap(), Some(input));
        assert_eq!(decoder.next_frame().unwrap(), None);
    }

    #[test]
    fn waits_for_partial_frame() {
        let input = frame(2, b"payload");
        let mut encoded = Vec::new();
        encode_frame(&input, &mut encoded).unwrap();

        let mut decoder = FrameDecoder::default();
        decoder.push(&encoded[..10]);
        assert_eq!(decoder.next_frame().unwrap(), None);
        decoder.push(&encoded[10..HEADER_LEN + 2]);
        assert_eq!(decoder.next_frame().unwrap(), None);
        decoder.push(&encoded[HEADER_LEN + 2..]);
        assert_eq!(decoder.next_frame().unwrap(), Some(input));
    }

    #[test]
    fn reads_multiple_frames_from_one_buffer() {
        let one = frame(1, b"a");
        let two = frame(2, b"bc");
        let mut encoded = Vec::new();
        encode_frame(&one, &mut encoded).unwrap();
        encode_frame(&two, &mut encoded).unwrap();

        let mut decoder = FrameDecoder::default();
        decoder.push(&encoded);
        assert_eq!(decoder.next_frame().unwrap(), Some(one));
        assert_eq!(decoder.next_frame().unwrap(), Some(two));
        assert_eq!(decoder.next_frame().unwrap(), None);
    }

    #[test]
    fn buffered_len_tracks_consumed_frames() {
        let one = frame(1, b"a");
        let two = frame(2, b"bc");
        let mut encoded = Vec::new();
        encode_frame(&one, &mut encoded).unwrap();
        encode_frame(&two, &mut encoded).unwrap();

        let mut decoder = FrameDecoder::default();
        decoder.push(&encoded);
        assert_eq!(decoder.buffered_len(), encoded.len());
        assert_eq!(decoder.next_frame().unwrap(), Some(one));
        assert_eq!(decoder.buffered_len(), HEADER_LEN + 2);
        assert_eq!(decoder.next_frame().unwrap(), Some(two));
        assert_eq!(decoder.buffered_len(), 0);
    }

    #[test]
    fn rejects_bad_magic() {
        let input = frame(1, b"x");
        let mut encoded = Vec::new();
        encode_frame(&input, &mut encoded).unwrap();
        encoded[0] = 0;

        let mut decoder = FrameDecoder::default();
        decoder.push(&encoded);
        assert!(matches!(decoder.next_frame(), Err(FrameError::BadMagic(_))));
    }

    #[test]
    fn rejects_bad_version() {
        let input = frame(1, b"x");
        let mut encoded = Vec::new();
        encode_frame(&input, &mut encoded).unwrap();
        encoded[5] = 2;

        let mut decoder = FrameDecoder::default();
        decoder.push(&encoded);
        assert!(matches!(
            decoder.next_frame(),
            Err(FrameError::BadVersion(_))
        ));
    }

    #[test]
    fn rejects_too_large_payload() {
        let input = frame(1, b"abcdef");
        let mut encoded = Vec::new();
        encode_frame(&input, &mut encoded).unwrap();

        let mut decoder = FrameDecoder::new(4);
        decoder.push(&encoded);
        assert!(matches!(
            decoder.next_frame(),
            Err(FrameError::PayloadTooLarge { .. })
        ));
    }

    #[test]
    fn percentile_uses_nearest_rank_ceiling() {
        let values = [10, 20, 30, 40];
        assert_eq!(percentile(&values, 50.0), Some(30));
        assert_eq!(percentile(&values, 99.0), Some(40));
        assert_eq!(percentile(&[], 50.0), None);
    }
}
