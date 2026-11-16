/// Bytes in a sockperf message header. Bytes 8 and 9 hold the flags and bytes 10 to 13 the whole
/// message's length, both big-endian.
pub const HEADER: usize = 14;
/// The largest message: 65507 bytes, the largest UDP payload.
pub const MAX_MESSAGE: usize = 65507;

/// A datagram that is not exactly one message, or a length field below [`HEADER`] or above the
/// limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Malformed;

fn length(header: &[u8], max: usize) -> Result<usize, Malformed> {
    let len = u32::from_be_bytes(header[10..14].try_into().unwrap()) as usize;
    if len < HEADER || len > max {
        Err(Malformed)
    } else {
        Ok(len)
    }
}

/// Accepts a datagram that is exactly one message: at least [`HEADER`] bytes, with a length field
/// equal to the datagram's length and at most [`MAX_MESSAGE`].
pub fn validate_udp(message: &[u8]) -> Result<(), Malformed> {
    if message.len() < HEADER || length(message, MAX_MESSAGE)? != message.len() {
        return Err(Malformed);
    }
    Ok(())
}

/// Turns a message that passed [`validate_udp`] or came from [`Framer::push`] into its reply in
/// place, and returns `true`, when its flags ask for a reply (`flags & 7 == 3`): the reply clears
/// the lowest flag bit. Otherwise it changes nothing and returns `false`.
///
/// # Panics
///
/// If `message` is shorter than 10 bytes.
pub fn reply(message: &mut [u8]) -> bool {
    let flags = u16::from_be_bytes([message[8], message[9]]);
    if flags & 7 != 3 {
        return false;
    }
    message[9] &= !1;
    true
}

/// Cuts a TCP byte stream into sockperf messages.
pub struct Framer {
    partial: Vec<u8>,
    max: usize,
}

impl Framer {
    /// A framer that rejects messages longer than `max` bytes.
    ///
    /// # Panics
    ///
    /// If `max` is below [`HEADER`].
    pub fn new(max: usize) -> Self {
        assert!(max >= HEADER);
        Self {
            partial: Vec::new(),
            max,
        }
    }

    /// Drops the partial message kept from earlier [`push`](Self::push) calls.
    pub fn reset(&mut self) {
        self.partial.clear();
    }

    /// Appends `bytes` to the stream and calls `complete` with each whole message, in order; the
    /// trailing partial message is kept for the next call. Returns [`Malformed`] when a length
    /// field is below [`HEADER`] or above the framer's limit; the framer then still holds the bad
    /// header, so call [`reset`](Self::reset) before reusing it.
    pub fn push(
        &mut self,
        mut bytes: &[u8],
        mut complete: impl FnMut(&mut [u8]),
    ) -> Result<(), Malformed> {
        while !bytes.is_empty() {
            let target = if self.partial.len() < HEADER {
                HEADER
            } else {
                length(&self.partial, self.max)?
            };
            let take = (target - self.partial.len()).min(bytes.len());
            self.partial.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
            if self.partial.len() >= HEADER {
                let len = length(&self.partial, self.max)?;
                if self.partial.len() == len {
                    complete(&mut self.partial);
                    self.partial.clear();
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(size: usize, flags: u16) -> Vec<u8> {
        let mut bytes: Vec<u8> = (0..size).map(|i| i as u8).collect();
        bytes[..8].copy_from_slice(&123456789u64.to_be_bytes());
        bytes[8..10].copy_from_slice(&flags.to_be_bytes());
        bytes[10..14].copy_from_slice(&(size as u32).to_be_bytes());
        bytes
    }

    #[test]
    fn reply_truth_table_preserves_every_other_bit_and_byte() {
        for flags in 0..=u16::MAX {
            let original = message(39, flags);
            let mut bytes = original.clone();
            let selected = flags & 7 == 3;
            assert_eq!(reply(&mut bytes), selected);
            let mut expected = original;
            if selected {
                expected[9] &= !1;
            }
            assert_eq!(bytes, expected);
        }
    }

    #[test]
    fn tcp_split_at_every_header_and_body_boundary() {
        for size in [HEADER, 65] {
            let bytes = message(size, 3);
            for split in 0..=size {
                let mut framer = Framer::new(MAX_MESSAGE);
                let mut frames = Vec::new();
                framer
                    .push(&bytes[..split], |m| frames.push(m.to_vec()))
                    .unwrap();
                framer
                    .push(&bytes[split..], |m| frames.push(m.to_vec()))
                    .unwrap();
                assert_eq!(frames, std::slice::from_ref(&bytes), "split {split}");
            }
        }
    }

    #[test]
    fn tcp_three_messages_in_one_read() {
        let frames = [message(14, 3), message(40, 1), message(65, 7)];
        let mut got = Vec::new();
        Framer::new(MAX_MESSAGE)
            .push(&frames.concat(), |m| got.push(m.to_vec()))
            .unwrap();
        assert_eq!(got, frames);
    }

    #[test]
    fn tcp_message_straddles_two_reads_between_complete_messages() {
        let frames = [message(19, 3), message(40, 3), message(17, 3)];
        let bytes = frames.concat();
        let mut framer = Framer::new(MAX_MESSAGE);
        let mut got = Vec::new();
        framer.push(&bytes[..29], |m| got.push(m.to_vec())).unwrap();
        assert_eq!(got, [frames[0].clone()]);
        framer.push(&bytes[29..], |m| got.push(m.to_vec())).unwrap();
        assert_eq!(got, frames);
    }

    #[test]
    fn tcp_invalid_lengths_fail_as_soon_as_header_completes() {
        for len in [0, 13, MAX_MESSAGE + 1] {
            let mut bytes = message(HEADER, 3);
            bytes[10..14].copy_from_slice(&(len as u32).to_be_bytes());
            let mut framer = Framer::new(MAX_MESSAGE);
            framer
                .push(&bytes[..13], |_| panic!("invalid frame delivered"))
                .unwrap();
            assert_eq!(
                framer.push(&bytes[13..], |_| panic!("invalid frame delivered")),
                Err(Malformed)
            );
        }
    }

    #[test]
    fn tcp_reset_discards_previous_connections_partial_frame() {
        let bytes = message(32, 3);
        let mut framer = Framer::new(MAX_MESSAGE);
        framer
            .push(&bytes[..15], |_| panic!("partial frame delivered"))
            .unwrap();
        framer.reset();
        let mut got = Vec::new();
        framer.push(&bytes, |m| got.push(m.to_vec())).unwrap();
        assert_eq!(got, [bytes]);
    }

    #[test]
    fn udp_short_and_length_mismatch_are_malformed() {
        let bytes = message(32, 3);
        for size in 0..HEADER {
            assert_eq!(validate_udp(&bytes[..size]), Err(Malformed));
        }
        assert_eq!(validate_udp(&bytes[..31]), Err(Malformed));
        let mut extra = bytes.clone();
        extra.push(0);
        assert_eq!(validate_udp(&extra), Err(Malformed));
        assert_eq!(validate_udp(&bytes), Ok(()));
        assert_eq!(validate_udp(&message(MAX_MESSAGE, 3)), Ok(()));
    }
}
