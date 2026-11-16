//! Incremental UTF-8 validation: input arrives in pieces, an invalid byte
//! fails the piece that contains it.

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Invalid;

/// Up to 3 bytes of a code point cut by a piece boundary.
#[derive(Default)]
pub(crate) struct Utf8 {
    tail: [u8; 4],
    tail_len: usize,
}

impl Utf8 {
    pub(crate) fn feed(&mut self, mut s: &[u8]) -> Result<(), Invalid> {
        if self.tail_len > 0 {
            let n = self.tail_len;
            let k = (4 - n).min(s.len());
            let mut tmp = self.tail;
            tmp[n..n + k].copy_from_slice(&s[..k]);
            match core::str::from_utf8(&tmp[..n + k]) {
                Ok(_) => {
                    self.tail_len = 0;
                    s = &s[k..];
                }
                Err(e) if e.valid_up_to() > 0 => {
                    // The cut code point completed; re-check what follows it.
                    self.tail_len = 0;
                    s = &s[e.valid_up_to() - n..];
                }
                Err(e) if e.error_len().is_none() => {
                    self.tail = tmp;
                    self.tail_len = n + k;
                    return Ok(());
                }
                Err(_) => return Err(Invalid),
            }
        }
        match core::str::from_utf8(s) {
            Ok(_) => Ok(()),
            Err(e) if e.error_len().is_none() => {
                let cut = &s[e.valid_up_to()..];
                self.tail[..cut.len()].copy_from_slice(cut);
                self.tail_len = cut.len();
                Ok(())
            }
            Err(_) => Err(Invalid),
        }
    }

    /// The input ended; a cut code point is invalid. Resets for the next message.
    pub(crate) fn finish(&mut self) -> Result<(), Invalid> {
        let n = core::mem::take(&mut self.tail_len);
        if n == 0 { Ok(()) } else { Err(Invalid) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Feed `s` cut at `cuts`; the index of the piece that fails, or `pieces.len()` if `finish` fails.
    fn fail_at(s: &[u8], cuts: &[usize]) -> Option<usize> {
        let mut v = Utf8::default();
        let mut bounds = vec![0];
        bounds.extend_from_slice(cuts);
        bounds.push(s.len());
        for (i, w) in bounds.windows(2).enumerate() {
            if v.feed(&s[w[0]..w[1]]).is_err() {
                return Some(i);
            }
        }
        v.finish().err().map(|_| bounds.len() - 1)
    }

    #[test]
    fn valid_text_at_every_cut_pair() {
        let s = "a€𝄞ßz€".as_bytes();
        for i in 0..=s.len() {
            for j in i..=s.len() {
                assert_eq!(fail_at(s, &[i, j]), None, "cuts {i} {j}");
            }
        }
    }

    #[test]
    fn invalid_byte_fails_the_piece_containing_it() {
        // E0 80: E0 needs A0..BF next, so the second byte is the first invalid one.
        let s = [b'a', 0xE0, 0x80, b'b'];
        assert_eq!(fail_at(&s, &[2]), Some(1));
        assert_eq!(fail_at(&s, &[3]), Some(0));
        assert_eq!(fail_at(&s, &[1]), Some(1));
        // A lone continuation byte, a byte never valid, an overlong lead.
        assert_eq!(fail_at(&[b'x', 0x80], &[1]), Some(1));
        assert_eq!(fail_at(&[b'x', 0xFF], &[1]), Some(1));
        assert_eq!(fail_at(&[0xC0, 0x80], &[]), Some(0));
        // Above U+10FFFF: F4 90 is already invalid at the second byte.
        assert_eq!(fail_at(&[0xF4, 0x90, 0x80, 0x80], &[1, 2, 3]), Some(1));
    }

    #[test]
    fn cut_code_point_at_end_fails_finish() {
        let s = "€".as_bytes();
        assert_eq!(fail_at(&s[..2], &[1]), Some(2));
        assert_eq!(fail_at(s, &[1, 2]), None);
    }

    #[test]
    fn finish_resets_for_next_message() {
        let mut v = Utf8::default();
        v.feed(&[0xE2]).unwrap();
        assert!(v.finish().is_err());
        v.feed(b"ok").unwrap();
        assert!(v.finish().is_ok());
    }
}
