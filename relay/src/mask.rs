/// XORs a frame payload piece, carrying its position across pieces.
pub(crate) fn apply(bytes: &mut [u8], mask: Option<[u8; 4]>, phase: &mut usize) {
    let Some(key) = mask else { return };
    let start = *phase & 3;
    let word = u64::from_ne_bytes(core::array::from_fn(|i| key[(start + i) & 3]));
    let mut chunks = bytes.chunks_exact_mut(8);
    for chunk in &mut chunks {
        let value = u64::from_ne_bytes(chunk.try_into().unwrap()) ^ word;
        chunk.copy_from_slice(&value.to_ne_bytes());
    }
    let tail = chunks.into_remainder();
    for (i, byte) in tail.iter_mut().enumerate() {
        *byte ^= key[(start + i) & 3];
    }
    *phase += bytes.len();
}

#[cfg(test)]
mod tests {
    use super::apply;

    #[test]
    fn matches_scalar_for_phases_lengths_offsets_and_pieces() {
        let key = [0x19, 0xa4, 0x63, 0xfe];
        for phase in 0..4 {
            for len in 0..=129 {
                for offset in 0..8 {
                    for mask in [None, Some(key)] {
                        let original: Vec<u8> = (0..offset + len + 8)
                            .map(|i| (i as u8).wrapping_mul(37).wrapping_add(11))
                            .collect();
                        let mut expected = original.clone();
                        let mut expected_phase = phase;
                        if let Some(key) = mask {
                            for byte in &mut expected[offset..offset + len] {
                                *byte ^= key[expected_phase % 4];
                                expected_phase += 1;
                            }
                        }
                        let mut actual = original.clone();
                        let mut actual_phase = phase;
                        apply(&mut actual[offset..offset + len], mask, &mut actual_phase);
                        assert_eq!(actual, expected, "phase={phase} len={len} offset={offset} mask={mask:?}");
                        assert_eq!(actual_phase, expected_phase);

                        let mut actual = original;
                        let mut actual_phase = phase;
                        let split = len / 3;
                        apply(&mut actual[offset..offset + split], mask, &mut actual_phase);
                        apply(&mut actual[offset + split..offset + len], mask, &mut actual_phase);
                        assert_eq!(actual, expected, "split phase={phase} len={len} offset={offset} mask={mask:?}");
                        assert_eq!(actual_phase, expected_phase);
                    }
                }
            }
        }
    }
}
