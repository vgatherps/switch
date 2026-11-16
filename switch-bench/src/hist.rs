//! A log-linear histogram of u64 values (ns): values below 128 are exact; above, each power of
//! two is split into 128 equal buckets, so a reported value is within 1/128 (< 0.8%) of the true
//! one. Fixed size, no allocation after construction; histograms of the same shape merge.

const SUB_BITS: u32 = 7;
const SUB: usize = 1 << SUB_BITS;
/// Exponents 7..=63 each take `SUB` buckets, after the `SUB` exact ones.
const BUCKETS: usize = SUB + (64 - SUB_BITS as usize) * SUB;

#[derive(Clone)]
pub struct Histogram {
    counts: Box<[u64; BUCKETS]>,
    count: u64,
    max: u64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for Histogram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Histogram")
            .field("count", &self.count)
            .field("max", &self.max)
            .finish()
    }
}

fn index(value: u64) -> usize {
    if value < SUB as u64 {
        return value as usize;
    }
    let exponent = 63 - value.leading_zeros();
    let sub = (value >> (exponent - SUB_BITS)) as usize & (SUB - 1);
    SUB + (exponent - SUB_BITS) as usize * SUB + sub
}

/// The largest value that lands in bucket `index`.
fn upper(index: usize) -> u64 {
    if index < SUB {
        return index as u64;
    }
    let exponent = (index - SUB) / SUB + SUB_BITS as usize;
    let sub = ((index - SUB) % SUB) as u64;
    let shift = exponent as u32 - SUB_BITS;
    let low = (SUB as u64 + sub) << shift;
    low + ((1u64 << shift) - 1)
}

impl Histogram {
    pub fn new() -> Histogram {
        Histogram {
            counts: Box::new([0; BUCKETS]),
            count: 0,
            max: 0,
        }
    }

    #[inline]
    pub fn record(&mut self, value: u64) {
        self.counts[index(value)] += 1;
        self.count += 1;
        self.max = self.max.max(value);
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn max(&self) -> u64 {
        self.max
    }

    /// The value at quantile `q` (0..=1): the upper bound of the bucket holding the
    /// `ceil(q * count)`-th smallest value, capped at the true max. `None` if empty.
    pub fn quantile(&self, q: f64) -> Option<u64> {
        if self.count == 0 {
            return None;
        }
        let rank = ((q * self.count as f64).ceil() as u64).clamp(1, self.count);
        let mut seen = 0;
        for (i, &c) in self.counts.iter().enumerate() {
            seen += c;
            if seen >= rank {
                return Some(upper(i).min(self.max));
            }
        }
        unreachable!("rank <= count")
    }

    pub fn merge(&mut self, other: &Histogram) {
        for (a, b) in self.counts.iter_mut().zip(other.counts.iter()) {
            *a += b;
        }
        self.count += other.count;
        self.max = self.max.max(other.max);
    }

    /// The non-empty buckets, the count and the max, for sending to the coordinator.
    pub fn to_sparse(&self) -> Sparse {
        let buckets = self
            .counts
            .iter()
            .enumerate()
            .filter(|(_, c)| **c > 0)
            .map(|(i, c)| (i as u32, *c))
            .collect();
        Sparse {
            count: self.count,
            max: self.max,
            buckets,
        }
    }

    pub fn from_sparse(s: &Sparse) -> Result<Histogram, String> {
        let mut h = Histogram::new();
        for &(i, c) in &s.buckets {
            *h.counts
                .get_mut(i as usize)
                .ok_or_else(|| format!("bucket {i} out of range"))? += c;
        }
        if s.buckets.iter().map(|b| b.1).sum::<u64>() != s.count {
            return Err("bucket counts do not add up to count".into());
        }
        h.count = s.count;
        h.max = s.max;
        Ok(h)
    }
}

/// A histogram on the wire: only non-empty buckets.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Sparse {
    pub count: u64,
    pub max: u64,
    pub buckets: Vec<(u32, u64)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every value lands in a bucket whose upper bound is at least the value and at most 1/128
    /// above it; bucket indexes are monotonic.
    #[test]
    fn bucket_error_is_below_one_part_in_128() {
        let mut values: Vec<u64> = (0..5000).collect();
        for shift in 7..64 {
            for delta in [0u64, 1, 2, 3] {
                let base = 1u64 << shift;
                values.extend([base - delta.min(base), base + delta, base | (base - 1)]);
            }
        }
        values.push(u64::MAX);
        values.sort_unstable();
        let mut last = 0;
        for v in values {
            let i = index(v);
            assert!(i >= last, "index not monotonic at {v}");
            last = i;
            let up = upper(i);
            assert!(up >= v, "{v} above its bucket's upper bound {up}");
            assert!(
                (up - v) as f64 <= v as f64 / 128.0,
                "{v}: bucket upper {up} too far"
            );
        }
    }

    /// Quantiles of a known set: exact below 128, within 1/128 above.
    #[test]
    fn quantiles_of_a_known_distribution() {
        let mut h = Histogram::new();
        assert_eq!(h.quantile(0.5), None);
        for v in 1..=100 {
            h.record(v);
        }
        assert_eq!(
            (h.quantile(0.5), h.quantile(0.99), h.quantile(1.0)),
            (Some(50), Some(99), Some(100))
        );
        let mut h = Histogram::new();
        for v in 1..=10_000u64 {
            h.record(v * 1000);
        }
        for (q, expected) in [
            (0.5, 5_000_000f64),
            (0.99, 9_900_000.0),
            (0.999, 9_990_000.0),
        ] {
            let got = h.quantile(q).unwrap() as f64;
            assert!(
                got >= expected && got <= expected * (1.0 + 1.0 / 128.0),
                "q{q}: {got}"
            );
        }
        assert_eq!(h.quantile(1.0), Some(10_000_000));
    }

    #[test]
    fn merge_equals_recording_both() {
        let (mut a, mut b, mut both) = (Histogram::new(), Histogram::new(), Histogram::new());
        for v in 0..1000u64 {
            let x = v * v * 37;
            if v % 3 == 0 {
                a.record(x)
            } else {
                b.record(x)
            }
            both.record(x);
        }
        a.merge(&b);
        for q in [0.0, 0.5, 0.9, 0.99, 1.0] {
            assert_eq!(a.quantile(q), both.quantile(q));
        }
        assert_eq!((a.count(), a.max()), (both.count(), both.max()));
    }

    /// A histogram sent as sparse buckets and rebuilt gives the same quantiles; tampered
    /// buckets are refused.
    #[test]
    fn sparse_round_trip() {
        let mut h = Histogram::new();
        for v in [3u64, 900, 900, 77_000, 5_000_000_000] {
            h.record(v);
        }
        let s = h.to_sparse();
        assert_eq!(s.buckets.len(), 4);
        let back = Histogram::from_sparse(&s).unwrap();
        for q in [0.0, 0.5, 0.99, 1.0] {
            assert_eq!(back.quantile(q), h.quantile(q));
        }
        assert_eq!((back.count(), back.max()), (5, 5_000_000_000));
        let mut bad = s.clone();
        bad.buckets[0].1 += 1;
        assert!(Histogram::from_sparse(&bad).is_err());
        bad = s;
        bad.buckets[0].0 = u32::MAX;
        assert!(Histogram::from_sparse(&bad).is_err());
    }
}
