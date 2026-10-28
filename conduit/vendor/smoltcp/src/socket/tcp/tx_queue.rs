#![cfg_attr(test, allow(unsafe_code))]

use alloc::{collections::VecDeque, vec::Vec};
use core::cell::Cell;

use crate::phy::{TxBuf, TxSource, UniqueTxBuf};

pub(crate) struct TxQueue {
    entries: VecDeque<Entry>,
    /// Stream position of the first unACKed byte.
    acked: u64,
    /// Stream position after the last queued byte: the last entry's `end`, or `acked` when empty.
    end: u64,
    byte_cap: usize,
    frame_cap: usize,
    hint: Cell<u32>,
    source: TxSource,
}

impl core::fmt::Debug for TxQueue {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("TxQueue")
            .field("bytes", &self.len())
            .field("frames", &self.frames())
            .field("byte_cap", &self.byte_cap)
            .field("frame_cap", &self.frame_cap)
            .finish()
    }
}

struct Entry {
    buf: Option<TxBuf>,
    end: u64,
}

impl Entry {
    /// Stream position of the frame's first byte.
    #[inline]
    fn start(&self) -> u64 {
        self.end - self.buf().len() as u64
    }

    #[inline]
    fn buf(&self) -> &TxBuf {
        self.buf.as_ref().expect("TX queue frame is taken")
    }
}

pub(crate) enum Seg<'a> {
    Empty,
    Whole { index: u32 },
    Cut(&'a [u8]),
}

// Positions are u64 byte counts that never wrap; lengths stay below `byte_cap`, a usize, and the
// queue lives on a 64-bit target, so the conversions between them below are lossless.
impl TxQueue {
    pub(crate) fn new(byte_cap: usize, frame_cap: usize, source: TxSource) -> TxQueue {
        TxQueue {
            entries: VecDeque::with_capacity(frame_cap),
            acked: 0,
            end: 0,
            byte_cap,
            frame_cap,
            hint: Cell::new(0),
            source,
        }
    }

    pub(crate) fn source(&self) -> &TxSource {
        &self.source
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        (self.end - self.acked) as usize
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.end == self.acked
    }

    #[inline]
    pub(crate) fn frames(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn byte_capacity(&self) -> usize {
        self.byte_cap
    }

    pub(crate) fn frame_capacity(&self) -> usize {
        self.frame_cap
    }

    #[inline]
    pub(crate) fn byte_room(&self) -> usize {
        self.byte_cap - self.len()
    }

    #[inline]
    pub(crate) fn frame_room(&self) -> usize {
        self.frame_cap - self.frames()
    }

    /// Append a frame. The caller checked it is not empty and fits the byte and frame room.
    #[inline]
    pub(crate) fn push(&mut self, buf: TxBuf) {
        debug_assert!(buf.len() != 0, "cannot queue an empty TX buffer");
        debug_assert!(buf.len() <= self.byte_room(), "TX buffer exceeds byte room");
        debug_assert!(self.frame_room() != 0, "TX queue has no frame room");
        self.end += buf.len() as u64;
        self.entries.push_back(Entry {
            buf: Some(buf),
            end: self.end,
        });
    }

    pub(crate) fn append_tail(&mut self, sent: usize, data: &[u8], cap: usize) -> usize {
        let first_never_sent = self.acked + sent as u64;
        let byte_room = self.byte_room();
        let Some(entry) = self.entries.back_mut() else {
            return 0;
        };
        if first_never_sent > entry.start() {
            return 0;
        }
        let buf = entry.buf.as_mut().expect("TX queue tail is taken");
        if !buf.unique() {
            return 0;
        }
        let old_len = buf.len();
        // A frame the app filled past `cap` (up to its capacity) takes no more bytes.
        let count = (buf.capacity() - old_len)
            .min(byte_room)
            .min(cap.saturating_sub(old_len))
            .min(data.len());
        let new_len = old_len + count;
        buf.room_mut()[old_len..new_len].copy_from_slice(&data[..count]);
        buf.set_len(new_len);
        entry.end += count as u64;
        self.end = entry.end;
        count
    }

    pub(crate) fn tail_room(&self, sent: usize, cap: usize) -> usize {
        let first_never_sent = self.acked + sent as u64;
        let Some(entry) = self.entries.back() else {
            return 0;
        };
        let buf = entry.buf();
        if first_never_sent > entry.start() || !buf.unique() {
            return 0;
        }
        cap.saturating_sub(buf.len())
            .min(buf.capacity() - buf.len())
            .min(self.byte_room())
    }

    /// ACK `n` bytes, at most `len()`: release every frame they complete.
    #[inline]
    pub(crate) fn ack(&mut self, n: usize) {
        debug_assert!(n <= self.len(), "ACK exceeds queued bytes");
        self.acked += n as u64;
        let mut popped = 0u32;
        while self
            .entries
            .front()
            .is_some_and(|entry| entry.end <= self.acked)
        {
            self.entries.pop_front();
            popped += 1;
        }
        self.hint.set(self.hint.get().saturating_sub(popped));
    }

    pub(crate) fn clear(&mut self) {
        self.acked = self.end;
        self.entries.clear();
        self.hint.set(0);
    }

    pub(crate) fn segment(&self, offset: usize, size: usize) -> Seg<'_> {
        if size == 0 || offset >= self.len() {
            return Seg::Empty;
        }
        let position = self.acked + offset as u64;
        let index = self.locate(position, || {});
        self.hint.set(index as u32);
        let entry = &self.entries[index];
        let buf = entry.buf();
        let start = entry.start();
        if position == start && size >= buf.len() && buf.unique() {
            return Seg::Whole {
                index: index as u32,
            };
        }
        let within = (position - start) as usize;
        let end = within + size.min(buf.len() - within);
        Seg::Cut(&buf.payload()[within..end])
    }

    /// Take the frame `segment(offset, size)` would send whole: one that starts at `offset`,
    /// fits `size` and holds its only reference, read once. `None` leaves the queue unchanged.
    #[inline(always)]
    pub(crate) fn take_whole(&mut self, offset: usize, size: usize) -> Option<(u32, UniqueTxBuf)> {
        if size == 0 || offset >= self.len() {
            return None;
        }
        let position = self.acked + offset as u64;
        let index = self.locate(position, || {});
        self.hint.set(index as u32);
        let entry = &mut self.entries[index];
        if position != entry.start() || size < entry.buf().len() {
            return None;
        }
        let buf = entry.buf.take().expect("TX queue frame is taken");
        match buf.into_unique() {
            Ok(frame) => Some((index as u32, frame)),
            Err(shared) => {
                entry.buf = Some(shared);
                None
            }
        }
    }

    pub(crate) fn segment_spans_frames(&self, offset: usize, size: usize) -> bool {
        if size == 0 || offset >= self.len() {
            return false;
        }
        let position = self.acked + offset as u64;
        let index = self.locate(position, || {});
        position + size as u64 > self.entries[index].end
    }

    /// Copy `size` bytes from `offset` across consecutive frames into `output`. The range lies
    /// in the queued bytes.
    pub(crate) fn copy_segment(&self, offset: usize, size: usize, output: &mut Vec<u8>) {
        debug_assert!(offset <= self.len(), "TX segment offset exceeds queued bytes");
        debug_assert!(size <= self.len() - offset, "TX segment size exceeds queued bytes");
        output.clear();
        output.reserve(size);
        if size == 0 {
            return;
        }
        let position = self.acked + offset as u64;
        let mut index = self.locate(position, || {});
        let mut copied = 0usize;
        while copied < size {
            let entry = &self.entries[index];
            let buf = entry.buf();
            let within = (position + copied as u64 - entry.start()) as usize;
            let count = (size - copied).min(buf.len() - within);
            output.extend_from_slice(&buf.payload()[within..within + count]);
            copied += count;
            index += 1;
        }
    }

    #[cfg(test)]
    pub(crate) fn take(&mut self, index: u32) -> TxBuf {
        self.entries[index as usize]
            .buf
            .take()
            .expect("TX queue frame is already taken")
    }

    /// Return the frame `take_whole` took from `index`.
    #[inline]
    pub(crate) fn put_back(&mut self, index: u32, buf: TxBuf) {
        let index = index as usize;
        debug_assert_eq!(buf.origin(), self.source.origin(), "TX queue frame has wrong origin");
        debug_assert!(
            index == 0 || self.entries[index - 1].end == self.entries[index].end - buf.len() as u64,
            "TX queue frame has wrong length"
        );
        let entry = &mut self.entries[index];
        debug_assert!(entry.buf.is_none(), "TX queue frame is not taken");
        entry.buf = Some(buf);
    }

    /// The entry holding stream position `position`: the head, the hint, the entry after it,
    /// or a binary search. `compared` counts comparisons for tests.
    #[inline(always)]
    fn locate(&self, position: u64, mut compared: impl FnMut()) -> usize {
        compared();
        if position < self.entries.front().expect("TX lookup on empty queue").end {
            return 0;
        }
        self.locate_after_head(position, compared)
    }

    fn locate_after_head(&self, position: u64, mut compared: impl FnMut()) -> usize {
        let contains = |index: usize, compared: &mut dyn FnMut()| {
            let entry = &self.entries[index];
            compared();
            position >= entry.start() && position < entry.end
        };
        let hint = self.hint.get() as usize;
        if hint != 0 && hint < self.entries.len() && contains(hint, &mut compared) {
            return hint;
        }
        let next = hint + 1;
        if next < self.entries.len() && contains(next, &mut compared) {
            return next;
        }

        let mut low = 0;
        let mut high = self.entries.len();
        while low < high {
            let mid = low + (high - low) / 2;
            compared();
            if self.entries[mid].end <= position {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        assert!(low < self.entries.len(), "TX segment position is not queued");
        low
    }

    #[cfg(test)]
    pub(crate) fn push_bytes(&mut self, bytes: &[u8]) {
        let mut buf = self.source.alloc().expect("test TX allocator is empty");
        buf.room_mut()[..bytes.len()].copy_from_slice(bytes);
        buf.set_len(bytes.len());
        self.push(buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::phy::TxPool;
    use alloc::rc::Rc;

    fn queue(byte_cap: usize, frame_cap: usize, room: usize) -> (TxQueue, Rc<TxPool>) {
        let alloc = TxPool::new(room);
        let source = TxSource::new(alloc.clone());
        (TxQueue::new(byte_cap, frame_cap, source), alloc)
    }

    fn cut(seg: Seg<'_>) -> &[u8] {
        match seg {
            Seg::Cut(bytes) => bytes,
            _ => panic!("expected cut segment"),
        }
    }

    #[test]
    fn partial_ack_moves_acked_only() {
        let (mut queue, alloc) = queue(16, 2, 8);
        queue.push_bytes(&[1, 2, 3, 4]);
        queue.ack(2);
        assert_eq!(queue.len(), 2);
        assert_eq!(queue.frames(), 1);
        assert_eq!(alloc.live(), 1);
        assert_eq!(cut(queue.segment(0, 8)), &[3, 4]);
    }

    #[test]
    fn ack_spanning_frames_releases_each_once() {
        let (mut queue, alloc) = queue(32, 4, 8);
        queue.push_bytes(&[1, 2]);
        queue.push_bytes(&[3, 4, 5]);
        queue.push_bytes(&[6, 7]);
        queue.ack(5);
        assert_eq!(queue.frames(), 1);
        assert_eq!(alloc.live(), 1);
        assert_eq!(alloc.releases(), 2);
    }

    #[test]
    fn clear_releases_all() {
        let (mut queue, alloc) = queue(32, 4, 8);
        queue.push_bytes(&[1]);
        queue.push_bytes(&[2]);
        queue.clear();
        assert!(queue.is_empty());
        assert_eq!(queue.frames(), 0);
        assert_eq!(alloc.live(), 0);
        assert_eq!(alloc.releases(), 2);
    }

    #[test]
    fn append_tail_caps_and_appends_only_before_send() {
        let (mut queue, _) = queue(8, 1, 6);
        queue.push_bytes(&[1, 2]);
        assert_eq!(queue.append_tail(0, &[3, 4, 5], 4), 2);
        assert_eq!(queue.len(), 4);
        assert_eq!(queue.entries[0].buf.as_ref().unwrap().payload(), &[1, 2, 3, 4]);
        assert_eq!(queue.append_tail(1, &[5], 1), 0);
    }

    #[test]
    fn append_tail_refuses_shared_frame() {
        let (mut queue, _) = queue(8, 1, 6);
        queue.push_bytes(&[1, 2]);
        let clone = queue.entries[0].buf.as_ref().unwrap().clone();
        assert_eq!(queue.append_tail(0, &[3], 1), 0);
        drop(clone);
    }

    #[test]
    fn lookup_head_hint_next_and_binary_search() {
        let (mut queue, _) = queue(64, 32, 2);
        for byte in 0..24 {
            queue.push_bytes(&[byte]);
        }
        assert!(matches!(queue.segment(0, 1), Seg::Whole { index: 0 }));
        assert_eq!(queue.hint.get(), 0);
        assert!(matches!(queue.segment(20, 1), Seg::Whole { index: 20 }));
        assert_eq!(queue.hint.get(), 20);
        assert!(matches!(queue.segment(20, 1), Seg::Whole { index: 20 }));
        assert!(matches!(queue.segment(21, 1), Seg::Whole { index: 21 }));
    }

    #[test]
    fn lookup_cost_bounded_for_46_frame_flight() {
        let (mut queue, _) = queue(46, 46, 1);
        for byte in 0..46 {
            queue.push_bytes(&[byte]);
        }
        for position in (0..46u64).rev() {
            queue.hint.set(0);
            let comparisons = Cell::new(0usize);
            let index = queue.locate(position, || {
                comparisons.set(comparisons.get().checked_add(1).unwrap())
            });
            assert_eq!(index, position as usize);
            assert!(comparisons.get() <= 9, "{} comparisons", comparisons.get());
        }
    }

    #[test]
    fn alternating_ack_and_segment_keeps_the_lookup_hint() {
        let (mut queue, _) = queue(8, 8, 1);
        for byte in 0..8 {
            queue.push_bytes(&[byte]);
        }
        assert!(matches!(queue.segment(5, 1), Seg::Whole { index: 5 }));
        for (acked, expected_index) in (1usize..=5).zip((0..5).rev()) {
            queue.ack(1);
            assert!(matches!(queue.segment(5 - acked, 1), Seg::Whole { index } if index == expected_index));
            let comparisons = Cell::new(0usize);
            let index = queue.locate(5, || {
                comparisons.set(comparisons.get().checked_add(1).unwrap())
            });
            assert_eq!(index, usize::try_from(expected_index).unwrap());
            assert_eq!(comparisons.get(), if expected_index == 0 { 1 } else { 2 });
        }
    }

    #[test]
    fn segment_empty_for_zero_size() {
        let (mut queue, _) = queue(8, 1, 8);
        queue.push_bytes(&[1, 2]);
        assert!(matches!(queue.segment(0, 0), Seg::Empty));
    }

    #[test]
    fn segment_empty_at_or_past_bytes() {
        let (mut queue, _) = queue(8, 1, 8);
        assert!(matches!(queue.segment(0, 1), Seg::Empty));
        queue.push_bytes(&[1, 2]);
        assert!(matches!(queue.segment(2, 1), Seg::Empty));
        assert!(matches!(queue.segment(3, 1), Seg::Empty));
    }

    #[test]
    fn segment_whole_when_frame_start_and_fits() {
        let (mut queue, _) = queue(8, 1, 8);
        queue.push_bytes(&[1, 2, 3]);
        assert!(matches!(queue.segment(0, 3), Seg::Whole { index: 0 }));
        assert_eq!(cut(queue.segment(0, 2)), &[1, 2]);
        let clone = queue.entries[0].buf.as_ref().unwrap().clone();
        assert_eq!(cut(queue.segment(0, 3)), &[1, 2, 3]);
        drop(clone);
    }

    #[test]
    fn segment_cut_never_spans_frames() {
        let (mut queue, _) = queue(16, 2, 8);
        queue.push_bytes(&[1, 2]);
        queue.push_bytes(&[3, 4]);
        assert_eq!(cut(queue.segment(1, 3)), &[2]);
    }

    #[test]
    fn retransmit_copy_spans_consecutive_frames() {
        let (mut queue, _) = queue(16, 3, 8);
        queue.push_bytes(&[1, 2]);
        queue.push_bytes(&[3, 4, 5]);
        queue.push_bytes(&[6, 7]);
        assert!(queue.segment_spans_frames(1, 5));
        let mut bytes = Vec::new();
        queue.copy_segment(1, 5, &mut bytes);
        assert_eq!(bytes, &[2, 3, 4, 5, 6]);
    }

    #[test]
    fn retransmit_copy_respects_partial_ack_position() {
        let (mut queue, _) = queue(16, 2, 8);
        queue.push_bytes(&[1, 2, 3]);
        queue.push_bytes(&[4, 5, 6]);
        queue.ack(2);
        let mut bytes = Vec::new();
        queue.copy_segment(0, 4, &mut bytes);
        assert_eq!(bytes, &[3, 4, 5, 6]);
    }

    /// `take_whole` takes only a frame that starts at the offset, fits the size and is unique;
    /// every refusal leaves the queue as it was, and `put_back` restores a taken frame.
    #[test]
    fn take_whole_takes_only_a_fitting_unique_frame() {
        let (mut queue, _) = queue(16, 2, 8);
        queue.push_bytes(&[1, 2]);
        queue.push_bytes(&[3, 4, 5]);
        assert!(queue.take_whole(0, 0).is_none());
        assert!(queue.take_whole(5, 3).is_none());
        assert!(queue.take_whole(1, 4).is_none());
        assert!(queue.take_whole(2, 2).is_none());
        let clone = queue.entries[1].buf.as_ref().unwrap().clone();
        assert!(queue.take_whole(2, 3).is_none());
        assert!(queue.entries[1].buf.is_some());
        drop(clone);
        let (index, frame) = queue.take_whole(2, 8).unwrap();
        assert_eq!((index, frame.payload()), (1, &[3, 4, 5][..]));
        assert!(queue.entries[1].buf.is_none());
        assert_eq!(queue.len(), 5);
        queue.put_back(index, frame.into_buf());
        assert_eq!(cut(queue.segment(3, 2)), &[4, 5]);
        let (index, frame) = queue.take_whole(0, 2).unwrap();
        assert_eq!((index, frame.payload()), (0, &[1, 2][..]));
        queue.put_back(index, frame.into_buf());
    }

    #[test]
    fn take_put_back_keeps_positions() {
        let (mut queue, _) = queue(16, 2, 8);
        queue.push_bytes(&[1, 2]);
        queue.push_bytes(&[3, 4, 5]);
        let frame = queue.take(1);
        assert_eq!(frame.payload(), &[3, 4, 5]);
        assert_eq!(queue.len(), 5);
        queue.put_back(1, frame);
        assert_eq!(cut(queue.segment(3, 1)), &[4]);
        assert_eq!(queue.entries[0].end, 2);
        assert_eq!(queue.entries[1].end, 5);
    }

    #[test]
    fn end_positions_survive_u64_far_from_zero() {
        let (mut queue, _) = queue(16, 2, 8);
        queue.acked = u64::MAX - 8;
        queue.end = queue.acked;
        queue.push_bytes(&[1, 2, 3]);
        queue.push_bytes(&[4, 5]);
        assert_eq!(queue.entries[0].end, u64::MAX - 5);
        assert_eq!(queue.entries[1].end, u64::MAX - 3);
        queue.ack(3);
        assert_eq!(cut(queue.segment(0, 1)), &[4]);
    }

    /// The queue releases its frames before its source handle: the pool outlives the last
    /// release.
    #[test]
    fn source_dropped_after_entries() {
        let pool = TxPool::new(64);
        let counts = pool.counts();
        let mut queue = TxQueue::new(8, 1, TxSource::new(pool));
        queue.push_bytes(&[1]);
        drop(queue);
        assert!(counts.pool_dropped());
        assert_eq!((counts.releases(), counts.released_after_drop()), (1, 0));
    }
}
