#![cfg_attr(test, allow(unsafe_code))]

use alloc::{collections::VecDeque, vec::Vec};
use core::cell::Cell;

use crate::phy::{TxBuf, TxSource};

pub(crate) struct TxQueue {
    entries: VecDeque<Entry>,
    acked: u64,
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

pub(crate) enum Seg<'a> {
    Empty,
    Whole { index: u32 },
    Cut(&'a [u8]),
}

impl TxQueue {
    pub(crate) fn new(byte_cap: usize, frame_cap: usize, source: TxSource) -> TxQueue {
        TxQueue {
            entries: VecDeque::with_capacity(frame_cap),
            acked: 0,
            byte_cap,
            frame_cap,
            hint: Cell::new(0),
            source,
        }
    }

    pub(crate) fn source(&self) -> &TxSource {
        &self.source
    }

    pub(crate) fn len(&self) -> usize {
        let end = self.entries.back().map_or(self.acked, |entry| entry.end);
        end.checked_sub(self.acked)
            .expect("TX queue end precedes ACK position")
            .try_into()
            .expect("TX queue length exceeds usize")
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn frames(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn byte_capacity(&self) -> usize {
        self.byte_cap
    }

    pub(crate) fn byte_room(&self) -> usize {
        self.byte_cap
            .checked_sub(self.len())
            .expect("TX queue exceeds byte capacity")
    }

    pub(crate) fn frame_room(&self) -> usize {
        self.frame_cap
            .checked_sub(self.frames())
            .expect("TX queue exceeds frame capacity")
    }

    pub(crate) fn push(&mut self, buf: TxBuf) {
        assert!(buf.len() != 0, "cannot queue an empty TX buffer");
        assert!(buf.len() <= self.byte_room(), "TX buffer exceeds byte room");
        assert!(self.frame_room() != 0, "TX queue has no frame room");
        let start = self.entries.back().map_or(self.acked, |entry| entry.end);
        let end = start
            .checked_add(buf.len().try_into().expect("TX buffer length exceeds u64"))
            .expect("TX queue position overflow");
        self.entries.push_back(Entry {
            buf: Some(buf),
            end,
        });
    }

    pub(crate) fn append_tail(&mut self, sent: usize, data: &[u8], cap: usize) -> usize {
        let first_never_sent = self
            .acked
            .checked_add(sent.try_into().expect("sent byte count exceeds u64"))
            .expect("sent position overflow");
        let byte_room = self.byte_room();
        let Some(entry) = self.entries.back_mut() else {
            return 0;
        };
        let buf = entry.buf.as_mut().expect("TX queue tail is taken");
        let start = entry
            .end
            .checked_sub(buf.len().try_into().expect("TX buffer length exceeds u64"))
            .expect("TX frame start underflow");
        if first_never_sent > start || !buf.unique() {
            return 0;
        }
        let old_len = buf.len();
        let cap_room = cap
            .checked_sub(old_len)
            .expect("TX tail length exceeds append cap");
        let count = buf
            .capacity()
            .checked_sub(old_len)
            .expect("TX buffer length exceeds capacity")
            .min(byte_room)
            .min(cap_room)
            .min(data.len());
        let new_len = old_len
            .checked_add(count)
            .expect("TX buffer length overflow");
        buf.room_mut()[old_len..new_len].copy_from_slice(&data[..count]);
        buf.set_len(new_len);
        entry.end = entry
            .end
            .checked_add(count.try_into().expect("TX tail append exceeds u64"))
            .expect("TX queue position overflow");
        count
    }

    pub(crate) fn tail_room(&self, sent: usize, cap: usize) -> usize {
        let first_never_sent = self
            .acked
            .checked_add(sent.try_into().expect("sent byte count exceeds u64"))
            .expect("sent position overflow");
        let Some(entry) = self.entries.back() else {
            return 0;
        };
        let buf = entry.buf.as_ref().expect("TX queue tail is taken");
        let start = entry
            .end
            .checked_sub(buf.len().try_into().expect("TX buffer length exceeds u64"))
            .expect("TX frame start underflow");
        if first_never_sent > start || !buf.unique() {
            return 0;
        }
        cap.checked_sub(buf.len())
            .expect("TX tail length exceeds append cap")
            .min(buf.capacity().checked_sub(buf.len()).expect("TX buffer length exceeds capacity"))
            .min(self.byte_room())
    }

    pub(crate) fn ack(&mut self, n: usize) {
        assert!(n <= self.len(), "ACK exceeds queued bytes");
        self.acked = self
            .acked
            .checked_add(n.try_into().expect("ACK count exceeds u64"))
            .expect("TX queue ACK position overflow");
        let mut popped = 0u32;
        while self
            .entries
            .front()
            .is_some_and(|entry| entry.end <= self.acked)
        {
            self.entries.pop_front();
            popped = popped.checked_add(1).expect("TX queue pop count exceeds u32");
        }
        let hint = self.hint.get();
        self.hint.set(if hint <= popped {
            0
        } else {
            hint.checked_sub(popped).expect("TX queue hint underflow")
        });
    }

    pub(crate) fn clear(&mut self) {
        if let Some(entry) = self.entries.back() {
            self.acked = entry.end;
        }
        self.entries.clear();
        self.hint.set(0);
    }

    pub(crate) fn segment(&self, offset: usize, size: usize) -> Seg<'_> {
        if size == 0 || offset >= self.len() {
            return Seg::Empty;
        }
        let position = self
            .acked
            .checked_add(offset.try_into().expect("TX segment offset exceeds u64"))
            .expect("TX segment position overflow");
        let index = self.locate(position, || {});
        self.hint
            .set(index.try_into().expect("TX queue index exceeds u32"));
        let entry = self.entries.get(index).expect("TX segment entry missing");
        let buf = entry.buf.as_ref().expect("TX segment frame is taken");
        let start = entry
            .end
            .checked_sub(buf.len().try_into().expect("TX buffer length exceeds u64"))
            .expect("TX frame start underflow");
        if position == start && position >= self.acked && size >= buf.len() && buf.unique() {
            return Seg::Whole {
                index: index.try_into().expect("TX queue index exceeds u32"),
            };
        }
        let within: usize = position
            .checked_sub(start)
            .expect("TX segment precedes frame")
            .try_into()
            .expect("TX frame offset exceeds usize");
        let end = within
            .checked_add(size.min(buf.len() - within))
            .expect("TX segment end overflow");
        Seg::Cut(&buf.payload()[within..end])
    }

    pub(crate) fn segment_spans_frames(&self, offset: usize, size: usize) -> bool {
        if size == 0 || offset >= self.len() {
            return false;
        }
        let position = self
            .acked
            .checked_add(offset.try_into().expect("TX segment offset exceeds u64"))
            .expect("TX segment position overflow");
        let index = self.locate(position, || {});
        let entry = self.entries.get(index).expect("TX segment entry missing");
        position
            .checked_add(size.try_into().expect("TX segment size exceeds u64"))
            .expect("TX segment end overflow")
            > entry.end
    }

    pub(crate) fn copy_segment(&self, offset: usize, size: usize, output: &mut Vec<u8>) {
        assert!(offset <= self.len(), "TX segment offset exceeds queued bytes");
        assert!(size <= self.len() - offset, "TX segment size exceeds queued bytes");
        output.clear();
        output.reserve(size);
        if size == 0 {
            return;
        }

        let position = self
            .acked
            .checked_add(offset.try_into().expect("TX segment offset exceeds u64"))
            .expect("TX segment position overflow");
        let mut index = self.locate(position, || {});
        let mut copied = 0usize;
        while copied < size {
            let entry = self.entries.get(index).expect("TX segment entry missing");
            let buf = entry.buf.as_ref().expect("TX segment frame is taken");
            let start = entry
                .end
                .checked_sub(buf.len().try_into().expect("TX buffer length exceeds u64"))
                .expect("TX frame start underflow");
            let within: usize = position
                .checked_add(copied.try_into().expect("TX copied length exceeds u64"))
                .expect("TX copy position overflow")
                .checked_sub(start)
                .expect("TX copy position precedes frame")
                .try_into()
                .expect("TX frame offset exceeds usize");
            let count = (size - copied).min(buf.len() - within);
            output.extend_from_slice(&buf.payload()[within..within + count]);
            copied = copied.checked_add(count).expect("TX copied length overflow");
            index = index.checked_add(1).expect("TX queue index overflow");
        }
    }

    pub(crate) fn take(&mut self, index: u32) -> TxBuf {
        let index: usize = index.try_into().expect("TX queue index exceeds usize");
        self.entries
            .get_mut(index)
            .expect("TX queue take index missing")
            .buf
            .take()
            .expect("TX queue frame is already taken")
    }

    pub(crate) fn put_back(&mut self, index: u32, buf: TxBuf) {
        let index: usize = index.try_into().expect("TX queue index exceeds usize");
        let previous_end = if index == 0 {
            self.acked
        } else {
            self.entries
                .get(index - 1)
                .expect("TX queue previous entry missing")
                .end
        };
        let entry = self.entries.get_mut(index).expect("TX queue put index missing");
        assert!(entry.buf.is_none(), "TX queue frame is not taken");
        let expected_len: usize = entry
            .end
            .checked_sub(previous_end)
            .expect("TX queue entry end precedes its start")
            .try_into()
            .expect("TX queue frame length exceeds usize");
        assert_eq!(buf.origin(), self.source.origin(), "TX queue frame has wrong origin");
        assert_eq!(buf.len(), expected_len, "TX queue frame has wrong length");
        entry.buf = Some(buf);
    }

    fn locate(&self, position: u64, mut compared: impl FnMut()) -> usize {
        let contains = |index: usize, compared: &mut dyn FnMut()| {
            let entry = self.entries.get(index).expect("TX lookup index missing");
            let buf = entry.buf.as_ref().expect("TX lookup frame is taken");
            let start = entry
                .end
                .checked_sub(buf.len().try_into().expect("TX buffer length exceeds u64"))
                .expect("TX frame start underflow");
            compared();
            position >= start && position < entry.end
        };

        compared();
        if position < self.entries.front().expect("TX lookup on empty queue").end {
            return 0;
        }
        let hint: usize = self.hint.get().try_into().expect("TX hint exceeds usize");
        if hint != 0 && hint < self.entries.len() && contains(hint, &mut compared) {
            return hint;
        }
        let next = hint.checked_add(1).expect("TX hint index overflow");
        if next < self.entries.len() && contains(next, &mut compared) {
            return next;
        }

        let mut low = 0;
        let mut high = self.entries.len();
        while low < high {
            let mid = low + (high - low) / 2;
            compared();
            if self.entries.get(mid).expect("TX binary lookup entry missing").end <= position {
                low = mid.checked_add(1).expect("TX binary lookup index overflow");
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
    use crate::phy::test_owner::CountingAlloc;
    use crate::phy::{TxAlloc, TxBufOps};
    use alloc::{boxed::Box, rc::Rc};
    use core::{cell::Cell, ptr::NonNull};

    fn queue(byte_cap: usize, frame_cap: usize, room: usize) -> (TxQueue, Rc<CountingAlloc>) {
        let alloc = CountingAlloc::new(room);
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
        queue.push_bytes(&[1, 2, 3]);
        queue.push_bytes(&[4, 5]);
        assert_eq!(queue.entries[0].end, u64::MAX - 5);
        assert_eq!(queue.entries[1].end, u64::MAX - 3);
        queue.ack(3);
        assert_eq!(cut(queue.segment(0, 1)), &[4]);
    }

    #[test]
    fn source_dropped_after_entries() {
        struct DropState {
            source_dropped: Cell<bool>,
            buffer_released_first: Cell<bool>,
        }
        struct Alloc {
            state: Rc<DropState>,
        }
        struct Owner {
            state: Rc<DropState>,
            bytes: Box<[u8]>,
        }
        unsafe fn release(owner: NonNull<()>) {
            // SAFETY: Alloc::alloc creates this pointer with Box::into_raw once.
            let owner = unsafe { Box::from_raw(owner.cast::<Owner>().as_ptr()) };
            owner
                .state
                .buffer_released_first
                .set(!owner.state.source_dropped.get());
        }
        unsafe fn share(_: NonNull<()>) {
            panic!("test owner is never shared");
        }
        unsafe fn unique(_: NonNull<()>) -> bool {
            true
        }
        static OPS: TxBufOps = TxBufOps {
            release,
            share,
            unique,
        };
        impl Drop for Alloc {
            fn drop(&mut self) {
                self.state.source_dropped.set(true);
            }
        }
        impl TxAlloc for Alloc {
            fn alloc(&self) -> Option<TxBuf> {
                let mut owner = Box::new(Owner {
                    state: self.state.clone(),
                    bytes: alloc::vec![0; 128].into_boxed_slice(),
                });
                let data = NonNull::new(owner.bytes.as_mut_ptr()).unwrap();
                // SAFETY: 64 bytes are headroom and the other 64 bytes are payload room.
                let data = unsafe { data.add(64) };
                let owner = NonNull::new(Box::into_raw(owner).cast::<()>()).unwrap();
                // SAFETY: owner owns the allocation until OPS releases it.
                Some(unsafe { TxBuf::from_raw_parts(owner, &OPS, self.origin(), data, 64) })
            }

            fn origin(&self) -> NonNull<()> {
                NonNull::from(self).cast()
            }
        }

        let state = Rc::new(DropState {
            source_dropped: Cell::new(false),
            buffer_released_first: Cell::new(false),
        });
        let source = TxSource::new(Rc::new(Alloc { state: state.clone() }));
        let mut queue = TxQueue::new(8, 1, source);
        queue.push_bytes(&[1]);
        drop(queue);
        assert!(state.buffer_released_first.get());
        assert!(state.source_dropped.get());
    }
}
