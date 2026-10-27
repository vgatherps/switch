#![cfg_attr(test, allow(unsafe_code))]

use alloc::collections::VecDeque;
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

struct Entry {
    buf: TxBuf,
    end: u64,
}

pub(crate) enum Seg<'a> {
    Empty,
    Whole(&'a TxBuf),
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
        assert!(buf.unique(), "cannot queue a shared TX buffer");
        let start = self.entries.back().map_or(self.acked, |entry| entry.end);
        let end = start
            .checked_add(buf.len().try_into().expect("TX buffer length exceeds u64"))
            .expect("TX queue position overflow");
        self.entries.push_back(Entry { buf, end });
    }

    pub(crate) fn tail_room(&mut self, sent: usize, cap: usize) -> Option<&mut [u8]> {
        let first_never_sent = self
            .acked
            .checked_add(sent.try_into().expect("sent byte count exceeds u64"))
            .expect("sent position overflow");
        let byte_room = self.byte_room();
        let entry = self.entries.back_mut()?;
        let start = entry
            .end
            .checked_sub(entry.buf.len().try_into().expect("TX buffer length exceeds u64"))
            .expect("TX frame start underflow");
        if first_never_sent > start || !entry.buf.unique() {
            return None;
        }
        let old_len = entry.buf.len();
        let room = entry
            .buf
            .capacity()
            .checked_sub(old_len)
            .expect("TX buffer length exceeds capacity")
            .min(byte_room)
            .min(cap);
        Some(&mut entry.buf.room_mut()[old_len..old_len + room])
    }

    pub(crate) fn commit_tail(&mut self, n: usize) {
        let byte_room = self.byte_room();
        let entry = self.entries.back_mut().expect("TX queue has no tail");
        let room = entry
            .buf
            .capacity()
            .checked_sub(entry.buf.len())
            .expect("TX buffer length exceeds capacity")
            .min(byte_room);
        assert!(n <= room, "TX tail commit exceeds room");
        let len = entry
            .buf
            .len()
            .checked_add(n)
            .expect("TX buffer length overflow");
        entry.buf.set_len(len);
        entry.end = entry
            .end
            .checked_add(n.try_into().expect("TX tail commit exceeds u64"))
            .expect("TX queue position overflow");
    }

    pub(crate) fn ack(&mut self, n: usize) {
        assert!(n <= self.len(), "ACK exceeds queued bytes");
        self.acked = self
            .acked
            .checked_add(n.try_into().expect("ACK count exceeds u64"))
            .expect("TX queue ACK position overflow");
        while self
            .entries
            .front()
            .is_some_and(|entry| entry.end <= self.acked)
        {
            self.entries.pop_front();
        }
        self.hint.set(0);
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
        let start = entry
            .end
            .checked_sub(entry.buf.len().try_into().expect("TX buffer length exceeds u64"))
            .expect("TX frame start underflow");
        if position == start
            && position >= self.acked
            && size >= entry.buf.len()
            && entry.buf.unique()
        {
            return Seg::Whole(&entry.buf);
        }
        let within: usize = position
            .checked_sub(start)
            .expect("TX segment precedes frame")
            .try_into()
            .expect("TX frame offset exceeds usize");
        let end = within
            .checked_add(size.min(entry.buf.len() - within))
            .expect("TX segment end overflow");
        Seg::Cut(&entry.buf.payload()[within..end])
    }

    fn locate(&self, position: u64, mut compared: impl FnMut()) -> usize {
        let contains = |index: usize, compared: &mut dyn FnMut()| {
            let entry = self.entries.get(index).expect("TX lookup index missing");
            let start = entry
                .end
                .checked_sub(entry.buf.len().try_into().expect("TX buffer length exceeds u64"))
                .expect("TX frame start underflow");
            compared();
            position >= start && position < entry.end
        };

        compared();
        if position < self.entries.front().expect("TX lookup on empty queue").end {
            return 0;
        }
        let hint: usize = self.hint.get().try_into().expect("TX hint exceeds usize");
        if hint < self.entries.len() && contains(hint, &mut compared) {
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
        let alloc = CountingAlloc::new(room, 64);
        let source = TxSource::new(alloc.clone());
        (TxQueue::new(byte_cap, frame_cap, source), alloc)
    }

    fn cut(seg: Seg<'_>) -> &[u8] {
        match seg {
            Seg::Cut(bytes) => bytes,
            _ => panic!("expected cut segment"),
        }
    }

    fn bytes(seg: Seg<'_>) -> &[u8] {
        match seg {
            Seg::Whole(buf) => buf.payload(),
            Seg::Cut(bytes) => bytes,
            Seg::Empty => panic!("expected payload segment"),
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
    fn tail_room_appends_only_before_send() {
        let (mut queue, _) = queue(8, 1, 6);
        queue.push_bytes(&[1, 2]);
        assert_eq!(queue.tail_room(0, 2).unwrap().len(), 2);
        queue.tail_room(0, 2).unwrap().copy_from_slice(&[3, 4]);
        queue.commit_tail(2);
        assert_eq!(queue.len(), 4);
        assert_eq!(bytes(queue.segment(0, 4)), &[1, 2, 3, 4]);
        assert!(queue.tail_room(1, 2).is_none());
    }

    #[test]
    fn tail_room_refuses_shared_frame() {
        let (mut queue, _) = queue(8, 1, 6);
        queue.push_bytes(&[1, 2]);
        let owner = queue.entries[0].buf.share_raw();
        assert!(queue.tail_room(0, 2).is_none());
        // SAFETY: share_raw added one reference owned by this test.
        unsafe { crate::phy::test_owner::release(owner) };
    }

    #[test]
    fn lookup_head_hint_next_and_binary_search() {
        let (mut queue, _) = queue(64, 32, 2);
        for byte in 0..24 {
            queue.push_bytes(&[byte]);
        }
        assert_eq!(bytes(queue.segment(0, 1)), &[0]);
        assert_eq!(queue.hint.get(), 0);
        assert_eq!(bytes(queue.segment(20, 1)), &[20]);
        assert_eq!(queue.hint.get(), 20);
        assert_eq!(bytes(queue.segment(20, 1)), &[20]);
        assert_eq!(bytes(queue.segment(21, 1)), &[21]);
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
    fn segment_whole_only_when_unique_and_fits() {
        let (mut queue, _) = queue(8, 1, 8);
        queue.push_bytes(&[1, 2, 3]);
        assert!(matches!(queue.segment(0, 3), Seg::Whole(_)));
        assert_eq!(cut(queue.segment(0, 2)), &[1, 2]);
        let owner = queue.entries[0].buf.share_raw();
        assert_eq!(cut(queue.segment(0, 3)), &[1, 2, 3]);
        // SAFETY: share_raw added one reference owned by this test.
        unsafe { crate::phy::test_owner::release(owner) };
        assert!(matches!(queue.segment(0, 3), Seg::Whole(_)));
    }

    #[test]
    fn segment_cut_never_spans_frames() {
        let (mut queue, _) = queue(16, 2, 8);
        queue.push_bytes(&[1, 2]);
        queue.push_bytes(&[3, 4]);
        assert_eq!(cut(queue.segment(1, 3)), &[2]);
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
        static OPS: TxBufOps = TxBufOps { release, share, unique };
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
                Some(unsafe { TxBuf::from_raw_parts(owner, &OPS, self.origin(), data, 64, 64) })
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
