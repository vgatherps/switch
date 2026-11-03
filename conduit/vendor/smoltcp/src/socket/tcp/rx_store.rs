use alloc::collections::VecDeque;
use core::ops::Range;

use crate::phy::RxBuf;

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Decision {
    New(NewView),
    Duplicate(Range<usize>),
    Refused,
}

/// A new view at `index` holding `kept`, replacing the `released` views it covers and cutting the
/// front off the view after them when `trimmed`.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct NewView {
    kept: Range<usize>,
    index: usize,
    released: usize,
    trimmed: bool,
}

impl Decision {
    pub(super) fn kept(&self) -> Option<Range<usize>> {
        match self {
            Self::New(view) => Some(view.kept.clone()),
            Self::Duplicate(_) | Self::Refused => None,
        }
    }
}

#[derive(Debug)]
struct View {
    start: usize,
    len: usize,
    frame: Option<RxBuf>,
}

impl View {
    fn range(&self) -> Range<usize> {
        self.start..self.end()
    }

    fn end(&self) -> usize {
        self.start
            .checked_add(self.len)
            .expect("view end exceeds usize")
    }
}

#[derive(Debug)]
pub(super) struct RxStore {
    views: VecDeque<View>,
    head: usize,
    in_order: usize,
    pending: bool,
}

impl RxStore {
    pub(super) fn new() -> Self {
        Self {
            views: VecDeque::new(),
            head: 0,
            in_order: 0,
            pending: false,
        }
    }

    fn absolute(&self, offset: usize) -> usize {
        self.head.checked_add(offset).expect("receive offset exceeds usize")
    }

    fn relative(&self, offset: usize) -> usize {
        offset.checked_sub(self.head).expect("view precedes read head")
    }

    fn absolute_range(&self, range: Range<usize>) -> Range<usize> {
        self.absolute(range.start)..self.absolute(range.end)
    }

    fn relative_range(&self, range: Range<usize>) -> Range<usize> {
        self.relative(range.start)..self.relative(range.end)
    }

    pub(super) fn decide(&self, offset: usize, len: usize) -> Decision {
        assert!(len > 0);
        let offset = self.absolute(offset);
        let end = offset.checked_add(len).expect("segment end exceeds usize");
        // A segment at or past the end of the last view (the next in-order segment in the
        // common case) overlaps nothing, so it needs no search.
        let after_last = self.views.back().is_none_or(|view| view.end() <= offset);
        if !after_last && self.is_held(offset..end) {
            return Decision::Duplicate(self.relative_range(offset..end));
        }

        let (start, index) = if after_last {
            (offset, self.held())
        } else {
            let before_offset = self.views.partition_point(|view| view.start < offset);
            let start = before_offset
                .checked_sub(1)
                .map(|index| &self.views[index])
                .map_or(offset, |view| offset.max(view.end()));
            (start, self.views.partition_point(|view| view.start < start))
        };
        let mut released = 0usize;
        let mut trimmed = false;
        for view in self.views.iter().skip(index) {
            if view.start >= end {
                break;
            }
            if view.start >= start {
                if view.end() <= end {
                    released = released.checked_add(1).expect("view count exceeds usize");
                } else {
                    trimmed = true;
                    break;
                }
            }
        }

        Decision::New(NewView {
            kept: self.relative_range(start..end),
            index,
            released,
            trimmed,
        })
    }

    fn is_held(&self, range: Range<usize>) -> bool {
        let mut covered = range.start;
        let index = self
            .views
            .partition_point(|view| view.start <= range.start)
            .checked_sub(1)
            .unwrap_or(0);
        for view in self.views.iter().skip(index) {
            if view.start > covered {
                break;
            }
            covered = covered.max(view.end());
            if covered >= range.end {
                return true;
            }
        }
        false
    }

    /// The store must be unchanged since decide returned.
    pub(super) fn place(&mut self, offset: usize, decision: Decision) {
        self.assert_attached();
        let offset = self.absolute(offset);
        let new = match decision {
            Decision::New(new) => new,
            Decision::Duplicate(range) => {
                debug_assert!(
                    {
                        let range = self.absolute_range(range);
                        range.start == offset && range.start < range.end && self.is_held(range)
                    },
                    "stale duplicate decision"
                );
                return;
            }
            Decision::Refused => return,
        };
        let kept = self.absolute_range(new.kept);
        debug_assert!(
            offset <= kept.start && kept.start < kept.end,
            "stale new decision"
        );
        let index = new.index;
        if index == self.held() && new.released == 0 && !new.trimmed {
            debug_assert!(
                self.views.back().is_none_or(|view| view.end() <= kept.start),
                "stale new decision: left neighbor"
            );
            self.views.push_back(View {
                start: kept.start,
                len: kept.end.checked_sub(kept.start).expect("empty kept range"),
                frame: None,
            });
            if kept.start == self.absolute(self.in_order_len()) {
                self.in_order = self.relative(kept.end);
            }
            self.pending = true;
            return;
        }
        let released_end = index
            .checked_add(new.released)
            .expect("view index exceeds usize");
        debug_assert!(released_end <= self.held(), "stale new decision: released views");
        debug_assert!(
            index
                .checked_sub(1)
                .is_none_or(|left| self.views[left].end() <= kept.start),
            "stale new decision: left neighbor"
        );
        debug_assert!(
            self.views
                .range(index..released_end)
                .all(|view| view.start >= kept.start && view.end() <= kept.end),
            "stale new decision: released views"
        );
        debug_assert!(
            self.views.get(released_end).is_none_or(|view| {
                if new.trimmed {
                    view.start < kept.end && view.end() > kept.end
                } else {
                    view.start >= kept.end
                }
            }),
            "stale new decision: right neighbor"
        );
        if new.released != 0 {
            drop(self.views.drain(index..released_end));
        }
        if new.trimmed {
            let view = &mut self.views[index];
            let head = kept
                .end
                .checked_sub(view.start)
                .expect("trim ends before view start");
            view.frame.as_mut().unwrap().narrow(head..view.len);
            view.len = view.len.checked_sub(head).expect("trim exceeds view");
            view.start = kept.end;
        }
        self.views.insert(
            index,
            View {
                start: kept.start,
                len: kept.end.checked_sub(kept.start).expect("empty kept range"),
                frame: None,
            },
        );
        if kept.start <= self.absolute(self.in_order_len()) {
            let mut end = kept.end;
            let mut next = index.checked_add(1).expect("view index exceeds usize");
            while let Some(view) = self.views.get(next).filter(|view| view.start == end) {
                end = view.end();
                next = next.checked_add(1).expect("view index exceeds usize");
            }
            self.in_order = self.relative(end);
        }
        self.pending = true;
    }

    pub(super) fn attach(&mut self, frame: RxBuf) {
        let view = self
            .views
            .iter_mut()
            .rev()
            .find(|view| view.frame.is_none())
            .expect("no pending receive view");
        assert!(self.pending, "no pending receive view");
        assert_eq!(
            frame.len(),
            view.len,
            "frame length differs from pending view"
        );
        view.frame = Some(frame);
        self.pending = false;
    }

    fn assert_attached(&self) {
        assert!(!self.pending, "pending receive view");
    }

    #[cfg(test)]
    pub(super) fn has_pending(&self) -> bool {
        self.pending
    }

    pub(super) fn in_order_len(&self) -> usize {
        self.in_order
    }

    #[cfg(test)]
    pub(super) fn out_of_order_len(&self) -> usize {
        let end = self.absolute(self.in_order_len());
        if self.views.back().is_none_or(|view| view.start < end) {
            return 0;
        }
        self.views
            .iter()
            .rev()
            .take_while(|view| view.start >= end)
            .try_fold(0usize, |total, view| total.checked_add(view.len))
            .expect("held byte count exceeds usize")
    }

    pub(super) fn out_of_order_span(&self) -> usize {
        self.views.back().map_or(0, |view| {
            view.end()
                .checked_sub(self.absolute(self.in_order_len()))
                .expect("last view ends before in-order data")
        })
    }

    pub(super) fn held(&self) -> usize {
        self.views.len()
    }


    pub(super) fn ranges(&self) -> impl Iterator<Item = (usize, usize)> + '_ {
        self.views.iter().map(|view| (self.relative(view.start), self.relative(view.end())))
    }

    pub(super) fn out_of_order_ranges(&self) -> impl Iterator<Item = (usize, usize)> + '_ {
        let in_order_end = self.absolute(self.in_order_len());
        let first = if self.out_of_order_span() == 0 {
            self.views.len()
        } else {
            self.views.partition_point(|view| view.start < in_order_end)
        };
        let mut views = self.views.range(first..).peekable();
        core::iter::from_fn(move || {
            let view = views.next()?;
            let start = view.start;
            let mut end = view.end();
            while let Some(next) = views.peek()
                && next.start == end
            {
                end = views.next().unwrap().end();
            }
            Some((
                start.checked_sub(in_order_end).expect("range starts before in-order end"),
                end.checked_sub(in_order_end).expect("range ends before in-order end"),
            ))
        })
    }

    #[inline]
    pub(super) fn pop_front(&mut self) -> Option<RxBuf> {
        let view = self.views.front()?;
        if view.start != self.head || view.frame.is_none() {
            return None;
        }
        let view = self.views.pop_front().unwrap();
        self.head = self.head.checked_add(view.len).expect("receive head exceeds usize");
        self.in_order = self.in_order.checked_sub(view.len).expect("frame exceeds in-order bytes");
        if self.views.is_empty() {
            self.head = 0;
        }
        view.frame
    }

    pub(super) fn clear(&mut self) {
        self.views.clear();
        self.head = 0;
        self.in_order = 0;
        self.pending = false;
    }

    /// Views past the in-order data: one frame each.
    pub(super) fn out_of_order_views(&self) -> usize {
        let end = self.absolute(self.in_order_len());
        self.views.len() - self.views.partition_point(|view| view.start < end)
    }

    /// Drop up to `limit` views past the in-order data, the farthest from it first, releasing
    /// their frames; returns how many it dropped. The in-order views stay as they are. The
    /// in-order data ends at a view boundary: a view that continues it would be in order itself.
    pub(super) fn drop_out_of_order(&mut self, limit: usize) -> usize {
        self.assert_attached();
        let end = self.absolute(self.in_order_len());
        let mut dropped = 0;
        while dropped < limit && self.views.back().is_some_and(|view| view.start >= end) {
            self.views.pop_back();
            dropped += 1;
        }
        debug_assert!(self.views.back().is_none_or(|view| view.start >= end || view.end() == end));
        dropped
    }

    pub(super) fn truncate_from(&mut self, offset: usize) {
        let offset = self.absolute(offset);
        assert!(
            self.views
                .iter()
                .all(|view| view.frame.is_some() || view.end() <= offset),
            "truncate cuts pending receive view"
        );
        while self.views.back().is_some_and(|view| view.start >= offset) {
            self.views.pop_back();
        }
        if let Some(view) = self.views.back_mut() {
            if view.end() > offset {
                let len = offset
                    .checked_sub(view.start)
                    .expect("truncate precedes view");
                view.frame.as_mut().unwrap().narrow(0..len);
                view.len = len;
            }
        }
        self.in_order = self.in_order_len().min(self.relative(offset));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::{rc::Rc, vec, vec::Vec};
    use core::cell::Cell;
    use rand::{Rng, SeedableRng, rngs::StdRng};

    fn drain_bytes(store: &mut RxStore) -> Vec<u8> {
        let mut bytes = Vec::new();
        while let Some(frame) = store.pop_front() {
            bytes.extend_from_slice(frame.as_slice());
        }
        bytes
    }

    fn counted_frame(bytes: &[u8]) -> (RxBuf, Rc<Cell<usize>>) {
        let releases = Rc::new(Cell::new(0));
        (
            RxBuf::counted_frame(bytes.to_vec(), 0, releases.clone()),
            releases,
        )
    }

    fn place_attach(store: &mut RxStore, offset: usize, mut frame: RxBuf, decision: Decision) {
        let kept = decision.kept();
        store.place(offset, decision);
        if let Some(kept) = kept {
            frame.narrow(
                kept.start.checked_sub(offset).unwrap()..kept.end.checked_sub(offset).unwrap(),
            );
            store.attach(frame);
        }
    }

    fn counted_frame_with_room(bytes: &[u8], room: usize) -> (RxBuf, Rc<Cell<usize>>) {
        let releases = Rc::new(Cell::new(0));
        (
            RxBuf::counted_frame(bytes.to_vec(), room, releases.clone()),
            releases,
        )
    }

    fn insert_with_room(
        store: &mut RxStore,
        offset: usize,
        bytes: &[u8],
        room: usize,
    ) -> Rc<Cell<usize>> {
        let (frame, releases) = counted_frame_with_room(bytes, room);
        let decision = store.decide(offset, bytes.len());
        assert!(matches!(decision, Decision::New(_)));
        place_attach(store, offset, frame, decision);
        assert_eq!(releases.get(), 0);
        releases
    }

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn in_order_count_crosses_sequence_and_u32_boundaries() {
        for prefix in [i32::MAX as usize, u32::MAX as usize] {
            // Model a large already-queued prefix without allocating its backing frames.
            let mut store = RxStore::new();
            store.in_order = prefix;
            store.views.push_back(View {
                start: 0,
                len: prefix,
                frame: Some(RxBuf::from_vec(vec![0])),
            });
            insert_with_room(&mut store, prefix, b"ab", 0);
            assert_eq!(store.in_order_len(), prefix + 2);
            insert_with_room(&mut store, prefix + 4, b"ef", 0);
            assert_eq!(store.out_of_order_ranges().collect::<Vec<_>>(), vec![(2, 4)]);
            insert_with_room(&mut store, prefix + 2, b"cd", 0);
            assert_eq!(store.in_order_len(), prefix + 6);
            assert_eq!(store.out_of_order_span(), 0);
        }
    }

    #[test]
    fn pop_front_requires_an_attached_in_order_frame() {
        let mut store = RxStore::new();
        assert!(store.pop_front().is_none());
        insert(&mut store, 4, b"ef");
        assert!(store.pop_front().is_none());
        let decision = store.decide(0, 4);
        store.place(0, decision);
        assert!(store.pop_front().is_none());
        store.attach(RxBuf::from_vec(b"abcd".to_vec()));
        assert_eq!(store.pop_front().unwrap().as_slice(), b"abcd");
        assert_eq!(store.pop_front().unwrap().as_slice(), b"ef");
        assert!(store.pop_front().is_none());
    }

    #[test]
    fn draining_frames_keeps_out_of_order_ranges() {
        let mut store = RxStore::new();
        for (offset, bytes) in [(0, &b"ab"[..]), (2, b"cde"), (9, b"jk"), (15, b"pq")] {
            insert(&mut store, offset, bytes);
        }
        assert_eq!(store.pop_front().unwrap().as_slice(), b"ab");
        assert_eq!(store.head, 2);
        assert_eq!(ranges(&store), vec![(0, 3), (7, 9), (13, 15)]);
        assert_eq!(store.pop_front().unwrap().as_slice(), b"cde");
        assert_eq!(store.head, 5);
        assert_eq!(ranges(&store), vec![(4, 6), (10, 12)]);
        assert_eq!(store.out_of_order_span(), 12);
        assert!(store.pop_front().is_none());
        insert(&mut store, 0, b"fghi");
        assert_eq!(store.in_order_len(), 6);
        assert_eq!(drain_bytes(&mut store), b"fghijk");
        assert_eq!(ranges(&store), vec![(4, 6)]);
    }

    #[test]
    fn pop_front_after_truncate_and_clear() {
        let mut store = RxStore::new();
        insert(&mut store, 0, b"ab");
        insert(&mut store, 2, b"cdef");
        insert(&mut store, 9, b"jk");
        assert_eq!(store.pop_front().unwrap().as_slice(), b"ab");
        store.truncate_from(2);
        assert_eq!(ranges(&store), vec![(0, 2)]);
        assert_eq!(store.pop_front().unwrap().as_slice(), b"cd");
        assert_eq!(store.head, 0);
        assert!(store.pop_front().is_none());
        insert(&mut store, 0, b"ef");
        insert(&mut store, 5, b"jk");
        drop(store.pop_front().unwrap());
        assert_eq!(store.head, 2);
        store.clear();
        assert_eq!(store.head, 0);
        assert!(store.pop_front().is_none());
        insert(&mut store, 0, b"lm");
        assert_eq!(store.pop_front().unwrap().as_slice(), b"lm");
    }

    fn insert(store: &mut RxStore, offset: usize, bytes: &[u8]) -> Rc<Cell<usize>> {
        let (frame, releases) = counted_frame(bytes);
        let decision = store.decide(offset, bytes.len());
        assert!(matches!(decision, Decision::New(_)));
        place_attach(store, offset, frame, decision);
        assert_eq!(releases.get(), 0);
        releases
    }

    fn ranges(store: &RxStore) -> Vec<(usize, usize)> {
        store.ranges().collect()
    }

    fn insert_stamped(
        store: &mut RxStore,
        offset: usize,
        bytes: &[u8],
        stamp: u64,
    ) -> Rc<Cell<usize>> {
        let (mut frame, releases) = counted_frame(bytes);
        frame.set_rx_timestamp(stamp);
        let decision = store.decide(offset, bytes.len());
        assert!(matches!(decision, Decision::New(_)));
        place_attach(store, offset, frame, decision);
        releases
    }

    fn drain_stamped(store: &mut RxStore) -> Vec<(Vec<u8>, u64)> {
        let mut frames = Vec::new();
        while let Some(frame) = store.pop_front() {
            frames.push((frame.as_slice().to_vec(), frame.rx_timestamp()));
        }
        frames
    }

    #[test]
    fn covering_segment_replaces_released_timestamps() {
        let mut store = RxStore::new();
        let first = insert_stamped(&mut store, 3, b"def", 1);
        let second = insert_stamped(&mut store, 6, b"ghi", 2);
        let replacement = insert_stamped(&mut store, 0, b"abcdefghi", 3);
        assert_eq!(store.held(), 1);
        assert_eq!((first.get(), second.get(), replacement.get()), (1, 1, 0));
        assert_eq!(drain_stamped(&mut store), vec![(b"abcdefghi".to_vec(), 3)]);
        assert_eq!((first.get(), second.get(), replacement.get()), (1, 1, 1));
    }

    #[test]
    fn right_neighbor_trim_keeps_timestamp() {
        let mut store = RxStore::new();
        let right = insert_stamped(&mut store, 3, b"defghi", 1);
        insert_stamped(&mut store, 0, b"ABCDE", 2);
        assert_eq!(ranges(&store), vec![(0, 5), (5, 9)]);
        assert_eq!(right.get(), 0);
        assert_eq!(
            drain_stamped(&mut store),
            vec![(b"ABCDE".to_vec(), 2), (b"fghi".to_vec(), 1)]
        );
        assert_eq!(right.get(), 1);
    }

    #[test]
    fn fin_truncate_keeps_timestamp() {
        let mut store = RxStore::new();
        let releases = insert_stamped(&mut store, 0, b"abcdef", 1);
        store.truncate_from(2);
        assert_eq!(releases.get(), 0);
        assert_eq!(drain_stamped(&mut store), vec![(b"ab".to_vec(), 1)]);
        assert_eq!(releases.get(), 1);
    }

    #[test]
    fn in_order_inserts_match_general_path() {
        let mut x = RxStore::new();
        let x_releases = [
            insert(&mut x, 0, b"abc"),
            insert(&mut x, 3, b"defg"),
            insert(&mut x, 7, b"hijkl"),
        ];
        let mut y = RxStore::new();
        let y_last = insert(&mut y, 7, b"hijkl");
        let y_middle = insert(&mut y, 3, b"defg");
        let y_first = insert(&mut y, 0, b"abc");
        let y_releases = [y_first, y_middle, y_last];

        assert_eq!(ranges(&x), vec![(0, 3), (3, 7), (7, 12)]);
        assert_eq!(ranges(&x), ranges(&y));
        for (index, expected) in [b"abc".as_slice(), b"defg", b"hijkl"].into_iter().enumerate() {
            let x_frame = x.pop_front().unwrap();
            let y_frame = y.pop_front().unwrap();
            assert_eq!(x_frame.as_slice(), expected);
            assert_eq!(y_frame.as_slice(), expected);
            assert_eq!(ranges(&x), ranges(&y));
            assert_eq!(x.in_order_len(), y.in_order_len());
            assert_eq!(x.out_of_order_len(), 0);
            assert_eq!(y.out_of_order_len(), 0);
            assert_eq!(x.held(), 2 - index);
            assert_eq!(y.held(), 2 - index);
            assert_eq!(x_releases[index].get(), 0);
            assert_eq!(y_releases[index].get(), 0);
            drop((x_frame, y_frame));
            assert_eq!(x_releases[index].get(), 1);
            assert_eq!(y_releases[index].get(), 1);
        }
        assert!(x.pop_front().is_none());
        assert!(y.pop_front().is_none());
        assert_eq!(x.in_order_len(), 0);
        assert_eq!(y.in_order_len(), 0);
        drop((x, y));
        assert!(x_releases.iter().all(|count| count.get() == 1));
        assert!(y_releases.iter().all(|count| count.get() == 1));
    }

    #[test]
    fn drain_all_in_order_releases_each_frame_once() {
        let mut store = RxStore::new();
        let first = insert(&mut store, 0, b"abc");
        let second = insert(&mut store, 3, b"defg");
        let third = insert(&mut store, 7, b"hijkl");
        drop(drain_bytes(&mut store));
        assert_eq!(store.held(), 0);
        assert_eq!(store.in_order_len(), 0);
        assert_eq!(store.out_of_order_len(), 0);
        assert_eq!((first.get(), second.get(), third.get()), (1, 1, 1));
        assert!(store.pop_front().is_none());
        drop(store);
        assert_eq!((first.get(), second.get(), third.get()), (1, 1, 1));

        let mut store = RxStore::new();
        let first = insert(&mut store, 0, b"abc");
        let second = insert(&mut store, 3, b"defg");
        let later = insert(&mut store, 10, b"kl");
        drop(drain_bytes(&mut store));
        assert_eq!(store.held(), 1);
        assert_eq!(store.in_order_len(), 0);
        assert_eq!(store.out_of_order_len(), 2);
        assert_eq!(ranges(&store), vec![(3, 5)]);
        assert_eq!(store.views[0].frame.as_ref().unwrap().as_slice(), b"kl");
        assert!(store.pop_front().is_none());
        assert_eq!((first.get(), second.get(), later.get()), (1, 1, 0));
        drop(store);
        assert_eq!((first.get(), second.get(), later.get()), (1, 1, 1));
    }

    #[test]
    fn out_of_order_ranges_in_order_only() {
        let mut store = RxStore::new();
        insert(&mut store, 0, b"abcd");
        insert(&mut store, 4, b"ef");
        assert_eq!(store.out_of_order_ranges().next(), None);
    }

    #[test]
    fn out_of_order_ranges_merge_adjacent_views() {
        let mut store = RxStore::new();
        insert(&mut store, 0, b"ab");
        insert(&mut store, 4, b"ef");
        insert(&mut store, 6, b"gh");
        insert(&mut store, 10, b"kl");
        insert(&mut store, 12, b"mn");
        assert_eq!(store.out_of_order_ranges().collect::<Vec<_>>(), vec![(2, 6), (8, 12)]);
    }

    #[test]
    fn out_of_order_ranges_after_partial_hole_fill() {
        let mut store = RxStore::new();
        insert(&mut store, 0, b"ab");
        insert(&mut store, 6, b"gh");
        insert(&mut store, 10, b"kl");
        insert(&mut store, 2, b"cd");
        assert_eq!(store.in_order_len(), 4);
        assert_eq!(store.out_of_order_ranges().collect::<Vec<_>>(), vec![(2, 4), (6, 8)]);
    }

    #[test]
    fn out_of_order_ranges_after_draining_in_order_frames() {
        let mut store = RxStore::new();
        insert(&mut store, 0, b"ab");
        insert(&mut store, 2, b"cd");
        insert(&mut store, 6, b"gh");
        insert(&mut store, 10, b"kl");
        let expected = vec![(2, 4), (6, 8)];
        assert_eq!(store.out_of_order_ranges().collect::<Vec<_>>(), expected);
        drop(store.pop_front().unwrap());
        assert_eq!(store.in_order_len(), 2);
        assert_eq!(store.out_of_order_ranges().collect::<Vec<_>>(), expected);
        drop(store.pop_front().unwrap());
        assert_eq!(store.in_order_len(), 0);
        assert_eq!(store.out_of_order_ranges().collect::<Vec<_>>(), expected);
    }

    #[test]
    fn out_of_order_span_tracks_last_view_end_from_in_order_end() {
        let mut store = RxStore::new();
        assert_eq!(store.out_of_order_span(), 0);
        insert(&mut store, 0, b"ab");
        insert(&mut store, 2, b"cde");
        assert_eq!(store.out_of_order_span(), 0);
        insert(&mut store, 7, b"hij");
        assert_eq!(store.out_of_order_span(), 5);
        insert(&mut store, 12, b"mn");
        assert_eq!(store.out_of_order_len(), 5);
        assert_eq!(store.out_of_order_span(), 9);
        drop(store.pop_front().unwrap());
        assert_eq!(store.out_of_order_span(), 9);
        store.truncate_from(11);
        assert_eq!(store.out_of_order_span(), 8);
        insert(&mut store, 3, b"fg");
        assert_eq!(store.out_of_order_span(), 3);
        insert(&mut store, 8, b"kl");
        assert_eq!(store.out_of_order_span(), 0);
        assert_eq!(drain_bytes(&mut store), b"cdefghijklm");
        assert_eq!(store.out_of_order_span(), 0);
        insert(&mut store, 2, b"c");
        assert_eq!(store.out_of_order_span(), 3);
        store.clear();
        assert_eq!(store.out_of_order_span(), 0);
    }


    #[test]
    fn hole_fill_joins_following_views() {
        let mut store = RxStore::new();
        insert(&mut store, 3, b"de");
        insert(&mut store, 5, b"fgh");
        insert(&mut store, 10, b"kl");
        assert_eq!((store.in_order_len(), store.out_of_order_len()), (0, 7));
        assert_eq!(ranges(&store), vec![(3, 5), (5, 8), (10, 12)]);

        insert(&mut store, 0, b"abc");
        assert_eq!((store.in_order_len(), store.out_of_order_len()), (8, 2));
        assert_eq!(ranges(&store), vec![(0, 3), (3, 5), (5, 8), (10, 12)]);

        insert(&mut store, 8, b"ij");
        assert_eq!((store.in_order_len(), store.out_of_order_len()), (12, 0));
        assert_eq!(
            ranges(&store),
            vec![(0, 3), (3, 5), (5, 8), (8, 10), (10, 12)]
        );
        assert_eq!(drain_bytes(&mut store), b"abcdefghijkl");
    }

    #[test]
    fn covering_segment_extends_in_order_over_released_views() {
        let mut store = RxStore::new();
        let first = insert(&mut store, 0, b"ab");
        let middle = insert(&mut store, 4, b"ef");
        let last = insert(&mut store, 7, b"hi");
        assert_eq!((store.in_order_len(), store.out_of_order_len()), (2, 4));

        let decision = store.decide(2, 7);
        assert!(matches!(&decision, Decision::New(new) if new.released == 2));
        store.place(2, decision);
        assert_eq!((store.in_order_len(), store.out_of_order_len()), (9, 0));
        assert_eq!(ranges(&store), vec![(0, 2), (2, 9)]);
        assert_eq!((first.get(), middle.get(), last.get()), (0, 1, 1));

        let (frame, new) = counted_frame(b"cdefghi");
        store.attach(frame);
        assert_eq!(drain_bytes(&mut store), b"abcdefghi");
        assert_eq!(
            (first.get(), middle.get(), last.get(), new.get()),
            (1, 1, 1, 1)
        );
        drop(store);
        assert_eq!(
            (first.get(), middle.get(), last.get(), new.get()),
            (1, 1, 1, 1)
        );
    }

    #[test]
    fn truncate_and_drain_update_counts() {
        let mut store = RxStore::new();
        insert(&mut store, 0, b"ab");
        insert(&mut store, 2, b"cd");
        insert(&mut store, 4, b"efgh");
        insert(&mut store, 10, b"kl");
        insert(&mut store, 14, b"op");
        assert_eq!((store.in_order_len(), store.out_of_order_len()), (8, 4));
        assert_eq!(store.pop_front().unwrap().as_slice(), b"ab");
        assert_eq!((store.in_order_len(), store.out_of_order_len()), (6, 4));
        assert_eq!(ranges(&store), vec![(0, 2), (2, 6), (8, 10), (12, 14)]);
        store.truncate_from(4);
        assert_eq!((store.in_order_len(), store.out_of_order_len()), (4, 0));
        assert_eq!(ranges(&store), vec![(0, 2), (2, 4)]);
        insert(&mut store, 8, b"kl");
        insert(&mut store, 12, b"op");
        assert_eq!((store.in_order_len(), store.out_of_order_len()), (4, 4));
        store.truncate_from(9);
        assert_eq!(ranges(&store), vec![(0, 2), (2, 4), (8, 9)]);
        assert_eq!(store.views[2].frame.as_ref().unwrap().as_slice(), b"k");
        assert_eq!(drain_bytes(&mut store), b"cdef");
        assert!(store.pop_front().is_none());
        store.clear();
        assert!(store.pop_front().is_none());
        assert_eq!(store.head, 0);
        insert(&mut store, 0, b"new");
        assert_eq!(store.pop_front().unwrap().as_slice(), b"new");
        assert_eq!(store.head, 0);
    }

    #[test]
    fn attach_finds_pending_view_before_later_views() {
        let mut store = RxStore::new();
        let later = insert(&mut store, 10, b"kl");
        store.place(0, store.decide(0, 3));
        assert!(store.has_pending());
        assert_eq!(ranges(&store), vec![(0, 3), (10, 12)]);

        let (frame, attached) = counted_frame(b"abc");
        store.attach(frame);
        assert!(!store.has_pending());
        assert_eq!((store.in_order_len(), store.out_of_order_len()), (3, 2));
        assert_eq!(drain_bytes(&mut store), b"abc");
        assert_eq!(ranges(&store), vec![(7, 9)]);
        assert_eq!((attached.get(), later.get()), (1, 0));
        store.clear();
        assert_eq!((attached.get(), later.get()), (1, 1));
    }

    #[test]
    fn append_after_last_view_decides_like_inner_offsets() {
        let mut store = RxStore::new();
        insert(&mut store, 0, b"abc");
        insert(&mut store, 5, b"fgh");
        let new = |kept: Range<usize>| {
            Decision::New(NewView {
                kept,
                index: 2,
                released: 0,
                trimmed: false,
            })
        };
        assert_eq!(store.decide(8, 2), new(8..10));
        assert_eq!(store.decide(9, 2), new(9..11));
        assert_eq!(store.decide(7, 3), new(8..10));
        assert_eq!(store.decide(6, 2), Decision::Duplicate(6..8));
        assert_eq!(ranges(&store), vec![(0, 3), (5, 8)]);
    }

    #[test]
    fn pending_layout_counts_bytes_and_blocks_reads() {
        let mut store = RxStore::new();
        let covered = insert(&mut store, 0, b"old");
        let trimmed = insert(&mut store, 6, b"ghi");
        let later = insert(&mut store, 10, b"klm");
        let decision = store.decide(0, 7);
        store.place(0, decision);
        assert!(store.has_pending());
        assert_eq!(ranges(&store), vec![(0, 7), (7, 9), (10, 13)]);
        assert_eq!(
            (store.in_order_len(), store.out_of_order_len(), store.held()),
            (9, 3, 3)
        );
        assert_eq!((covered.get(), trimmed.get(), later.get()), (1, 0, 0));
        assert!(store.pop_front().is_none());
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            store.place(0, store.decide(0, 1));
        })).unwrap_err();
        assert_eq!(panic.downcast_ref::<&str>(), Some(&"pending receive view"));
        assert_eq!(ranges(&store), vec![(0, 7), (7, 9), (10, 13)]);
        assert_eq!((covered.get(), trimmed.get(), later.get()), (1, 0, 0));
        let (frame, attached) = counted_frame(b"abcdefg");
        store.attach(frame);
        assert!(!store.has_pending());
        assert_eq!(drain_bytes(&mut store), b"abcdefghi");
        assert_eq!((attached.get(), trimmed.get(), later.get()), (1, 1, 0));
    }

    #[test]
    fn truncate_preserves_pending_view_before_boundary() {
        let mut store = RxStore::new();
        let later = insert(&mut store, 5, b"fgh");
        store.place(0, store.decide(0, 3));
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            store.truncate_from(2);
        }))
        .unwrap_err();
        assert_eq!(
            panic.downcast_ref::<&str>(),
            Some(&"truncate cuts pending receive view")
        );
        assert_eq!(ranges(&store), vec![(0, 3), (5, 8)]);
        assert_eq!(later.get(), 0);
        store.truncate_from(3);
        assert_eq!(ranges(&store), vec![(0, 3)]);
        assert_eq!(later.get(), 1);
        assert!(store.has_pending());
        store.attach(RxBuf::from_vec(b"abc".to_vec()));
        assert_eq!(store.views.front().unwrap().frame.as_ref().unwrap().as_slice(), b"abc");
    }

    #[test]
    fn clear_and_drop_release_frames_with_pending_view() {
        for clear in [false, true] {
            let mut store = RxStore::new();
            let held = insert(&mut store, 5, b"fgh");
            store.place(0, store.decide(0, 3));
            if clear {
                store.clear();
                assert!(!store.has_pending());
                assert_eq!(store.held(), 0);
                assert_eq!(held.get(), 1);
            }
            drop(store);
            assert_eq!(held.get(), 1);
        }
    }

    #[test]
    fn left_neighbor_wins_overlap() {
        let mut store = RxStore::new();
        let left = insert(&mut store, 5, b"abcdefghij");
        let (frame, new) = counted_frame(b"ABCDEFGHIJ");
        let decision = store.decide(10, 10);
        assert_eq!(decision.kept(), Some(15..20));
        assert_eq!(ranges(&store), vec![(5, 15)]);
        place_attach(&mut store, 10, frame, decision);
        assert_eq!(ranges(&store), vec![(5, 15), (15, 20)]);
        assert_eq!(
            store.views[0].frame.as_ref().unwrap().as_slice(),
            b"abcdefghij"
        );
        assert_eq!(store.views[1].frame.as_ref().unwrap().as_slice(), b"FGHIJ");
        assert_eq!(store.in_order_len(), 0);
        assert_eq!(store.out_of_order_len(), 15);
        assert!(store.pop_front().is_none());
        assert!(store.pop_front().is_none());
        assert!(store.pop_front().is_none());
        assert_eq!((left.get(), new.get()), (0, 0));
        store.clear();
        assert_eq!((left.get(), new.get()), (1, 1));
    }

    #[test]
    fn covering_segment_releases_neighbors() {
        let mut store = RxStore::new();
        let first = insert(&mut store, 5, b"first");
        let second = insert(&mut store, 12, b"two");
        let last = insert(&mut store, 20, b"last!");
        let (frame, new) = counted_frame(b"ABCDEFGHIJKL");
        let decision = store.decide(4, 12);
        assert_eq!(
            decision,
            Decision::New(NewView {
                kept: 4..16,
                index: 0,
                released: 2,
                trimmed: false,
            })
        );
        assert_eq!((first.get(), second.get()), (0, 0));
        place_attach(&mut store, 4, frame, decision);
        assert_eq!(ranges(&store), vec![(4, 16), (20, 25)]);
        assert_eq!(
            store.views[0].frame.as_ref().unwrap().as_slice(),
            b"ABCDEFGHIJKL"
        );
        assert_eq!(
            (first.get(), second.get(), last.get(), new.get()),
            (1, 1, 0, 0)
        );
        store.clear();
        assert_eq!(
            (first.get(), second.get(), last.get(), new.get()),
            (1, 1, 1, 1)
        );
    }

    #[test]
    fn right_neighbor_head_trimmed() {
        let mut store = RxStore::new();
        let left = insert(&mut store, 5, b"abcdefghij");
        let covered = insert(&mut store, 20, b"abcde");
        let right = insert(&mut store, 28, b"abcdefghijkl");
        let new = insert(&mut store, 10, b"ABCDEFGHIJKLMNOPQRST");
        assert_eq!(ranges(&store), vec![(5, 15), (15, 30), (30, 40)]);
        assert_eq!(
            store.views[1].frame.as_ref().unwrap().as_slice(),
            b"FGHIJKLMNOPQRST"
        );
        assert_eq!(
            store.views[2].frame.as_ref().unwrap().as_slice(),
            b"cdefghijkl"
        );
        assert_eq!(
            (left.get(), covered.get(), right.get(), new.get()),
            (0, 1, 0, 0)
        );
        drop(store);
        assert_eq!(
            (left.get(), covered.get(), right.get(), new.get()),
            (1, 1, 1, 1)
        );
    }

    #[test]
    fn duplicate_inside_view_released() {
        let mut store = RxStore::new();
        let held = insert(&mut store, 0, b"abcdefghij");
        let (frame, duplicate) = counted_frame(b"XYZ");
        let decision = store.decide(3, 3);
        assert_eq!(decision, Decision::Duplicate(3..6));
        place_attach(&mut store, 3, frame, decision);
        assert_eq!(duplicate.get(), 1);
        assert_eq!(held.get(), 0);
        assert_eq!(store.views.front().unwrap().frame.as_ref().unwrap().as_slice(), b"abcdefghij");
        store.clear();
        assert_eq!(held.get(), 1);

        let first = insert(&mut store, 0, b"abcde");
        let second = insert(&mut store, 5, b"fghij");
        let (frame, duplicate) = counted_frame(b"XXXXXXXX");
        let decision = store.decide(1, 8);
        assert_eq!(decision, Decision::Duplicate(1..9));
        place_attach(&mut store, 1, frame, decision);
        assert_eq!(ranges(&store), vec![(0, 5), (5, 10)]);
        assert_eq!((first.get(), second.get(), duplicate.get()), (0, 0, 1));
        store.clear();
        assert_eq!((first.get(), second.get(), duplicate.get()), (1, 1, 1));
    }

    #[test]
    fn equal_start_longer_wins() {
        let mut store = RxStore::new();
        let short = insert(&mut store, 0, b"old");
        let long = insert(&mut store, 0, b"longer");
        assert_eq!(ranges(&store), vec![(0, 6)]);
        assert_eq!(store.views.front().unwrap().frame.as_ref().unwrap().as_slice(), b"longer");
        assert_eq!(short.get(), 1);
        for bytes in [&b"short"[..], &b"equal!"[..]] {
            let (frame, duplicate) = counted_frame(bytes);
            let decision = store.decide(0, bytes.len());
            assert_eq!(decision, Decision::Duplicate(0..bytes.len()));
            place_attach(&mut store, 0, frame, decision);
            assert_eq!(duplicate.get(), 1);
            assert_eq!(store.views.front().unwrap().frame.as_ref().unwrap().as_slice(), b"longer");
        }
        store.clear();
        assert_eq!(long.get(), 1);
    }

    #[test]
    fn hole_fill_and_overlap_release() {
        let mut store = RxStore::new();
        insert(&mut store, 0, b"abcde");
        insert(&mut store, 10, b"klmno");
        insert(&mut store, 5, b"fghij");
        assert_eq!(store.held(), 3);
        assert_eq!(store.in_order_len(), 15);
        assert_eq!(store.out_of_order_len(), 0);
        store.clear();

        insert(&mut store, 20, b"uvwxy");
        insert(&mut store, 0, b"abcde");
        let covered = insert(&mut store, 5, b"fghij");
        let (frame, new) = counted_frame(b"ABCDEFGHIJ");
        let decision = store.decide(2, 10);
        assert_eq!(decision.kept(), Some(5..12));
        place_attach(&mut store, 2, frame, decision);
        assert_eq!(store.held(), 3);
        assert_eq!(store.in_order_len(), 12);
        assert_eq!(ranges(&store), vec![(0, 5), (5, 12), (20, 25)]);
        assert_eq!(
            store.views[1].frame.as_ref().unwrap().as_slice(),
            b"DEFGHIJ"
        );
        assert_eq!((covered.get(), new.get()), (1, 0));
    }

    #[test]
    fn truncate_from_drops_views_past_offset() {
        let mut store = RxStore::new();
        let first = insert(&mut store, 0, b"abcde");
        let middle = insert(&mut store, 10, b"klmno");
        let last = insert(&mut store, 20, b"uvwxy");
        store.truncate_from(30);
        assert_eq!(store.held(), 3);
        store.truncate_from(12);
        assert_eq!(ranges(&store), vec![(0, 5), (10, 12)]);
        assert_eq!(store.views[1].frame.as_ref().unwrap().as_slice(), b"kl");
        assert_eq!((first.get(), middle.get(), last.get()), (0, 0, 1));
        store.truncate_from(10);
        assert_eq!(ranges(&store), vec![(0, 5)]);
        assert_eq!(middle.get(), 1);
        store.truncate_from(3);
        assert_eq!(store.views.front().unwrap().frame.as_ref().unwrap().as_slice(), b"abc");
        assert_eq!(first.get(), 0);
        store.truncate_from(0);
        assert_eq!(store.held(), 0);
        assert_eq!(first.get(), 1);
    }

    fn check_oracle(
        store: &RxStore,
        stream: &[u8],
        oracle: &[bool],
        head: usize,
        releases: &[Rc<Cell<usize>>],
    ) {
        let contiguous = oracle[head..].iter().take_while(|&&held| held).count();
        assert_eq!(store.in_order_len(), contiguous);
        let total = oracle[head..].iter().filter(|&&held| held).count();
        assert_eq!(store.out_of_order_len(), total.checked_sub(contiguous).unwrap());

        let mut previous_end = 0;
        let mut union: Vec<(usize, usize)> = Vec::new();
        let mut actual = vec![false; stream.len()];
        let mut readable = Vec::new();
        for (view, (start, end)) in store.views.iter().zip(store.ranges()) {
            assert!(start >= previous_end);
            assert!(start < end);
            previous_end = end;
            let absolute_start = head.checked_add(start).unwrap();
            let absolute_end = head.checked_add(end).unwrap();
            let payload = view.frame.as_ref().unwrap().as_slice();
            assert_eq!(payload, &stream[absolute_start..absolute_end]);
            actual[absolute_start..absolute_end].fill(true);
            if start < contiguous {
                readable.extend_from_slice(payload);
            }
            if let Some(last) = union.last_mut() {
                if last.1 == start {
                    last.1 = end;
                    continue;
                }
            }
            union.push((start, end));
        }
        assert_eq!(readable, stream[head..head.checked_add(contiguous).unwrap()]);
        assert_eq!(actual, oracle);
        assert_eq!(store.ranges().count(), store.held());
        let out_of_order: Vec<(usize, usize)> = union
            .iter()
            .filter(|&&(start, _)| start > contiguous)
            .map(|&(start, end)| {
                (
                    start.checked_sub(contiguous).unwrap(),
                    end.checked_sub(contiguous).unwrap(),
                )
            })
            .collect();
        assert_eq!(store.out_of_order_ranges().collect::<Vec<_>>(), out_of_order);
        assert!(releases.iter().all(|count| count.get() <= 1));
        assert_eq!(releases.iter().filter(|count| count.get() == 0).count(), store.held());
    }

    #[test]
    fn randomized_against_byte_oracle() {
        let stream: Vec<u8> = (0..4096).map(|index| (index % 251) as u8).collect();
        let mut new_segments = 0usize;
        let mut duplicates = 0usize;
        let mut consumes = 0usize;
        for seed in [1, 7, 0x12345678, 0xabcdef01] {
            {
                let mut rng = StdRng::seed_from_u64(seed);
                let mut store = RxStore::new();
                let mut oracle = vec![false; stream.len()];
                let mut releases = Vec::new();
                let mut head = 0usize;
                for _ in 0..1500 {
                    if rng.gen_ratio(1, 4) {
                        for _ in 0..rng.gen_range(0..=store.held()) {
                            let Some(frame) = store.pop_front() else { break; };
                            let end = head.checked_add(frame.len()).unwrap();
                            assert_eq!(frame.as_slice(), &stream[head..end]);
                            oracle[head..end].fill(false);
                            head = end;
                        }
                        consumes = consumes.checked_add(1).unwrap();
                    } else if head < stream.len() {
                        let remaining = stream.len().checked_sub(head).unwrap();
                        let offset = match rng.gen_range(0..4) {
                            0 => 0,
                            1 => store.in_order_len().min(remaining.checked_sub(1).unwrap()),
                            _ => rng.gen_range(0..remaining.min(96)),
                        };
                        let len =
                            rng.gen_range(1..=remaining.checked_sub(offset).unwrap().min(24));
                        let end = offset.checked_add(len).unwrap();
                        let absolute_start = head.checked_add(offset).unwrap();
                        let absolute_end = head.checked_add(end).unwrap();
                        let (frame, count) =
                            counted_frame(&stream[absolute_start..absolute_end]);
                        releases.push(count.clone());
                        let before = ranges(&store);
                        let decision = store.decide(offset, len);
                        assert_eq!(ranges(&store), before);
                        assert_eq!(count.get(), 0);
                        {
                            let contiguous = store.in_order_len();
                            if matches!(decision, Decision::Duplicate(_)) {
                                duplicates = duplicates.checked_add(1).unwrap();
                                place_attach(&mut store, offset, frame, decision);
                            } else {
                                new_segments = new_segments.checked_add(1).unwrap();
                                place_attach(&mut store, offset, frame, decision);
                            }
                            oracle[absolute_start..absolute_end].fill(true);
                            assert!(store.in_order_len() >= contiguous);
                        }
                    }
                    check_oracle(&store, &stream, &oracle, head, &releases);
                }
                store.clear();
                oracle.fill(false);
                check_oracle(&store, &stream, &oracle, head, &releases);
                drop(store);
                assert!(releases.iter().all(|count| count.get() == 1));
            }
        }
        assert!(new_segments > 0);
        assert!(duplicates > 0);
        assert!(consumes > 0);
    }
}
