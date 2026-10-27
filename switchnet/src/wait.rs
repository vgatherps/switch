//! Wait registry: one waker slot per socket entry, the list of entries with a
//! parked task, and the scratch the post-turn scan moves ready wakers into.
//! Pure data; waking and dropping wakers is the caller's job, outside any
//! borrow of the shared cell. Storage grows only when `insert` adds an entry
//! slot (opening a socket), so `remove`, `park`, `clear`, `scan` and
//! `restore_wakes` never allocate. One exception: wakers a panicking wake
//! left undelivered stay in the scratch, and if their tasks park again
//! before the next scan, that scan can outgrow it.

use std::mem;
use std::task::Waker;

/// What a parked task waits for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Interest {
    Connect,
    TcpRecv,
    UdpRecv,
}

struct Entry<T> {
    socket: T,
    waker: Option<Waker>,
    interest: Interest,
    /// In `waiting`; stays true after `clear` until the next scan drops it.
    listed: bool,
}

pub(crate) struct Registry<T> {
    entries: Vec<Option<Entry<T>>>,
    free: Vec<u32>,
    waiting: Vec<u32>,
    wakes: Vec<Waker>,
}

impl<T> Registry<T> {
    pub(crate) fn new() -> Self {
        Self { entries: Vec::new(), free: Vec::new(), waiting: Vec::new(), wakes: Vec::new() }
    }

    /// Adds an entry with no parked task. A new slot reserves `free`,
    /// `waiting` and the wake scratch for every slot; a reused slot is
    /// already covered.
    pub(crate) fn insert(&mut self, socket: T) -> u32 {
        let entry = Entry { socket, waker: None, interest: Interest::Connect, listed: false };
        if let Some(index) = self.free.pop() {
            self.entries[index as usize] = Some(entry);
            return index;
        }
        self.entries.push(Some(entry));
        let slots = self.entries.len();
        self.free.reserve(slots - self.free.len());
        self.waiting.reserve(slots - self.waiting.len());
        // While `take_wakes` has the scratch out this reserves the empty
        // stand-in; `restore_wakes` keeps whichever covers every slot.
        self.wakes.reserve(slots - self.wakes.len());
        u32::try_from(slots - 1).expect("more than u32::MAX entries")
    }

    /// Removes the entry; returns its socket and any parked waker.
    pub(crate) fn remove(&mut self, index: u32) -> (T, Option<Waker>) {
        let entry = self.entries[index as usize].take().expect("entry removed twice");
        if entry.listed {
            let at = self.waiting.iter().position(|&i| i == index).expect("listed entry is waiting");
            self.waiting.swap_remove(at);
        }
        self.free.push(index);
        (entry.socket, entry.waker)
    }

    pub(crate) fn socket(&self, index: u32) -> &T {
        &self.entry(index).socket
    }

    /// Whether the entry's parked waker already wakes `waker`'s task.
    /// `Waker::will_wake` compares pointers and runs no waker code.
    pub(crate) fn parked_as(&self, index: u32, waker: &Waker) -> bool {
        self.entry(index).waker.as_ref().is_some_and(|parked| parked.will_wake(waker))
    }

    /// Parks `waker` for `interest`; returns the waker it replaced.
    pub(crate) fn park(&mut self, index: u32, interest: Interest, waker: Waker) -> Option<Waker> {
        let entry = self.entries[index as usize].as_mut().expect("entry is live");
        entry.interest = interest;
        if !entry.listed {
            entry.listed = true;
            self.waiting.push(index);
        }
        entry.waker.replace(waker)
    }

    /// Takes the parked waker, if any. The entry leaves `waiting` at the next scan.
    pub(crate) fn clear(&mut self, index: u32) -> Option<Waker> {
        self.entries[index as usize].as_mut().expect("entry is live").waker.take()
    }

    /// For each waiting entry: a cleared one leaves `waiting`; one for which
    /// `ready` holds moves its waker into the scratch and leaves `waiting`.
    /// Visits only waiting entries. Returns whether the scratch holds wakers.
    pub(crate) fn scan(&mut self, mut ready: impl FnMut(&T, Interest) -> bool) -> bool {
        let mut at = 0;
        while at < self.waiting.len() {
            let entry = self.entries[self.waiting[at] as usize].as_mut().expect("waiting entry is live");
            let woken = match &entry.waker {
                None => None,
                Some(_) if ready(&entry.socket, entry.interest) => entry.waker.take(),
                Some(_) => {
                    at += 1;
                    continue;
                }
            };
            entry.listed = false;
            self.waiting.swap_remove(at);
            if let Some(waker) = woken {
                self.wakes.push(waker);
            }
        }
        !self.wakes.is_empty()
    }

    /// Whether a scan has anything to do: a waiting entry, or wakers left
    /// undelivered by a panicking wake.
    #[inline]
    pub(crate) fn pending(&self) -> bool {
        !self.waiting.is_empty() || !self.wakes.is_empty()
    }

    /// The scratch with the collected wakers, to wake outside the borrow.
    /// Leaves an unallocated stand-in until `restore_wakes`.
    pub(crate) fn take_wakes(&mut self) -> Vec<Waker> {
        mem::take(&mut self.wakes)
    }

    /// Puts back the scratch from `take_wakes`, holding any wakers not yet
    /// delivered. Keeps it if it covers every slot, else (slots were added
    /// meanwhile) moves the wakers into the stand-in `insert` reserved.
    pub(crate) fn restore_wakes(&mut self, mut wakes: Vec<Waker>) {
        debug_assert!(self.wakes.is_empty(), "scanned while the scratch was out");
        if wakes.capacity() >= self.entries.len() {
            self.wakes = wakes;
        } else {
            debug_assert!(self.wakes.capacity() >= self.entries.len());
            self.wakes.append(&mut wakes);
        }
    }

    #[cfg(test)]
    fn waiting(&self) -> usize {
        self.waiting.len()
    }

    fn entry(&self, index: u32) -> &Entry<T> {
        self.entries[index as usize].as_ref().expect("entry is live")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Wake;

    use super::*;
    use crate::alloc_count::allocations;

    struct Count(AtomicUsize);

    impl Wake for Count {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn waker() -> (Arc<Count>, Waker) {
        let count = Arc::new(Count(AtomicUsize::new(0)));
        (count.clone(), Waker::from(count))
    }

    #[test]
    fn scan_visits_only_parked_entries() {
        let mut registry = Registry::new();
        let ids: Vec<u32> = (0..16).map(|socket| registry.insert(socket)).collect();
        let mut visits = 0;
        registry.scan(|_, _| {
            visits += 1;
            true
        });
        assert_eq!(visits, 0);
        let (_, w) = waker();
        assert!(registry.park(ids[5], Interest::TcpRecv, w).is_none());
        let mut seen = Vec::new();
        registry.scan(|&socket, interest| {
            seen.push((socket, interest));
            false
        });
        assert_eq!(seen, [(5, Interest::TcpRecv)]);
    }

    #[test]
    fn ready_entry_moves_its_waker_and_leaves_waiting() {
        let mut registry = Registry::new();
        let [a, b] = [registry.insert('a'), registry.insert('b')];
        let (count_a, wa) = waker();
        let (count_b, wb) = waker();
        registry.park(a, Interest::UdpRecv, wa);
        registry.park(b, Interest::UdpRecv, wb);
        registry.scan(|&socket, _| socket == 'b');
        assert_eq!(registry.waiting(), 1);
        let mut wakes = registry.take_wakes();
        assert_eq!(wakes.len(), 1);
        wakes.drain(..).for_each(Waker::wake);
        registry.restore_wakes(wakes);
        assert_eq!(count_a.0.load(Ordering::Relaxed), 0);
        assert_eq!(count_b.0.load(Ordering::Relaxed), 1);
        assert!(!registry.parked_as(b, &waker().1));
        assert!(registry.parked_as(a, &registry.entry(a).waker.clone().unwrap()));
    }

    #[test]
    fn repark_replaces_waker_and_lists_once() {
        let mut registry = Registry::new();
        let id = registry.insert(());
        let (first, w1) = waker();
        let (_, w2) = waker();
        registry.park(id, Interest::TcpRecv, w1);
        let old = registry.park(id, Interest::TcpRecv, w2).unwrap();
        assert!(old.will_wake(&Waker::from(first)));
        assert_eq!(registry.waiting(), 1);
    }

    #[test]
    fn cleared_entry_is_dropped_from_waiting_without_wake() {
        let mut registry = Registry::new();
        let id = registry.insert(());
        let (count, w) = waker();
        registry.park(id, Interest::TcpRecv, w);
        assert!(registry.clear(id).is_some());
        registry.scan(|_, _| panic!("cleared entry checked"));
        assert_eq!(registry.waiting(), 0);
        assert!(registry.take_wakes().is_empty());
        assert_eq!(count.0.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn remove_unlists_and_reuses_index() {
        let mut registry = Registry::new();
        let id = registry.insert(1);
        let (_, w) = waker();
        registry.park(id, Interest::TcpRecv, w);
        let (socket, parked) = registry.remove(id);
        assert_eq!(socket, 1);
        assert!(parked.is_some());
        assert_eq!(registry.waiting(), 0);
        assert_eq!(registry.insert(2), id);
    }

    #[test]
    fn park_and_scan_do_not_grow_storage() {
        let mut registry = Registry::new();
        let ids: Vec<u32> = (0..8).map(|socket| registry.insert(socket)).collect();
        let caps = (registry.waiting.capacity(), registry.wakes.capacity());
        for &id in &ids {
            registry.park(id, Interest::TcpRecv, waker().1);
        }
        let (woke, allocs) = allocations(|| registry.scan(|_, _| false));
        assert!(!woke);
        let (woke, more) = allocations(|| registry.scan(|_, _| true));
        assert!(woke);
        let mut wakes = registry.take_wakes();
        assert_eq!(wakes.len(), 8);
        wakes.clear();
        let ((), restore) = allocations(|| registry.restore_wakes(wakes));
        assert_eq!(allocs + more + restore, 0);
        assert_eq!((registry.waiting.capacity(), registry.wakes.capacity()), caps);
        assert!(caps.1 >= 8);
    }

    #[test]
    fn remove_does_not_allocate() {
        let mut registry = Registry::new();
        let first = registry.insert(0);
        let ((_, parked), allocs) = allocations(|| registry.remove(first));
        assert!(parked.is_none());
        assert_eq!(allocs, 0, "removing the first entry allocated");
        let ids: Vec<u32> = (0..9).map(|socket| registry.insert(socket)).collect();
        let ((), allocs) = allocations(|| ids.iter().for_each(|&id| drop(registry.remove(id))));
        assert_eq!(allocs, 0);
        let (reused, allocs) = allocations(|| registry.insert(42));
        assert!(ids.contains(&reused));
        assert_eq!(allocs, 0, "reusing a slot allocated");
    }

    /// A wake callback opens sockets while the scratch is out: insert grows
    /// the stand-in, and the restore must keep that one, not regrow the old.
    #[test]
    fn slots_added_while_scratch_is_out_do_not_allocate_restore_or_scan() {
        let mut registry = Registry::new();
        let ids: Vec<u32> = (0..2).map(|socket| registry.insert(socket)).collect();
        for &id in &ids {
            registry.park(id, Interest::UdpRecv, waker().1);
        }
        assert!(registry.scan(|_, _| true));
        let mut wakes = registry.take_wakes();
        let taken = wakes.capacity();
        let added: Vec<u32> = (2..=taken as u32 + 4).map(|socket| registry.insert(socket)).collect();
        assert!(registry.entries.len() > taken);
        wakes.drain(..).for_each(Waker::wake);
        let ((), allocs) = allocations(|| registry.restore_wakes(wakes));
        assert_eq!(allocs, 0, "restore regrew the old scratch");
        assert!(registry.wakes.capacity() >= registry.entries.len());
        for &id in ids.iter().chain(&added) {
            registry.park(id, Interest::UdpRecv, waker().1);
        }
        let (woke, allocs) = allocations(|| registry.scan(|_, _| true));
        assert!(woke);
        assert_eq!(allocs, 0, "scan of every slot outgrew the scratch");
        assert_eq!(registry.take_wakes().len(), registry.entries.len());
    }

    /// A wake panicked after one delivery: the rest go back with the scratch
    /// and the next scan reports them, with no waiting entry left.
    #[test]
    fn undelivered_wakers_survive_restore_for_the_next_scan() {
        let mut registry = Registry::new();
        let counts: Vec<Arc<Count>> = (0..3)
            .map(|socket| {
                let id = registry.insert(socket);
                let (count, w) = waker();
                registry.park(id, Interest::TcpRecv, w);
                count
            })
            .collect();
        assert!(registry.scan(|_, _| true));
        let mut wakes = registry.take_wakes();
        let (ptr, cap) = (wakes.as_ptr(), wakes.capacity());
        wakes.pop().unwrap().wake();
        registry.restore_wakes(wakes);
        assert_eq!((registry.wakes.as_ptr(), registry.wakes.capacity()), (ptr, cap), "scratch replaced");
        assert_eq!(registry.waiting(), 0);
        assert!(registry.pending());
        assert!(registry.scan(|_, _| panic!("no entry is waiting")));
        registry.take_wakes().into_iter().for_each(Waker::wake);
        assert!(counts.iter().all(|count| count.0.load(Ordering::Relaxed) == 1));
    }
}
