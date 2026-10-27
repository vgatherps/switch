use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    pub allocations: usize,
    pub deallocations: usize,
    pub reallocations: usize,
    pub allocated_bytes: usize,
    pub deallocated_bytes: usize,
    pub net_bytes: i128,
    pub peak_extra_bytes: usize,
}
thread_local! {
    static ENABLED: Cell<bool> = const { Cell::new(false) };
    static STATS: Cell<Stats> = const { Cell::new(Stats { allocations: 0, deallocations: 0, reallocations: 0, allocated_bytes: 0, deallocated_bytes: 0, net_bytes: 0, peak_extra_bytes: 0 }) };
}

fn record(allocated: usize, deallocated: usize, kind: u8) {
    let _ = ENABLED.try_with(|enabled| {
        if enabled.get() {
            STATS.with(|slot| {
                let mut stats = slot.get();
                match kind {
                    0 => stats.allocations += 1,
                    1 => stats.deallocations += 1,
                    _ => stats.reallocations += 1,
                }
                stats.allocated_bytes += allocated;
                stats.deallocated_bytes += deallocated;
                stats.net_bytes += allocated as i128 - deallocated as i128;
                stats.peak_extra_bytes =
                    stats.peak_extra_bytes.max(stats.net_bytes.max(0) as usize);
                slot.set(stats);
            });
        }
    });
}
pub struct CountingGlobalAlloc;
unsafe impl GlobalAlloc for CountingGlobalAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            record(layout.size(), 0, 0);
        }
        ptr
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            record(layout.size(), 0, 0);
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        record(0, layout.size(), 1);
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(ptr, layout, size) };
        if !result.is_null() {
            record(size, layout.size(), 2);
        }
        result
    }
}
struct Window;
impl Drop for Window {
    fn drop(&mut self) {
        ENABLED.with(|enabled| enabled.set(false));
    }
}
pub fn measure<T>(action: impl FnOnce() -> T) -> (T, Stats) {
    ENABLED.with(|enabled| assert!(!enabled.get(), "nested allocation measurement"));
    STATS.with(|stats| stats.set(Stats::default()));
    ENABLED.with(|enabled| enabled.set(true));
    let window = Window;
    let result = action();
    drop(window);
    (result, STATS.with(Cell::get))
}
pub fn snapshot() -> Stats {
    STATS.with(Cell::get)
}
pub fn no_alloc(stats: Stats) {
    assert_eq!(
        (stats.allocations, stats.reallocations),
        (0, 0),
        "{stats:?}"
    );
}
