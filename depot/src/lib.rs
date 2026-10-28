//! # depot
//!
//! A cache-optimized pool allocator returning compact typed handles.
//! Designed for high-frequency alloc/free of packet metadata and ECS-style indices.

use std::cell::UnsafeCell;
use std::num::NonZeroU32;

mod algorithm;
mod dyn_depot;
pub mod fixed;
mod index_ring;
pub mod pool;

#[cfg(test)]
mod stress_tests;
#[cfg(any(test, feature = "test-utils"))]
mod test_heap;

pub use dyn_depot::DynDepot;
pub use fixed::FixedDepot;
#[doc(hidden)]
pub use pool::{
    Claim, ClaimBorrowError, ClaimError, Component, ComponentRef, ComponentRefMut, Entity,
    EntityId, EntityRef, GlobalPool, PoolBinding, PoolClaim, PoolHandle, StaticClaim,
    StaticPoolHandle,
};
#[cfg(any(test, feature = "test-utils"))]
#[doc(hidden)]
pub use pool::{LocalPool, LocalPoolBinding, OwnedClaim};
#[cfg(any(test, feature = "test-utils"))]
pub use test_heap::{HeapOp, SlabHeap, run_heap_oracle};

/// Types that expose a fixed slot capacity for use as rake index spaces.
pub trait HasSlots {
    const SLOTS: usize;
}

/// Alloc/free interface over a depot — enables generic code to work with any depot type.
pub trait DepotAlloc {
    fn alloc(&mut self) -> Option<DepotHandle>;

    /// Resolve an optional handle for null-tolerant access.
    ///
    /// `None` and slot-0 handles return `Some(0)`. Live handles return their slot.
    /// Stale, dead, poisoned, and out-of-range handles return `None`.
    fn get_or_sentinel(&self, handle: Option<DepotHandle>) -> Option<u32>;

    /// Validate a handle as a live allocation. The default implementation reports every failure as
    /// `StaleGeneration`; `FixedDepot` overrides it and reports slot 0 and out-of-range slots as
    /// `OutOfRange`.
    fn validate(&self, handle: DepotHandle) -> Result<u32, AccessError> {
        self.validate_live(handle)
            .ok_or(AccessError::StaleGeneration)
    }

    /// Strictly validate a public/raw handle as a currently live allocation.
    ///
    /// Stale, out-of-range, freed, and poisoned handles return `None`. Unlike `get_or_sentinel`,
    /// a slot-0 handle also returns `None` here instead of `Some(0)`.
    fn validate_live(&self, handle: DepotHandle) -> Option<u32>;

    /// # Safety
    /// `handle` must be currently allocated in this depot.
    unsafe fn free_unchecked(&mut self, handle: DepotHandle);

    /// Permanently invalidate a currently allocated slot without making it reusable.
    ///
    /// This is available to owners layered above depot when a slot must become dead without being
    /// reused. Stale ids fail generation checks immediately and the slot is intentionally leaked.
    ///
    /// # Safety
    /// `handle` must refer to a currently allocated slot in this depot.
    unsafe fn poison_unchecked(&mut self, handle: DepotHandle);
}

const HANDLE_SLOT_BITS: u32 = 25;
const HANDLE_SLOT_MASK: u32 = (1 << HANDLE_SLOT_BITS) - 1;
pub(crate) const MAX_SLOTS: usize = 1 << HANDLE_SLOT_BITS;

/// Compact typed handle: 25-bit slot + 7-bit generation packed into a NonZeroU32.
/// Raw 0 is unrepresentable, so `Option<DepotHandle>` is the same size as `u32`
/// with `None` as null. Live generations are 1 through 127 and wrap to 1.
/// Slot 0 is still reserved as rake's sentinel row.
// Guarantees an all-zero `Option<DepotHandle>` is `None`; `FixedDepot::new()` relies on it.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DepotHandle(NonZeroU32);

impl DepotHandle {
    #[inline(always)]
    pub const fn raw(self) -> u32 {
        self.0.get()
    }

    #[inline(always)]
    pub const fn slot(self) -> u32 {
        self.0.get() & HANDLE_SLOT_MASK
    }

    #[inline(always)]
    pub const fn generation(self) -> u8 {
        (self.0.get() >> HANDLE_SLOT_BITS) as u8
    }

    #[inline(always)]
    #[cfg(test)]
    pub(crate) const fn from_raw(raw: u32) -> Self {
        match NonZeroU32::new(raw) {
            Some(v) => Self(v),
            None => panic!("DepotHandle::from_raw(0) is invalid"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FreeError {
    OutOfRange,
    StaleGeneration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessError {
    OutOfRange,
    StaleGeneration,
}

pub(crate) const GENERATION_MASK: u8 = (1u8 << (32 - HANDLE_SLOT_BITS)) - 1; // 0x7F for 7 bits
pub(crate) const DEAD_BIT: u8 = 1u8 << (32 - HANDLE_SLOT_BITS); // 0x80
pub(crate) const BAD_GENERATION: u8 = 0;
pub(crate) const FIRST_GENERATION: u8 = 1;

#[inline(always)]
pub(crate) const fn next_live_generation(generation: u8) -> u8 {
    let next = generation.wrapping_add(1) & GENERATION_MASK;
    next + (next == BAD_GENERATION) as u8
}

#[inline(always)]
pub(crate) fn encode_handle(slot: u32, generation: u8) -> DepotHandle {
    debug_assert!((slot as usize) < MAX_SLOTS);
    let g = (generation & GENERATION_MASK) as u32;
    let raw = (g << HANDLE_SLOT_BITS) | slot;
    debug_assert!(raw != 0, "encode_handle produced zero (slot=0, gen=0)");
    debug_assert!(
        slot == 0 || (generation & GENERATION_MASK) != BAD_GENERATION,
        "live handle encoded with bad generation"
    );
    DepotHandle(unsafe { NonZeroU32::new_unchecked(raw) })
}

/// Define a static pool with a ZST handle.
///
/// ```
/// depot::define_static_ecs!(OrderPool, 64);
///
/// fn main() {
///     let _claim = OrderPool::claim().unwrap();
/// }
/// ```
///
/// ```compile_fail,E0080
/// depot::define_static_ecs!(OversizedPool, (1usize << 25) + 1);
///
/// fn main() {}
/// ```
#[macro_export]
macro_rules! define_static_ecs {
    ($name:ident, $capacity:expr) => {
        $crate::define_static_ecs!($name, $capacity, core: () = ());
    };
    ($name:ident, $capacity:expr, core: $core_ty:ty = $core_init:expr) => {
        const _: () = <$crate::fixed_depot_type!($capacity)>::validate_config();

        #[derive(Clone, Copy)]
        pub struct $name;

        impl $name {
            #[inline]
            fn __pool() -> &'static $crate::GlobalPool<
                $crate::fixed_depot_type!($capacity), $core_ty,
            > {
                static POOL: $crate::GlobalPool<
                    $crate::fixed_depot_type!($capacity),
                    $core_ty,
                > = $crate::GlobalPool::new(
                    <$crate::fixed_depot_type!($capacity)>::new(),
                    $core_init,
                );
                &POOL
            }

            #[inline]
            pub fn claim() -> Result<$crate::StaticClaim<Self>, $crate::ClaimError> {
                <Self as $crate::StaticPoolHandle>::claim_static()
            }
        }

        unsafe impl $crate::PoolBinding for $name {
            type Depot = $crate::fixed_depot_type!($capacity);
            type Core = $core_ty;

            #[inline(always)]
            fn pool_id(&self) -> *const () { Self::__pool().pool_id() }
            #[inline(always)]
            fn depot_ptr(&self) -> *mut Self::Depot { Self::__pool().depot_ptr() }
            #[inline(always)]
            fn core_ptr(&self) -> *mut Self::Core { Self::__pool().core_ptr() }
            #[inline(always)]
            fn borrow_set_ptr(
                &self,
            ) -> *const <Self::Depot as $crate::pool::SlotBorrowStorage>::SlotBorrows {
                Self::__pool().borrow_set_ptr()
            }
        }

        unsafe impl $crate::PoolHandle for $name {}

        unsafe impl $crate::StaticPoolHandle for $name {
            #[inline(always)]
            fn static_pool() -> &'static $crate::GlobalPool<Self::Depot, Self::Core> {
                Self::__pool()
            }

            #[inline(always)]
            fn static_handle() -> Self {
                $name
            }
        }
    };
}

/// `Sync` wrapper for `UnsafeCell<T>` used by static component and follower storage.
/// Safety is guaranteed by the pool's thread-affinity claim.
#[doc(hidden)]
pub struct SyncCell<T>(pub UnsafeCell<T>);
unsafe impl<T> Sync for SyncCell<T> {}

impl<T> SyncCell<T> {
    pub const fn new(val: T) -> Self {
        Self(UnsafeCell::new(val))
    }
    pub fn get(&self) -> *mut T {
        self.0.get()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DefaultValidateDepot(fixed_depot_type!(256));

    impl DepotAlloc for DefaultValidateDepot {
        fn alloc(&mut self) -> Option<DepotHandle> {
            self.0.alloc()
        }

        fn get_or_sentinel(&self, handle: Option<DepotHandle>) -> Option<u32> {
            self.0.get_or_sentinel(handle)
        }

        fn validate_live(&self, handle: DepotHandle) -> Option<u32> {
            self.0.validate_live(handle)
        }

        unsafe fn free_unchecked(&mut self, handle: DepotHandle) {
            // SAFETY: This method has the same contract as the delegated method.
            unsafe { self.0.free_unchecked(handle) }
        }

        unsafe fn poison_unchecked(&mut self, handle: DepotHandle) {
            // SAFETY: This method has the same contract as the delegated method.
            unsafe { self.0.poison_unchecked(handle) }
        }
    }

    #[test]
    fn handle_roundtrip() {
        let h = encode_handle(12345, 17);
        assert_eq!(h.slot(), 12345);
        assert_eq!(h.generation(), 17);
    }

    #[test]
    fn option_handle_is_u32_sized() {
        assert_eq!(
            std::mem::size_of::<Option<DepotHandle>>(),
            std::mem::size_of::<u32>()
        );
    }

    #[test]
    fn handle_layout_25_slot_7_generation() {
        let h = encode_handle((1 << HANDLE_SLOT_BITS) - 1, GENERATION_MASK);
        assert_eq!(h.slot(), (1 << HANDLE_SLOT_BITS) - 1);
        assert_eq!(h.generation(), 127);
    }

    #[test]
    fn next_live_generation_wraps_127_to_1() {
        assert_eq!(next_live_generation(126), 127);
        assert_eq!(next_live_generation(127), FIRST_GENERATION);
    }

    #[test]
    fn trait_default_validate_collapses_to_stale_generation() {
        let mut depot = DefaultValidateDepot(<fixed_depot_type!(256)>::new());
        let live = depot.alloc().unwrap();
        assert_eq!(DepotAlloc::validate(&depot, live), Ok(live.slot()));

        let out_of_range = DepotHandle::from_raw((1 << HANDLE_SLOT_BITS) | 999);
        assert_eq!(
            DepotAlloc::validate(&depot, out_of_range),
            Err(AccessError::StaleGeneration)
        );
    }
}
