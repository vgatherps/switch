use core::cell::{Cell, UnsafeCell};
use core::fmt;
use core::hash::{Hash, Hasher};
use core::marker::PhantomData;
use std::rc::Rc;
use std::sync::Mutex;

use crate::{DepotAlloc, DepotHandle, FixedDepot, HasSlots};

// ---------------------------------------------------------------------------
// Traits
// ---------------------------------------------------------------------------

/// Pool identity plus the depot/core types used by that pool.
///
/// # Safety
/// - `pool_id` must return the same identity for all bindings pointing at the same pool and
///   different identities for different live pools.
/// - `depot_ptr` / `core_ptr` must return stable pointers for all clones of a given value.
/// - `borrow_set_ptr` must return the one stable, live borrow set for this pool's depot, the same
///   for every clone and every binding of the pool. Component borrows only exclude each other when
///   they land in the same set.
/// - The depot/core storage must remain alive while any clone of the binding can be used by a
///   `Claim`, `Entity`, `EntityId`, component, or follower.
/// - Mutation through `depot_ptr` / `core_ptr` must be externally constrained to this crate's
///   single-thread claim model, or by an implementation-specific synchronization rule that gives
///   equivalent aliasing guarantees.
pub unsafe trait PoolBinding: Clone {
    type Depot: DepotAlloc + SlotBorrowStorage;
    type Core;

    fn pool_id(&self) -> *const ();
    fn depot_ptr(&self) -> *mut Self::Depot;
    fn core_ptr(&self) -> *mut Self::Core;
    fn borrow_set_ptr(&self) -> *const <Self::Depot as SlotBorrowStorage>::SlotBorrows;
}

/// Marker for zero/ref-cost bindings. New generic code should bound on [`PoolBinding`]
/// unless it specifically requires `Copy`.
///
/// # Safety
/// Implementors meet every [`PoolBinding`] requirement. Each copy counts as a clone under that
/// contract.
pub unsafe trait PoolHandle: PoolBinding + Copy {}

/// A component attached to a pool identified by binding type `H`.
///
/// The pool layer never forms a reference to a component's whole storage on the safe paths: it
/// passes the raw storage pointer and a slot to `get` / `get_mut`, and the implementation projects
/// to that one slot before forming a reference. A live `&mut` into one slot therefore stays valid
/// while other slots are read or written.
///
/// # Safety
/// - `storage_ptr` must return storage for the exact pool identified by the binding, stay valid
///   for that pool's lifetime, and point into interior-mutable memory (`UnsafeCell`, e.g. a
///   `SyncCell` static or the pool core), since slots are written while the pool is shared.
/// - The storage must hold a distinct element for every slot of the pool's depot, including the
///   sentinel slot 0.
/// - `get` and `get_mut` must return references only into `slot`'s element and must not create a
///   reference, even a temporary one, to any byte outside it.
/// - If multiple component impls share storage, their combined `get` / `get_mut` behavior must
///   still obey Rust aliasing rules.
pub unsafe trait Component<H: PoolBinding> {
    type Storage;
    type Ref<'a>: 'a;
    type RefMut<'a>: 'a;

    fn storage_ptr(handle: &H) -> *mut Self::Storage;

    /// # Safety
    /// `storage` comes from `storage_ptr` for this pool, `slot` is a slot of this pool's depot,
    /// and no `RefMut` for `slot` is live for `'a`.
    unsafe fn get<'a>(storage: *const Self::Storage, slot: u32) -> Self::Ref<'a>;

    /// # Safety
    /// `storage` comes from `storage_ptr` for this pool, `slot` is a slot of this pool's depot,
    /// and no other `Ref` or `RefMut` for `slot` is live for `'a`.
    unsafe fn get_mut<'a>(storage: *mut Self::Storage, slot: u32) -> Self::RefMut<'a>;
}

// ---------------------------------------------------------------------------
// Slot borrow state used by safe id reads and entity mutable access
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimBorrowError {
    AlreadyBorrowed,
    TooManySharedBorrows,
}

#[doc(hidden)]
pub trait SlotBorrowStorage: HasSlots + sealed::SlotBorrowStorageSealed {
    type SlotBorrows;

    const NEW_SLOT_BORROWS: Self::SlotBorrows;

    fn try_slot_shared(
        borrows: &Self::SlotBorrows,
        slot: u32,
    ) -> Result<SharedSlotBorrow<'_>, ClaimBorrowError>;
    fn try_slot_unique(
        borrows: &Self::SlotBorrows,
        slot: u32,
    ) -> Result<UniqueSlotBorrow<'_>, ClaimBorrowError>;
}

/// Per-slot guard counts: 0 unborrowed, `n > 0` shared guards, -1 one unique guard.
///
/// One `Cell` over the whole array keeps `new()` cheap to const-evaluate for large pools; each
/// guard holds the `Cell<i16>` of its slot.
#[doc(hidden)]
pub struct SlotBorrowSet<const SLOTS: usize> {
    slots: Cell<[i16; SLOTS]>,
}

impl<const SLOTS: usize> SlotBorrowSet<SLOTS> {
    pub const fn new() -> Self {
        Self {
            slots: Cell::new([0; SLOTS]),
        }
    }

    #[inline(always)]
    fn slot(&self, slot: u32) -> &Cell<i16> {
        let slots: &Cell<[i16]> = &self.slots;
        slots
            .as_slice_of_cells()
            .get(slot as usize)
            .expect("component access slot out of pool bounds")
    }
}

impl<const SLOTS: usize> Default for SlotBorrowSet<SLOTS> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const SLABS: usize, const SLOTS: usize, const AW: usize, const BW: usize, const CW: usize>
    SlotBorrowStorage for FixedDepot<SLABS, SLOTS, AW, BW, CW>
{
    type SlotBorrows = SlotBorrowSet<SLOTS>;
    // A template copied into each new pool's borrow set when the pool is built; it is never
    // borrowed itself.
    #[allow(clippy::declare_interior_mutable_const)]
    const NEW_SLOT_BORROWS: Self::SlotBorrows = SlotBorrowSet::new();

    #[inline(always)]
    fn try_slot_shared(
        borrows: &Self::SlotBorrows,
        slot: u32,
    ) -> Result<SharedSlotBorrow<'_>, ClaimBorrowError> {
        SharedSlotBorrow::acquire(borrows.slot(slot))
    }

    #[inline(always)]
    fn try_slot_unique(
        borrows: &Self::SlotBorrows,
        slot: u32,
    ) -> Result<UniqueSlotBorrow<'_>, ClaimBorrowError> {
        UniqueSlotBorrow::acquire(borrows.slot(slot))
    }
}

impl<const SLABS: usize, const SLOTS: usize, const AW: usize, const BW: usize, const CW: usize>
    sealed::SlotBorrowStorageSealed for FixedDepot<SLABS, SLOTS, AW, BW, CW>
{
}

#[doc(hidden)]
pub struct SharedSlotBorrow<'a> {
    state: &'a Cell<i16>,
}

impl<'a> SharedSlotBorrow<'a> {
    #[inline(always)]
    fn acquire(state: &'a Cell<i16>) -> Result<Self, ClaimBorrowError> {
        let count = state.get();
        if count < 0 {
            return Err(ClaimBorrowError::AlreadyBorrowed);
        }
        let next = count
            .checked_add(1)
            .ok_or(ClaimBorrowError::TooManySharedBorrows)?;
        state.set(next);
        Ok(Self { state })
    }
}

impl Drop for SharedSlotBorrow<'_> {
    #[inline(always)]
    fn drop(&mut self) {
        let count = self.state.get();
        debug_assert!(count > 0, "release shared borrow without shared borrow");
        self.state.set(count - 1);
    }
}

#[doc(hidden)]
pub struct UniqueSlotBorrow<'a> {
    state: &'a Cell<i16>,
}

impl<'a> UniqueSlotBorrow<'a> {
    #[inline(always)]
    fn acquire(state: &'a Cell<i16>) -> Result<Self, ClaimBorrowError> {
        if state.get() != 0 {
            return Err(ClaimBorrowError::AlreadyBorrowed);
        }
        state.set(-1);
        Ok(Self { state })
    }
}

impl Drop for UniqueSlotBorrow<'_> {
    #[inline(always)]
    fn drop(&mut self) {
        debug_assert_eq!(
            self.state.get(),
            -1,
            "release unique borrow without unique borrow"
        );
        self.state.set(0);
    }
}

/// Guard returned by safe non-owning-id component reads.
///
/// The guard may outlive the `Entity` that occupied its slot and keeps reading the value stored
/// there. If that slot is reused while the guard is live, `get_mut` / `try_get_mut` for the new
/// tenant fails with [`ClaimBorrowError::AlreadyBorrowed`] until the guard drops. Leaking the guard
/// with `mem::forget` leaves that slot shared-borrowed for every later tenant.
pub struct ComponentRef<'a, B, C>
where
    B: PoolBinding,
    C: Component<B>,
    C::Storage: 'a,
{
    storage: *mut C::Storage,
    slot: u32,
    _borrow: SharedSlotBorrow<'a>,
    _binding: PhantomData<&'a B>,
}

impl<'a, B, C> ComponentRef<'a, B, C>
where
    B: PoolBinding,
    C: Component<B>,
{
    #[inline(always)]
    pub fn view(&self) -> C::Ref<'_> {
        // SAFETY: `storage` came from `storage_ptr` for this guard's pool and `slot` passed depot
        // validation (or is the sentinel). The shared borrow excludes a unique borrow, the only
        // safe source of a `RefMut`, for the guard's lifetime.
        unsafe { C::get(self.storage, self.slot) }
    }
}

/// Guard returned by safe mutable entity component access.
///
/// It borrows the `Entity` mutably, so the entity cannot be dropped while it is live. (This
/// assumes the entity belongs to the claim's pool. `LocalPool` is test-only, and using an entity
/// of one `LocalPool` with another `LocalPool`'s claim is not supported.)
///
/// If the guard is leaked with `mem::forget`, the entity can then be dropped and its slot is freed
/// and reused as usual, but the slot's unique borrow is never released: every later `get_mut` /
/// `try_get_mut` and id-based read (`get_or_none` and friends) on that slot, for any future
/// tenant, fails with `ClaimBorrowError::AlreadyBorrowed` (the non-`try` forms panic).
pub struct ComponentRefMut<'a, B, C>
where
    B: PoolBinding,
    C: Component<B>,
    C::Storage: 'a,
{
    storage: *mut C::Storage,
    slot: u32,
    _borrow: UniqueSlotBorrow<'a>,
    _binding: PhantomData<&'a B>,
}

impl<'a, B, C> ComponentRefMut<'a, B, C>
where
    B: PoolBinding,
    C: Component<B>,
{
    #[inline(always)]
    pub fn view(&self) -> C::Ref<'_> {
        // SAFETY: `storage` came from `storage_ptr` for this guard's pool and `slot` is the live
        // entity's slot. The unique borrow excludes every other guard on the slot, and `view_mut`
        // needs `&mut self`, so no `RefMut` is live while this `Ref` is.
        unsafe { C::get(self.storage, self.slot) }
    }

    #[inline(always)]
    pub fn view_mut(&mut self) -> C::RefMut<'_> {
        // SAFETY: As in `view`; the unique borrow plus `&mut self` exclude every other `Ref` and
        // `RefMut` for this slot obtainable through safe access.
        unsafe { C::get_mut(self.storage, self.slot) }
    }
}

// ---------------------------------------------------------------------------
// Claims — proof of pool access
// ---------------------------------------------------------------------------

mod sealed {
    pub trait PoolClaimSealed {}
    pub trait SlotBorrowStorageSealed {}
}

/// Live proof of safe access to one pool instance.
///
/// This trait is sealed. Generic code can allocate entities and access components only
/// through entity-scoped `get` / `get_mut`. Pool-wide core/storage access is not here;
/// use [`PoolClaimUnsafeExt`] explicitly for that.
pub trait PoolClaim: sealed::PoolClaimSealed {
    type Binding: PoolBinding;

    /// Binding for the exact pool this claim owns.
    fn binding(&self) -> &Self::Binding;

    /// Runtime identity for non-static pools; the binding type alone is not enough.
    fn pool_id(&self) -> *const ();

    #[inline]
    fn alloc(&self) -> Option<Entity<Self::Binding>> {
        let binding = self.binding().clone();
        let slot = unsafe { (*binding.depot_ptr()).alloc()? };
        Some(Entity {
            id: EntityId {
                claim: Claim::new(binding),
                slot,
            },
            _not_send_sync: PhantomData,
        })
    }

    #[inline(always)]
    fn get<'a, C: Component<Self::Binding>>(
        &'a self,
        entity: &'a Entity<Self::Binding>,
    ) -> C::Ref<'a>
    where
        C::Storage: 'a,
    {
        // SAFETY: The storage belongs to this claim's pool and a live entity's slot is in range.
        // `get_mut` / `try_get_mut` need `&mut Entity`, so no guard can hand out a `RefMut` for
        // this slot while `entity` is borrowed for `'a`.
        unsafe { C::get(C::storage_ptr(self.binding()), entity.id.raw_slot()) }
    }

    #[inline(always)]
    fn try_get_mut<'a, C: Component<Self::Binding>>(
        &'a self,
        entity: &'a mut Entity<Self::Binding>,
    ) -> Result<ComponentRefMut<'a, Self::Binding, C>, ClaimBorrowError>
    where
        C::Storage: 'a,
    {
        let borrow = <Self::Binding as PoolBinding>::Depot::try_slot_unique(
            borrow_set(self.binding()),
            entity.id.raw_slot(),
        )?;
        let storage = C::storage_ptr(self.binding());
        Ok(ComponentRefMut {
            storage,
            slot: entity.id.raw_slot(),
            _borrow: borrow,
            _binding: PhantomData,
        })
    }

    #[inline(always)]
    fn get_mut<'a, C: Component<Self::Binding>>(
        &'a self,
        entity: &'a mut Entity<Self::Binding>,
    ) -> ComponentRefMut<'a, Self::Binding, C>
    where
        C::Storage: 'a,
    {
        self.try_get_mut::<C>(entity)
            .unwrap_or_else(|err| panic_claim_borrow_error(err))
    }

    /// Shared guard through a typed non-owning id. `None` maps to row 0. Stale and
    /// invalid ids return `None`.
    #[inline(always)]
    fn try_get_or_none<'a, C: Component<Self::Binding>>(
        &'a self,
        entity: Option<EntityId<Self::Binding>>,
    ) -> Result<Option<ComponentRef<'a, Self::Binding, C>>, ClaimBorrowError>
    where
        C::Storage: 'a,
    {
        self.try_get_or_none_ref::<C>(entity.as_ref())
    }

    #[inline(always)]
    fn get_or_none<'a, C: Component<Self::Binding>>(
        &'a self,
        entity: Option<EntityId<Self::Binding>>,
    ) -> Option<ComponentRef<'a, Self::Binding, C>>
    where
        C::Storage: 'a,
    {
        self.try_get_or_none::<C>(entity)
            .unwrap_or_else(|err| panic_claim_borrow_error(err))
    }

    /// Borrowed form of [`PoolClaim::try_get_or_none`]. Use this for non-`Copy`
    /// bindings to avoid cloning the id's pool binding on every lookup.
    #[inline(always)]
    fn try_get_or_none_ref<'a, C: Component<Self::Binding>>(
        &'a self,
        entity: Option<&EntityId<Self::Binding>>,
    ) -> Result<Option<ComponentRef<'a, Self::Binding, C>>, ClaimBorrowError>
    where
        C::Storage: 'a,
    {
        let Some(slot) = slot_or_sentinel(self.binding(), entity) else {
            return Ok(None);
        };
        let borrow = <Self::Binding as PoolBinding>::Depot::try_slot_shared(
            borrow_set(self.binding()),
            slot,
        )?;
        let storage = C::storage_ptr(self.binding());
        Ok(Some(ComponentRef {
            storage,
            slot,
            _borrow: borrow,
            _binding: PhantomData,
        }))
    }

    #[inline(always)]
    fn get_or_none_ref<'a, C: Component<Self::Binding>>(
        &'a self,
        entity: Option<&EntityId<Self::Binding>>,
    ) -> Option<ComponentRef<'a, Self::Binding, C>>
    where
        C::Storage: 'a,
    {
        self.try_get_or_none_ref::<C>(entity)
            .unwrap_or_else(|err| panic_claim_borrow_error(err))
    }

    /// Reconstruct a typed, non-owning id from a raw depot handle only if it is live
    /// in this claim's pool. This is identity validation, not nullable component access:
    /// failures return `None` and never map to the sentinel row.
    ///
    /// Raw depot handles are pool-local; validation checks only liveness in this claim's pool.
    #[inline(always)]
    fn entity_id_from_handle(&self, raw: DepotHandle) -> Option<EntityId<Self::Binding>> {
        let binding = self.binding().clone();
        unsafe { (&*binding.depot_ptr()).validate_live(raw)? };
        Some(EntityId {
            claim: Claim::new(binding),
            slot: raw,
        })
    }
}

/// Unsafe pool-wide escape hatches for audited internals.
///
/// Keep most generic code bounded by [`PoolClaim`]. Use this extension only when
/// manually proving no mainline component/entity access can alias the returned
/// core or storage reference.
pub trait PoolClaimUnsafeExt: PoolClaim {
    /// # Safety
    /// Caller must ensure the returned core reference does not alias any component storage
    /// or other access derived from this claim.
    #[inline(always)]
    unsafe fn core(&self) -> &<Self::Binding as PoolBinding>::Core {
        unsafe { &*self.binding().core_ptr() }
    }

    /// # Safety
    /// Caller must ensure the returned core reference is the only live access to the core,
    /// including component storage that may be backed by the core.
    #[inline(always)]
    unsafe fn core_mut(&mut self) -> &mut <Self::Binding as PoolBinding>::Core {
        unsafe { &mut *self.binding().core_ptr() }
    }

    /// # Safety
    /// Caller must ensure returned storage does not alias any live mutable component access.
    #[inline(always)]
    unsafe fn component<C: Component<Self::Binding>>(&self) -> &C::Storage {
        unsafe { &*C::storage_ptr(self.binding()) }
    }

    /// # Safety
    /// Caller must ensure no entity/component accessor aliases the returned storage.
    #[inline(always)]
    unsafe fn component_mut<C: Component<Self::Binding>>(&mut self) -> &mut C::Storage {
        unsafe { &mut *C::storage_ptr(self.binding()) }
    }

    /// # Safety
    /// Caller must ensure no other reference aliases the returned mutable component view.
    /// Entity ids cannot prove per-slot uniqueness.
    /// This takes the id by value; add a borrowed variant only when a hot mutable-id caller exists.
    ///
    /// # Panics
    /// Panics if a present id is stale or invalid.
    #[inline(always)]
    unsafe fn get_mut_or_none<'a, C: Component<Self::Binding>>(
        &'a self,
        entity: Option<EntityId<Self::Binding>>,
    ) -> C::RefMut<'a>
    where
        C::Storage: 'a,
    {
        let slot = slot_or_sentinel_assert(self.binding(), entity.as_ref());
        let storage = C::storage_ptr(self.binding());
        unsafe { C::get_mut(storage, slot) }
    }
}

impl<T: PoolClaim + ?Sized> PoolClaimUnsafeExt for T {}

#[doc(hidden)]
/// Static pool handle generated by `define_static_ecs!`.
///
/// # Safety
/// - `static_pool` must return the single stable global pool represented by this handle type.
/// - `static_handle` must return a handle for that exact pool.
/// - Every value of `Self` must return `pool_id`, `depot_ptr`, `core_ptr`, and `borrow_set_ptr` for
///   `static_pool`.
/// - No value of `Self` may point at storage other than `static_pool`.
/// - An override of `claim_static` must enforce the same permanent single-thread ownership as the
///   provided method.
pub unsafe trait StaticPoolHandle: PoolBinding + Copy + 'static {
    fn static_pool() -> &'static GlobalPool<Self::Depot, Self::Core>;
    fn static_handle() -> Self;

    fn claim_static() -> Result<StaticClaim<Self>, ClaimError> {
        Self::static_pool().acquire_static_claim()?;
        Ok(Claim::new(Self::static_handle()))
    }
}

/// Copyable/cloneable hot-path proof of access to one pool binding.
pub struct Claim<B: PoolBinding> {
    binding: B,
    _not_send_sync: PhantomData<Rc<()>>,
}

impl<B: PoolBinding> Clone for Claim<B> {
    #[inline(always)]
    fn clone(&self) -> Self {
        Self {
            binding: self.binding.clone(),
            _not_send_sync: PhantomData,
        }
    }
}

impl<B: PoolBinding + Copy> Copy for Claim<B> {}

impl<B: PoolBinding> Claim<B> {
    #[inline(always)]
    fn new(binding: B) -> Self {
        Self {
            binding,
            _not_send_sync: PhantomData,
        }
    }

    #[inline(always)]
    pub fn binding(&self) -> &B {
        &self.binding
    }

    #[doc(hidden)]
    #[inline(always)]
    pub fn pool_id(&self) -> *const () {
        self.binding.pool_id()
    }

    #[inline(always)]
    pub fn alloc(&self) -> Option<Entity<B>> {
        PoolClaim::alloc(self)
    }

    /// # Safety
    /// Caller must ensure the returned core reference does not alias any component storage
    /// or other access derived from this claim.
    #[inline(always)]
    pub unsafe fn core(&self) -> &B::Core {
        unsafe { <Self as PoolClaimUnsafeExt>::core(self) }
    }

    /// # Safety
    /// Caller must ensure the returned core reference is the only live access to the core,
    /// including component storage that may be backed by the core.
    #[inline(always)]
    pub unsafe fn core_mut(&mut self) -> &mut B::Core {
        unsafe { <Self as PoolClaimUnsafeExt>::core_mut(self) }
    }

    /// # Safety
    /// Caller must ensure returned storage does not alias any live mutable component access.
    #[inline(always)]
    pub unsafe fn component<C: Component<B>>(&self) -> &C::Storage {
        unsafe { <Self as PoolClaimUnsafeExt>::component::<C>(self) }
    }

    /// # Safety
    /// Caller must ensure no entity/component accessor aliases the returned storage.
    #[inline(always)]
    pub unsafe fn component_mut<C: Component<B>>(&mut self) -> &mut C::Storage {
        unsafe { <Self as PoolClaimUnsafeExt>::component_mut::<C>(self) }
    }

    #[inline(always)]
    pub fn get<'a, C: Component<B>>(&'a self, entity: &'a Entity<B>) -> C::Ref<'a>
    where
        C::Storage: 'a,
    {
        PoolClaim::get::<C>(self, entity)
    }

    #[inline(always)]
    pub fn try_get_mut<'a, C: Component<B>>(
        &'a self,
        entity: &'a mut Entity<B>,
    ) -> Result<ComponentRefMut<'a, B, C>, ClaimBorrowError>
    where
        C::Storage: 'a,
    {
        PoolClaim::try_get_mut::<C>(self, entity)
    }

    #[inline(always)]
    pub fn get_mut<'a, C: Component<B>>(
        &'a self,
        entity: &'a mut Entity<B>,
    ) -> ComponentRefMut<'a, B, C>
    where
        C::Storage: 'a,
    {
        PoolClaim::get_mut::<C>(self, entity)
    }

    #[inline(always)]
    pub fn try_get_or_none<'a, C: Component<B>>(
        &'a self,
        entity: Option<EntityId<B>>,
    ) -> Result<Option<ComponentRef<'a, B, C>>, ClaimBorrowError>
    where
        C::Storage: 'a,
    {
        PoolClaim::try_get_or_none::<C>(self, entity)
    }

    #[inline(always)]
    pub fn get_or_none<'a, C: Component<B>>(
        &'a self,
        entity: Option<EntityId<B>>,
    ) -> Option<ComponentRef<'a, B, C>>
    where
        C::Storage: 'a,
    {
        PoolClaim::get_or_none::<C>(self, entity)
    }

    #[inline(always)]
    pub fn try_get_or_none_ref<'a, C: Component<B>>(
        &'a self,
        entity: Option<&EntityId<B>>,
    ) -> Result<Option<ComponentRef<'a, B, C>>, ClaimBorrowError>
    where
        C::Storage: 'a,
    {
        PoolClaim::try_get_or_none_ref::<C>(self, entity)
    }

    #[inline(always)]
    pub fn get_or_none_ref<'a, C: Component<B>>(
        &'a self,
        entity: Option<&EntityId<B>>,
    ) -> Option<ComponentRef<'a, B, C>>
    where
        C::Storage: 'a,
    {
        PoolClaim::get_or_none_ref::<C>(self, entity)
    }

    #[inline(always)]
    pub fn entity_id_from_handle(&self, raw: DepotHandle) -> Option<EntityId<B>> {
        PoolClaim::entity_id_from_handle(self, raw)
    }

    /// # Safety
    /// Caller must ensure no other reference aliases the returned mutable component view.
    /// This takes the id by value; add a borrowed variant only when a hot mutable-id caller exists.
    ///
    /// # Panics
    /// Panics if a present id is stale or invalid.
    #[inline(always)]
    pub unsafe fn get_mut_or_none<'a, C: Component<B>>(
        &'a self,
        entity: Option<EntityId<B>>,
    ) -> C::RefMut<'a>
    where
        C::Storage: 'a,
    {
        unsafe { <Self as PoolClaimUnsafeExt>::get_mut_or_none::<C>(self, entity) }
    }
}

impl<B: PoolBinding> PoolClaim for Claim<B> {
    type Binding = B;

    #[inline(always)]
    fn binding(&self) -> &B {
        &self.binding
    }

    #[inline(always)]
    fn pool_id(&self) -> *const () {
        self.binding.pool_id()
    }
}

impl<B: PoolBinding> sealed::PoolClaimSealed for Claim<B> {}

pub type StaticClaim<H> = Claim<H>;
#[cfg(any(test, feature = "test-utils"))]
pub type OwnedClaim<D, C = ()> = Claim<LocalPoolBinding<D, C>>;

#[inline(always)]
fn panic_claim_borrow_error(error: ClaimBorrowError) -> ! {
    match error {
        ClaimBorrowError::AlreadyBorrowed => panic!("component slot already borrowed"),
        ClaimBorrowError::TooManySharedBorrows => panic!("too many shared component borrows"),
    }
}

#[inline(always)]
fn borrow_set<B: PoolBinding>(binding: &B) -> &<B::Depot as SlotBorrowStorage>::SlotBorrows {
    unsafe { &*binding.borrow_set_ptr() }
}

#[inline(always)]
fn slot_or_sentinel<B: PoolBinding>(binding: &B, entity: Option<&EntityId<B>>) -> Option<u32> {
    match entity {
        None => Some(0),
        Some(id) => unsafe { &*binding.depot_ptr() }.get_or_sentinel(Some(id.raw_depot_handle())),
    }
}

#[inline(always)]
fn slot_or_sentinel_assert<B: PoolBinding>(binding: &B, entity: Option<&EntityId<B>>) -> u32 {
    slot_or_sentinel(binding, entity)
        .expect("nullable mutable component access with stale entity id")
}

// ---------------------------------------------------------------------------
// EntityId<B> / Entity<B> — typed view + RAII entity handle.
// ---------------------------------------------------------------------------

/// Typed, non-owning entity id. Drop does not free the slot.
/// `Option<EntityId<B>>` is the nullable form.
pub struct EntityId<B: PoolBinding> {
    claim: Claim<B>,
    slot: DepotHandle,
}

impl<B: PoolBinding> Clone for EntityId<B> {
    fn clone(&self) -> Self {
        Self {
            claim: self.claim.clone(),
            slot: self.slot,
        }
    }
}

impl<B: PoolBinding + Copy> Copy for EntityId<B> {}

impl<B: PoolBinding> EntityId<B> {
    #[inline(always)]
    pub fn claim(&self) -> &Claim<B> {
        &self.claim
    }

    #[inline(always)]
    /// Raw slot bits without a generation/liveness check. Use claim accessors for indexing.
    pub fn raw_slot(&self) -> u32 {
        self.slot.slot()
    }

    #[inline(always)]
    /// Raw depot handle without a generation/liveness check. Use claim accessors for indexing.
    pub fn raw_depot_handle(&self) -> DepotHandle {
        self.slot
    }

    #[doc(hidden)]
    #[inline(always)]
    pub fn pool_id(&self) -> *const () {
        self.claim.binding().pool_id()
    }
}

impl<B: PoolBinding> fmt::Debug for EntityId<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EntityId")
            .field("pool_id", &self.pool_id())
            .field("handle", &self.slot)
            .finish()
    }
}

impl<B: PoolBinding> PartialEq for EntityId<B> {
    fn eq(&self, other: &Self) -> bool {
        self.pool_id() == other.pool_id() && self.slot == other.slot
    }
}

impl<B: PoolBinding> Eq for EntityId<B> {}

impl<B: PoolBinding> Hash for EntityId<B> {
    fn hash<S: Hasher>(&self, state: &mut S) {
        (self.pool_id() as usize).hash(state);
        self.slot.hash(state);
    }
}

/// Reads the slot, binding, and raw handle of an `Entity` or `EntityId`.
///
/// This does no liveness check. Code that uses it to reach storage must validate `entity_handle()`
/// against the pool's depot and must not trust `entity_slot()` on its own.
pub trait EntityRef<B: PoolBinding> {
    fn entity_slot(&self) -> u32;
    fn entity_binding(&self) -> &B;
    fn entity_handle(&self) -> DepotHandle;
}

impl<B: PoolBinding> EntityRef<B> for EntityId<B> {
    #[inline(always)]
    fn entity_slot(&self) -> u32 {
        self.raw_slot()
    }

    #[inline(always)]
    fn entity_binding(&self) -> &B {
        self.claim.binding()
    }

    #[inline(always)]
    fn entity_handle(&self) -> DepotHandle {
        self.raw_depot_handle()
    }
}

/// RAII entity handle. Drop frees the slot even if an id-based component guard is live. The guard
/// keeps reading the slot's stored value, and its shared-borrow count can block mutable access by a
/// later tenant of the same slot until the guard drops. Leaking the guard leaves the slot
/// shared-borrowed for every later tenant. Safe component access goes through a live claim.
pub struct Entity<B: PoolBinding> {
    id: EntityId<B>,
    _not_send_sync: PhantomData<Rc<()>>,
}

impl<B: PoolBinding> Entity<B> {
    #[inline(always)]
    pub fn slot(&self) -> u32 {
        self.id.raw_slot()
    }

    #[inline(always)]
    pub fn depot_handle(&self) -> DepotHandle {
        self.id.raw_depot_handle()
    }

    #[inline(always)]
    pub fn id(&self) -> EntityId<B> {
        self.id.clone()
    }

    #[doc(hidden)]
    #[inline(always)]
    pub fn pool_id(&self) -> *const () {
        self.id.pool_id()
    }
}

impl<B: PoolBinding> EntityRef<B> for Entity<B> {
    #[inline(always)]
    fn entity_slot(&self) -> u32 {
        self.id.entity_slot()
    }

    #[inline(always)]
    fn entity_binding(&self) -> &B {
        self.id.entity_binding()
    }

    #[inline(always)]
    fn entity_handle(&self) -> DepotHandle {
        self.id.entity_handle()
    }
}

impl<B: PoolBinding> Drop for Entity<B> {
    fn drop(&mut self) {
        let binding = self.id.claim.binding();
        let depot = binding.depot_ptr();

        // SAFETY: The entity's claim keeps the depot alive and its handle is allocated in it. The
        // `&mut` is exclusive: every other depot reference is formed inside a single depot call and
        // ends there, pool access is confined to the owner thread, and borrow counters and
        // component storage live outside the depot.
        unsafe { <B::Depot as DepotAlloc>::free_unchecked(&mut *depot, self.id.slot) };
    }
}

// ---------------------------------------------------------------------------
// PoolStorage<D, C> — depot/core storage shared by pool ownership policies
// ---------------------------------------------------------------------------

struct PoolStorage<D: SlotBorrowStorage, C = ()> {
    depot: UnsafeCell<D>,
    core: UnsafeCell<C>,
    borrows: D::SlotBorrows,
}

impl<D: SlotBorrowStorage, C> PoolStorage<D, C> {
    pub const fn new(depot: D, core: C) -> Self {
        Self {
            depot: UnsafeCell::new(depot),
            core: UnsafeCell::new(core),
            borrows: D::NEW_SLOT_BORROWS,
        }
    }

    #[inline(always)]
    fn depot_ptr(&self) -> *mut D {
        self.depot.get()
    }

    #[inline(always)]
    fn core_ptr(&self) -> *mut C {
        self.core.get()
    }

    #[inline(always)]
    fn borrow_set_ptr(&self) -> *const D::SlotBorrows {
        &self.borrows
    }

    #[inline(always)]
    fn pool_id(&self) -> *const () {
        self as *const Self as *const ()
    }
}

// ---------------------------------------------------------------------------
// GlobalPool<D, C> — the shipped implementation
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimError {
    OwnedByOtherThread,
}

pub struct GlobalPool<D: SlotBorrowStorage, C = ()> {
    storage: PoolStorage<D, C>,
    owner: Mutex<OwnerState>,
}

struct OwnerState {
    thread: Option<u64>,
}

// SAFETY: static depot/core access is gated by `StaticPoolHandle::claim_static`.
unsafe impl<D: SlotBorrowStorage + Send, C: Send> Sync for GlobalPool<D, C> {}

impl<D: SlotBorrowStorage, C> GlobalPool<D, C> {
    pub const fn new(depot: D, core: C) -> Self {
        Self {
            storage: PoolStorage::new(depot, core),
            owner: Mutex::new(OwnerState { thread: None }),
        }
    }

    #[doc(hidden)]
    #[inline(always)]
    pub fn depot_ptr(&self) -> *mut D {
        self.storage.depot_ptr()
    }

    #[doc(hidden)]
    #[inline(always)]
    pub fn core_ptr(&self) -> *mut C {
        self.storage.core_ptr()
    }

    #[doc(hidden)]
    #[inline(always)]
    pub fn borrow_set_ptr(&self) -> *const D::SlotBorrows {
        self.storage.borrow_set_ptr()
    }

    #[doc(hidden)]
    #[inline(always)]
    pub fn pool_id(&self) -> *const () {
        self.storage.pool_id()
    }

    fn acquire_static_claim(&self) -> Result<(), ClaimError> {
        let tid = thread_id();
        let mut owner = self.owner.lock().unwrap();
        match owner.thread {
            None => owner.thread = Some(tid),
            Some(existing) if existing == tid => {}
            Some(_) => return Err(ClaimError::OwnedByOtherThread),
        }
        Ok(())
    }
}

impl<D: SlotBorrowStorage> GlobalPool<D, ()> {
    pub const fn new_depot(depot: D) -> Self {
        Self::new(depot, ())
    }
}

// ---------------------------------------------------------------------------
// LocalPool — single-thread refcount-backed owned pool
// ---------------------------------------------------------------------------

#[cfg(any(test, feature = "test-utils"))]
struct LocalPoolInner<D: SlotBorrowStorage, C = ()> {
    storage: PoolStorage<D, C>,
}

#[cfg(any(test, feature = "test-utils"))]
impl<D: SlotBorrowStorage, C> LocalPoolInner<D, C> {
    fn new(depot: D, core: C) -> Self {
        Self {
            storage: PoolStorage::new(depot, core),
        }
    }

    #[inline(always)]
    fn pool_id(&self) -> *const () {
        self.storage.pool_id()
    }
}

/// Single-thread owned pool whose bindings keep storage alive with `Rc`.
#[cfg(any(test, feature = "test-utils"))]
pub struct LocalPool<D: SlotBorrowStorage, C = ()> {
    inner: Rc<LocalPoolInner<D, C>>,
}

#[cfg(any(test, feature = "test-utils"))]
impl<D: SlotBorrowStorage, C> Clone for LocalPool<D, C> {
    fn clone(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner),
        }
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl<D: SlotBorrowStorage, C> LocalPool<D, C> {
    pub fn new(depot: D, core: C) -> Self {
        Self {
            inner: Rc::new(LocalPoolInner::new(depot, core)),
        }
    }

    #[inline]
    pub fn claim(&self) -> OwnedClaim<D, C>
    where
        D: DepotAlloc,
    {
        Claim::new(LocalPoolBinding {
            inner: Rc::clone(&self.inner),
        })
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl<D: SlotBorrowStorage> LocalPool<D, ()> {
    pub fn new_depot(depot: D) -> Self {
        Self::new(depot, ())
    }
}

#[cfg(any(test, feature = "test-utils"))]
pub struct LocalPoolBinding<D: SlotBorrowStorage, C = ()> {
    inner: Rc<LocalPoolInner<D, C>>,
}

#[cfg(any(test, feature = "test-utils"))]
impl<D: SlotBorrowStorage, C> Clone for LocalPoolBinding<D, C> {
    fn clone(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner),
        }
    }
}

#[cfg(any(test, feature = "test-utils"))]
unsafe impl<D: DepotAlloc + SlotBorrowStorage, C> PoolBinding for LocalPoolBinding<D, C> {
    type Depot = D;
    type Core = C;

    #[inline(always)]
    fn pool_id(&self) -> *const () {
        self.inner.pool_id()
    }

    #[inline(always)]
    fn depot_ptr(&self) -> *mut D {
        self.inner.storage.depot_ptr()
    }

    #[inline(always)]
    fn core_ptr(&self) -> *mut C {
        self.inner.storage.core_ptr()
    }

    #[inline(always)]
    fn borrow_set_ptr(&self) -> *const D::SlotBorrows {
        self.inner.storage.borrow_set_ptr()
    }
}

// ---------------------------------------------------------------------------
// thread_id helper
// ---------------------------------------------------------------------------

fn thread_id() -> u64 {
    use core::sync::atomic::{AtomicU64, Ordering};
    use std::cell::Cell;
    static NEXT: AtomicU64 = AtomicU64::new(1);
    thread_local! { static TID: Cell<u64> = const { Cell::new(0) }; }
    TID.with(|c| {
        let v = c.get();
        if v != 0 {
            return v;
        }
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        c.set(id);
        id
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixed_depot_type;
    type Depot64 = fixed_depot_type!(64);

    #[derive(Clone, Copy)]
    struct HandWrittenPool;

    static HAND_WRITTEN_POOL: GlobalPool<Depot64> = GlobalPool::new(Depot64::new(), ());

    unsafe impl PoolBinding for HandWrittenPool {
        type Depot = Depot64;
        type Core = ();

        fn pool_id(&self) -> *const () {
            HAND_WRITTEN_POOL.pool_id()
        }

        fn depot_ptr(&self) -> *mut Self::Depot {
            HAND_WRITTEN_POOL.depot_ptr()
        }

        fn core_ptr(&self) -> *mut Self::Core {
            HAND_WRITTEN_POOL.core_ptr()
        }

        fn borrow_set_ptr(&self) -> *const <Self::Depot as SlotBorrowStorage>::SlotBorrows {
            HAND_WRITTEN_POOL.borrow_set_ptr()
        }
    }

    unsafe impl PoolHandle for HandWrittenPool {}

    unsafe impl StaticPoolHandle for HandWrittenPool {
        fn static_pool() -> &'static GlobalPool<Self::Depot, Self::Core> {
            &HAND_WRITTEN_POOL
        }

        fn static_handle() -> Self {
            Self
        }
    }

    #[test]
    fn static_pool_depot_is_all_zero_bytes() {
        crate::define_static_ecs!(ZeroCorePool, 64, core: u64 = 9);
        let claim = ZeroCorePool::claim().unwrap();
        // SAFETY: No component storage aliases this pool's `u64` core.
        unsafe { assert_eq!(*claim.core(), 9) };
        let first = claim.alloc().unwrap();
        let second = claim.alloc().unwrap();
        let third = claim.alloc().unwrap();
        assert_eq!(first.slot(), 1);
        assert_eq!(second.slot(), 2);
        assert_eq!(third.slot(), 3);
    }

    #[test]
    fn test_owned_claim_with_core() {
        let pool = LocalPool::new(Depot64::new(), "hello");
        let handle = pool.claim();
        unsafe { assert_eq!(*handle.core(), "hello") };
    }

    #[test]
    fn test_claim_convenience() {
        let pool = LocalPool::new_depot(Depot64::new());
        let handle = pool.claim();
        let entity = handle.alloc().unwrap();
        assert!(entity.slot() > 0);
    }

    #[test]
    fn local_claim_is_cloneable_refcounted_capability() {
        let pool = LocalPool::new_depot(Depot64::new());
        let claim = pool.claim();
        let claim2 = claim.clone();
        let _entity = claim2.alloc().unwrap();
    }

    #[test]
    fn test_cross_thread_claim_rejected() {
        crate::define_static_ecs!(CrossThreadPool, 64);
        let _handle = CrossThreadPool::claim().unwrap();
        let t = std::thread::spawn(|| {
            matches!(
                CrossThreadPool::claim(),
                Err(ClaimError::OwnedByOtherThread)
            )
        });
        assert!(t.join().unwrap());
    }

    #[test]
    fn claim_static_default_path_thread_ownership() {
        crate::define_static_ecs!(DefaultStaticClaimPool, 64);
        crate::define_static_ecs!(OtherThreadFirstPool, 64);

        assert!(<DefaultStaticClaimPool as StaticPoolHandle>::claim_static().is_ok());
        assert!(<DefaultStaticClaimPool as StaticPoolHandle>::claim_static().is_ok());
        assert!(DefaultStaticClaimPool::claim().is_ok());

        let rejected = std::thread::spawn(|| {
            matches!(
                <DefaultStaticClaimPool as StaticPoolHandle>::claim_static(),
                Err(ClaimError::OwnedByOtherThread)
            )
        });
        assert!(rejected.join().unwrap());

        let claimed = std::thread::spawn(|| OtherThreadFirstPool::claim().is_ok());
        assert!(claimed.join().unwrap());
        assert!(matches!(
            <OtherThreadFirstPool as StaticPoolHandle>::claim_static(),
            Err(ClaimError::OwnedByOtherThread)
        ));
    }

    #[test]
    fn hand_written_static_pool_handle_uses_default_claim_static() {
        let claim = <HandWrittenPool as StaticPoolHandle>::claim_static().unwrap();
        let entity = claim.alloc().unwrap();
        assert_eq!(entity.slot(), 1);
    }

    #[test]
    fn test_const_construction() {
        crate::define_static_ecs!(ConstConstructedPool, 64);
        let handle = ConstConstructedPool::claim().unwrap();
        let entity = handle.alloc().unwrap();
        assert!(entity.slot() > 0);
    }

    #[test]
    fn test_entity_auto_frees() {
        let pool = LocalPool::new_depot(Depot64::new());
        let handle = pool.claim();
        let slot;
        {
            let entity = handle.alloc().unwrap();
            slot = entity.slot();
        }
        let entity2 = handle.alloc().unwrap();
        assert_eq!(entity2.slot(), slot);
    }

    #[test]
    fn test_static_entity_and_id_are_niche_sized() {
        crate::define_static_ecs!(SizePool, 64);
        type OwnedBinding = LocalPoolBinding<Depot64>;
        let _claim = SizePool::claim().unwrap();

        assert_eq!(core::mem::size_of::<EntityId<SizePool>>(), 4);
        assert_eq!(core::mem::size_of::<Option<EntityId<SizePool>>>(), 4);
        assert_eq!(core::mem::size_of::<Entity<SizePool>>(), 4);
        assert_eq!(core::mem::size_of::<Option<Entity<SizePool>>>(), 4);

        assert_eq!(
            core::mem::size_of::<Option<EntityId<OwnedBinding>>>(),
            core::mem::size_of::<EntityId<OwnedBinding>>()
        );
        assert_eq!(
            core::mem::size_of::<Entity<OwnedBinding>>(),
            core::mem::size_of::<EntityId<OwnedBinding>>()
        );
        assert_eq!(
            core::mem::size_of::<Option<Entity<OwnedBinding>>>(),
            core::mem::size_of::<Entity<OwnedBinding>>()
        );
    }

    #[test]
    fn entity_id_has_value_semantics() {
        use std::collections::HashSet;

        crate::define_static_ecs!(ValueSemanticsPool, 64);
        let claim = ValueSemanticsPool::claim().unwrap();
        let entity_a = claim.alloc().unwrap();
        let entity_b = claim.alloc().unwrap();
        let id_a = entity_a.id();
        let id_a_copy = entity_a.id();
        let id_b = entity_b.id();

        assert_eq!(id_a, id_a_copy);
        assert_ne!(id_a, id_b);
        assert!(format!("{id_a:?}").contains("EntityId"));

        let mut ids = HashSet::new();
        assert!(ids.insert(id_a));
        assert!(!ids.insert(id_a_copy));
        assert!(ids.insert(id_b));
    }

    #[test]
    fn entity_ref_agrees_for_entity_and_id() {
        crate::define_static_ecs!(EntityRefPool, 64);

        fn properties<B: PoolBinding, E: EntityRef<B>>(
            entity: &E,
        ) -> (u32, DepotHandle, *const ()) {
            (
                entity.entity_slot(),
                entity.entity_handle(),
                entity.entity_binding().pool_id(),
            )
        }

        let claim = EntityRefPool::claim().unwrap();
        let entity = claim.alloc().unwrap();
        let id = entity.id();
        let expected = (
            entity.slot(),
            entity.depot_handle(),
            claim.binding().pool_id(),
        );

        assert_eq!(properties(&entity), expected);
        assert_eq!(properties(&id), expected);
    }

    #[test]
    fn entity_id_equality_after_slot_reuse() {
        use std::collections::HashSet;

        crate::define_static_ecs!(ReusedSlotIdentityPool, 64);
        let claim = ReusedSlotIdentityPool::claim().unwrap();
        let entity = claim.alloc().unwrap();
        let id = entity.id();
        drop(entity);

        let replacement = claim.alloc().unwrap();
        let replacement_id = replacement.id();
        assert_eq!(id.raw_slot(), replacement_id.raw_slot());
        assert_ne!(id, replacement_id);

        let mut ids = HashSet::new();
        assert!(ids.insert(id));
        assert!(ids.insert(replacement_id));
        assert_eq!(ids.len(), 2);
    }

    #[test]
    fn owned_entity_id_value_semantics_include_pool_identity() {
        let pool_a = LocalPool::new_depot(Depot64::new());
        let pool_b = LocalPool::new_depot(Depot64::new());

        let claim_a = pool_a.claim();
        let claim_b = pool_b.claim();
        let entity_a = claim_a.alloc().unwrap();
        let entity_b = claim_b.alloc().unwrap();

        assert_eq!(entity_a.id(), entity_a.id());
        assert_ne!(entity_a.id(), entity_b.id());
    }

    #[test]
    fn test_core_type_mutable() {
        let pool = LocalPool::new(Depot64::new(), vec![0u64; 64]);
        let mut handle = pool.claim();
        let entity = handle.alloc().unwrap();
        let slot = entity.slot() as usize;
        unsafe { handle.core_mut()[slot] = 42 };
        unsafe { assert_eq!(handle.core()[slot], 42) };
    }

    #[test]
    fn test_static_ecs_with_core() {
        crate::define_static_ecs!(CorePool, 64, core: u64 = 0);
        let mut handle = CorePool::claim().unwrap();
        unsafe { assert_eq!(*handle.core(), 0) };
        unsafe { *handle.core_mut() = 99 };
        unsafe { assert_eq!(*handle.core(), 99) };
    }

    #[test]
    fn test_plain_type_component_via_entity() {
        crate::define_static_ecs!(PlainPool, 64);

        struct Timestamps;
        type PlainDepot = <PlainPool as crate::PoolBinding>::Depot;

        static TIMESTAMPS: crate::SyncCell<[u64; <PlainDepot as crate::HasSlots>::SLOTS]> =
            crate::SyncCell::new([0u64; <PlainDepot as crate::HasSlots>::SLOTS]);

        unsafe impl crate::Component<PlainPool> for Timestamps {
            type Storage = [u64; <PlainDepot as crate::HasSlots>::SLOTS];
            type Ref<'a> = &'a u64;
            type RefMut<'a> = &'a mut u64;

            fn storage_ptr(_handle: &PlainPool) -> *mut Self::Storage {
                TIMESTAMPS.get()
            }

            unsafe fn get<'a>(storage: *const Self::Storage, slot: u32) -> &'a u64 {
                // SAFETY: The caller passes this pool's storage and an in-range slot; indexing the
                // array place projects to that one element before the reference is formed.
                unsafe { &(*storage)[slot as usize] }
            }

            unsafe fn get_mut<'a>(storage: *mut Self::Storage, slot: u32) -> &'a mut u64 {
                // SAFETY: As in `get`, and the caller guarantees no other reference to this slot.
                unsafe { &mut (*storage)[slot as usize] }
            }
        }

        let handle = PlainPool::claim().unwrap();
        let mut entity = handle.alloc().unwrap();

        fn write_timestamp<P>(claim: &P, entity: &mut Entity<P::Binding>, value: u64)
        where
            P: crate::PoolClaim,
            for<'a> Timestamps:
                crate::Component<P::Binding, Ref<'a> = &'a u64, RefMut<'a> = &'a mut u64>,
        {
            *<P as crate::PoolClaim>::get_mut::<Timestamps>(claim, entity).view_mut() = value;
            assert_eq!(
                *<P as crate::PoolClaim>::get::<Timestamps>(claim, entity),
                value
            );
        }

        write_timestamp(&handle, &mut entity, 123_456_789);
    }

    struct LocalTimestamps;

    unsafe impl crate::Component<LocalPoolBinding<Depot64, [u64; 64]>> for LocalTimestamps {
        type Storage = [u64; 64];
        type Ref<'a> = &'a u64;
        type RefMut<'a> = &'a mut u64;

        fn storage_ptr(binding: &LocalPoolBinding<Depot64, [u64; 64]>) -> *mut Self::Storage {
            crate::PoolBinding::core_ptr(binding)
        }

        unsafe fn get<'a>(storage: *const Self::Storage, slot: u32) -> &'a u64 {
            // SAFETY: The caller passes this pool's storage and an in-range slot; indexing the
            // array place projects to that one element before the reference is formed.
            unsafe { &(*storage)[slot as usize] }
        }

        unsafe fn get_mut<'a>(storage: *mut Self::Storage, slot: u32) -> &'a mut u64 {
            // SAFETY: As in `get`, and the caller guarantees no other reference to this slot.
            unsafe { &mut (*storage)[slot as usize] }
        }
    }

    type LocalClaim = OwnedClaim<Depot64, [u64; 64]>;

    fn check_pool_invariants(claim: &LocalClaim) {
        // SAFETY: The claim keeps the depot alive for this call.
        let depot = unsafe { &*claim.binding().depot_ptr() };
        depot.check_invariants();
    }

    #[test]
    fn component_views_do_not_invalidate_other_slot_mut() {
        let pool = LocalPool::new(Depot64::new(), [0u64; 64]);
        let claim = pool.claim();
        let mut a = claim.alloc().unwrap();
        let b = claim.alloc().unwrap();
        let mut c = claim.alloc().unwrap();
        let id_b = b.id();

        let mut write_a = claim.get_mut::<LocalTimestamps>(&mut a);
        let a_value = write_a.view_mut();
        *a_value = 1;

        assert_eq!(*claim.get::<LocalTimestamps>(&b), 0);
        *a_value = 2;
        assert_eq!(
            *claim
                .get_or_none_ref::<LocalTimestamps>(Some(&id_b))
                .unwrap()
                .view(),
            0
        );
        *a_value = 3;
        assert_eq!(*claim.get_or_none::<LocalTimestamps>(None).unwrap().view(), 0);
        *a_value = 4;
        let write_c = claim.get_mut::<LocalTimestamps>(&mut c);
        assert_eq!(*write_c.view(), 0);
        *a_value = 5;
        drop(write_c);

        assert_eq!(*write_a.view(), 5);
    }

    #[test]
    fn static_component_views_do_not_invalidate_other_slot_mut() {
        crate::define_static_ecs!(AliasingPool, 64);

        struct AliasingTimestamps;
        type AliasingDepot = <AliasingPool as crate::PoolBinding>::Depot;

        static TIMESTAMPS: crate::SyncCell<[u64; <AliasingDepot as crate::HasSlots>::SLOTS]> =
            crate::SyncCell::new([0u64; <AliasingDepot as crate::HasSlots>::SLOTS]);

        // SAFETY: Storage is a `SyncCell` static with one element per slot of `AliasingPool`, and
        // both projections touch only the requested element.
        unsafe impl crate::Component<AliasingPool> for AliasingTimestamps {
            type Storage = [u64; <AliasingDepot as crate::HasSlots>::SLOTS];
            type Ref<'a> = &'a u64;
            type RefMut<'a> = &'a mut u64;

            fn storage_ptr(_handle: &AliasingPool) -> *mut Self::Storage {
                TIMESTAMPS.get()
            }

            unsafe fn get<'a>(storage: *const Self::Storage, slot: u32) -> &'a u64 {
                // SAFETY: The caller passes this pool's storage and an in-range slot; indexing the
                // array place projects to that one element before the reference is formed.
                unsafe { &(*storage)[slot as usize] }
            }

            unsafe fn get_mut<'a>(storage: *mut Self::Storage, slot: u32) -> &'a mut u64 {
                // SAFETY: As in `get`, and the caller guarantees no other reference to this slot.
                unsafe { &mut (*storage)[slot as usize] }
            }
        }

        let claim = AliasingPool::claim().unwrap();
        let mut a = claim.alloc().unwrap();
        let b = claim.alloc().unwrap();
        let mut c = claim.alloc().unwrap();
        let id_b = b.id();

        let mut write_a = claim.get_mut::<AliasingTimestamps>(&mut a);
        let a_value = write_a.view_mut();
        *a_value = 1;

        assert_eq!(*claim.get::<AliasingTimestamps>(&b), 0);
        *a_value = 2;
        assert_eq!(
            *claim
                .get_or_none::<AliasingTimestamps>(Some(id_b))
                .unwrap()
                .view(),
            0
        );
        *a_value = 3;
        let write_c = claim.get_mut::<AliasingTimestamps>(&mut c);
        assert_eq!(*write_c.view(), 0);
        *a_value = 4;
        drop(write_c);

        assert_eq!(*write_a.view(), 4);
    }

    #[test]
    fn id_guard_on_other_slot_does_not_block_drop() {
        let pool = LocalPool::new(Depot64::new(), [0u64; 64]);
        let claim = pool.claim();
        let a = claim.alloc().unwrap();
        let b = claim.alloc().unwrap();
        let id_a = a.id();
        let raw_b = b.depot_handle();

        let read = claim.get_or_none_ref::<LocalTimestamps>(Some(&id_a)).unwrap();
        drop(b);
        assert_eq!(claim.entity_id_from_handle(raw_b), None);
        check_pool_invariants(&claim);

        let reused = claim.alloc().unwrap();
        assert_eq!(reused.slot(), raw_b.slot());
        assert_eq!(*read.view(), 0);
        drop(read);
        drop(a);
        check_pool_invariants(&claim);
    }

    #[test]
    fn reused_slot_get_mut_blocked_by_previous_tenant_guard() {
        let pool = LocalPool::new(Depot64::new(), [0u64; 64]);
        let claim = pool.claim();
        let mut first = claim.alloc().unwrap();
        *claim.get_mut::<LocalTimestamps>(&mut first).view_mut() = 19;
        let first_id = first.id();
        let slot = first.slot();
        let read = claim
            .get_or_none_ref::<LocalTimestamps>(Some(&first_id))
            .unwrap();

        drop(first);
        let mut second = claim.alloc().unwrap();
        assert_eq!(second.slot(), slot);
        assert!(matches!(
            claim.try_get_mut::<LocalTimestamps>(&mut second),
            Err(ClaimBorrowError::AlreadyBorrowed)
        ));

        drop(read);
        assert!(claim.try_get_mut::<LocalTimestamps>(&mut second).is_ok());
        check_pool_invariants(&claim);
    }

    #[test]
    fn leaked_id_guard_keeps_slot_shared_for_next_tenant() {
        let pool = LocalPool::new(Depot64::new(), [0u64; 64]);
        let claim = pool.claim();
        let mut first = claim.alloc().unwrap();
        *claim.get_mut::<LocalTimestamps>(&mut first).view_mut() = 23;
        let first_id = first.id();
        let slot = first.slot();

        core::mem::forget(
            claim
                .get_or_none_ref::<LocalTimestamps>(Some(&first_id))
                .unwrap(),
        );
        drop(first);

        let mut second = claim.alloc().unwrap();
        assert_eq!(second.slot(), slot);
        assert_eq!(*claim.get::<LocalTimestamps>(&second), 23);
        let second_id = second.id();
        assert_eq!(
            *claim
                .get_or_none_ref::<LocalTimestamps>(Some(&second_id))
                .unwrap()
                .view(),
            23
        );
        assert!(matches!(
            claim.try_get_mut::<LocalTimestamps>(&mut second),
            Err(ClaimBorrowError::AlreadyBorrowed)
        ));
        check_pool_invariants(&claim);
    }

    #[test]
    fn leaked_mut_guard_frees_on_drop_and_keeps_slot_unique_borrowed() {
        let pool = LocalPool::new(Depot64::new(), [0u64; 64]);
        let claim = pool.claim();
        let mut entity = claim.alloc().unwrap();
        let slot = entity.slot();
        let raw = entity.depot_handle();

        core::mem::forget(claim.get_mut::<LocalTimestamps>(&mut entity));
        drop(entity);
        assert_eq!(claim.entity_id_from_handle(raw), None);
        check_pool_invariants(&claim);

        let mut tenant = claim.alloc().unwrap();
        assert_eq!(tenant.slot(), slot);
        let tenant_id = tenant.id();
        assert!(matches!(
            claim.try_get_mut::<LocalTimestamps>(&mut tenant),
            Err(ClaimBorrowError::AlreadyBorrowed)
        ));
        assert!(matches!(
            claim.try_get_or_none_ref::<LocalTimestamps>(Some(&tenant_id)),
            Err(ClaimBorrowError::AlreadyBorrowed)
        ));
        assert_eq!(*claim.get::<LocalTimestamps>(&tenant), 0);
        drop(tenant);
        check_pool_invariants(&claim);
    }

    fn shared_count(claim: &LocalClaim, slot: u32) -> i16 {
        borrow_set(claim.binding()).slot(slot).get()
    }

    // 32767 live guards on one cell: minutes under Tree Borrows. The error path returns before
    // touching memory, so the native run covers it.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn too_many_shared_borrows_leaves_counter_unchanged() {
        let pool = LocalPool::new(Depot64::new(), [0u64; 64]);
        let claim = pool.claim();
        let entity = claim.alloc().unwrap();
        let slot = entity.slot();
        let id = entity.id();

        let guards: Vec<_> = (0..i16::MAX)
            .map(|_| claim.get_or_none_ref::<LocalTimestamps>(Some(&id)).unwrap())
            .collect();
        assert!(matches!(
            claim.try_get_or_none_ref::<LocalTimestamps>(Some(&id)),
            Err(ClaimBorrowError::TooManySharedBorrows)
        ));
        assert_eq!(shared_count(&claim, slot), i16::MAX);
        check_pool_invariants(&claim);

        drop(guards);
        assert_eq!(shared_count(&claim, slot), 0);
        check_pool_invariants(&claim);
        drop(entity);
        check_pool_invariants(&claim);
    }

    #[test]
    fn entity_id_claim_accessor() {
        let pool = LocalPool::new(Depot64::new(), [0u64; 64]);
        let claim = pool.claim();
        let mut entity = claim.alloc().unwrap();
        *claim.get_mut::<LocalTimestamps>(&mut entity).view_mut() = 144;
        let id = entity.id();

        assert_eq!(
            *id.claim()
                .get_or_none_ref::<LocalTimestamps>(Some(&id))
                .unwrap()
                .view(),
            144
        );
    }

    #[test]
    fn static_get_or_none_none_reads_sentinel_stale_is_none() {
        crate::define_static_ecs!(NullablePool, 64);

        struct NullableTimestamps;
        type NullableDepot = <NullablePool as crate::PoolBinding>::Depot;

        static TIMESTAMPS: crate::SyncCell<[u64; <NullableDepot as crate::HasSlots>::SLOTS]> =
            crate::SyncCell::new([0u64; <NullableDepot as crate::HasSlots>::SLOTS]);

        unsafe impl crate::Component<NullablePool> for NullableTimestamps {
            type Storage = [u64; <NullableDepot as crate::HasSlots>::SLOTS];
            type Ref<'a> = &'a u64;
            type RefMut<'a> = &'a mut u64;

            fn storage_ptr(_handle: &NullablePool) -> *mut Self::Storage {
                TIMESTAMPS.get()
            }

            unsafe fn get<'a>(storage: *const Self::Storage, slot: u32) -> &'a u64 {
                // SAFETY: The caller passes this pool's storage and an in-range slot; indexing the
                // array place projects to that one element before the reference is formed.
                unsafe { &(*storage)[slot as usize] }
            }

            unsafe fn get_mut<'a>(storage: *mut Self::Storage, slot: u32) -> &'a mut u64 {
                // SAFETY: As in `get`, and the caller guarantees no other reference to this slot.
                unsafe { &mut (*storage)[slot as usize] }
            }
        }

        let claim = NullablePool::claim().unwrap();
        unsafe { *claim.get_mut_or_none::<NullableTimestamps>(None) = 7 };

        let mut entity = claim.alloc().unwrap();
        let id = entity.id();
        *claim.get_mut::<NullableTimestamps>(&mut entity).view_mut() = 11;

        assert_eq!(
            *claim
                .get_or_none_ref::<NullableTimestamps>(Some(&id))
                .unwrap()
                .view(),
            11
        );
        assert_eq!(
            *claim
                .get_or_none::<NullableTimestamps>(Some(id))
                .unwrap()
                .view(),
            11
        );
        drop(entity);
        assert!(claim.get_or_none_ref::<NullableTimestamps>(Some(&id)).is_none());
        assert!(claim.get_or_none::<NullableTimestamps>(Some(id)).is_none());
        assert_eq!(
            *claim
                .get_or_none::<NullableTimestamps>(None)
                .unwrap()
                .view(),
            7
        );
        assert_eq!(
            *claim
                .get_or_none_ref::<NullableTimestamps>(None)
                .unwrap()
                .view(),
            7
        );
    }

    #[test]
    fn owned_get_or_none_none_reads_sentinel_stale_is_none() {
        let pool = LocalPool::new(Depot64::new(), [0u64; 64]);
        let mut claim = pool.claim();
        unsafe { claim.core_mut()[0] = 13 };

        let mut entity = claim.alloc().unwrap();
        let id = entity.id();
        *claim.get_mut::<LocalTimestamps>(&mut entity).view_mut() = 21;

        assert_eq!(
            *claim
                .get_or_none_ref::<LocalTimestamps>(Some(&id))
                .unwrap()
                .view(),
            21
        );
        assert_eq!(
            *claim
                .get_or_none::<LocalTimestamps>(Some(id.clone()))
                .unwrap()
                .view(),
            21
        );
        drop(entity);
        assert!(claim.get_or_none_ref::<LocalTimestamps>(Some(&id)).is_none());
        assert!(
            claim
                .get_or_none::<LocalTimestamps>(Some(id.clone()))
                .is_none()
        );
        assert_eq!(
            *claim
                .get_or_none::<LocalTimestamps>(None)
                .unwrap()
                .view(),
            13
        );
        assert_eq!(
            *claim
                .get_or_none_ref::<LocalTimestamps>(None)
                .unwrap()
                .view(),
            13
        );
    }

    #[test]
    fn owned_get_mut_or_none_can_write_sentinel() {
        let pool = LocalPool::new(Depot64::new(), [0u64; 64]);
        let claim = pool.claim();

        unsafe { *claim.get_mut_or_none::<LocalTimestamps>(None) = 34 };
        assert_eq!(
            *claim
                .get_or_none::<LocalTimestamps>(None)
                .unwrap()
                .view(),
            34
        );
    }

    #[test]
    fn try_get_or_none_stale_id_is_ok_none_and_takes_no_borrow() {
        let pool = LocalPool::new(Depot64::new(), [0u64; 64]);
        let claim = pool.claim();
        let entity = claim.alloc().unwrap();
        let id = entity.id();
        let slot = entity.slot();
        drop(entity);

        let mut replacement = claim.alloc().unwrap();
        assert_eq!(replacement.slot(), slot);
        let write = claim.get_mut::<LocalTimestamps>(&mut replacement);
        assert!(matches!(
            claim.try_get_or_none_ref::<LocalTimestamps>(Some(&id)),
            Ok(None)
        ));
        drop(write);
    }

    #[test]
    fn static_try_get_or_none_stale_id_is_ok_none() {
        crate::define_static_ecs!(StaticStalePool, 64);

        struct StaticStaleTimestamps;
        type StaticStaleDepot = <StaticStalePool as crate::PoolBinding>::Depot;

        static TIMESTAMPS: crate::SyncCell<
            [u64; <StaticStaleDepot as crate::HasSlots>::SLOTS],
        > = crate::SyncCell::new([0u64; <StaticStaleDepot as crate::HasSlots>::SLOTS]);

        // SAFETY: This component uses storage owned by `StaticStalePool` and indexes its slots.
        unsafe impl crate::Component<StaticStalePool> for StaticStaleTimestamps {
            type Storage = [u64; <StaticStaleDepot as crate::HasSlots>::SLOTS];
            type Ref<'a> = &'a u64;
            type RefMut<'a> = &'a mut u64;

            fn storage_ptr(_handle: &StaticStalePool) -> *mut Self::Storage {
                TIMESTAMPS.get()
            }

            unsafe fn get<'a>(storage: *const Self::Storage, slot: u32) -> &'a u64 {
                // SAFETY: The caller passes this pool's storage and an in-range slot; indexing the
                // array place projects to that one element before the reference is formed.
                unsafe { &(*storage)[slot as usize] }
            }

            unsafe fn get_mut<'a>(storage: *mut Self::Storage, slot: u32) -> &'a mut u64 {
                // SAFETY: As in `get`, and the caller guarantees no other reference to this slot.
                unsafe { &mut (*storage)[slot as usize] }
            }
        }

        let claim = StaticStalePool::claim().unwrap();
        let entity = claim.alloc().unwrap();
        let id = entity.id();
        drop(entity);

        assert!(matches!(
            claim.try_get_or_none::<StaticStaleTimestamps>(Some(id)),
            Ok(None)
        ));
    }

    #[test]
    fn get_or_none_stale_after_slot_reuse_is_none() {
        let pool = LocalPool::new(Depot64::new(), [0u64; 64]);
        let claim = pool.claim();
        let entity = claim.alloc().unwrap();
        let stale = entity.id();
        let slot = entity.slot();
        drop(entity);

        let current = claim.alloc().unwrap();
        assert_eq!(current.slot(), slot);
        assert!(
            claim
                .get_or_none_ref::<LocalTimestamps>(Some(&stale))
                .is_none()
        );
        assert!(
            claim
                .get_or_none::<LocalTimestamps>(Some(current.id()))
                .is_some()
        );
    }

    #[test]
    #[should_panic(expected = "nullable mutable component access with stale entity id")]
    fn get_mut_or_none_stale_id_panics() {
        let pool = LocalPool::new(Depot64::new(), [0u64; 64]);
        let claim = pool.claim();
        let entity = claim.alloc().unwrap();
        let stale = entity.id();
        drop(entity);

        // SAFETY: This test checks the documented panic before mutable storage access.
        let _ = unsafe { claim.get_mut_or_none::<LocalTimestamps>(Some(stale)) };
    }

    #[test]
    fn id_read_guard_blocks_entity_mut_until_dropped() {
        let pool = LocalPool::new(Depot64::new(), [0u64; 64]);
        let claim = pool.claim();
        let mut entity = claim.alloc().unwrap();
        let id = entity.id();

        let read = claim
            .get_or_none_ref::<LocalTimestamps>(Some(&id))
            .unwrap();
        assert!(matches!(
            claim.try_get_mut::<LocalTimestamps>(&mut entity),
            Err(ClaimBorrowError::AlreadyBorrowed)
        ));

        drop(read);
        assert!(claim.try_get_mut::<LocalTimestamps>(&mut entity).is_ok());
    }

    #[test]
    fn entity_mut_guard_blocks_id_read_until_dropped() {
        let pool = LocalPool::new(Depot64::new(), [0u64; 64]);
        let claim = pool.claim();
        let mut entity = claim.alloc().unwrap();
        let id = entity.id();

        let write = claim.get_mut::<LocalTimestamps>(&mut entity);
        assert!(matches!(
            claim.try_get_or_none_ref::<LocalTimestamps>(Some(&id)),
            Err(ClaimBorrowError::AlreadyBorrowed)
        ));

        drop(write);
        assert!(matches!(
            claim.try_get_or_none_ref::<LocalTimestamps>(Some(&id)),
            Ok(Some(_))
        ));
    }

    #[test]
    fn entity_drop_with_live_id_guard_frees_slot() {
        let pool = LocalPool::new(Depot64::new(), [0u64; 64]);
        let claim = pool.claim();
        let mut entity = claim.alloc().unwrap();
        *claim.get_mut::<LocalTimestamps>(&mut entity).view_mut() = 77;
        let slot = entity.slot();
        let id = entity.id();
        let raw = entity.depot_handle();
        let read = claim
            .get_or_none_ref::<LocalTimestamps>(Some(&id))
            .unwrap();

        drop(entity);
        assert_eq!(claim.entity_id_from_handle(raw), None);
        assert!(claim.get_or_none_ref::<LocalTimestamps>(Some(&id)).is_none());
        assert!(claim.get_or_none::<LocalTimestamps>(Some(id.clone())).is_none());
        assert_eq!(*read.view(), 77);

        let mut next = claim.alloc().unwrap();
        assert_eq!(next.slot(), slot);
        assert_ne!(next.depot_handle().generation(), raw.generation());
        assert!(matches!(
            claim.try_get_mut::<LocalTimestamps>(&mut next),
            Err(ClaimBorrowError::AlreadyBorrowed)
        ));

        drop(read);
        check_pool_invariants(&claim);
    }

    #[test]
    fn entity_read_does_not_mark_slot() {
        let pool = LocalPool::new(Depot64::new(), [0u64; 64]);
        let claim = pool.claim();
        let mut entity = claim.alloc().unwrap();
        let id = entity.id();
        *claim.get_mut::<LocalTimestamps>(&mut entity).view_mut() = 89;

        let entity_read = claim.get::<LocalTimestamps>(&entity);
        let id_read = claim
            .get_or_none_ref::<LocalTimestamps>(Some(&id))
            .unwrap();

        assert_eq!(*entity_read, *id_read.view());
    }

    #[test]
    fn local_pool_binding_keeps_storage_alive_without_lifetime() {
        let pool = LocalPool::new(Depot64::new(), [0u64; 64]);
        let claim = pool.claim();
        let mut entity = claim.alloc().unwrap();
        let id = entity.id();
        let raw = entity.depot_handle();

        drop(pool);

        *claim.get_mut::<LocalTimestamps>(&mut entity).view_mut() = 55;
        assert_eq!(*claim.get::<LocalTimestamps>(&entity), 55);
        assert_eq!(
            *claim
                .get_or_none::<LocalTimestamps>(Some(id))
                .unwrap()
                .view(),
            55
        );
        assert_eq!(claim.entity_id_from_handle(raw), Some(entity.id()));
    }

    #[test]
    fn static_entity_id_from_handle_strictly_reconstructs_live_ids() {
        crate::define_static_ecs!(IdentityStaticPool, 64);
        let claim = IdentityStaticPool::claim().unwrap();

        let entity = claim.alloc().unwrap();
        let raw = entity.depot_handle();
        assert_eq!(claim.entity_id_from_handle(raw), Some(entity.id()));

        drop(entity);
        assert_eq!(claim.entity_id_from_handle(raw), None);

        let forged_current_dead =
            crate::encode_handle(raw.slot(), crate::next_live_generation(raw.generation()));
        assert_eq!(claim.entity_id_from_handle(forged_current_dead), None);

        let entity2 = claim.alloc().unwrap();
        assert_eq!(entity2.slot(), raw.slot());
        assert_eq!(claim.entity_id_from_handle(raw), None);
        assert_eq!(
            claim.entity_id_from_handle(entity2.depot_handle()),
            Some(entity2.id())
        );

        let oob = DepotHandle::from_raw((1u32 << crate::HANDLE_SLOT_BITS) | 999);
        assert_eq!(claim.entity_id_from_handle(oob), None);
    }

    #[test]
    fn owned_entity_id_from_handle_strictly_reconstructs_live_ids() {
        let pool = LocalPool::new_depot(Depot64::new());
        let claim = pool.claim();

        let entity = claim.alloc().unwrap();
        let raw = entity.depot_handle();
        assert_eq!(claim.entity_id_from_handle(raw), Some(entity.id()));

        drop(entity);
        assert_eq!(claim.entity_id_from_handle(raw), None);

        let forged_current_dead =
            crate::encode_handle(raw.slot(), crate::next_live_generation(raw.generation()));
        assert_eq!(claim.entity_id_from_handle(forged_current_dead), None);

        let entity2 = claim.alloc().unwrap();
        assert_eq!(entity2.slot(), raw.slot());
        assert_eq!(claim.entity_id_from_handle(raw), None);
        assert_eq!(
            claim.entity_id_from_handle(entity2.depot_handle()),
            Some(entity2.id())
        );

        let oob = DepotHandle::from_raw((1u32 << crate::HANDLE_SLOT_BITS) | 999);
        assert_eq!(claim.entity_id_from_handle(oob), None);
    }

}
