use depot::{AccessError, DepotHandle, DynDepot, FreeError, fixed_depot_type};

trait FreeSlotDepot {
    fn alloc(&mut self) -> Option<DepotHandle>;
    fn free(&mut self, handle: DepotHandle) -> Result<(), FreeError>;
    fn validate(&self, handle: DepotHandle) -> Result<u32, AccessError>;
    fn validate_live(&self, handle: DepotHandle) -> Option<u32>;
    fn check_invariants(&self);
    unsafe fn free_slot_unchecked(&mut self, slot: u32);
}

type Fixed = fixed_depot_type!(256);

impl FreeSlotDepot for Fixed {
    fn alloc(&mut self) -> Option<DepotHandle> {
        Fixed::alloc(self)
    }

    fn free(&mut self, handle: DepotHandle) -> Result<(), FreeError> {
        Fixed::free(self, handle)
    }

    fn validate(&self, handle: DepotHandle) -> Result<u32, AccessError> {
        Fixed::validate(self, handle)
    }

    fn validate_live(&self, handle: DepotHandle) -> Option<u32> {
        Fixed::validate_live(self, handle)
    }

    fn check_invariants(&self) {
        Fixed::check_invariants(self)
    }

    unsafe fn free_slot_unchecked(&mut self, slot: u32) {
        unsafe { Fixed::free_slot_unchecked(self, slot) }
    }
}

impl FreeSlotDepot for DynDepot {
    fn alloc(&mut self) -> Option<DepotHandle> {
        DynDepot::alloc(self)
    }

    fn free(&mut self, handle: DepotHandle) -> Result<(), FreeError> {
        DynDepot::free(self, handle)
    }

    fn validate(&self, handle: DepotHandle) -> Result<u32, AccessError> {
        DynDepot::validate(self, handle)
    }

    fn validate_live(&self, handle: DepotHandle) -> Option<u32> {
        DynDepot::validate_live(self, handle)
    }

    fn check_invariants(&self) {
        DynDepot::check_invariants(self)
    }

    unsafe fn free_slot_unchecked(&mut self, slot: u32) {
        unsafe { DynDepot::free_slot_unchecked(self, slot) }
    }
}

fn assert_free_slot_matches_free(
    mut by_handle: impl FreeSlotDepot,
    mut by_slot: impl FreeSlotDepot,
) {
    let mut handle_a = by_handle.alloc().unwrap();
    let mut handle_b = by_slot.alloc().unwrap();
    assert_eq!(handle_a, handle_b);

    for expected_generation in (2..=127).chain(std::iter::once(1)) {
        by_handle.free(handle_a).unwrap();
        by_handle.check_invariants();
        unsafe { by_slot.free_slot_unchecked(handle_b.slot()) };
        by_slot.check_invariants();

        assert_eq!(
            by_slot.validate(handle_b),
            Err(AccessError::StaleGeneration)
        );
        assert_eq!(by_slot.validate_live(handle_b), None);

        handle_a = by_handle.alloc().unwrap();
        handle_b = by_slot.alloc().unwrap();
        assert_eq!(handle_a, handle_b);
        assert_eq!(handle_b.slot(), 1);
        assert_eq!(handle_b.generation(), expected_generation);
    }

    let mut burst = vec![handle_b];
    burst.extend((0..19).map(|_| by_slot.alloc().unwrap()));
    assert_eq!(
        burst.iter().map(|handle| handle.slot()).collect::<Vec<_>>(),
        (1..=20).collect::<Vec<_>>()
    );

    for handle in burst {
        unsafe { by_slot.free_slot_unchecked(handle.slot()) };
        assert_eq!(by_slot.validate(handle), Err(AccessError::StaleGeneration));
        assert_eq!(by_slot.validate_live(handle), None);
        by_slot.check_invariants();
    }

    for expected_slot in (5..=20).rev().chain(1..=4).chain(std::iter::once(21)) {
        let handle = by_slot.alloc().unwrap();
        assert_eq!(handle.slot(), expected_slot);
        assert_eq!(handle.generation(), if expected_slot == 21 { 1 } else { 2 });
        by_slot.check_invariants();
    }
}

#[test]
fn fixed_free_slot_matches_handle_free() {
    assert_free_slot_matches_free(Fixed::new(), Fixed::new());
}

#[test]
fn dyn_free_slot_matches_handle_free() {
    assert_free_slot_matches_free(DynDepot::new(256), DynDepot::new(256));
}
