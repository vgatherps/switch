depot::define_static_ecs!(MutGuardEscapePool, 64);

struct MutGuardEscapeMeta;
type MutGuardEscapeDepot = <MutGuardEscapePool as depot::PoolBinding>::Depot;
const SLOTS: usize = <MutGuardEscapeDepot as depot::HasSlots>::SLOTS;

static MUT_GUARD_ESCAPE_META: depot::SyncCell<[u32; SLOTS]> =
    depot::SyncCell::new([0u32; SLOTS]);

unsafe impl depot::Component<MutGuardEscapePool> for MutGuardEscapeMeta {
    type Storage = [u32; SLOTS];
    type Ref<'a> = &'a u32;
    type RefMut<'a> = &'a mut u32;

    fn storage_ptr(_pool: &MutGuardEscapePool) -> *mut Self::Storage {
        MUT_GUARD_ESCAPE_META.get()
    }

    unsafe fn get<'a>(storage: *const Self::Storage, slot: u32) -> &'a u32 {
        // SAFETY: The caller passes this pool's storage and an in-range slot.
        unsafe { &(*storage)[slot as usize] }
    }

    unsafe fn get_mut<'a>(storage: *mut Self::Storage, slot: u32) -> &'a mut u32 {
        // SAFETY: As in `get`, and the caller guarantees no other reference to this slot.
        unsafe { &mut (*storage)[slot as usize] }
    }
}

fn main() {
    let claim = MutGuardEscapePool::claim().unwrap();
    let mut entity = claim.alloc().unwrap();

    let escaped = {
        let mut guard = claim.get_mut::<MutGuardEscapeMeta>(&mut entity);
        guard.view_mut()
    };

    *escaped = 1;
}
