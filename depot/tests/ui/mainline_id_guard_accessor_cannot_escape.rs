depot::define_static_ecs!(IdGuardEscapePool, 64);

struct IdGuardEscapeMeta;
type IdGuardEscapeDepot = <IdGuardEscapePool as depot::PoolBinding>::Depot;
const SLOTS: usize = <IdGuardEscapeDepot as depot::HasSlots>::SLOTS;

static ID_GUARD_ESCAPE_META: depot::SyncCell<[u32; SLOTS]> =
    depot::SyncCell::new([0u32; SLOTS]);

unsafe impl depot::Component<IdGuardEscapePool> for IdGuardEscapeMeta {
    type Storage = [u32; SLOTS];
    type Ref<'a> = &'a u32;
    type RefMut<'a> = &'a mut u32;

    fn storage_ptr(_pool: &IdGuardEscapePool) -> *mut Self::Storage {
        ID_GUARD_ESCAPE_META.get()
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
    let claim = IdGuardEscapePool::claim().unwrap();
    let entity = claim.alloc().unwrap();
    let id = entity.id();

    let escaped = {
        let guard = claim
            .get_or_none_ref::<IdGuardEscapeMeta>(Some(&id))
            .unwrap();
        guard.view()
    };

    let _ = *escaped;
}
