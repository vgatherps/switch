depot::define_static_ecs!(ClaimEscapePool, 64);

struct ClaimEscapeMeta;
type ClaimEscapeDepot = <ClaimEscapePool as depot::PoolBinding>::Depot;
const SLOTS: usize = <ClaimEscapeDepot as depot::HasSlots>::SLOTS;

static CLAIM_ESCAPE_META: depot::SyncCell<[u32; SLOTS]> = depot::SyncCell::new([0u32; SLOTS]);

unsafe impl depot::Component<ClaimEscapePool> for ClaimEscapeMeta {
    type Storage = [u32; SLOTS];
    type Ref<'a> = &'a u32;
    type RefMut<'a> = &'a mut u32;

    fn storage_ptr(_pool: &ClaimEscapePool) -> *mut Self::Storage {
        CLAIM_ESCAPE_META.get()
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
    let entity = ClaimEscapePool::claim().unwrap().alloc().unwrap();
    let id = entity.id();

    let guard = {
        let claim = ClaimEscapePool::claim().unwrap();
        claim
            .get_or_none_ref::<ClaimEscapeMeta>(Some(&id))
            .unwrap()
    };

    let _ = *guard.view();
}
