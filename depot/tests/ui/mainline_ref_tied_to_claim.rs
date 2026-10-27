depot::define_static_ecs!(MainlineLifetimePool, 64);

struct MainlineLifetimeMeta;
type MainlineLifetimeDepot = <MainlineLifetimePool as depot::PoolBinding>::Depot;
const SLOTS: usize = <MainlineLifetimeDepot as depot::HasSlots>::SLOTS;

static MAINLINE_LIFETIME_META: depot::SyncCell<[u32; SLOTS]> =
    depot::SyncCell::new([0u32; SLOTS]);

unsafe impl depot::Component<MainlineLifetimePool> for MainlineLifetimeMeta {
    type Storage = [u32; SLOTS];
    type Ref<'a> = &'a u32;
    type RefMut<'a> = &'a mut u32;

    fn storage_ptr(_pool: &MainlineLifetimePool) -> *mut Self::Storage {
        MAINLINE_LIFETIME_META.get()
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
    let entity;
    let value = {
        let claim = MainlineLifetimePool::claim().unwrap();
        entity = claim.alloc().unwrap();
        claim.get::<MainlineLifetimeMeta>(&entity)
    };

    let _ = *value;
}
