depot::define_static_ecs!(GetMutPool, 64);

struct Meta;
type GetMutDepot = <GetMutPool as depot::PoolBinding>::Depot;
const SLOTS: usize = <GetMutDepot as depot::HasSlots>::SLOTS;

static META: depot::SyncCell<[u32; SLOTS]> = depot::SyncCell::new([0u32; SLOTS]);

unsafe impl depot::Component<GetMutPool> for Meta {
    type Storage = [u32; SLOTS];
    type Ref<'a> = &'a u32;
    type RefMut<'a> = &'a mut u32;

    fn storage_ptr(_pool: &GetMutPool) -> *mut Self::Storage {
        META.get()
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
    let pool = GetMutPool::claim().unwrap();
    let entity = pool.alloc().unwrap();
    let _value = pool.get_mut::<Meta>(&mut entity);
}
