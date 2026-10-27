use depot::PoolClaim;

depot::define_static_ecs!(SealedClaimPool, 64);

struct FakeClaim;

impl PoolClaim for FakeClaim {
    type Binding = SealedClaimPool;

    fn binding(&self) -> &Self::Binding {
        static BINDING: SealedClaimPool = SealedClaimPool;
        &BINDING
    }

    fn pool_id(&self) -> *const () {
        core::ptr::null()
    }
}

fn main() {}
