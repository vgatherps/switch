type CoreUnsafeDepot = depot::fixed_depot_type!(64);

fn main() {
    let pool = depot::LocalPool::new(CoreUnsafeDepot::new(), [0u64; 64]);
    let claim = pool.claim();
    let _core = claim.core();
}
