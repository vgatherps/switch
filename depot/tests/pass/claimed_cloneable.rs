use depot::StaticClaim;

depot::define_static_ecs!(ClonePool, 64);

fn main() {
    let claim: StaticClaim<ClonePool> = ClonePool::claim().unwrap();
    let _second = claim.clone();
}
