use depot::PoolClaim;

fn core_from_safe_claim<P: PoolClaim>(claim: &P) {
    let _core = unsafe { <P as PoolClaim>::core(claim) };
}

fn main() {}
