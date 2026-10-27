fn pointer_methods<B: depot::PoolBinding>(binding: &B) {
    let _ = binding.depot_ptr();
    let _ = binding.core_ptr();
    let _ = binding.borrow_set_ptr();
}

fn main() {}
