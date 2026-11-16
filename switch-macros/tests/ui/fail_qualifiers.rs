#[switch_runtime::task]
const fn constant() -> impl std::future::Future<Output = ()> {
    std::future::ready(())
}

#[switch_runtime::task]
async unsafe fn not_safe() {}

#[switch_runtime::task]
extern "C" fn foreign() -> impl std::future::Future<Output = ()> {
    std::future::ready(())
}

fn main() {}
