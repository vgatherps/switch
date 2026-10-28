#[switch::task]
const fn constant() -> impl std::future::Future<Output = ()> {
    std::future::ready(())
}

#[switch::task]
async unsafe fn not_safe() {}

#[switch::task]
extern "C" fn foreign() -> impl std::future::Future<Output = ()> {
    std::future::ready(())
}

fn main() {}
