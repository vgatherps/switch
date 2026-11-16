use std::future::Future;

#[switch_runtime::task]
fn sendable() -> impl Future<Output = ()> + Send {
    async {}
}

#[switch_runtime::task]
fn boxed() -> std::pin::Pin<Box<dyn Future<Output = ()>>> {
    Box::pin(async {})
}

fn main() {}
