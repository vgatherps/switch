use std::future::Future;

#[switch::task]
fn sendable() -> impl Future<Output = ()> + Send {
    async {}
}

#[switch::task]
fn boxed() -> std::pin::Pin<Box<dyn Future<Output = ()>>> {
    Box::pin(async {})
}

fn main() {}
