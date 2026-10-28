use std::future::Future;

#[switch::task]
async fn borrowed(value: &u8) {
    let _ = value;
}

#[switch::task]
async fn returns_value() -> u8 {
    1
}

#[switch::task]
fn not_a_future() -> u8 {
    1
}

#[switch::task]
fn wrong_output() -> impl Future<Output = u8> {
    async { 1 }
}

fn main() {}
