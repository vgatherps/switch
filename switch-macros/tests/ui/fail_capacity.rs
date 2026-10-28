#[switch::task(capacity = 0)]
async fn zero() {}

fn main() {
    drop(zero());
}
