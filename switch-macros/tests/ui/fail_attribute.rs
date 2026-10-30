#[switch::task(name = "x")]
async fn unknown() {}

#[switch::task(capacity = 1)]
async fn capacity() {}

fn main() {}
