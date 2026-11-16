#[switch_runtime::task(name = "x")]
async fn unknown() {}

#[switch_runtime::task(capacity = 1)]
async fn capacity() {}

fn main() {}
