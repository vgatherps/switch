#[switch::task(name = "x")]
async fn unknown() {}

#[switch::task(capacity = 1, capacity = 2)]
async fn twice() {}

fn main() {}
