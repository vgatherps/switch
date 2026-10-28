#[switch::task]
async fn destructured((left, right): (u8, u8)) {
    let _ = (left, right);
}

#[switch::task]
async fn by_ref(ref value: u8) {
    let _ = value;
}

fn main() {}
