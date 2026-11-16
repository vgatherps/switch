#[switch_runtime::task]
async fn borrowed(value: &u8) {
    let _ = value;
}

#[switch_runtime::task]
fn not_a_future() -> u8 {
    1
}

fn main() {}
