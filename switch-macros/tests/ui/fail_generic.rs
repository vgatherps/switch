#[switch::task]
async fn type_parameter<T: 'static>(value: T) {
    drop(value);
}

#[switch::task]
async fn lifetime_parameter<'a>(value: &'a u8) {
    let _ = value;
}

#[switch::task]
async fn const_parameter<const N: usize>() {}

#[switch::task]
async fn bounded() where (): Sized {}

fn main() {}
