#[switch::task]
async fn anonymous(value: impl Send + 'static) {
    drop(value);
}

#[switch::task]
async fn nested(value: Box<dyn Fn(impl Send)>) {
    drop(value);
}

fn main() {}
