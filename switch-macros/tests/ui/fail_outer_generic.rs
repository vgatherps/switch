use std::marker::PhantomData;

struct Holder<T>(PhantomData<T>);

impl<T: 'static> Holder<T> {
    // The body moves into a nested function, which cannot see the impl's `T`.
    #[switch::task]
    async fn run(value: T) {
        drop(value);
    }
}

fn main() {}
