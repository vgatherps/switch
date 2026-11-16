//! A task in a generic impl may use the impl's parameters: each instantiation is
//! its own future type with its own pool.

use std::cell::Cell;
use std::marker::PhantomData;
use std::rc::Rc;

struct Holder<T>(PhantomData<T>);

impl<T: Into<u32> + 'static> Holder<T> {
    #[switch_runtime::task]
    async fn add(value: T, count: Rc<Cell<u32>>) {
        count.set(count.get() + value.into());
    }
}

fn main() {
    let mut runtime = switch_runtime::Runtime::new(0);
    let spawner = runtime.spawner();
    let count = Rc::new(Cell::new(0));
    spawner.spawn(Holder::<u8>::add(1, count.clone())).unwrap();
    spawner
        .spawn(Holder::<u32>::add(10, count.clone()))
        .unwrap();
    while runtime.poll(0).unwrap().live_tasks != 0 {}
    assert_eq!(count.get(), 11);
}
