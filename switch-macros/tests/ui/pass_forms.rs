use std::cell::Cell;
use std::future::Future;
use std::marker::PhantomData;
use std::rc::Rc;

const LIMIT: u32 = 4;

/// Documented, public, async, with a `'static` reference argument.
#[switch::task(capacity = LIMIT * 2)]
pub async fn countdown(spawner: switch::Spawner, left: u32, label: &'static str) {
    if left > 0 {
        spawner.spawn(countdown(spawner.clone(), left - 1, label)).unwrap();
    }
}

/// A `!Send` future returned by a plain function.
#[switch::task]
fn local(count: Rc<Cell<u32>>) -> impl Future<Output = ()> {
    async move { count.set(count.get() + 1) }
}

struct Holder<T>(PhantomData<T>);

/// In a generic impl whose parameter the task does not use. The future type
/// still differs per `Holder<T>`, so each instantiation has its own pool.
impl<T> Holder<T> {
    #[switch::task]
    async fn bump(count: Rc<Cell<u32>>) {
        count.set(count.get() + 1);
    }
}

fn main() {
    let mut runtime = switch::Runtime::new(0);
    let spawner = runtime.spawner();
    spawner.spawn(countdown(spawner.clone(), 3, "x")).unwrap();
    let count = Rc::new(Cell::new(0));
    spawner.spawn(local(count.clone())).unwrap();
    spawner.spawn(Holder::<u8>::bump(count.clone())).unwrap();
    spawner.spawn(Holder::<String>::bump(count.clone())).unwrap();
    while runtime.poll(0).unwrap().live_tasks != 0 {}
    assert_eq!(count.get(), 3);
}
