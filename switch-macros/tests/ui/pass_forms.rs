use std::cell::Cell;
use std::future::Future;
use std::marker::PhantomData;
use std::rc::Rc;

// ANCHOR: countdown
/// Documented, public, async, with a `'static` reference argument.
#[switch_runtime::task]
pub async fn countdown(spawner: switch_runtime::Spawner, left: u32, label: &'static str) {
    if left > 0 {
        spawner.spawn(countdown(spawner.clone(), left - 1, label)).unwrap();
    }
}
// ANCHOR_END: countdown

// ANCHOR: plain_fn
/// A `!Send` future returned by a plain function.
#[switch_runtime::task]
fn local(count: Rc<Cell<u32>>) -> impl Future<Output = ()> {
    async move { count.set(count.get() + 1) }
}
// ANCHOR_END: plain_fn

struct Holder<T>(PhantomData<T>);

/// In a generic impl whose parameter the task does not use. The future type
/// still differs per `Holder<T>`, so each instantiation has its own pool.
impl<T> Holder<T> {
    #[switch_runtime::task]
    async fn bump(count: Rc<Cell<u32>>) {
        count.set(count.get() + 1);
    }
}

fn main() {
    let mut runtime = switch_runtime::Runtime::new(0);
    let spawner = runtime.spawner();
    spawner.spawn(countdown(spawner.clone(), 3, "x")).unwrap();
    let count = Rc::new(Cell::new(0));
    spawner.spawn(local(count.clone())).unwrap();
    spawner.spawn(Holder::<u8>::bump(count.clone())).unwrap();
    spawner.spawn(Holder::<String>::bump(count.clone())).unwrap();
    while runtime.poll(0).unwrap().live_tasks != 0 {}
    assert_eq!(count.get(), 3);
}
