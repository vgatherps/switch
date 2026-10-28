//! Generic task functions: each instantiation has its own pool and capacity, also
//! when its future type does not depend on the parameter.

use std::cell::RefCell;
use std::fmt::Display;
use std::future::Future;
use std::rc::Rc;

use switch::RuntimeError;

type Log = Rc<RefCell<Vec<String>>>;

#[switch::task]
async fn show<T: Display + 'static>(value: T, log: Log) {
    log.borrow_mut().push(value.to_string());
}

#[switch::task]
async fn anonymous(value: impl Display + 'static, log: Log) {
    log.borrow_mut().push(value.to_string());
}

#[switch::task]
async fn nested(value: Option<impl Display + 'static>, log: Log) {
    log.borrow_mut().push(value.unwrap().to_string());
}

#[switch::task]
async fn constant<const N: usize>(log: Log) {
    log.borrow_mut().push(N.to_string());
}

trait Limit {
    const N: u32;
}

struct Two;
impl Limit for Two {
    const N: u32 = 2;
}

struct Three;
impl Limit for Three {
    const N: u32 = 3;
}

#[switch::task(capacity = L::N)]
async fn limited<L>()
where
    L: Limit + 'static,
{
}

/// The future type is the same for every `L`.
#[switch::task(capacity = L::N)]
fn plain_limited<L: Limit + 'static>(_unused: L) -> impl Future<Output = ()> {
    std::future::ready(())
}

#[switch::task(capacity = L::N)]
async fn async_limited<L: Limit + 'static>(_unused: L) {}

/// `T` need not be `'static` when the future does not hold it.
#[switch::task(capacity = T::N)]
fn borrowed_limited<T: Limit>(_unused: T) -> impl Future<Output = ()> {
    std::future::ready(())
}

impl Limit for &Two {
    const N: u32 = 2;
}

/// The future type is the same for every `N`.
#[switch::task(capacity = N)]
fn const_limited<const N: u32>() -> impl Future<Output = ()> {
    std::future::ready(())
}

/// One pool per `impl Trait` argument type, though the future type is the same.
#[switch::task(capacity = 1)]
fn anonymous_limited(_unused: impl Limit + 'static) -> impl Future<Output = ()> {
    std::future::ready(())
}

/// Spawns `make()` until the pool is full; the number spawned.
fn fill<T: switch::__private::Task>(
    spawner: &switch::Spawner,
    make: impl Fn() -> switch::SpawnToken<T>,
) -> u32 {
    let mut spawned = 0;
    while spawner.spawn(make()).is_ok() {
        spawned += 1;
    }
    spawned
}

fn main() {
    let mut runtime = switch::Runtime::new(0);
    let spawner = runtime.spawner();
    let log = Log::default();
    spawner.spawn(show(1u8, log.clone())).unwrap();
    spawner
        .spawn(show(String::from("two"), log.clone()))
        .unwrap();
    spawner.spawn(anonymous(3u32, log.clone())).unwrap();
    spawner.spawn(anonymous("four", log.clone())).unwrap();
    spawner.spawn(nested(Some(5i64), log.clone())).unwrap();
    spawner.spawn(constant::<6>(log.clone())).unwrap();
    for _ in 0..2 {
        spawner.spawn(limited::<Two>()).unwrap();
    }
    let reason = spawner.spawn(limited::<Two>()).unwrap_err().reason;
    assert_eq!(reason, RuntimeError::Exhausted);
    for _ in 0..3 {
        spawner.spawn(limited::<Three>()).unwrap();
    }
    let reason = spawner.spawn(limited::<Three>()).unwrap_err().reason;
    assert_eq!(reason, RuntimeError::Exhausted);
    assert_eq!(fill(&spawner, || plain_limited(Two)), 2);
    assert_eq!(fill(&spawner, || plain_limited(Three)), 3);
    assert_eq!(fill(&spawner, || async_limited(Two)), 2);
    assert_eq!(fill(&spawner, || async_limited(Three)), 3);
    let two = Two;
    assert_eq!(fill(&spawner, || borrowed_limited(&two)), 2);
    assert_eq!(fill(&spawner, || borrowed_limited(Three)), 3);
    assert_eq!(fill(&spawner, const_limited::<2>), 2);
    assert_eq!(fill(&spawner, const_limited::<3>), 3);
    assert_eq!(fill(&spawner, || anonymous_limited(Two)), 1);
    assert_eq!(fill(&spawner, || anonymous_limited(Three)), 1);
    while runtime.poll(0).unwrap().live_tasks != 0 {}
    let mut log = log.take();
    log.sort();
    assert_eq!(log, ["1", "3", "5", "6", "four", "two"]);
}
