//! Generic task functions: each instantiation is its own future type, with its own
//! pool and capacity.

use std::cell::RefCell;
use std::fmt::Display;
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
    while runtime.poll(0).unwrap().live_tasks != 0 {}
    let mut log = log.take();
    log.sort();
    assert_eq!(log, ["1", "3", "5", "6", "four", "two"]);
}
