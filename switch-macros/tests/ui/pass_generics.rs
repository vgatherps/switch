//! Generic task functions in every form compile and run: type and const parameters, `where`
//! clauses, `impl Trait` arguments, parameters the future type does not depend on.

use std::cell::RefCell;
use std::fmt::Display;
use std::future::Future;
use std::rc::Rc;

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

trait Tag {}

struct Two;
impl Tag for Two {}

struct Three;
impl Tag for Three {}

#[switch::task]
async fn bounded<L>()
where
    L: Tag + 'static,
{
}

/// The future type is the same for every `L`.
#[switch::task]
fn plain<L: Tag + 'static>(_unused: L) -> impl Future<Output = ()> {
    std::future::ready(())
}

#[switch::task]
async fn async_plain<L: Tag + 'static>(_unused: L) {}

/// `T` need not be `'static` when the future does not hold it.
#[switch::task]
fn borrowed<T: Tag>(_unused: T) -> impl Future<Output = ()> {
    std::future::ready(())
}

impl Tag for &Two {}

/// The future type is the same for every `N`.
#[switch::task]
fn const_plain<const N: u32>() -> impl Future<Output = ()> {
    std::future::ready(())
}

#[switch::task]
fn anonymous_plain(_unused: impl Tag + 'static) -> impl Future<Output = ()> {
    std::future::ready(())
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
    spawner.spawn(bounded::<Two>()).unwrap();
    spawner.spawn(bounded::<Three>()).unwrap();
    spawner.spawn(plain(Two)).unwrap();
    spawner.spawn(plain(Three)).unwrap();
    spawner.spawn(async_plain(Two)).unwrap();
    spawner.spawn(async_plain(Three)).unwrap();
    let two = Two;
    spawner.spawn(borrowed(&two)).unwrap();
    spawner.spawn(borrowed(Three)).unwrap();
    spawner.spawn(const_plain::<2>()).unwrap();
    spawner.spawn(const_plain::<3>()).unwrap();
    spawner.spawn(anonymous_plain(Two)).unwrap();
    spawner.spawn(anonymous_plain(Three)).unwrap();
    let report = runtime.poll(0).unwrap();
    assert_eq!((report.tasks_polled, report.live_tasks), (18, 0));
    let mut log = log.take();
    log.sort();
    assert_eq!(log, ["1", "3", "5", "6", "four", "two"]);
}
