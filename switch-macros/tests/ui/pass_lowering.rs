//! The task body keeps the meaning it has in a plain `async fn` or `fn`.

use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

fn bump(count: &Cell<u32>, by: u32) {
    count.set(count.get() + by);
}

#[switch_runtime::task]
async fn early(skip: bool, count: Rc<Cell<u32>>) {
    if skip {
        return;
    }
    bump(&count, 1);
}

/// Ready at once; bumps the counter if it holds one.
struct Bump(Option<Rc<Cell<u32>>>);

impl Future for Bump {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        if let Some(count) = &self.0 {
            bump(count, 10);
        }
        Poll::Ready(())
    }
}

#[switch_runtime::task]
fn early_plain(skip: bool, count: Rc<Cell<u32>>) -> impl Future<Output = ()> {
    if skip {
        return Bump(None);
    }
    Bump(Some(count))
}

/// The task's own name in its body is the task function.
#[switch_runtime::task]
async fn countdown(spawner: switch_runtime::Spawner, left: u32, count: Rc<Cell<u32>>) {
    bump(&count, 1);
    if left > 0 {
        spawner
            .spawn(countdown(spawner.clone(), left - 1, count))
            .unwrap();
    }
}

/// Counts its drops.
struct Dropped(Rc<Cell<u32>>);

impl Drop for Dropped {
    fn drop(&mut self) {
        bump(&self.0, 1);
    }
}

/// An `async fn` keeps every argument in its future, used or not.
#[switch_runtime::task]
async fn holds(_unused: Dropped) {
    std::future::pending::<()>().await;
}

/// A plain `fn` drops the arguments its future does not keep when it returns.
#[switch_runtime::task]
fn drops_unused(_unused: Dropped) -> impl Future<Output = ()> {
    std::future::pending()
}

#[switch_runtime::task]
async fn configured(
    #[cfg(any())] removed: u8,
    #[cfg(all())] kept: u32,
    #[cfg_attr(all(), allow(unused_mut))] mut tagged: u32,
    count: Rc<Cell<u32>>,
) {
    tagged += 1;
    bump(&count, kept + tagged);
}

#[switch_runtime::task]
fn configured_plain(
    #[cfg(any())] removed: u8,
    #[cfg(all())] kept: u32,
    count: Rc<Cell<u32>>,
) -> impl Future<Output = ()> {
    bump(&count, kept);
    std::future::ready(())
}

fn main() {
    let mut runtime = switch_runtime::Runtime::new(0);
    let spawner = runtime.spawner();
    let returns = Rc::new(Cell::new(0));
    spawner.spawn(early(true, returns.clone())).unwrap();
    spawner.spawn(early(false, returns.clone())).unwrap();
    spawner.spawn(early_plain(true, returns.clone())).unwrap();
    spawner.spawn(early_plain(false, returns.clone())).unwrap();
    let levels = Rc::new(Cell::new(0));
    spawner
        .spawn(countdown(spawner.clone(), 3, levels.clone()))
        .unwrap();
    let configured_count = Rc::new(Cell::new(0));
    spawner
        .spawn(configured(100, 10, configured_count.clone()))
        .unwrap();
    spawner
        .spawn(configured_plain(1_000, configured_count.clone()))
        .unwrap();
    while runtime.poll(0).unwrap().live_tasks != 0 {}
    assert_eq!(returns.get(), 11);
    assert_eq!(levels.get(), 4);
    assert_eq!(configured_count.get(), 1_111);

    let drops = Rc::new(Cell::new(0));
    let token = holds(Dropped(drops.clone()));
    spawner.spawn(token).unwrap();
    runtime.poll(0).unwrap();
    assert_eq!(
        drops.get(),
        0,
        "an async fn's future holds its unused argument"
    );
    let token = drops_unused(Dropped(drops.clone()));
    assert_eq!(drops.get(), 0, "creating a token drops nothing");
    spawner.spawn(token).unwrap();
    assert_eq!(
        drops.get(),
        1,
        "a plain fn's unused argument drops at spawn"
    );
    runtime.shutdown();
    assert_eq!(
        drops.get(),
        2,
        "the async fn's argument drops with its future"
    );
}
