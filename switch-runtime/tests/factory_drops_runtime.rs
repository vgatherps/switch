//! A task factory that drops its own `Runtime` while its slot is reserved.
//!
//! This used to abort the process (cleanup refused during the reservation, and the
//! drop's cleanup retry panicked again while unwinding), so it runs in a child.

use std::cell::{Cell, RefCell};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use switch_runtime::{Runtime, RuntimeError};

struct Counted(Rc<Cell<usize>>);

impl Future for Counted {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        Poll::Pending
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

#[switch_runtime::task]
fn drops_its_runtime(
    owner: Rc<RefCell<Option<Runtime>>>,
    drops: Rc<Cell<usize>>,
) -> impl Future<Output = ()> {
    drop(owner.borrow_mut().take());
    Counted(drops)
}

#[switch_runtime::task]
fn parked(drops: Rc<Cell<usize>>) -> impl Future<Output = ()> {
    Counted(drops)
}

#[cfg(not(miri))]
#[test]
fn dropping_the_runtime_inside_a_factory_closes_it_without_aborting() {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "factory_drop_child", "--ignored", "--nocapture"])
        .env("SWITCH_FACTORY_DROP_CHILD", "1")
        .env("RUST_BACKTRACE", "0")
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "child failed: {}\n{stderr}",
        output.status
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("factory-drop-child-done"),
        "{stderr}"
    );
}

#[cfg(not(miri))]
#[test]
#[ignore = "invoked only by the supervised factory-drop subprocess test"]
fn factory_drop_child() {
    assert_eq!(std::env::var("SWITCH_FACTORY_DROP_CHILD").unwrap(), "1");
    let owner = Rc::new(RefCell::new(Some(Runtime::new(0))));
    let spawner = owner.borrow().as_ref().unwrap().spawner();
    let earlier = Rc::new(Cell::new(0));
    spawner.spawn(parked(earlier.clone())).unwrap();
    owner.borrow_mut().as_mut().unwrap().poll(0).unwrap();
    let drops = Rc::new(Cell::new(0));
    spawner
        .spawn(drops_its_runtime(owner.clone(), drops.clone()))
        .unwrap();
    assert!(owner.borrow().is_none());
    assert_eq!((earlier.get(), drops.get()), (1, 1));
    let rejected = spawner.spawn(parked(Rc::new(Cell::new(0)))).unwrap_err();
    assert_eq!(rejected.reason, RuntimeError::Closed);
    println!("factory-drop-child-done");
}
