//! An async task's value coerces to the declared return type, as in an `async fn`.

use std::fmt::Debug;

#[switch::task]
async fn bytes() -> &'static [u8] {
    &[1, 2]
}

#[switch::task]
async fn boxed(flag: bool) -> Box<dyn Debug> {
    if flag {
        // A `return` gets no coercion context: it needs the cast.
        return Box::new("early") as Box<dyn Debug>;
    }
    Box::new(1u8)
}

#[switch::task]
async fn early(flag: bool) -> u32 {
    if flag {
        return 3;
    }
    4
}

#[switch::task]
async fn opaque() -> impl Debug {
    5u8
}

fn main() {
    let mut runtime = switch::Runtime::new(0);
    let spawner = runtime.spawner();
    let handles = (
        spawner.spawn_join(bytes()).unwrap(),
        spawner.spawn_join(boxed(true)).unwrap(),
        spawner.spawn_join(boxed(false)).unwrap(),
        spawner.spawn_join(early(true)).unwrap(),
        spawner.spawn_join(early(false)).unwrap(),
        spawner.spawn_join(opaque()).unwrap(),
    );
    let out = std::rc::Rc::new(std::cell::RefCell::new(String::new()));
    #[switch::task]
    async fn collect(
        handles: (
            switch::JoinHandle<&'static [u8]>,
            switch::JoinHandle<Box<dyn Debug>>,
            switch::JoinHandle<Box<dyn Debug>>,
            switch::JoinHandle<u32>,
            switch::JoinHandle<u32>,
            switch::JoinHandle<impl Debug + 'static>,
        ),
        out: std::rc::Rc<std::cell::RefCell<String>>,
    ) {
        let (a, b, c, d, e, f) = handles;
        *out.borrow_mut() = format!(
            "{:?} {:?} {:?} {} {} {:?}",
            a.await.unwrap(),
            b.await.unwrap(),
            c.await.unwrap(),
            d.await.unwrap(),
            e.await.unwrap(),
            f.await.unwrap(),
        );
    }
    spawner.spawn(collect(handles, out.clone())).unwrap();
    while runtime.poll(0).unwrap().live_tasks != 0 {}
    assert_eq!(*out.borrow(), "[1, 2] \"early\" 1 3 4 5");
}
