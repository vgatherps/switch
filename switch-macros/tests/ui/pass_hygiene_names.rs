//! User names that look like generated ones keep their meaning.
#![allow(non_snake_case)]

use std::cell::Cell;
use std::rc::Rc;

/// Types named like generated items, used as argument types.
mod types {
    pub struct Key(pub u32);
    pub struct POOL(pub u32);
    pub struct Marker(pub u32);

    impl Marker {
        pub const N: u32 = 3;
    }
}

use types::{Key, Marker, POOL};

#[switch::task]
async fn typed(key: Key, pool: POOL, marker: Marker, count: Rc<Cell<u32>>) {
    count.set(count.get() + key.0 + pool.0 + marker.0);
}

/// Arguments named like generated items.
mod arguments {
    use std::cell::Cell;
    use std::rc::Rc;

    #[switch::task]
    #[allow(clippy::too_many_arguments)]
    pub async fn named(
        Key: u32,
        POOL: u32,
        build: u32,
        task: u32,
        __task: u32,
        __switch_task: u32,
        capacity: u32,
        Marker: u32,
        count: Rc<Cell<u32>>,
    ) {
        count.set(Key + POOL + build + task + __task + __switch_task + capacity + Marker);
    }
}

/// Helpers named like generated items, called from task bodies.
mod helpers {
    use std::cell::Cell;
    use std::future::Future;
    use std::rc::Rc;

    fn __task(count: &Cell<u32>) {
        count.set(count.get() + 1);
    }
    fn __switch_task(count: &Cell<u32>) {
        count.set(count.get() + 10);
    }
    fn __switch_task_1(count: &Cell<u32>) {
        count.set(count.get() + 100);
    }
    fn build(count: &Cell<u32>) {
        count.set(count.get() + 1_000);
    }
    fn task(count: &Cell<u32>) {
        count.set(count.get() + 10_000);
    }
    fn Marker(count: &Cell<u32>) {
        count.set(count.get() + 100_000);
    }

    #[switch::task]
    pub async fn calls(count: Rc<Cell<u32>>) {
        __task(&count);
        __switch_task(&count);
        __switch_task_1(&count);
        build(&count);
        task(&count);
        Marker(&count);
    }

    #[switch::task]
    pub fn calls_plain(count: Rc<Cell<u32>>) -> impl Future<Output = ()> {
        __task(&count);
        build(&count);
        async move { task(&count) }
    }
}

struct Limits;

impl Limits {
    #[switch::task]
    async fn limited() {}
}

fn main() {
    let mut runtime = switch::Runtime::new(0);
    let spawner = runtime.spawner();
    let counts: Vec<_> = (0..4).map(|_| Rc::new(Cell::new(0))).collect();
    for _ in 0..Marker::N {
        spawner
            .spawn(typed(Key(1), POOL(10), Marker(100), counts[0].clone()))
            .unwrap();
    }
    spawner
        .spawn(arguments::named(
            1,
            2,
            4,
            8,
            16,
            32,
            64,
            128,
            counts[1].clone(),
        ))
        .unwrap();
    spawner.spawn(helpers::calls(counts[2].clone())).unwrap();
    spawner
        .spawn(helpers::calls_plain(counts[3].clone()))
        .unwrap();
    spawner.spawn(Limits::limited()).unwrap();
    while runtime.poll(0).unwrap().live_tasks != 0 {}
    assert_eq!(counts[0].get(), 3 * 111);
    assert_eq!(counts[1].get(), 255);
    assert_eq!(counts[2].get(), 111_111);
    assert_eq!(counts[3].get(), 11_001);
}
