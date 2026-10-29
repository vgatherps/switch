//! Spawn builds a task's future directly in its block slot.
//!
//! Size-sensitive measurement: 60 KiB futures (near the 64 KiB block limit) are
//! spawned on a thread with a 32 KiB stack, so any stack copy of a future
//! overflows and aborts. Rust does
//! not guarantee placement; this holds for optimized builds only (unoptimized
//! builds copy through the stack), so the check runs under `cargo test --release`.
#![cfg_attr(debug_assertions, allow(dead_code, unused_imports, unused_macros))]

use std::pin::Pin;
use std::task::{Context, Poll};

const SIZE: usize = 60 * 1024;

struct Huge {
    tag: u8,
    bulk: [u8; SIZE],
}

impl Future for Huge {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        std::hint::black_box(&self.bulk);
        assert_eq!(self.tag, 7);
        Poll::Ready(())
    }
}

#[switch::task]
fn huge_struct(tag: u8) -> impl Future<Output = ()> {
    Huge {
        tag,
        bulk: [0; SIZE],
    }
}

#[switch::task]
async fn huge_async(tag: u8) {
    let mut bulk = [0u8; SIZE];
    bulk[0] = tag;
    std::future::ready(()).await;
    std::hint::black_box(&mut bulk);
    assert_eq!(bulk[0], 7);
}

#[cfg(not(debug_assertions))]
#[test]
fn sixty_kib_futures_spawn_on_a_32_kib_stack() {
    std::thread::Builder::new()
        .stack_size(32 * 1024)
        .spawn(|| {
            let mut runtime = switch::Runtime::new(0);
            let spawner = runtime.spawner();
            spawner.spawn(huge_struct(7)).unwrap();
            spawner.spawn(huge_async(7)).unwrap();
            let report = runtime.poll(0).unwrap();
            assert_eq!((report.tasks_polled, report.live_tasks), (2, 0));
        })
        .unwrap()
        .join()
        .unwrap();
}
