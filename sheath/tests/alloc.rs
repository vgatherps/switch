//! What a connection's halves allocate when made: nothing. Their buffers (the receive scratch
//! for records straddling frames, the send plaintext copy) are allocated on first use and
//! kept, so an idle connection costs neither.

// ring's assembly cannot run under Miri.
#![cfg(not(miri))]

mod common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use common::*;
use rustls::crypto::ring::default_provider;
use sheath::{FrameTx, TlsRx, TlsTx, TxFrame};

thread_local! {
    /// Bytes this thread allocated.
    static ALLOCATED: Cell<usize> = const { Cell::new(0) };
}

struct Counting;

// SAFETY: forwards to the system allocator; the count is a thread-local `Cell` (no allocation).
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCATED.try_with(|a| a.set(a.get() + layout.size()));
        // SAFETY: the caller's layout, passed on unchanged.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` came from `alloc` above with this layout.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static COUNTING: Counting = Counting;

/// Bytes `f` allocates on this thread.
fn allocated<T>(f: impl FnOnce() -> T) -> (usize, T) {
    let before = ALLOCATED.with(Cell::get);
    let out = f();
    (ALLOCATED.with(Cell::get) - before, out)
}

/// The receive half allocates nothing when made (it reserved 16,645 B, one record, before).
#[test]
fn a_new_receive_half_allocates_nothing() {
    let (bytes, rx) = allocated(TlsRx::<VecFrame, TestTx>::new);
    assert_eq!(bytes, 0);
    drop(rx);
}

/// The send half allocates nothing when made from a connection (it reserved the frame's
/// plaintext room before); its first seal still goes out and the server reads it.
#[test]
fn a_new_send_half_allocates_nothing() {
    let (client, mut server) = handshake(default_provider(), None);
    let wire = Wire::default();
    let (bytes, mut tx) = allocated(|| TlsTx::new(client, TestTx(wire.clone()), None));
    assert_eq!(bytes, 0);
    let mut f = ready(tx.alloc()).unwrap();
    let (head, _) = tx.plain();
    f.room_mut()[head..head + 5].copy_from_slice(b"first");
    let sealed = tx.seal(f, 5).unwrap();
    tx.try_send(sealed).unwrap();
    let mut tls = &wire.sent.borrow()[..];
    while !tls.is_empty() {
        server.read_tls(&mut tls).unwrap();
    }
    server.process_new_packets().unwrap();
    let mut got = [0; 5];
    std::io::Read::read_exact(&mut server.reader(), &mut got).unwrap();
    assert_eq!(&got, b"first");
}
