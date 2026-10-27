//! Task abstraction.
//!
//! A task is a heap-allocated structure containing:
//! - An ownership header with executor pointer and function pointers
//! - The future being executed
//!
//! The header layout enables wakers to wake tasks without thread-local access.

mod header;
mod storage;

pub use header::{TaskHeader, TaskState};
pub use storage::TaskBox;
