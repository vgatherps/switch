//! Task storage and allocation.

use std::alloc::{self, Layout};
use std::future::Future;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::ptr::NonNull;
use std::task::{Context, Poll};

use super::header::{TaskHeader, TaskState};

/// Complete task: header + future.
#[repr(C)]
pub(crate) struct Task<F> {
    pub(crate) header: TaskHeader,
    future: MaybeUninit<F>,
}

/// Monomorphized poll.
unsafe fn poll_impl<F: Future<Output = ()>>(
    header: *mut TaskHeader,
    cx: &mut Context<'_>,
) -> Poll<()> {
    unsafe {
        let task = header as *mut Task<F>;
        Pin::new_unchecked((&mut *std::ptr::addr_of_mut!((*task).future)).assume_init_mut())
            .poll(cx)
    }
}

unsafe fn drop_impl<F>(header: *mut TaskHeader) {
    unsafe {
        let task = header as *mut Task<F>;
        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*task).future).cast::<F>());
    }
}

unsafe fn free_impl<F>(header: *mut TaskHeader) {
    unsafe { alloc::dealloc(header.cast(), Layout::new::<Task<F>>()) }
}

/// Owned task pointer. Manages allocation lifetime.
pub struct TaskBox {
    ptr: NonNull<TaskHeader>,
}

impl TaskBox {
    /// Allocate and initialize a task.
    pub fn new<F>(executor: *mut (), future: F) -> Self
    where
        F: Future<Output = ()> + 'static,
    {
        let layout = Layout::new::<Task<F>>();
        let ptr = unsafe { alloc::alloc(layout) as *mut Task<F> };

        if ptr.is_null() {
            alloc::handle_alloc_error(layout);
        }

        unsafe {
            (*ptr).header = TaskHeader {
                executor,
                poll_fn: poll_impl::<F>,
                drop_fn: drop_impl::<F>,
                free_fn: free_impl::<F>,
                prev: std::ptr::null_mut(),
                next: std::ptr::null_mut(),
                queued: false,
                drop_started: false,
                reg_head: crate::intrusive::NIL,
                state: TaskState::Scheduled,
            };
            (*ptr).future.write(future);
        }

        Self {
            ptr: unsafe { NonNull::new_unchecked(ptr as *mut TaskHeader) },
        }
    }

    /// Leak allocation, return raw pointer.
    #[inline]
    pub fn into_raw(self) -> *mut TaskHeader {
        let ptr = self.ptr.as_ptr();
        std::mem::forget(self);
        ptr
    }
}

impl Drop for TaskBox {
    fn drop(&mut self) {
        struct Free(*mut TaskHeader);
        impl Drop for Free {
            fn drop(&mut self) {
                unsafe { TaskHeader::free(self.0) }
            }
        }
        let ptr = self.ptr.as_ptr();
        let _free = Free(ptr);
        unsafe {
            (*ptr).state = TaskState::Terminal;
            TaskHeader::drop_future(ptr);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::rc::Rc;

    struct Immediate;
    impl Future for Immediate {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            Poll::Ready(())
        }
    }

    #[test]
    fn test_task_size_zst_future() {
        assert_eq!(std::mem::size_of::<Task<Immediate>>(), 56);
    }

    #[test]
    fn test_task_size_with_data() {
        struct WithData(#[allow(dead_code)] u64, #[allow(dead_code)] u64);
        impl Future for WithData {
            type Output = ();
            fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
                Poll::Ready(())
            }
        }
        // Header plus 16 bytes of future data
        assert_eq!(std::mem::size_of::<Task<WithData>>(), 72);
    }

    #[test]
    fn test_taskbox_drop() {
        let dropped = Rc::new(Cell::new(false));
        let dropped_clone = dropped.clone();

        struct DropTracker(Rc<Cell<bool>>);
        impl Future for DropTracker {
            type Output = ();
            fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
                Poll::Ready(())
            }
        }
        impl Drop for DropTracker {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }

        {
            let _task = TaskBox::new(std::ptr::null_mut(), DropTracker(dropped_clone));
        }

        assert!(dropped.get());
    }

    #[test]
    fn test_taskbox_into_raw_destroy() {
        let task = TaskBox::new(std::ptr::null_mut(), Immediate);
        let ptr = task.into_raw();

        // Destroy directly (as executor would do)
        unsafe {
            TaskHeader::drop_future(ptr);
            TaskHeader::free(ptr);
        }
    }
}
