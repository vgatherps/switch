use std::io;
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, BorrowedFd};

pub(crate) fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

pub(crate) fn native_page_size() -> io::Result<usize> {
    // SAFETY: sysconf has no preconditions for this selector.
    let size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    usize::try_from(size)
        .ok()
        .filter(|&size| size != 0)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid native page size"))
}

pub(crate) fn page_size(fd: BorrowedFd<'_>) -> io::Result<usize> {
    #[cfg(target_os = "linux")]
    {
        const HUGETLBFS_MAGIC: libc::c_long = 0x9584_58f6;
        let mut stat = MaybeUninit::<libc::statfs>::uninit();
        // SAFETY: fd is open and stat points to writable storage for one statfs.
        if unsafe { libc::fstatfs(fd.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful fstatfs initialized stat.
        let stat = unsafe { stat.assume_init() };
        if stat.f_type == HUGETLBFS_MAGIC {
            return usize::try_from(stat.f_bsize)
                .ok()
                .filter(|&size| size != 0)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid hugetlbfs page size")
                });
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = fd;
    native_page_size()
}

pub(crate) fn file_len(fd: BorrowedFd<'_>) -> io::Result<usize> {
    let mut stat = MaybeUninit::<libc::stat>::uninit();
    // SAFETY: fd is open and stat points to writable storage for one stat.
    if unsafe { libc::fstat(fd.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful fstat initialized stat.
    let len = unsafe { stat.assume_init() }.st_size;
    usize::try_from(len)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "file length is out of range"))
}
