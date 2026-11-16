use std::io;
use std::os::fd::{AsRawFd, BorrowedFd};
use std::ptr;

use crate::system;

/// Part of a [`Backing`](crate::Backing) mapped into this process. Unmapped on drop.
///
/// The crate does not synchronize, initialize or interpret the bytes: the caller owns that,
/// and the lifetime of anything derived from [`as_ptr`](Self::as_ptr).
pub struct Mapping {
    ptr: *mut u8,
    len: usize,
    mapped_len: usize,
}

impl Mapping {
    /// Maps `len` bytes of the backing at `offset`, readable and writable.
    ///
    /// Fails with `InvalidInput` when `len` is 0, `offset` is not a multiple of the backing's
    /// [page size](crate::Backing::page_size), or the range runs past the end of the backing.
    /// `len` need not be a page multiple; the mapping covers whole pages.
    pub fn new(fd: BorrowedFd<'_>, offset: usize, len: usize) -> io::Result<Self> {
        Self::map(fd, offset, len, libc::PROT_READ | libc::PROT_WRITE)
    }

    /// Maps `len` bytes of the backing at `offset`, read-only, with the same rules as
    /// [`new`](Self::new).
    pub fn read_only(fd: BorrowedFd<'_>, offset: usize, len: usize) -> io::Result<Self> {
        Self::map(fd, offset, len, libc::PROT_READ)
    }

    fn map(fd: BorrowedFd<'_>, offset: usize, len: usize, prot: libc::c_int) -> io::Result<Self> {
        let page = system::page_size(fd)?;
        let offset = checked_extent(fd, offset, len, page)?;
        let mapped_len = round_up(len, page)?;
        if mapped_len > isize::MAX as usize {
            return Err(system::invalid_input("mapping length is out of range"));
        }
        // SAFETY: no address is requested; offset and the usable file extent are validated.
        let ptr = unsafe {
            mmap(
                ptr::null_mut(),
                mapped_len,
                prot,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                offset,
            )?
        };
        Ok(Self {
            ptr,
            len,
            mapped_len,
        })
    }

    /// The first mapped byte.
    #[inline]
    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr
    }

    /// The `len` the mapping was made with.
    #[inline]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Asks the kernel to keep the mapped pages in memory (`mlock`).
    pub fn lock(&self) -> io::Result<()> {
        lock(self.ptr, self.mapped_len)
    }
}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: ptr and mapped_len describe this mapping's complete owned interval.
        unsafe { libc::munmap(self.ptr.cast(), self.mapped_len) };
    }
}

/// The same part of a backing mapped twice, back to back, so a record that starts near the
/// end of the first view continues into the second as one contiguous slice. Unmapped on drop.
///
/// ```text
///  backing:                 [ A A A A B B B B ]        len bytes
///
///  address space:  as_ptr()            as_ptr()+len          as_ptr()+2*len
///                   | first view        | second view (same bytes)
///                   [ A A A A B B B B ][ A A A A B B B B ]
/// ```
///
/// A write through either view is visible through the other.
pub struct DoubleMapping {
    region: Region,
    len: usize,
}

impl DoubleMapping {
    /// Maps `len` bytes of the backing at `offset` twice, back to back, readable and writable.
    ///
    /// `len` must be a multiple of the backing's [page size](crate::Backing::page_size), and the
    /// rules of [`Mapping::new`] apply; violations fail with `InvalidInput`. On Linux,
    /// `populate` faults every page in while mapping; elsewhere it has no effect.
    pub fn new(fd: BorrowedFd<'_>, offset: usize, len: usize, populate: bool) -> io::Result<Self> {
        let page = system::page_size(fd)?;
        let offset = checked_extent(fd, offset, len, page)?;
        if !len.is_multiple_of(page) {
            return Err(system::invalid_input(
                "double-mapping length is not page-aligned",
            ));
        }
        let native_page = system::native_page_size()?;
        if page % native_page != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "backing page size is not a multiple of native page size",
            ));
        }
        let aliases_len = len
            .checked_mul(2)
            .ok_or_else(|| system::invalid_input("double-mapping length overflows"))?;
        let reservation_len = aliases_len
            .checked_add(page - native_page)
            .filter(|&len| len <= isize::MAX as usize)
            .ok_or_else(|| system::invalid_input("reservation length is out of range"))?;
        let mut reservation = Region::reserve(reservation_len)?;
        let aligned = round_up(reservation.ptr as usize, page)?;
        let prefix = aligned - reservation.ptr as usize;
        let base = reservation.ptr.wrapping_add(prefix);
        let suffix = reservation_len - prefix - aliases_len;
        // SAFETY: both slack intervals are native-page-aligned and owned by the reservation.
        unsafe {
            if prefix != 0 {
                libc::munmap(reservation.ptr.cast(), prefix);
            }
            if suffix != 0 {
                libc::munmap(base.wrapping_add(aliases_len).cast(), suffix);
            }
        }
        reservation.ptr = base;
        reservation.len = aliases_len;
        let flags = libc::MAP_FIXED | libc::MAP_SHARED;
        #[cfg(target_os = "linux")]
        let flags = flags | if populate { libc::MAP_POPULATE } else { 0 };
        #[cfg(not(target_os = "linux"))]
        let _ = populate;
        for half in 0..2 {
            // SAFETY: this target is an untouched half of our live reservation.
            let result = unsafe {
                mmap(
                    base.wrapping_add(half * len),
                    len,
                    libc::PROT_READ | libc::PROT_WRITE,
                    flags,
                    fd.as_raw_fd(),
                    offset,
                )
            };
            if let Err(err) = result {
                // Failed MAP_FIXED may remove its target, allowing another thread to claim it.
                // Leave that half alone (at most len bytes leaked); only the other half is ours.
                reservation.ptr = base.wrapping_add((1 - half) * len);
                reservation.len = len;
                return Err(err);
            }
        }
        Ok(Self {
            region: reservation,
            len,
        })
    }

    /// The start of the first view.
    #[inline]
    pub fn as_ptr(&self) -> *mut u8 {
        self.region.ptr
    }

    /// The length of one view.
    #[inline]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Locks both virtual aliases, not only the first view of the backing pages.
    pub fn lock(&self) -> io::Result<()> {
        lock(self.region.ptr, self.region.len)
    }
}

struct Region {
    ptr: *mut u8,
    len: usize,
}

impl Region {
    fn reserve(len: usize) -> io::Result<Self> {
        let flags = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS;
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        let flags = flags | libc::MAP_NORESERVE;
        // SAFETY: anonymous mapping without an address hint cannot replace existing mappings.
        let ptr = unsafe { mmap(ptr::null_mut(), len, libc::PROT_NONE, flags, -1, 0)? };
        Ok(Self { ptr, len })
    }
}

impl Drop for Region {
    fn drop(&mut self) {
        // SAFETY: ptr and len describe exactly the mapping this region owns.
        unsafe { libc::munmap(self.ptr.cast(), self.len) };
    }
}

fn checked_extent(
    fd: BorrowedFd<'_>,
    offset: usize,
    len: usize,
    page: usize,
) -> io::Result<libc::off_t> {
    if len == 0 || !offset.is_multiple_of(page) {
        return Err(system::invalid_input(
            "mapping requires nonzero length and page-aligned offset",
        ));
    }
    let end = offset
        .checked_add(len)
        .ok_or_else(|| system::invalid_input("mapping extent overflows"))?;
    let file_len = system::file_len(fd)?;
    if end > file_len {
        return Err(system::invalid_input(
            "mapping extent exceeds backing length",
        ));
    }
    libc::off_t::try_from(offset)
        .map_err(|_| system::invalid_input("mapping offset is out of range"))
}

fn round_up(value: usize, alignment: usize) -> io::Result<usize> {
    let remainder = value % alignment;
    if remainder == 0 {
        Ok(value)
    } else {
        value
            .checked_add(alignment - remainder)
            .ok_or_else(|| system::invalid_input("page alignment overflows"))
    }
}

fn lock(ptr: *mut u8, len: usize) -> io::Result<()> {
    // SAFETY: callers pass a complete, live owned mapping interval.
    if unsafe { libc::mlock(ptr.cast(), len) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Callers validate the extent; MAP_FIXED targets must lie inside their own live reservation.
unsafe fn mmap(
    addr: *mut u8,
    len: usize,
    prot: libc::c_int,
    flags: libc::c_int,
    fd: libc::c_int,
    offset: libc::off_t,
) -> io::Result<*mut u8> {
    // SAFETY: callers validate the arguments and own any interval replaced with MAP_FIXED.
    let ptr = unsafe { libc::mmap(addr.cast(), len, prot, flags, fd, offset) };
    if ptr == libc::MAP_FAILED {
        Err(io::Error::last_os_error())
    } else {
        Ok(ptr.cast())
    }
}
