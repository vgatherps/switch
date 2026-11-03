use std::io;
use std::os::fd::{AsRawFd, BorrowedFd};
use std::ptr;

use crate::system;

/// A shared mapping with a requested usable length and page-rounded ownership.
pub struct Mapping {
    ptr: *mut u8,
    len: usize,
    mapped_len: usize,
}

impl Mapping {
    pub fn new(fd: BorrowedFd<'_>, offset: usize, len: usize) -> io::Result<Self> {
        Self::map(fd, offset, len, libc::PROT_READ | libc::PROT_WRITE)
    }

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

    #[inline]
    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr
    }

    #[inline]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.len
    }

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

/// Two adjacent aliases of the same file extent.
pub struct DoubleMapping {
    first: Region,
    _second: Region,
}

impl DoubleMapping {
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
        // Address hints can collide with concurrent allocations; retry placement, not mmap errors.
        for _ in 0..16 {
            let reservation = Region::reserve(reservation_len)?;
            let aligned = round_up(reservation.ptr as usize, page)?;
            let ptr = reservation
                .ptr
                .wrapping_add(aligned - reservation.ptr as usize);
            // MAP_FIXED failure can remove its target before returning an error. Release the
            // address-finding reservation and own each non-destructive mapping independently.
            drop(reservation);
            let first = match Region::map_at(ptr, len, populate, fd, offset) {
                Ok(region) => region,
                Err(err) if err.kind() == io::ErrorKind::AddrNotAvailable => continue,
                Err(err) => return Err(err),
            };
            let second = match Region::map_at(ptr.wrapping_add(len), len, populate, fd, offset) {
                Ok(region) => region,
                Err(err) if err.kind() == io::ErrorKind::AddrNotAvailable => continue,
                Err(err) => return Err(err),
            };
            return Ok(Self {
                first,
                _second: second,
            });
        }
        Err(io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            "could not place adjacent double mappings",
        ))
    }

    #[inline]
    pub fn as_ptr(&self) -> *mut u8 {
        self.first.ptr
    }

    #[inline]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.first.len
    }

    /// Locks both virtual aliases, not only the first view of the backing pages.
    pub fn lock(&self) -> io::Result<()> {
        lock(self.first.ptr, self.first.len * 2)
    }
}

struct Region {
    ptr: *mut u8,
    len: usize,
}

impl Region {
    fn reserve(len: usize) -> io::Result<Self> {
        // SAFETY: anonymous mapping without an address hint cannot replace existing mappings.
        let ptr = unsafe {
            mmap(
                ptr::null_mut(),
                len,
                libc::PROT_NONE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )?
        };
        Ok(Self { ptr, len })
    }

    fn map_at(
        hint: *mut u8,
        len: usize,
        populate: bool,
        fd: BorrowedFd<'_>,
        offset: libc::off_t,
    ) -> io::Result<Self> {
        let flags = libc::MAP_SHARED;
        #[cfg(target_os = "linux")]
        let flags = flags | if populate { libc::MAP_POPULATE } else { 0 };
        #[cfg(not(target_os = "linux"))]
        let _ = populate;
        // SAFETY: an address hint without MAP_FIXED never replaces an existing mapping.
        let ptr = unsafe {
            mmap(
                hint,
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                flags,
                fd.as_raw_fd(),
                offset,
            )?
        };
        let region = Self { ptr, len };
        if ptr != hint {
            return Err(io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "double-mapping address is no longer available",
            ));
        }
        Ok(region)
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

/// The address is a hint only; callers must not pass MAP_FIXED.
unsafe fn mmap(
    addr: *mut u8,
    len: usize,
    prot: libc::c_int,
    flags: libc::c_int,
    fd: libc::c_int,
    offset: libc::off_t,
) -> io::Result<*mut u8> {
    // SAFETY: address selection is non-destructive, and callers validate the other arguments.
    let ptr = unsafe { libc::mmap(addr.cast(), len, prot, flags, fd, offset) };
    if ptr == libc::MAP_FAILED {
        Err(io::Error::last_os_error())
    } else {
        Ok(ptr.cast())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Backing, unique_name};

    #[test]
    fn occupied_alias_address_is_not_replaced() {
        let page = system::native_page_size().unwrap();
        let occupied_backing = Backing::create_shm(&unique_name("/occupied").unwrap()).unwrap();
        occupied_backing.resize(page).unwrap();
        let occupied = Mapping::new(occupied_backing.fd(), 0, page).unwrap();
        // SAFETY: this test exclusively owns and accesses the occupied mapping.
        unsafe { occupied.as_ptr().write_volatile(0xa5) };
        let alias_backing = Backing::create_shm(&unique_name("/alias").unwrap()).unwrap();
        alias_backing.resize(page).unwrap();

        let result = Region::map_at(occupied.as_ptr(), page, true, alias_backing.fd(), 0);
        assert_eq!(
            result.err().unwrap().kind(),
            io::ErrorKind::AddrNotAvailable
        );
        // SAFETY: a failed alias mapping must leave the existing mapping live and unchanged.
        assert_eq!(unsafe { occupied.as_ptr().read_volatile() }, 0xa5);
        let alias = Mapping::new(alias_backing.fd(), 0, page).unwrap();
        // SAFETY: this separate, freshly sized backing contains zero bytes.
        assert_eq!(unsafe { alias.as_ptr().read_volatile() }, 0);
    }
}
