use std::ffi::CString;
use std::io;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd};
use std::path::Path;

use crate::system;

#[derive(Clone, Copy)]
enum NameKind {
    Shm,
    File,
}

/// An open backing descriptor, optionally owning its name's removal.
pub struct Backing {
    fd: OwnedFd,
    name: CString,
    kind: NameKind,
    owns_name: bool,
}

impl Backing {
    /// Exclusively creates a zero-length POSIX shared-memory object, mode 0600.
    pub fn create_shm(name: &str) -> io::Result<Self> {
        Self::open(name, NameKind::Shm, true)
    }

    /// Opens a POSIX shared-memory object without taking ownership of its name.
    pub fn open_shm(name: &str) -> io::Result<Self> {
        Self::open(name, NameKind::Shm, false)
    }

    /// Exclusively creates a zero-length file, mode 0600.
    pub fn create_file(path: &Path) -> io::Result<Self> {
        Self::open(Self::path_name(path)?, NameKind::File, true)
    }

    /// Opens a file without taking ownership of its name.
    pub fn open_file(path: &Path) -> io::Result<Self> {
        Self::open(Self::path_name(path)?, NameKind::File, false)
    }

    fn path_name(path: &Path) -> io::Result<&str> {
        path.to_str()
            .ok_or_else(|| system::invalid_input("backing path is not UTF-8"))
    }

    fn open(name: &str, kind: NameKind, create: bool) -> io::Result<Self> {
        if name.is_empty() {
            return Err(system::invalid_input("backing name is empty"));
        }
        let name =
            CString::new(name).map_err(|_| system::invalid_input("backing name contains NUL"))?;
        let mut flags = libc::O_RDWR;
        if create {
            flags |= libc::O_CREAT | libc::O_EXCL;
        }
        // SAFETY: name is NUL-terminated; both calls return a fresh owned descriptor.
        let fd = unsafe {
            match kind {
                NameKind::Shm => libc::shm_open(name.as_ptr(), flags, 0o600),
                NameKind::File => libc::open(
                    name.as_ptr(),
                    flags | libc::O_CLOEXEC,
                    0o600 as libc::c_uint,
                ),
            }
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            // SAFETY: the successful open returned a descriptor owned only by this Backing.
            fd: unsafe { OwnedFd::from_raw_fd(fd) },
            name,
            kind,
            owns_name: create,
        })
    }

    #[inline]
    pub fn fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }

    #[inline]
    pub fn name(&self) -> &str {
        // SAFETY: constructors accept UTF-8 and CString only appends a trailing NUL.
        unsafe { std::str::from_utf8_unchecked(self.name.as_bytes()) }
    }

    pub fn resize(&self, len: usize) -> io::Result<()> {
        let len = libc::off_t::try_from(len)
            .map_err(|_| system::invalid_input("backing length is out of range"))?;
        // SAFETY: fd is open and len is a representable nonnegative file length.
        if unsafe { libc::ftruncate(self.fd.as_raw_fd(), len) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Reserves storage for [0, len) before a mapping can fault in its pages.
    /// macOS POSIX shared memory supports resizing only.
    pub fn allocate(&self, len: usize) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            let len = libc::off_t::try_from(len)
                .map_err(|_| system::invalid_input("backing length is out of range"))?;
            // SAFETY: fd is open and the requested extent is representable.
            let error = unsafe { libc::posix_fallocate(self.fd.as_raw_fd(), 0, len) };
            if error != 0 {
                return Err(io::Error::from_raw_os_error(error));
            }
            Ok(())
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.resize(len)
        }
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> io::Result<usize> {
        system::file_len(self.fd())
    }

    pub fn page_size(&self) -> io::Result<usize> {
        system::page_size(self.fd())
    }

    /// Removes a creator-owned name once; opened handles never remove names.
    pub fn unlink(&mut self) -> io::Result<()> {
        if !self.owns_name {
            return Ok(());
        }
        // SAFETY: name remains NUL-terminated for this Backing's lifetime.
        let result = unsafe {
            match self.kind {
                NameKind::Shm => libc::shm_unlink(self.name.as_ptr()),
                NameKind::File => libc::unlink(self.name.as_ptr()),
            }
        };
        if result != 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ENOENT) {
                return Err(error);
            }
        }
        self.owns_name = false;
        Ok(())
    }
}

impl Drop for Backing {
    fn drop(&mut self) {
        let _ = self.unlink();
    }
}
