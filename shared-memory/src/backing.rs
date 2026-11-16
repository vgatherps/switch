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

/// A named POSIX shared-memory object or file that two processes open to share memory.
///
/// Creating is exclusive: a name that already exists fails with `AlreadyExists`. Objects and
/// files are created with mode 0600, so only their owner (and root) can open them. The handle
/// that created a name owns it and removes it when dropped or on [`unlink`](Self::unlink);
/// handles that only opened it never remove it. Mappings made earlier stay valid after the
/// name is removed and every descriptor is closed; a later open of the name fails with
/// `NotFound`, so the creator keeps its `Backing` until the other side has opened it.
/// Names must be non-empty UTF-8 without NUL bytes, or the constructors fail with `InvalidInput`.
///
/// ```
/// use shared_memory::{unique_name, Backing, Mapping};
///
/// let name = unique_name("/doc-")?;
/// let creator = Backing::create_shm(&name)?;
/// creator.resize(4096)?;
/// let writer = Mapping::new(creator.fd(), 0, 4096)?;
///
/// // Normally another process.
/// let opened = Backing::open_shm(&name)?;
/// let reader = Mapping::read_only(opened.fd(), 0, 4096)?;
///
/// // SAFETY: both mappings are live and 4096 bytes long; only `writer` modifies the bytes.
/// unsafe {
///     writer.as_ptr().write_volatile(7);
///     assert_eq!(reader.as_ptr().read_volatile(), 7);
/// }
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct Backing {
    fd: OwnedFd,
    name: CString,
    kind: NameKind,
    owns_name: bool,
}

impl Backing {
    /// Creates a zero-length POSIX shared-memory object and owns its name.
    pub fn create_shm(name: &str) -> io::Result<Self> {
        Self::open(name, NameKind::Shm, true)
    }

    /// Opens an existing POSIX shared-memory object.
    pub fn open_shm(name: &str) -> io::Result<Self> {
        Self::open(name, NameKind::Shm, false)
    }

    /// Creates a zero-length file and owns its name.
    pub fn create_file(path: &Path) -> io::Result<Self> {
        Self::open(Self::path_name(path)?, NameKind::File, true)
    }

    /// Opens an existing file.
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

    /// The backing's descriptor, for [`Mapping`](crate::Mapping) and
    /// [`DoubleMapping`](crate::DoubleMapping).
    #[inline]
    pub fn fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }

    /// The name or path the backing was created or opened with.
    #[inline]
    pub fn name(&self) -> &str {
        // SAFETY: constructors accept UTF-8 and CString only appends a trailing NUL.
        unsafe { std::str::from_utf8_unchecked(self.name.as_bytes()) }
    }

    /// Sets the length. Never shrink a backing while it is mapped.
    pub fn resize(&self, len: usize) -> io::Result<()> {
        let len = libc::off_t::try_from(len)
            .map_err(|_| system::invalid_input("backing length is out of range"))?;
        // SAFETY: fd is open and len is a representable nonnegative file length.
        if unsafe { libc::ftruncate(self.fd.as_raw_fd(), len) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Makes the backing at least `len` bytes long. On Linux it also reserves the storage, so
    /// a full filesystem is an error here instead of a fault when a mapped page is first
    /// touched. Elsewhere it is [`resize`](Self::resize).
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

    /// The current length in bytes.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> io::Result<usize> {
        system::file_len(self.fd())
    }

    /// The page size that mapping offsets and lengths are counted in: the native page size,
    /// or for a file on a Linux hugetlbfs mount, that mount's huge-page size. On hugetlbfs
    /// mappings use huge pages only and never fall back to small pages.
    pub fn page_size(&self) -> io::Result<usize> {
        system::page_size(self.fd())
    }

    /// Removes the name if this handle created it and has not removed it yet; otherwise
    /// returns `Ok` and does nothing. A name already removed by someone else counts as removed.
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
