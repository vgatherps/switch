//! Owned shared-memory backings and raw single or mirrored mappings.
//!
//! This crate gives two processes the same memory. A [`Backing`] is a named POSIX shared-memory object or a file; [`Mapping`] maps part of it into the address space, and [`DoubleMapping`] maps the same extent twice, back to back, so a record that runs past the end of a ring buffer is still one contiguous slice. The two ring crates under junction, `ceph-ring` and `slice_ringbuffer`, are built on it. It depends only on `libc` and has no queue logic: the caller owns synchronization, initialization and the lifetime of anything derived from `as_ptr`.
//!
//! Creating a backing is exclusive (an existing name gives `AlreadyExists`), and files are mode 0600. The handle that created a name removes it on drop or [`Backing::unlink`]; handles that only opened it never remove it. Existing mappings stay valid after the name is removed and descriptors are closed. A new backing has length zero: size it with [`Backing::resize`], or with [`Backing::allocate`], which on Linux also reserves the storage so a full filesystem is an error instead of a later fault. Never shrink a mapped backing.
//!
//! Mapping offsets, and the length of each `DoubleMapping` view, must be multiples of the backing's page size: the native page size, or the huge-page size for a file on a Linux hugetlbfs mount. [`page_size_in`] returns the page size for files created in a directory, so sizes can be chosen before creation. On hugetlbfs the crate uses huge pages only and never falls back to small pages.
//!
//! ```
//! use shared_memory::{unique_name, Backing, Mapping};
//!
//! let name = unique_name("/doc-")?;
//! let creator = Backing::create_shm(&name)?;
//! creator.resize(4096)?;
//! let writer = Mapping::new(creator.fd(), 0, 4096)?;
//!
//! // Normally another process.
//! let opened = Backing::open_shm(&name)?;
//! let reader = Mapping::read_only(opened.fd(), 0, 4096)?;
//!
//! // SAFETY: both mappings are live and 4096 bytes long; only `writer` modifies the bytes.
//! unsafe {
//!     writer.as_ptr().write_volatile(7);
//!     assert_eq!(reader.as_ptr().read_volatile(), 7);
//! }
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! See the switch book, chapter "Shared memory and the receive ring".

mod backing;
mod mapping;
mod system;

pub use backing::Backing;
pub use mapping::{DoubleMapping, Mapping};

use std::fmt::Write as _;
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;

/// Returns the mapping page size used by files created in `dir`.
pub fn page_size_in(dir: &Path) -> io::Result<usize> {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsFd;
        system::page_size(File::open(dir)?.as_fd())
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = dir;
        system::native_page_size()
    }
}

/// Appends eight random bytes, encoded as hex, to `prefix`.
pub fn unique_name(prefix: &str) -> io::Result<String> {
    let capacity = prefix
        .len()
        .checked_add(16)
        .ok_or_else(|| system::invalid_input("name length overflows"))?;
    let mut bytes = [0_u8; 8];
    File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    let mut name = String::with_capacity(capacity);
    name.push_str(prefix);
    for byte in bytes {
        write!(&mut name, "{byte:02x}").expect("writing to a String cannot fail");
    }
    Ok(name)
}
