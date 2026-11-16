//! Memory that two processes share: a named shared-memory object or file ([`Backing`]),
//! mapped once ([`Mapping`]) or twice back to back ([`DoubleMapping`]). It has no queue logic.
//! The daemon's receive rings and the cephalopod send rings are built on it; applications
//! never use it directly.

mod backing;
mod mapping;
mod system;

pub use backing::Backing;
pub use mapping::{DoubleMapping, Mapping};

use std::fmt::Write as _;
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;

/// The page size of files created in `dir`: the native page size, or on a Linux hugetlbfs
/// mount, that mount's huge-page size. On Linux `dir` must exist and be readable; elsewhere
/// it is ignored and the result is the native page size.
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

/// `prefix` followed by 16 random hex digits read from `/dev/urandom`.
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
