//! Owned shared-memory backings and raw single or double virtual mappings.

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
