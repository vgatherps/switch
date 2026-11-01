#![cfg(target_os = "linux")]

use std::ffi::CString;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use shared_memory::{Backing, DoubleMapping, Mapping, page_size_in, unique_name};

const HUGETLBFS_MAGIC: libc::c_long = 0x9584_58f6;

fn mount_stat(dir: &Path) -> io::Result<libc::statfs> {
    let dir = CString::new(dir.as_os_str().as_bytes()).unwrap();
    let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: dir is NUL-terminated and stat points to writable storage for one statfs.
    if unsafe { libc::statfs(dir.as_ptr(), stat.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful statfs initialized stat.
    Ok(unsafe { stat.assume_init() })
}

fn exercise_mappings(dir: &Path, page: usize) -> io::Result<()> {
    let path = dir.join(unique_name("smt")?);
    let backing = Backing::create_file(&path)?;
    assert_eq!(backing.page_size()?, page);
    backing.resize(2 * page)?;
    let metadata = Mapping::new(backing.fd(), 0, 4096)?;
    assert_eq!(metadata.len(), 4096);
    let aliases = DoubleMapping::new(backing.fd(), page, page, true)?;
    assert_eq!(aliases.as_ptr() as usize % page, 0);
    let opened = Backing::open_file(&path)?;
    let attached = Mapping::read_only(opened.fd(), page, page)?;
    // SAFETY: metadata and the file extent mapped by aliases are disjoint and fully backed.
    unsafe {
        metadata.as_ptr().write_volatile(0xa5);
        metadata.as_ptr().add(4095).write_volatile(0x5a);
        for i in 0..page {
            let byte = (i % 251) as u8;
            aliases.as_ptr().add(i).write_volatile(byte);
            assert_eq!(aliases.as_ptr().add(page + i).read_volatile(), byte);
            assert_eq!(attached.as_ptr().add(i).read_volatile(), byte);
        }
        let wrapped = [11, 22, 33, 44, 55, 66, 77, 88];
        for (i, &byte) in wrapped.iter().enumerate() {
            aliases.as_ptr().add(page - 4 + i).write_volatile(byte);
        }
        for (i, &byte) in wrapped.iter().enumerate() {
            assert_eq!(
                attached.as_ptr().add((page - 4 + i) % page).read_volatile(),
                byte
            );
        }
        assert_eq!(metadata.as_ptr().read_volatile(), 0xa5);
        assert_eq!(metadata.as_ptr().add(4095).read_volatile(), 0x5a);
    }
    drop(attached);
    drop(opened);
    drop(aliases);
    drop(metadata);
    drop(backing);
    assert!(!path.exists());
    Ok(())
}

#[test]
fn hugepage_aliases_wrap_at_nonzero_offset_and_cleanup() {
    let dir = Path::new("/dev/hugepages");
    let Ok(mount) = mount_stat(dir) else {
        eprintln!("skipping: /dev/hugepages is not mounted");
        return;
    };
    if mount.f_type != HUGETLBFS_MAGIC {
        eprintln!("skipping: /dev/hugepages is not hugetlbfs");
        return;
    }
    let page = usize::try_from(mount.f_bsize).unwrap();
    assert!(page >= 4096);
    assert_eq!(page_size_in(dir).unwrap(), page);
    match exercise_mappings(dir, page) {
        Ok(()) => {}
        Err(error) if error.raw_os_error() == Some(libc::ENOMEM) => {
            eprintln!("skipping: no free huge pages");
            return;
        }
        Err(error) => panic!("{error}"),
    }
    // The second allocation also exercises release of the first set of huge mappings.
    exercise_mappings(dir, page).unwrap();
}
