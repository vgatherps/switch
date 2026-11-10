use std::ffi::CString;
use std::io;
use std::os::fd::AsRawFd;
use std::path::Path;

use shared_memory::{Backing, DoubleMapping, Mapping, unique_name};

#[derive(Clone, Copy)]
enum Kind {
    Shm,
    File,
}

const KINDS: [Kind; 2] = [Kind::Shm, Kind::File];

fn create(kind: Kind) -> Backing {
    let name = unique_name("smt").unwrap();
    match kind {
        Kind::Shm => Backing::create_shm(&format!("/{name}")).unwrap(),
        Kind::File => Backing::create_file(&std::env::temp_dir().join(name)).unwrap(),
    }
}

fn open(kind: Kind, name: &str) -> io::Result<Backing> {
    match kind {
        Kind::Shm => Backing::open_shm(name),
        Kind::File => Backing::open_file(Path::new(name)),
    }
}

fn recreate(kind: Kind, name: &str) -> io::Result<Backing> {
    match kind {
        Kind::Shm => Backing::create_shm(name),
        Kind::File => Backing::create_file(Path::new(name)),
    }
}

fn error_kind<T>(result: io::Result<T>) -> io::ErrorKind {
    result.err().expect("operation must fail").kind()
}

#[test]
fn independent_handles_share_bytes_and_mappings_outlive_descriptors() {
    for kind in KINDS {
        let backing = create(kind);
        assert_eq!(backing.len().unwrap(), 0);
        backing.resize(4096).unwrap();
        let name = backing.name().to_owned();
        let writer = Mapping::new(backing.fd(), 0, 4096).unwrap();
        let opened = open(kind, &name).unwrap();
        let reader = Mapping::read_only(opened.fd(), 0, 4096).unwrap();
        assert_eq!(writer.len(), 4096);
        assert_eq!(reader.len(), 4096);
        drop(opened);
        drop(backing);
        assert_eq!(error_kind(open(kind, &name)), io::ErrorKind::NotFound);
        // SAFETY: both mappings remain live; only writer is used to modify the shared bytes.
        unsafe {
            for i in 0..4096 {
                assert_eq!(reader.as_ptr().add(i).read_volatile(), 0);
                let byte = (i % 251) as u8;
                writer.as_ptr().add(i).write_volatile(byte);
                assert_eq!(reader.as_ptr().add(i).read_volatile(), byte);
            }
        }
    }
}

#[test]
fn nonzero_offset_aliases_wrap_without_touching_metadata_or_following_bytes() {
    for kind in KINDS {
        let backing = create(kind);
        let page = backing.page_size().unwrap();
        let len = 2 * page;
        backing.resize(page + len + page).unwrap();
        let whole = Mapping::new(backing.fd(), 0, page + len + page).unwrap();
        let aliases = DoubleMapping::new(backing.fd(), page, len, true).unwrap();
        assert_eq!(aliases.len(), len);
        // SAFETY: all accesses lie in the live mappings, and aliases are accessed only as raw bytes.
        unsafe {
            for i in 0..page {
                whole.as_ptr().add(i).write_volatile(0xa5);
                whole.as_ptr().add(page + len + i).write_volatile(0x3c);
            }
            for i in 0..len {
                let byte = (i % 251) as u8;
                aliases.as_ptr().add(i).write_volatile(byte);
                assert_eq!(aliases.as_ptr().add(len + i).read_volatile(), byte);
                assert_eq!(whole.as_ptr().add(page + i).read_volatile(), byte);
            }
            let wrapped = [11, 22, 33, 44, 55, 66, 77, 88];
            for (i, &byte) in wrapped.iter().enumerate() {
                aliases.as_ptr().add(len - 4 + i).write_volatile(byte);
            }
            for (i, &byte) in wrapped.iter().enumerate() {
                let physical = (len - 4 + i) % len;
                assert_eq!(aliases.as_ptr().add(physical).read_volatile(), byte);
                assert_eq!(whole.as_ptr().add(page + physical).read_volatile(), byte);
            }
            for i in 0..page {
                assert_eq!(whole.as_ptr().add(i).read_volatile(), 0xa5);
                assert_eq!(whole.as_ptr().add(page + len + i).read_volatile(), 0x3c);
            }
        }
    }
}

#[test]
fn both_aliases_share_writes_across_the_wrap() {
    for kind in KINDS {
        let backing = create(kind);
        let len = backing.page_size().unwrap();
        backing.resize(3 * len).unwrap();
        for populate in [false, true] {
            let aliases = DoubleMapping::new(backing.fd(), len, len, populate).unwrap();
            let base = aliases.as_ptr();
            // SAFETY: all offsets lie in the two live aliases, accessed only through raw pointers.
            unsafe {
                base.write_volatile(11);
                base.add(len - 1).write_volatile(22);
                assert_eq!(base.add(len).read_volatile(), 11);
                assert_eq!(base.add(2 * len - 1).read_volatile(), 22);
                for (i, byte) in [33, 44].into_iter().enumerate() {
                    base.add(len - 1 + i).write_volatile(byte);
                }
                assert_eq!(base.add(len - 1).read_volatile(), 33);
                assert_eq!(base.add(2 * len - 1).read_volatile(), 33);
                assert_eq!(base.read_volatile(), 44);
                assert_eq!(base.add(len).read_volatile(), 44);
            }
        }
    }
}

#[test]
fn concurrent_double_mappings_survive_anonymous_mapping_churn() {
    const THREADS: usize = 8;
    const ITERATIONS: usize = 2_000;
    let backing = create(Kind::Shm);
    let page = backing.page_size().unwrap();
    backing.resize(page).unwrap();
    let start = std::sync::Barrier::new(2 * THREADS);
    std::thread::scope(|scope| {
        for _ in 0..THREADS {
            scope.spawn(|| {
                start.wait();
                for _ in 0..ITERATIONS {
                    drop(DoubleMapping::new(backing.fd(), 0, page, false).unwrap());
                }
            });
            scope.spawn(|| {
                start.wait();
                for i in 0..ITERATIONS * 4 {
                    let len = page * (1 + i % 4);
                    // SAFETY: anonymous mmap chooses its own address and replaces no mapping.
                    let ptr = unsafe {
                        libc::mmap(
                            std::ptr::null_mut(),
                            len,
                            libc::PROT_NONE,
                            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                            -1,
                            0,
                        )
                    };
                    assert_ne!(ptr, libc::MAP_FAILED, "{}", io::Error::last_os_error());
                    // SAFETY: this thread owns precisely the interval just mapped.
                    assert_eq!(unsafe { libc::munmap(ptr, len) }, 0);
                }
            });
        }
    });
}

#[test]
fn opened_handles_cannot_unlink_and_creation_is_exclusive() {
    for kind in KINDS {
        let backing = create(kind);
        let name = backing.name().to_owned();
        assert_eq!(
            error_kind(recreate(kind, &name)),
            io::ErrorKind::AlreadyExists
        );
        let mut opened = open(kind, &name).unwrap();
        opened.unlink().unwrap();
        drop(opened);
        drop(open(kind, &name).unwrap());
        drop(backing);
        assert_eq!(error_kind(open(kind, &name)), io::ErrorKind::NotFound);
    }
}

#[test]
fn explicit_unlink_and_drop_cannot_remove_a_replacement() {
    for kind in KINDS {
        let mut original = create(kind);
        let name = original.name().to_owned();
        original.resize(4096).unwrap();
        let old_mapping = Mapping::new(original.fd(), 0, 4096).unwrap();
        original.unlink().unwrap();
        let replacement = recreate(kind, &name).unwrap();
        replacement.resize(4096).unwrap();
        let new_mapping = Mapping::new(replacement.fd(), 0, 4096).unwrap();
        // SAFETY: these are distinct live backing objects despite their identical names.
        unsafe {
            old_mapping.as_ptr().write_volatile(17);
            new_mapping.as_ptr().write_volatile(29);
            assert_eq!(old_mapping.as_ptr().read_volatile(), 17);
            assert_eq!(new_mapping.as_ptr().read_volatile(), 29);
        }
        original.unlink().unwrap();
        drop(original);
        let opened = open(kind, &name).unwrap();
        let attached = Mapping::read_only(opened.fd(), 0, 4096).unwrap();
        // SAFETY: attached is a live read-only mapping of replacement.
        assert_eq!(unsafe { attached.as_ptr().read_volatile() }, 29);
        drop(replacement);
        assert_eq!(error_kind(open(kind, &name)), io::ErrorKind::NotFound);
    }
}

#[test]
fn an_already_missing_name_disarms_creator_cleanup() {
    for kind in KINDS {
        let mut original = create(kind);
        let name = original.name().to_owned();
        let c_name = CString::new(name.as_str()).unwrap();
        // SAFETY: c_name is NUL-terminated and identifies the original backing.
        let result = unsafe {
            match kind {
                Kind::Shm => libc::shm_unlink(c_name.as_ptr()),
                Kind::File => libc::unlink(c_name.as_ptr()),
            }
        };
        assert_eq!(result, 0);
        original.unlink().unwrap();
        let replacement = recreate(kind, &name).unwrap();
        drop(original);
        drop(open(kind, &name).unwrap());
        drop(replacement);
    }
}

#[test]
fn invalid_and_unaligned_extents_are_rejected_before_access() {
    let backing = create(Kind::Shm);
    let page = backing.page_size().unwrap();
    backing.resize(3 * page).unwrap();
    let overflowing_offset = usize::MAX - (page - 1);
    for (offset, len) in [
        (0, 0),
        (1, page),
        (0, 3 * page + 1),
        (3 * page, page),
        (overflowing_offset, page),
    ] {
        assert_eq!(
            error_kind(Mapping::new(backing.fd(), offset, len)),
            io::ErrorKind::InvalidInput,
        );
        assert_eq!(
            error_kind(Mapping::read_only(backing.fd(), offset, len)),
            io::ErrorKind::InvalidInput,
        );
        assert_eq!(
            error_kind(DoubleMapping::new(backing.fd(), offset, len, false)),
            io::ErrorKind::InvalidInput,
        );
    }
    for (offset, len) in [(0, 1), (page, page - 1)] {
        assert_eq!(
            error_kind(DoubleMapping::new(backing.fd(), offset, len, false)),
            io::ErrorKind::InvalidInput,
        );
    }
    assert_eq!(
        error_kind(backing.resize(usize::MAX)),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(backing.len().unwrap(), 3 * page);
    let partial = Mapping::new(backing.fd(), 2 * page, page - 1).unwrap();
    assert_eq!(partial.len(), page - 1);
    // SAFETY: these positions lie in the requested partial extent of the live mapping.
    unsafe {
        assert_eq!(partial.as_ptr().read_volatile(), 0);
        assert_eq!(partial.as_ptr().add(page - 2).read_volatile(), 0);
    }
}

#[test]
fn backing_permissions_are_owner_only() {
    for kind in KINDS {
        let backing = create(kind);
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        // SAFETY: fd is open and stat points to writable storage for one stat.
        assert_eq!(
            unsafe { libc::fstat(backing.fd().as_raw_fd(), stat.as_mut_ptr()) },
            0
        );
        // SAFETY: successful fstat initialized stat.
        let stat = unsafe { stat.assume_init() };
        assert_eq!(stat.st_mode & 0o177, 0);
    }
}

#[test]
fn writable_double_mapping_rejects_a_read_only_descriptor() {
    use std::os::fd::AsFd;

    let backing = create(Kind::File);
    let page = backing.page_size().unwrap();
    backing.resize(page).unwrap();
    let read_only = std::fs::File::open(backing.name()).unwrap();
    assert_eq!(
        error_kind(DoubleMapping::new(read_only.as_fd(), 0, page, false)),
        io::ErrorKind::PermissionDenied,
    );
    let readable = Mapping::read_only(read_only.as_fd(), 0, page).unwrap();
    assert_eq!(readable.len(), page);
}
