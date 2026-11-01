//! Shared-memory ring layout plus its single consumer and per-CPU writers.
//!
//! The consumer advances metadata but never writes into a ring.

use std::io;
use std::os::fd::BorrowedFd;
#[cfg(target_os = "linux")]
use std::path::Path;
use std::path::PathBuf;
#[cfg(any(not(target_os = "linux"), feature = "test-util"))]
use std::ptr;
use std::ptr::NonNull;
use std::slice;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicU32, AtomicU64};

use ceph_rseq::{
    LEN_OFFSET, Private, RingArgs, SEQ_OFFSET, STREAM_OFFSET, TAG_OFFSET, record_total, tag_for,
};
#[cfg(target_os = "linux")]
use ceph_rseq::{Push, RecordArgs, ThreadArea, push_record};

use shared_memory::{Backing, DoubleMapping, Mapping, page_size_in, unique_name};

const MAGIC: u64 = u64::from_le_bytes(*b"cephring");
const TAILS_OFFSET: usize = 4096;
// Spread initial tag words across L1 cache sets.
const STAGGER: u64 = 128;

/// Set in a record's `seq` to mark the stream's last record. Sequence numbers are below 2^63.
pub const END_OF_STREAM: u64 = 1 << 63;

#[repr(C)]
struct ShmHeader {
    magic: AtomicU64,
    nr_rings: u64,
    ring_size: u64,
    max_record: u64,
    #[cfg_attr(target_os = "linux", allow(dead_code))]
    lock: AtomicU32,
}

pub struct Config {
    pub ring_size: usize,
    pub max_record: usize,
    /// Directory that holds the shared file. Ignored on macOS.
    pub dir: PathBuf,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Header {
    pub stream: u64,
    pub seq: u64,
    pub len: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PushError {
    Full,
    TooLarge,
}

/// One ring: its double mapping and its two metadata cells.
struct Ring {
    mapping: DoubleMapping,
    private: NonNull<Private>,
    shared_tail: NonNull<AtomicU64>,
}

impl Ring {
    fn at(&self, pos: u64) -> *mut u8 {
        // SAFETY: the mapping aliases its extent twice; the mask keeps the offset in its first half.
        unsafe {
            self.mapping
                .as_ptr()
                .add((pos & (self.mapping.len() as u64 - 1)) as usize)
        }
    }

    fn tag(&self, pos: u64) -> &AtomicU64 {
        // SAFETY: record positions are 8-aligned and `at` returns storage inside the mapping.
        unsafe { &*self.at(pos).add(TAG_OFFSET).cast::<AtomicU64>() }
    }

    fn header(&self, pos: u64, max_record: usize) -> Option<Header> {
        if self.tag(pos).load(Acquire) != tag_for(pos) {
            return None;
        }
        let start = self.at(pos);
        // SAFETY: acquiring the committed tag makes the record header visible.
        let len = unsafe { start.add(LEN_OFFSET).cast::<u64>().read() as usize };
        assert!(len <= max_record, "record length {len} at {pos}");
        // SAFETY: acquiring the committed tag makes the record header visible.
        let stream = unsafe { start.add(STREAM_OFFSET).cast::<u64>().read() };
        // SAFETY: acquiring the committed tag makes the record header visible.
        let seq = unsafe { start.add(SEQ_OFFSET).cast::<u64>().read() };
        Some(Header { stream, seq, len })
    }

    fn private(&self) -> &Private {
        // SAFETY: `private` points into the live metadata mapping owned by the containing `Map`.
        unsafe { self.private.as_ref() }
    }

    fn shared_tail(&self) -> &AtomicU64 {
        // SAFETY: `shared_tail` points into the live metadata mapping owned by the containing `Map`.
        unsafe { self.shared_tail.as_ref() }
    }

    fn args(&self) -> RingArgs {
        RingArgs {
            base: self.mapping.as_ptr(),
            private: self.private.as_ptr(),
            mask: self.mapping.len() as u64 - 1,
            shared_tail: self.shared_tail.as_ptr(),
        }
    }
}

/// The metadata pages of one shm object and its rings.
struct Map {
    meta: Mapping,
    rings: Vec<Ring>,
}

impl Map {
    fn new(fd: BorrowedFd<'_>, nr_rings: usize, ring_size: usize, page: usize) -> io::Result<Map> {
        let meta_bytes = meta_bytes(nr_rings, page);
        let meta = Mapping::new(fd, 0, meta_bytes)?;
        let mut map = Map {
            meta,
            rings: Vec::with_capacity(nr_rings),
        };
        for i in 0..nr_rings {
            let offset = meta_bytes + i * ring_size;
            let mapping = DoubleMapping::new(fd, offset, ring_size, true)?;
            let tail = map.meta.as_ptr().wrapping_add(TAILS_OFFSET).cast::<AtomicU64>();
            let private = map
                .meta
                .as_ptr()
                .wrapping_add(private_offset(nr_rings))
                .cast::<Private>();
            // SAFETY: this cell is inside the non-null metadata mapping for index `i`.
            let private = unsafe { NonNull::new_unchecked(private.add(i)) };
            // SAFETY: this cell is inside the non-null metadata mapping for index `i`.
            let shared_tail = unsafe { NonNull::new_unchecked(tail.add(i)) };
            map.rings.push(Ring {
                mapping,
                private,
                shared_tail,
            });
        }
        Ok(map)
    }
}

struct Cursor {
    tail: u64,
    dirty: bool,
}

/// Owns the shared object and removes it on drop.
pub struct Reader {
    map: Map,
    backing: Backing,
    max_record: usize,
    cursors: Vec<Cursor>,
}

// SAFETY: `Reader` owns its mappings and cursor state; moving it transfers the sole consumer.
unsafe impl Send for Reader {}

pub struct Record<'a> {
    pub stream: u64,
    pub seq: u64,
    pub payload: &'a [u8],
}

pub struct Committed<'a> {
    ring: &'a Ring,
    pos: u64,
    max_record: usize,
}

impl Iterator for Committed<'_> {
    type Item = Header;

    fn next(&mut self) -> Option<Header> {
        let pos = self.pos;
        let header = self.ring.header(pos, self.max_record)?;
        self.pos = pos + record_total(header.len as u64);
        Some(header)
    }
}

impl Reader {
    pub fn create(cfg: Config) -> io::Result<Reader> {
        let Config {
            ring_size,
            max_record,
            dir,
        } = cfg;
        let page = page_size_in(&dir)?;
        assert!(
            ring_size.is_power_of_two() && ring_size % page == 0,
            "ring_size {ring_size} must be a power of two and a multiple of page size {page}"
        );
        // The largest record plus the cleared tag word after it must fit in the ring.
        assert!(
            max_record <= ring_size - ceph_rseq::HEADER_LEN as usize - 8,
            "max_record {max_record} does not fit in ring_size {ring_size}"
        );
        #[cfg(target_os = "linux")]
        let nr_rings = possible_cpus()?;
        #[cfg(all(not(target_os = "linux"), not(feature = "test-util")))]
        let nr_rings = 1;
        #[cfg(all(not(target_os = "linux"), feature = "test-util"))]
        let nr_rings = 2;
        // From here on, an error or panic drops `backing`, which unlinks the object.
        let name = unique_name("cph")?;
        #[cfg(target_os = "linux")]
        let backing = Backing::create_file(&dir.join(name))?;
        #[cfg(target_os = "macos")]
        let backing = Backing::create_shm(&name)?;
        let len = meta_bytes(nr_rings, page) + nr_rings * ring_size;
        backing.resize(len)?;
        let map = Map::new(backing.fd(), nr_rings, ring_size, page)?;
        let mut cursors = Vec::with_capacity(nr_rings);
        for k in 0..nr_rings {
            let position = k as u64 * STAGGER;
            let ring = &map.rings[k];
            ring.private().head.store(position, Relaxed);
            ring.shared_tail().store(position, Relaxed);
            cursors.push(Cursor {
                tail: position,
                dirty: false,
            });
        }
        let header = map.meta.as_ptr().cast::<ShmHeader>();
        // SAFETY: the header is in the mapping; writers read the other fields only after seeing
        // `magic`.
        unsafe {
            (*header).nr_rings = nr_rings as u64;
            (*header).ring_size = ring_size as u64;
            (*header).max_record = max_record as u64;
            (*header).magic.store(MAGIC, Release);
        }
        Ok(Reader {
            map,
            backing,
            max_record,
            cursors,
        })
    }

    pub fn nr_rings(&self) -> usize {
        self.cursors.len()
    }

    pub fn name(&self) -> &str {
        self.backing.name()
    }

    /// Appends to `out` every ring whose current tail appears to hold a committed record. This
    /// is only a hint: `front` and `view` perform the Acquire load required before consuming a
    /// record.
    pub fn poll(&self, out: &mut Vec<usize>) {
        for ring in 0..self.nr_rings() {
            let tail = self.cursors[ring].tail;
            if self.map.rings[ring].tag(tail).load(Relaxed) == tag_for(tail) {
                out.push(ring);
            }
        }
    }

    /// The committed records of `ring` from its read position, front first. Each step
    /// acquire-loads the next tag; the iterator ends at the first uncommitted position. Nothing is
    /// consumed.
    pub fn committed(&self, ring: usize) -> Committed<'_> {
        Committed {
            ring: &self.map.rings[ring],
            pos: self.cursors[ring].tail,
            max_record: self.max_record,
        }
    }

    /// Acquire-loads the tag at `ring`'s tail and returns a copy of the committed record's
    /// header. Nothing is consumed.
    pub fn front(&self, ring: usize) -> Option<Header> {
        self.committed(ring).next()
    }

    /// The front record's view: Acquire-loads the tag at the tail and panics if nothing is
    /// committed there. Nothing is consumed.
    pub fn view(&self, ring: usize) -> Record<'_> {
        let tail = self.cursors[ring].tail;
        let ring_map = &self.map.rings[ring];
        let header = ring_map
            .header(tail, self.max_record)
            .unwrap_or_else(|| panic!("ring {ring}: no record at {tail}"));
        Record {
            stream: header.stream,
            seq: header.seq,
            // SAFETY: the length was validated, the ring is double-mapped, and no tail moves
            // while the returned record borrows the reader.
            payload: unsafe {
                slice::from_raw_parts(
                    ring_map.at(tail).add(ceph_rseq::HEADER_LEN as usize),
                    header.len,
                )
            },
        }
    }

    /// Consumes the front record: Acquire-loads the tag (panics if nothing is committed),
    /// advances the local tail by its total, and marks the ring for `publish`. Shared memory is
    /// not touched.
    pub fn advance(&mut self, ring: usize) {
        let cursor = &mut self.cursors[ring];
        let tail = cursor.tail;
        let header = self.map.rings[ring]
            .header(tail, self.max_record)
            .unwrap_or_else(|| panic!("ring {ring}: no record at {tail}"));
        cursor.tail = tail + record_total(header.len as u64);
        cursor.dirty = true;
    }

    /// Release-stores every local tail that moved since the previous publish.
    pub fn publish(&mut self) {
        for ring in 0..self.nr_rings() {
            let cursor = &mut self.cursors[ring];
            if cursor.dirty {
                self.map.rings[ring]
                    .shared_tail()
                    .store(cursor.tail, Release);
                cursor.dirty = false;
            }
        }
    }

    /// Tests only. Writes one record directly into `ring`, bypassing rseq and the lock.
    ///
    /// # Safety
    ///
    /// No `Writer` may push into `ring` while this runs, and no two calls may run on the same
    /// ring at once: the ring's private head and the record bytes are written without any
    /// synchronization against other writers.
    #[cfg(feature = "test-util")]
    pub unsafe fn write_unchecked(
        &self,
        ring: usize,
        stream: u64,
        seq: u64,
        payload: &[u8],
    ) -> Result<(), PushError> {
        write_record(&self.map.rings[ring], self.max_record, stream, seq, payload)
    }
}

pub struct Writer {
    #[allow(dead_code)] // keeps the mappings referenced by `table` alive
    map: Map,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    table: Vec<RingArgs>,
    max_record: usize,
}

// SAFETY: the mappings are process-wide. `try_push` writes a ring only inside `push_record` on the
// calling thread's current CPU (off Linux, only under the header spinlock), and the tails and
// private lines are atomics.
unsafe impl Send for Writer {}
unsafe impl Sync for Writer {}

impl Writer {
    pub fn attach(path: &str) -> io::Result<Writer> {
        #[cfg(target_os = "linux")]
        if !ceph_rseq::registered() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "glibc did not register rseq",
            ));
        }
        #[cfg(target_os = "linux")]
        let backing = Backing::open_file(Path::new(path))?;
        #[cfg(target_os = "macos")]
        let backing = Backing::open_shm(path)?;
        let page = backing.page_size()?;
        let (nr_rings, ring_size, max_record) = {
            let mapping = Mapping::read_only(backing.fd(), 0, page)?;
            let header = mapping.as_ptr().cast::<ShmHeader>();
            // SAFETY: the header is in the temporary mapping.
            let magic = unsafe { (*header).magic.load(Acquire) };
            if magic != MAGIC {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "shm object has no ring magic",
                ));
            }
            // SAFETY: seeing `magic` with Acquire synchronizes with `create` publishing the layout.
            unsafe {
                (
                    (*header).nr_rings as usize,
                    (*header).ring_size as usize,
                    (*header).max_record as usize,
                )
            }
        };
        let map = Map::new(backing.fd(), nr_rings, ring_size, page)?;
        let table = map.rings.iter().map(Ring::args).collect();
        Ok(Writer {
            map,
            table,
            max_record,
        })
    }

    pub fn try_push(&self, payload: &[u8], stream: u64, seq: u64) -> Result<(), PushError> {
        if payload.len() > self.max_record {
            return Err(PushError::TooLarge);
        }
        self.push(payload, stream, seq)
    }

    /// Pushes the stream's last record with an empty payload. The player must keep its socket
    /// open until the sequencer closes it; `try_finish` only enqueues the end record.
    pub fn try_finish(&self, stream: u64, seq: u64) -> Result<(), PushError> {
        self.try_push(&[], stream, seq | END_OF_STREAM)
    }

    #[cfg(target_os = "linux")]
    fn push(&self, payload: &[u8], stream: u64, seq: u64) -> Result<(), PushError> {
        let area = ThreadArea::current().expect("attach checked that rseq is registered");
        if area.cpu_id() < 0 {
            panic!("rseq is not registered for this thread");
        }
        let mut small = [0_u8; 16];
        let src = if payload.len() < 16 {
            small[..payload.len()].copy_from_slice(payload);
            small.as_ptr()
        } else {
            payload.as_ptr()
        };
        let record = RecordArgs {
            len: payload.len() as u64,
            stream,
            seq,
            src,
        };
        loop {
            // SAFETY: `table` has one live entry per possible CPU, the source has the required
            // readable span, and create's size check reserves the record and following tag word.
            match unsafe { push_record(area, self.table.as_ptr(), &record) } {
                Push::Done => return Ok(()),
                Push::Full => return Err(PushError::Full),
                Push::Aborted => {}
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn push(&self, payload: &[u8], stream: u64, seq: u64) -> Result<(), PushError> {
        use std::sync::atomic::Ordering::Relaxed;

        static WARN: std::sync::Once = std::sync::Once::new();
        WARN.call_once(|| {
            eprintln!(
                "WARNING: ceph-ring: rseq is unavailable on this OS, so every push takes a \
                 spinlock on a single ring. Development only, NOT for production."
            )
        });
        // SAFETY: `ShmHeader` starts at the beginning of the metadata mapping.
        let lock = unsafe { &(*self.map.meta.as_ptr().cast::<ShmHeader>()).lock };
        while lock.compare_exchange(0, 1, Acquire, Relaxed).is_err() {
            std::hint::spin_loop();
        }
        let result = write_record(&self.map.rings[0], self.max_record, stream, seq, payload);
        lock.store(0, Release);
        result
    }
}

#[cfg(any(not(target_os = "linux"), feature = "test-util"))]
fn write_record(
    ring: &Ring,
    max_record: usize,
    stream: u64,
    seq: u64,
    payload: &[u8],
) -> Result<(), PushError> {
    if payload.len() > max_record {
        return Err(PushError::TooLarge);
    }
    let ring_size = ring.mapping.len() as u64;
    let private = ring.private();
    let mut at = private.head.load(Relaxed);
    if let Some(header) = ring.header(at, max_record) {
        at += record_total(header.len as u64);
    }
    let total = record_total(payload.len() as u64);
    if at + total + 8 > ring.shared_tail().load(Acquire) + ring_size {
        return Err(PushError::Full);
    }
    private.head.store(at, Relaxed);
    // SAFETY: create's size check keeps this record and the following tag word inside the double
    // mapping. The consumer has released the space, and the caller excludes concurrent writers.
    unsafe {
        let start = ring.at(at);
        (&*start.add(total as usize).cast::<AtomicU64>()).store(0, Relaxed);
        start
            .add(LEN_OFFSET)
            .cast::<u64>()
            .write(payload.len() as u64);
        start.add(STREAM_OFFSET).cast::<u64>().write(stream);
        start.add(SEQ_OFFSET).cast::<u64>().write(seq);
        ptr::copy_nonoverlapping(
            payload.as_ptr(),
            start.add(ceph_rseq::HEADER_LEN as usize),
            payload.len(),
        );
        (&*start.add(TAG_OFFSET).cast::<AtomicU64>()).store(tag_for(at), Release);
    }
    Ok(())
}

fn meta_bytes(nr_rings: usize, page: usize) -> usize {
    (private_offset(nr_rings) + nr_rings * size_of::<Private>()).next_multiple_of(page)
}

fn private_offset(nr_rings: usize) -> usize {
    (TAILS_OFFSET + nr_rings * size_of::<AtomicU64>()).next_multiple_of(align_of::<Private>())
}

/// Max id in the possible-CPU list, plus one.
#[cfg(target_os = "linux")]
fn possible_cpus() -> io::Result<usize> {
    let list = std::fs::read_to_string("/sys/devices/system/cpu/possible")?;
    let mut max = 0;
    for id in list.trim().split([',', '-']) {
        max = max.max(id.parse::<usize>().map_err(io::Error::other)?);
    }
    Ok(max + 1)
}

