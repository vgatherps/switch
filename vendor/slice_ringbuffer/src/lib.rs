//! Single-producer, single-consumer shared-memory RX record ring.
//!
//! One backing contains a header page followed by a power-of-two data region,
//! mapped twice contiguously. The producer publishes monotonically increasing
//! byte positions in `head`; the consumer publishes reclaimed positions in `tail`.
//! Records have a 40-byte header and an eight-byte-aligned stride.
//!
//! Header/payload writes -> head Release -> reader head Acquire. Frame reads ->
//! refs fetch_sub Release -> reclaim refs Acquire -> tail Release -> writer tail
//! Acquire -> overwrite. Nothing else is shared. Create on a fresh backing before
//! opening it; each ring supports exactly one writer and one reader, ever.
//! Concurrent initialization through other backing handles or processes is outside the contract.

use shared_memory::{Backing, DoubleMapping, Mapping};
use std::convert::TryFrom;
use std::io;
use std::mem::{align_of, offset_of, size_of};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

const MAGIC: u64 = u64::from_le_bytes(*b"JNCTRING");
const HEADER_SIZE: usize = size_of::<RecordHeader>();

#[repr(C, align(128))]
struct CacheLine<T>(T);

#[repr(C)]
struct RingHeader {
    magic: u64,
    size: u64,
    reader: AtomicU32,
    head: CacheLine<AtomicU64>,
    tail: CacheLine<AtomicU64>,
}

#[repr(C)]
struct RecordHeader {
    refs: AtomicU32,
    len: u32,
    meta: PacketMeta,
}

/// Packet metadata stored immediately before a record's payload.
#[repr(C)]
pub struct PacketMeta {
    pub slot: u32,
    pub generation: u32,
    pub kind: u8,
    _pad0: [u8; 3],
    pub src_addr: [u8; 4],
    pub src_port: u16,
    _pad1: u16,
    pub dst_addr: [u8; 4],
    pub rx_timestamp: u64,
}

const _: () = {
    assert!(size_of::<RingHeader>() <= 4096);
    assert!(offset_of!(RingHeader, magic) == 0);
    assert!(offset_of!(RingHeader, size) == 8);
    assert!(offset_of!(RingHeader, reader) == 16);
    assert!(offset_of!(RingHeader, head) == 128);
    assert!(offset_of!(RingHeader, tail) == 256);
    assert!(size_of::<RecordHeader>() == 40);
    assert!(align_of::<RecordHeader>() == 8);
    assert!(offset_of!(RecordHeader, refs) == 0);
    assert!(offset_of!(RecordHeader, len) == 4);
    assert!(offset_of!(RecordHeader, meta) == 8);
    assert!(size_of::<PacketMeta>() == 32);
    assert!(align_of::<PacketMeta>() == 8);
    assert!(offset_of!(PacketMeta, slot) == 0);
    assert!(offset_of!(PacketMeta, generation) == 4);
    assert!(offset_of!(PacketMeta, kind) == 8);
    assert!(offset_of!(PacketMeta, _pad0) == 9);
    assert!(offset_of!(PacketMeta, src_addr) == 12);
    assert!(offset_of!(PacketMeta, src_port) == 16);
    assert!(offset_of!(PacketMeta, _pad1) == 18);
    assert!(offset_of!(PacketMeta, dst_addr) == 20);
    assert!(offset_of!(PacketMeta, rx_timestamp) == 24);
};

fn stride(len: usize) -> usize {
    (HEADER_SIZE + len + 7) & !7
}

/// The sole producer of a ring.
pub struct RingWriter {
    header: Mapping,
    data: DoubleMapping,
    size: usize,
    head: u64,
    cached_tail: u64,
}

// SAFETY: mappings are process-wide; this value owns the sole producer cursor.
unsafe impl Send for RingWriter {}

impl RingWriter {
    /// Initializes a fresh backing. The caller retains ownership of its name/unlink.
    /// Size must be a power of two, a backing-page multiple, and at most 2 GiB
    /// so every payload length is representable in the record header.
    pub fn create(backing: &mut Backing, size: usize) -> io::Result<RingWriter> {
        if backing.len()? != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "backing is not fresh",
            ));
        }
        let page = backing.page_size()?;
        if !size.is_power_of_two() || !size.is_multiple_of(page) || size > (1usize << 31) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid ring size",
            ));
        }
        let file_len = page.checked_add(size).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "ring file length overflows")
        })?;
        backing.resize(file_len)?;
        let header = Mapping::new(backing.fd(), 0, page)?;
        let data = DoubleMapping::new(backing.fd(), page, size, true)?;
        // SAFETY: a fresh, page-aligned mapping has room for RingHeader; no reader
        // may open the ring until initialization completes.
        unsafe {
            let ptr = header.as_ptr().cast::<RingHeader>();
            ptr.write(RingHeader {
                magic: MAGIC,
                size: size as u64,
                reader: AtomicU32::new(0),
                head: CacheLine(AtomicU64::new(0)),
                tail: CacheLine(AtomicU64::new(0)),
            });
        }
        Ok(Self {
            header,
            data,
            size,
            head: 0,
            cached_tail: 0,
        })
    }

    pub fn max_payload(&self) -> usize {
        self.size - HEADER_SIZE
    }

    /// Returns an unpublished record, or None when the ring is full.
    /// Panics if len exceeds max_payload().
    pub fn reserve(&mut self, len: usize) -> Option<Reserved<'_>> {
        assert!(len <= self.max_payload(), "payload exceeds ring capacity");
        let required = stride(len) as u64;
        if self.size as u64 - self.head.wrapping_sub(self.cached_tail) < required {
            // SAFETY: header is initialized and remains mapped; tail is atomic.
            let tail = unsafe { &*self.header.as_ptr().cast::<RingHeader>() }
                .tail
                .0
                .load(Ordering::Acquire);
            let used = self.head.wrapping_sub(tail);
            if used > self.size as u64 {
                return None;
            }
            self.cached_tail = tail;
            if self.size as u64 - used < required {
                return None;
            }
        }
        // SAFETY: tail Acquire makes this stride exclusively writable. Its start
        // is aligned and the double mapping makes the whole record contiguous.
        let record = unsafe {
            let ptr = self
                .data
                .as_ptr()
                .add(self.head as usize & (self.size - 1))
                .cast::<RecordHeader>();
            ptr.write(RecordHeader {
                refs: AtomicU32::new(1),
                len: len as u32,
                meta: PacketMeta {
                    slot: 0,
                    generation: 0,
                    kind: 0,
                    _pad0: [0; 3],
                    src_addr: [0; 4],
                    src_port: 0,
                    _pad1: 0,
                    dst_addr: [0; 4],
                    rx_timestamp: 0,
                },
            });
            NonNull::new_unchecked(ptr)
        };
        Some(Reserved {
            writer: self,
            record,
            len,
        })
    }
}

/// An unpublished record. Dropping it publishes nothing.
pub struct Reserved<'a> {
    writer: &'a mut RingWriter,
    record: NonNull<RecordHeader>,
    len: usize,
}

impl Reserved<'_> {
    pub fn meta(&mut self) -> &mut PacketMeta {
        // SAFETY: the exclusive writer borrow owns this unpublished record.
        unsafe { &mut self.record.as_mut().meta }
    }

    pub fn payload(&mut self) -> &mut [u8] {
        // SAFETY: the reserved stride owns exactly len payload bytes contiguously;
        // the exclusive borrow prevents simultaneous header/payload access.
        unsafe {
            std::slice::from_raw_parts_mut(
                self.record.as_ptr().cast::<u8>().add(HEADER_SIZE),
                self.len,
            )
        }
    }

    pub fn commit(self) {
        self.writer.head = self.writer.head.wrapping_add(stride(self.len) as u64);
        // SAFETY: the header remains mapped and head is an initialized atomic.
        unsafe { &*self.writer.header.as_ptr().cast::<RingHeader>() }
            .head
            .0
            .store(self.writer.head, Ordering::Release);
    }
}

struct Shared {
    header: Mapping,
    data: DoubleMapping,
    size: usize,
}

// SAFETY: mappings are process-wide; record access follows the publication and
// reclamation contract, and shared mutable fields are accessed only atomically.
unsafe impl Send for Shared {}
// SAFETY: frames only read their records; the sole reader updates atomic tail.
unsafe impl Sync for Shared {}

impl Shared {
    fn header(&self) -> &RingHeader {
        // SAFETY: open validated this page and initialization precedes open.
        unsafe { &*self.header.as_ptr().cast::<RingHeader>() }
    }

    fn record(&self, pos: u64) -> NonNull<RecordHeader> {
        // SAFETY: positions are eight-byte aligned; masking selects a mapped
        // record start, and a second alias covers any crossing record.
        unsafe {
            NonNull::new_unchecked(
                self.data
                    .as_ptr()
                    .add(pos as usize & (self.size - 1))
                    .cast::<RecordHeader>(),
            )
        }
    }
}

/// The sole consumer; held frames may be released in any order.
pub struct RingReader {
    shared: Arc<Shared>,
    read: u64,
    tail: u64,
    cached_head: u64,
}

impl RingReader {
    /// Opens an initialized ring without retaining the backing. One reader per ring, ever.
    pub fn open(backing: &Backing) -> io::Result<RingReader> {
        let page = backing.page_size()?;
        let header = Mapping::new(backing.fd(), 0, page)?;
        // SAFETY: the page fits an aligned RingHeader, initialized before open.
        let raw = unsafe { &*header.as_ptr().cast::<RingHeader>() };
        if raw.magic != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid ring magic",
            ));
        }
        let size = usize::try_from(raw.size)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid ring size"))?;
        if !size.is_power_of_two() || !size.is_multiple_of(page) || size > (1usize << 31) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid ring size",
            ));
        }
        let data = DoubleMapping::new(backing.fd(), page, size, true)?;
        let tail = raw.tail.0.load(Ordering::Acquire);
        let head = raw.head.0.load(Ordering::Acquire);
        if !tail.is_multiple_of(8) || head.wrapping_sub(tail) > size as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid ring cursors",
            ));
        }
        if raw
            .reader
            .compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "reader already claimed",
            ));
        }
        Ok(Self {
            shared: Arc::new(Shared { header, data, size }),
            read: tail,
            tail,
            cached_head: tail,
        })
    }

    /// Takes the next published record, or None if none is available.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<RxFrame> {
        if self.read == self.cached_head {
            self.cached_head = self.shared.header().head.0.load(Ordering::Acquire);
            if self.read == self.cached_head {
                return None;
            }
        }
        let avail = self.cached_head.wrapping_sub(self.read);
        assert!(avail <= self.shared.size as u64, "corrupt published cursor");
        assert!(avail >= HEADER_SIZE as u64, "unpublished record header");
        let record = self.shared.record(self.read);
        // SAFETY: head Acquire published this header; tail has not reclaimed it.
        let len = unsafe { record.as_ref() }.len;
        assert!(
            len as usize <= self.shared.size - HEADER_SIZE,
            "corrupt record length"
        );
        let stride = stride(len as usize) as u64;
        assert!(stride <= avail, "unpublished record bytes");
        self.read = self.read.wrapping_add(stride);
        Some(RxFrame {
            _shared: Arc::clone(&self.shared),
            len,
            record,
        })
    }

    /// Publishes only the contiguous released prefix of records already read.
    pub fn reclaim(&mut self) {
        let old_tail = self.tail;
        while self.tail != self.read {
            // SAFETY: this record was read and has not yet been reclaimed; its
            // length is immutable, and refs Acquire observes the frame's last read.
            let record = unsafe { self.shared.record(self.tail).as_ref() };
            if record.refs.load(Ordering::Acquire) != 0 {
                break;
            }
            let stride = stride(record.len as usize) as u64;
            assert!(
                stride <= self.read.wrapping_sub(self.tail),
                "corrupt reclaim stride"
            );
            self.tail = self.tail.wrapping_add(stride);
        }
        if self.tail != old_tail {
            self.shared
                .header()
                .tail
                .0
                .store(self.tail, Ordering::Release);
        }
    }
}

/// An owned, immutable record that keeps its mappings alive. Not Clone or Sync.
pub struct RxFrame {
    _shared: Arc<Shared>,
    len: u32,
    record: NonNull<RecordHeader>,
}

// SAFETY: the owned record remains published until this sole frame releases it;
// the Arc keeps both mappings alive on whichever thread receives the frame.
unsafe impl Send for RxFrame {}

impl RxFrame {
    pub fn meta(&self) -> &PacketMeta {
        // SAFETY: the frame's live ref prevents reclaim, and _shared owns the mapping.
        unsafe { &self.record.as_ref().meta }
    }

    pub fn payload(&self) -> &[u8] {
        // SAFETY: publication validated the length, the record is contiguous, and
        // its live ref prevents the producer from overwriting any payload byte.
        unsafe {
            std::slice::from_raw_parts(
                self.record.as_ptr().cast::<u8>().add(HEADER_SIZE),
                self.len as usize,
            )
        }
    }
}

impl Drop for RxFrame {
    fn drop(&mut self) {
        // SAFETY: the frame's owning Arc remains live through this final access.
        unsafe { self.record.as_ref() }
            .refs
            .fetch_sub(1, Ordering::Release);
    }
}

#[cfg(test)]
mod tests;
