//! Junction's receive ring: one writer (the daemon) and one reader (an application's
//! session) over shared memory. Each record is a [`PacketMeta`] and a payload, and the reader
//! gets each payload back as a frame that points into the ring. Applications see only
//! `PacketMeta`, as `switch::junction::rx::PacketMeta`.

// Memory ordering: header and payload writes, then head Release, then reader head Acquire.
// After publication, each plain reference count is accessed only on the reader's thread; the
// writer never reads it. Frame release, then reclaim, then tail Release, then writer tail
// Acquire orders those accesses before reuse. Concurrent initialization through other backing
// handles or processes is outside the contract.

use switch_frame::OwnedFrame;
use shared_memory::{Backing, DoubleMapping, Mapping};
use std::convert::TryFrom;
use std::io;
use std::mem::{align_of, offset_of, size_of};
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

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
    refs: u16,
    _pad: u16,
    len: u32,
    meta: PacketMeta,
}

/// Where a received packet came from, delivered with each packet.
#[repr(C)]
#[derive(Copy, Clone)]
pub struct PacketMeta {
    /// The receiving socket's slot.
    pub slot: u32,
    /// The receiving socket's generation.
    pub generation: u32,
    /// `TCP` (1) for a chunk of a TCP connection's data, `UDP` (2) for a whole datagram.
    pub kind: u8,
    _pad0: [u8; 3],
    /// The sender's IPv4 address, for UDP; zero for TCP.
    pub src_addr: [u8; 4],
    /// The sender's port, for UDP; zero for TCP.
    pub src_port: u16,
    _pad1: u16,
    /// The address the datagram was sent to, for UDP; zero for TCP.
    pub dst_addr: [u8; 4],
    /// The network card's hardware receive timestamp, as the driver reports it; 0 for none.
    pub rx_timestamp: u64,
}

impl PacketMeta {
    /// Metadata with the given fields, for the writer.
    pub fn new(
        slot: u32,
        generation: u32,
        kind: u8,
        src_addr: [u8; 4],
        src_port: u16,
        dst_addr: [u8; 4],
        rx_timestamp: u64,
    ) -> Self {
        Self {
            slot,
            generation,
            kind,
            _pad0: [0; 3],
            src_addr,
            src_port,
            _pad1: 0,
            dst_addr,
            rx_timestamp,
        }
    }
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
    assert!(offset_of!(RecordHeader, _pad) == 2);
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

/// The only writer of a ring. Can move to another thread.
///
/// ```
/// use shared_memory::{unique_name, Backing};
/// use slice_ringbuffer::{PacketMeta, RingReader, RingWriter};
///
/// let name = unique_name("/junction-ring-")?;
/// let mut producer = Backing::create_shm(&name)?;
/// let size = producer.page_size()?;
/// let mut writer = RingWriter::create(&mut producer, size)?;
///
/// // Normally the other process.
/// let consumer = Backing::open_shm(&name)?;
/// let mut reader = RingReader::open(&consumer)?;
///
/// let meta = PacketMeta::new(0, 0, 0, [0; 4], 0, [0; 4], 0);
/// let mut record = writer.reserve(5, meta).expect("ring has room");
/// record.payload().copy_from_slice(b"hello");
/// record.commit();
///
/// let (_meta, frame) = reader.next().expect("one record");
/// assert_eq!(&*frame, b"hello");
/// drop(frame);
/// reader.reclaim();
/// # Ok::<(), std::io::Error>(())
/// ```
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
    /// Lays a ring of `size` bytes out on a fresh backing: one page of header, then the
    /// records, mapped twice back to back so every payload is one contiguous slice. The caller
    /// keeps the backing and its name.
    ///
    /// Fails with `InvalidInput` if the backing's length is not zero, or if `size` is not a
    /// power of two, not a multiple of the backing's page size, or larger than 2 GiB.
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
        backing.allocate(file_len)?;
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

    /// The largest payload: `size` minus the 40-byte record header. Each record takes its
    /// header plus its payload rounded up to 8 bytes.
    pub fn max_payload(&self) -> usize {
        self.size - HEADER_SIZE
    }

    /// A record of `len` payload bytes to fill, or `None` when the ring has no room until the
    /// reader reclaims space.
    ///
    /// # Panics
    /// Panics if `len` is larger than [`max_payload`](Self::max_payload).
    pub fn reserve(&mut self, len: usize, meta: PacketMeta) -> Option<Reserved<'_>> {
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
                refs: 1,
                _pad: 0,
                len: len as u32,
                meta,
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

/// A reserved record. [`commit`](Self::commit) makes it visible to the reader; dropping it
/// without committing publishes nothing.
pub struct Reserved<'a> {
    writer: &'a mut RingWriter,
    record: NonNull<RecordHeader>,
    len: usize,
}

impl Reserved<'_> {
    /// The record's payload bytes, to fill before committing.
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

    /// Makes the record visible to the reader.
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

/// The only reader of a ring, and the frames it hands out.
///
/// The reader, and every frame it returns, stays on the thread that opened it. Frames can be
/// dropped in any order and stay valid after the reader, the writer and both backings are
/// dropped. The reader trusts the writer: corrupt cursors or lengths in the shared memory make
/// [`next`](Self::next) and [`reclaim`](Self::reclaim) panic.
pub struct RingReader {
    shared: Rc<Shared>,
    read: u64,
    tail: u64,
    cached_head: u64,
}

impl RingReader {
    /// Opens the ring in `backing` and claims its reader place, for the life of the ring: a
    /// second open fails with `AlreadyExists`, even after the first reader is dropped. Fails
    /// with `InvalidData` if the backing does not hold a valid ring. Does not keep `backing`.
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
            shared: Rc::new(Shared { header, data, size }),
            read: tail,
            tail,
            cached_head: tail,
        })
    }

    /// Number of records still retained by frames, including leaked clones.
    #[cfg(feature = "testkit")]
    pub fn outstanding_frames(&self) -> usize {
        Rc::strong_count(&self.shared) - 1
    }

    /// The next committed record and a frame over its payload, or `None` if there is none.
    /// Never waits.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<(PacketMeta, OwnedFrame)> {
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
        // SAFETY: publication validated the payload, its count starts at one,
        // and the retained Rc keeps the mapping alive through the last release.
        let frame = unsafe {
            OwnedFrame::from_raw_parts(
                NonNull::new_unchecked(record.as_ptr().cast::<u8>().add(HEADER_SIZE)),
                len,
                NonNull::new_unchecked(std::ptr::addr_of_mut!((*record.as_ptr()).refs)),
                ring_release,
                Rc::into_raw(Rc::clone(&self.shared)).cast_mut().cast(),
            )
        };
        // SAFETY: the live frame prevents reuse of this published metadata.
        Some((unsafe { record.as_ref() }.meta, frame))
    }

    /// Returns space to the writer: every record already read whose frames are all dropped,
    /// in order, stopping at the first record that still has a live frame. One long-held
    /// frame keeps every later record's space from being reused.
    pub fn reclaim(&mut self) {
        let old_tail = self.tail;
        while self.tail != self.read {
            // SAFETY: this record was read and has not yet been reclaimed. Its
            // length is immutable; only this thread accesses the published count.
            let record = unsafe { self.shared.record(self.tail).as_ref() };
            if record.refs != 0 {
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

unsafe extern "C" fn ring_release(rc: NonNull<u16>, ctx: *mut ()) {
    // SAFETY: the last frame owns this count and the Rc retained by next().
    // Zero is visible to reclaim on this thread before its tail Release store.
    unsafe {
        rc.as_ptr().write(0);
        drop(Rc::from_raw(ctx.cast::<Shared>()));
    }
}

#[cfg(test)]
mod tests;
