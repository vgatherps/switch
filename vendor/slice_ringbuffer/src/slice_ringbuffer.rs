use std::sync::Arc;

use anyhow::{bail, Context};
use shared_memory::{unique_name, Backing, DoubleMapping, Mapping};

#[cfg(loom)]
mod my_loom {
    pub type AU = loom::sync::atomic::AtomicUsize;
    pub type AB = loom::sync::atomic::AtomicBool;
    pub type Ord = loom::sync::atomic::Ordering;
}

#[cfg(not(loom))]
mod my_loom {
    pub type AU = std::sync::atomic::AtomicUsize;
    pub type AB = std::sync::atomic::AtomicBool;
    pub type Ord = std::sync::atomic::Ordering;
}

#[derive(Eq, PartialEq, Debug, thiserror::Error)]
pub enum BufferReadError {
    #[error("The write side of the buffer was closed")]
    Closed,
    #[error("There is no room left to write in the buffer")]
    NoData,
}

#[derive(Eq, PartialEq, Debug, thiserror::Error)]
pub enum BufferWriteError {
    #[error("The read side of the buffer was closed")]
    Closed,
    #[error("There is no room left to write into the buffer")]
    NoRoom,
}

// This is weird to ensure default is implemented
type CacheBuffer = [[u8; 32]; 4];

type AtomicUsize = my_loom::AU;
type AtomicBool = my_loom::AB;
type Ordering = my_loom::Ord;

#[repr(C)]
struct SliceRingBufferMeta {
    _buf2: CacheBuffer,
    head: AtomicUsize,
    _buf3: CacheBuffer,
    tail: AtomicUsize,
    _buf4: CacheBuffer,
    has_writer: AtomicBool,
    has_reader: AtomicBool,
    size: usize,
    _buf5: CacheBuffer,
}

#[repr(C)]
struct SliceRingBuffer {
    queue_buffer: DoubleMapping,
    meta_buffer: Mapping,
    queue_backing: Backing,
    meta_backing: Backing,
}

impl SliceRingBuffer {
    fn create(size: usize, queue_name: &str, meta_name: &str) -> anyhow::Result<Self> {
        let meta_backing =
            Backing::create_shm(meta_name).context("Could not open metadata buffer")?;
        meta_backing.resize(4096)?;
        let meta_buffer = Mapping::new(meta_backing.fd(), 0, 4096)?;

        let size = size.max(16 * 1024).next_power_of_two();
        let queue_backing =
            Backing::create_shm(queue_name).context("Could not open queue buffer")?;
        queue_backing.resize(size.checked_mul(2).context("Queue size overflow")?)?;
        let queue_buffer = DoubleMapping::new(queue_backing.fd(), 0, size, false)?;

        Ok(Self {
            queue_buffer,
            meta_buffer,
            queue_backing,
            meta_backing,
        })
    }

    #[inline]
    fn meta(&self) -> &SliceRingBufferMeta {
        unsafe { &*(self.meta_buffer.as_ptr() as *const SliceRingBufferMeta) }
    }
    #[inline]
    fn meta_mut(&mut self) -> &mut SliceRingBufferMeta {
        unsafe { &mut *(self.meta_buffer.as_ptr() as *mut SliceRingBufferMeta) }
    }
}

pub struct SliceRingBufferReader {
    buffer: Arc<SliceRingBuffer>,
    tail: usize,
}

pub struct SliceRingBufferView<'a, T> {
    reader: &'a mut SliceRingBufferReader,
    data_length: usize,
    val: T,
}

pub struct SliceRingBufferWriter {
    buffer: Arc<SliceRingBuffer>,
    head: usize,
}

pub struct SliceRingBufferWriteView<'a> {
    reader: &'a mut SliceRingBufferWriter,
    data: &'a mut [u8],
}

impl SliceRingBufferReader {
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.tail == self.buffer.meta().head.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn on_bytes<F: FnMut(&[u8]) -> Option<usize>>(
        &mut self,
        mut f: F,
    ) -> Result<Option<usize>, BufferReadError> {
        let view = self.view_bytes()?;
        let len = view.data().len();
        Ok(match f(view.data()).map(|r| r.min(len)) {
            Some(read) if read > 0 => {
                view.commit(read);
                Some(read)
            }
            _ => None,
        })
    }

    // TODO should this return an option or an empty view?
    #[inline]
    pub fn view_bytes(&mut self) -> Result<SliceRingBufferView<&mut [u8]>, BufferReadError> {
        let buffer = &*self.buffer;
        let head = buffer.meta().head.load(Ordering::Acquire);

        if head == self.tail {
            return if !buffer.meta().has_writer.load(Ordering::Relaxed) {
                Err(BufferReadError::Closed)
            } else {
                Err(BufferReadError::NoData)
            };
        }

        debug_assert!(head >= self.tail);
        let bytes_available = head - self.tail;
        debug_assert!(bytes_available <= buffer.queue_buffer.len());
        let tail_idx = self.tail & (buffer.queue_buffer.len() - 1);
        debug_assert!(tail_idx < buffer.queue_buffer.len());

        if bytes_available == 0 {
            return Err(BufferReadError::NoData);
        }

        let data = unsafe {
            let base = buffer.queue_buffer.as_ptr().add(tail_idx);
            std::slice::from_raw_parts_mut(base, bytes_available)
        };

        Ok(SliceRingBufferView {
            reader: self,
            val: data,
            data_length: bytes_available,
        })
    }

    pub fn lock(&mut self) -> anyhow::Result<()> {
        Ok(self.buffer.queue_buffer.lock()?)
    }

    pub fn reader_meta(&self) -> &str {
        self.buffer.meta_backing.name()
    }

    pub fn reader_buffer(&self) -> &str {
        self.buffer.queue_backing.name()
    }

    pub fn pos(&self) -> (usize, usize) {
        let buffer = &*self.buffer;
        let head = buffer.meta().head.load(Ordering::Acquire);
        (head, self.tail)
    }
}

impl<'a, T> SliceRingBufferView<'a, T> {
    #[inline]
    pub fn commit(self, size_read: usize) {
        let size_read = size_read.min(self.data_length);
        self.reader.tail += size_read;
        self.reader
            .buffer
            .meta()
            .tail
            .store(self.reader.tail, Ordering::Release);
    }

    // These really live a shorter time than the reader by implicit rules on self,
    // but it doesn't hurt to be explicit anyways
    #[inline]
    pub fn data(&self) -> &T {
        &self.val
    }

    #[inline]
    pub fn data_mut(&mut self) -> &mut T {
        &mut self.val
    }

    // can I get all of this by implementing iterator?
    #[inline]
    pub fn map<R, F: FnOnce(T) -> R>(self, f: F) -> SliceRingBufferView<'a, R> {
        SliceRingBufferView {
            reader: self.reader,
            val: f(self.val),
            data_length: self.data_length,
        }
    }

    #[inline]
    pub fn try_map<R, E, F: FnOnce(T) -> Result<R, E>>(
        self,
        f: F,
    ) -> Result<SliceRingBufferView<'a, R>, E> {
        Ok(SliceRingBufferView {
            reader: self.reader,
            val: f(self.val)?,
            data_length: self.data_length,
        })
    }
}

impl SliceRingBufferWriter {
    #[inline]
    pub fn try_write(&mut self, bytes: &[u8]) -> Result<(), BufferWriteError> {
        let len = bytes.len();
        let mut view = self.write_view()?;
        let queue_bytes = view.bytes();
        if queue_bytes.len() < len {
            Err(BufferWriteError::NoRoom)
        } else {
            queue_bytes[..len].copy_from_slice(bytes);
            view.commit(len);
            Ok(())
        }
    }

    #[inline]
    pub fn write_view(&mut self) -> Result<SliceRingBufferWriteView, BufferWriteError> {
        let buffer = &*self.buffer;
        if !buffer.meta().has_reader.load(Ordering::Relaxed) {
            return Err(BufferWriteError::Closed);
        };
        let size = buffer.queue_buffer.len();
        let head = self.head;

        let tail = self.buffer.meta().tail.load(Ordering::Acquire);
        debug_assert!(tail + size >= head);
        let available = (tail + size) - head;

        if available == 0 {
            return Err(BufferWriteError::NoRoom);
        }

        let head_idx = head & (size - 1);
        let data = unsafe {
            std::slice::from_raw_parts_mut(buffer.queue_buffer.as_ptr().add(head_idx), available)
        };

        Ok(SliceRingBufferWriteView { reader: self, data })
    }

    #[inline]
    pub fn queue_buffer(&self) -> &str {
        self.buffer.queue_backing.name()
    }

    #[inline]
    pub fn meta_buffer(&self) -> &str {
        self.buffer.meta_backing.name()
    }
}

impl<'a> SliceRingBufferWriteView<'a> {
    #[inline]
    pub fn commit(self, bytes: usize) {
        let bytes = bytes.min(self.data.len());
        self.reader.head += bytes;
        self.reader
            .buffer
            .meta()
            .head
            .store(self.reader.head, Ordering::Release);
    }

    #[inline]
    pub fn bytes(&mut self) -> &mut [u8] {
        self.data
    }
}

impl Drop for SliceRingBufferReader {
    #[inline]
    fn drop(&mut self) {
        let buffer = &*self.buffer;
        buffer.meta().has_reader.store(false, Ordering::Relaxed);
    }
}

impl Drop for SliceRingBufferWriter {
    #[inline]
    fn drop(&mut self) {
        let buffer = &*self.buffer;
        buffer.meta().has_writer.store(false, Ordering::Relaxed);
    }
}

unsafe impl Send for SliceRingBufferReader {}
unsafe impl Send for SliceRingBufferWriter {}
unsafe impl Send for SliceRingBuffer {}
unsafe impl Sync for SliceRingBuffer {}

pub fn create_slice_ringbuffer(
    size: usize,
) -> anyhow::Result<(SliceRingBufferReader, SliceRingBufferWriter)> {
    let queue_name = unique_name("/srb")?;
    let meta_name = unique_name("/srb")?;
    let mut buffer = SliceRingBuffer::create(size, &queue_name, &meta_name)?;
    buffer.queue_backing.unlink()?;
    buffer.meta_backing.unlink()?;
    let real_size = buffer.queue_buffer.len();
    let meta = buffer.meta_mut();

    meta.head = AtomicUsize::new(0);
    meta.tail = AtomicUsize::new(0);
    meta.has_reader = AtomicBool::new(true);
    meta.has_writer = AtomicBool::new(true);
    meta.size = real_size;

    let buffer = Arc::new(buffer);

    let reader = SliceRingBufferReader {
        buffer: buffer.clone(),
        tail: 0,
    };

    let writer = SliceRingBufferWriter { buffer, head: 0 };

    Ok((reader, writer))
}

pub fn load_reader_from(
    queue_buffer: &str,
    meta_buffer: &str,
) -> anyhow::Result<SliceRingBufferReader> {
    let meta_backing = Backing::open_shm(meta_buffer)?;
    let meta_size = meta_backing.len()?;
    anyhow::ensure!(
        meta_size % 4096 == 0,
        "File {} size is not multiple of page size, is {}",
        meta_backing.name(),
        meta_size
    );
    let meta_buffer = Mapping::new(meta_backing.fd(), 0, meta_size)?;
    let meta = unsafe { &*(meta_buffer.as_ptr() as *const SliceRingBufferMeta) };

    let queue_backing = Backing::open_shm(queue_buffer)?;
    let double_size = queue_backing.len()?;
    let size = double_size / 2;
    anyhow::ensure!(
        double_size.is_power_of_two(),
        "Size of file {} is not power of two",
        queue_backing.name()
    );
    anyhow::ensure!(
        size.is_power_of_two(),
        "Half size of file {} is not power of two",
        queue_backing.name()
    );
    let queue_buffer = DoubleMapping::new(queue_backing.fd(), 0, size, false)?;

    if meta.has_reader.swap(true, Ordering::SeqCst) {
        bail!("Queue at {} already has a reader", queue_backing.name())
    }

    let buffer = SliceRingBuffer {
        queue_buffer,
        meta_buffer,
        queue_backing,
        meta_backing,
    };

    let buffer = Arc::new(buffer);

    let tail = meta.tail.load(Ordering::SeqCst);

    let reader = SliceRingBufferReader { buffer, tail };

    Ok(reader)
}

pub fn create_writer_at(
    queue_size: usize,
    queue_buffer: &str,
    meta_buffer: &str,
) -> anyhow::Result<SliceRingBufferWriter> {
    let buffer = SliceRingBuffer::create(queue_size, queue_buffer, meta_buffer)?;
    let meta = buffer.meta();

    if meta.has_writer.swap(true, Ordering::SeqCst) {
        bail!("Queue at {} already has a writer", buffer.queue_backing.name())
    }

    let head = meta.head.load(Ordering::SeqCst);

    let buffer = Arc::new(buffer);

    let writer = SliceRingBufferWriter { buffer, head };

    Ok(writer)
}

pub fn create_writer(queue_size: usize) -> anyhow::Result<SliceRingBufferWriter> {
    let meta_name = unique_name("/srb")?;
    let queue_name = unique_name("/srb")?;
    create_writer_at(queue_size, &queue_name, &meta_name)
}

#[cfg(test)]
#[cfg(not(loom))]
mod test {
    use super::*;

    #[test]
    fn test_one() {
        let (mut reader, mut writer) = create_slice_ringbuffer(1).unwrap();
        let validate = |buf: &[u8]| {
            assert_eq!(buf.len(), 4);
            for i in buf {
                assert_eq!(*i, 1);
            }
            Some(4)
        };
        assert_eq!(reader.on_bytes(&validate), Err(BufferReadError::NoData));
        assert_eq!(writer.try_write(&[1; 4]), Ok(()));
        assert_eq!(reader.on_bytes(&validate), Ok(Some(4)));
        assert_eq!(reader.on_bytes(&validate), Err(BufferReadError::NoData));
    }

    #[test]
    fn test_multi_read() {
        let (mut reader, mut writer) = create_slice_ringbuffer(1).unwrap();
        let mut read_so_far = 0;
        let mut validate = |buf: &[u8]| {
            if buf.is_empty() {
                return None;
            }
            assert_eq!(buf.len(), 4 - read_so_far);
            for i in buf {
                assert_eq!(*i, 1);
            }
            read_so_far += 1;
            Some(1)
        };
        assert_eq!(reader.on_bytes(&mut validate), Err(BufferReadError::NoData));
        assert_eq!(writer.try_write(&[1; 4]), Ok(()));
        assert_eq!(reader.on_bytes(&mut validate), Ok(Some(1)));
        assert_eq!(reader.on_bytes(&mut validate), Ok(Some(1)));
        assert_eq!(reader.on_bytes(&mut validate), Ok(Some(1)));
        assert_eq!(reader.on_bytes(&mut validate), Ok(Some(1)));
        assert_eq!(reader.on_bytes(&mut validate), Err(BufferReadError::NoData));
    }

    #[test]
    fn test_multi_threaded() {
        let weird_mult = 7;
        let mut base: usize = 23951234;
        let num_rounds = 1000000;
        let max = 32;

        let (mut reader, mut writer) = create_slice_ringbuffer(1).unwrap();

        let write_thread = std::thread::spawn(move || {
            for _ in 0..num_rounds {
                let mut my_data = Vec::new();
                base = base.wrapping_mul(weird_mult) + 1;
                let start = base % max;
                let start: usize = if start == 0 { 1 } else { start };
                for i in 0..start {
                    my_data.push(start.wrapping_add(i) as u8);
                }
                // Here I know we can't overflow due to size limitations
                while writer.try_write(&my_data).is_err() {}
            }

            // return writer from thread so it doesn't overlap
            writer
        });

        for _ in 0..num_rounds {
            let validate = |buf: &[u8]| {
                let start: usize = buf[0] as usize;
                assert!(buf.len() >= start);
                for (i, val) in buf.iter().enumerate().take(start) {
                    assert_eq!(*val, start.wrapping_add(i) as u8);
                }
                Some(start)
            };

            loop {
                match reader.on_bytes(&validate) {
                    Err(BufferReadError::Closed) => panic!("Closed Early"),
                    Ok(None) | Err(BufferReadError::NoData) => continue,
                    Ok(Some(_)) => break,
                }
            }
        }

        assert_eq!(reader.on_bytes(|_| Some(1)), Err(BufferReadError::NoData));

        let writer = write_thread.join().unwrap();

        drop(writer);

        assert_eq!(reader.on_bytes(|_| Some(1)), Err(BufferReadError::Closed));
    }
}

#[cfg(loom)]
#[cfg(test)]
mod test_loom {
    use super::*;

    #[cfg(debug_assertions)]
    const LOOM_ROUNDS: usize = 2;

    #[cfg(not(debug_assertions))]
    const LOOM_ROUNDS: usize = 4;

    #[test]
    fn test_multi_threaded_loom() {
        let mut read_so_far = 0;
        let weird_mult: usize = 7;
        let mut base: usize = 23951234;
        let num_rounds = LOOM_ROUNDS;
        let max = 32;
        loom::model(move || {
            let (mut reader, mut writer) = create_slice_ringbuffer(1).unwrap();
            let write_thread = loom::thread::spawn(move || {
                let mut total = 0;
                for _ in 0..num_rounds {
                    let mut my_data = Vec::new();
                    base = base.wrapping_mul(weird_mult) + 1;
                    let start = base % max;
                    let start: usize = if start == 0 { 1 } else { start };
                    for i in 0..start {
                        my_data.push(start.wrapping_add(i) as u8);
                    }
                    total += start;
                    // Here I know we can't overflow due to size limitations
                    writer.try_write(&my_data).unwrap();
                }

                // return writer from thread so it doesn't overlap
                writer
            });

            let read_thread = loom::thread::spawn(move || {
                for i in 0..num_rounds {
                    let validate = |buf: &[u8]| {
                        let start: usize = buf[0] as usize;
                        assert!(buf.len() >= start);
                        for i in 0..start {
                            assert_eq!(buf[i], start.wrapping_add(i) as u8);
                        }
                        Some(start)
                    };

                    loop {
                        match reader.on_bytes(&validate) {
                            Err(BufferReadError::Closed) => panic!("Closed Early"),
                            Ok(None) | Err(BufferReadError::NoData) => {
                                loom::thread::yield_now();
                                continue;
                            }
                            Ok(Some(_)) => break,
                        }
                    }
                }

                assert_eq!(reader.on_bytes(|_| Some(1)), Err(BufferReadError::NoData));
                reader
            });

            let _writer = write_thread.join().unwrap();
            read_thread.join().unwrap();
        });
    }
}
