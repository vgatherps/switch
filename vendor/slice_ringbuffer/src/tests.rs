use super::*;
use netframe::OwnedFrame;
use shared_memory::unique_name;
use std::rc::Rc;
use std::thread;

fn ring() -> (Backing, Backing, RingWriter, RingReader) {
    let name = unique_name("/junction-ring-").unwrap();
    let mut producer = Backing::create_shm(&name).unwrap();
    let size = producer.page_size().unwrap();
    let writer = RingWriter::create(&mut producer, size).unwrap();
    let consumer = Backing::open_shm(&name).unwrap();
    let reader = RingReader::open(&consumer).unwrap();
    (producer, consumer, writer, reader)
}

fn meta(slot: u32) -> PacketMeta {
    PacketMeta::new(slot, 0, 0, [0; 4], 0, [0; 4], 0)
}

fn publish(writer: &mut RingWriter, len: usize, value: u8) {
    let mut record = writer.reserve(len, meta(0)).unwrap();
    record.payload().fill(value);
    record.commit();
}

#[test]
fn record_crosses_physical_end_as_one_slice() {
    let (_producer, _consumer, mut writer, mut reader) = ring();
    publish(&mut writer, reader.shared.size - 64 - HEADER_SIZE, 0);
    drop(reader.next().unwrap());
    reader.reclaim();
    let expected: Vec<_> = (0..160).map(|i| i as u8).collect();
    let mut record = writer.reserve(expected.len(), meta(0)).unwrap();
    record.payload().copy_from_slice(&expected);
    record.commit();
    let (_, frame) = reader.next().unwrap();
    assert_eq!(&*frame, expected.as_slice());
}

#[test]
fn reclaim_only_releases_contiguous_prefix() {
    let (_producer, _consumer, mut writer, mut reader) = ring();
    let quarter = reader.shared.size / 4;
    publish(&mut writer, quarter - HEADER_SIZE, 1);
    publish(&mut writer, quarter - HEADER_SIZE, 2);
    publish(&mut writer, 2 * quarter - HEADER_SIZE, 3);
    let (_, a) = reader.next().unwrap();
    let (_, b) = reader.next().unwrap();
    let (_, c) = reader.next().unwrap();
    drop(b);
    drop(c);
    reader.reclaim();
    assert!(writer.reserve(0, meta(0)).is_none(), "held A must block all reuse");
    drop(a);
    reader.reclaim();
    assert!(writer.reserve(writer.max_payload(), meta(0)).is_some());
}

#[test]
fn cloned_frame_blocks_reclaim_until_the_last_drop() {
    let (_producer, _consumer, mut writer, mut reader) = ring();
    let len = writer.max_payload();
    publish(&mut writer, len, 0x5a);
    let (_, frame) = reader.next().unwrap();
    let frame = frame.freeze();
    let first = frame.clone();
    let last = frame.clone();
    drop(frame);
    drop(first);
    reader.reclaim();
    assert!(writer.reserve(len, meta(0)).is_none(), "last clone must block reuse");
    drop(last);
    reader.reclaim();
    assert!(writer.reserve(len, meta(0)).is_some(), "last drop must free the ring");
}

#[cfg(feature = "testkit")]
#[test]
fn outstanding_frames_detects_a_retained_clone_without_reclaiming() {
    let (_producer, _consumer, mut writer, mut reader) = ring();
    publish(&mut writer, 8, 0x5a);
    assert_eq!(reader.outstanding_frames(), 0, "unread records are not held frames");
    let (_, frame) = reader.next().unwrap();
    let frame = frame.freeze();
    let retained = frame.clone();
    drop(frame);
    assert_eq!(reader.outstanding_frames(), 1);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        assert_eq!(reader.outstanding_frames(), 0, "RX frame outlived the body");
    })).is_err());
    drop(retained);
    assert_eq!(reader.outstanding_frames(), 0, "last drop is visible before reclaim");
}

#[test]
fn dropping_frame_after_wrap_frees_exact_stride() {
    let (_producer, _consumer, mut writer, mut reader) = ring();
    let size = reader.shared.size;
    publish(&mut writer, size - 64 - HEADER_SIZE, 0);
    drop(reader.next().unwrap());
    reader.reclaim();
    publish(&mut writer, 128 - HEADER_SIZE, 1);
    let (_, held) = reader.next().unwrap();
    publish(&mut writer, size - 128 - HEADER_SIZE, 2);
    assert!(writer.reserve(0, meta(0)).is_none());
    drop(held);
    reader.reclaim();
    assert!(
        writer.reserve(128 + 8 - HEADER_SIZE, meta(0)).is_none(),
        "not eight bytes more"
    );
    let reservation = writer
        .reserve(128 - HEADER_SIZE, meta(0))
        .expect("exact stride is free");
    reservation.commit();
    assert!(
        writer.reserve(0, meta(0)).is_none(),
        "only the held frame's stride was freed"
    );
}

#[test]
fn frame_outlives_reader_writer_and_backings() {
    let (frame, shared) = {
        let (producer, consumer, mut writer, mut reader) = ring();
        let shared = Rc::downgrade(&reader.shared);
        publish(&mut writer, 256, 0xa5);
        let (_, frame) = reader.next().unwrap();
        assert_eq!(shared.strong_count(), 2, "reader and frame retain the mappings");
        drop(reader);
        drop(writer);
        drop(consumer);
        drop(producer);
        (frame, shared)
    };
    assert_eq!(shared.strong_count(), 1, "frame retains the mappings");
    assert_eq!(&*frame, &[0xa5; 256]);
    drop(frame);
    assert!(shared.upgrade().is_none(), "frame releases the mappings");
}

#[test]
fn open_rejects_wrong_magic() {
    let (producer, consumer, _writer, _reader) = ring();
    let header = Mapping::new(producer.fd(), 0, producer.page_size().unwrap()).unwrap();
    // SAFETY: this separate mapping covers the aligned magic field; neither
    // writer nor reader accesses magic while this test corrupts it.
    unsafe { header.as_ptr().cast::<u64>().write(0) };
    assert!(
        matches!(RingReader::open(&consumer), Err(error) if error.kind() == io::ErrorKind::InvalidData)
    );
}

#[test]
fn reader_claim_is_permanent() {
    let (_producer, consumer, _writer, reader) = ring();
    assert!(
        matches!(RingReader::open(&consumer), Err(error) if error.kind() == io::ErrorKind::AlreadyExists)
    );
    drop(reader);
    assert!(
        matches!(RingReader::open(&consumer), Err(error) if error.kind() == io::ErrorKind::AlreadyExists)
    );
}

#[test]
fn open_rejects_misaligned_tail() {
    let name = unique_name("/junction-ring-").unwrap();
    let mut producer = Backing::create_shm(&name).unwrap();
    let page = producer.page_size().unwrap();
    let _writer = RingWriter::create(&mut producer, page).unwrap();
    let consumer = Backing::open_shm(&name).unwrap();
    let header = Mapping::new(producer.fd(), 0, page).unwrap();
    // SAFETY: the separate mapping covers an initialized RingHeader; the
    // atomic cursor stores occur before any reader is opened.
    unsafe {
        let header = &*header.as_ptr().cast::<RingHeader>();
        header.tail.0.store(9, Ordering::Relaxed);
        header.head.0.store(9, Ordering::Relaxed);
    }
    assert!(
        matches!(RingReader::open(&consumer), Err(error) if error.kind() == io::ErrorKind::InvalidData)
    );
}

#[test]
fn recreate_rejects_active_backing_without_resetting_records() {
    let (mut producer, _consumer, mut writer, mut reader) = ring();
    publish(&mut writer, 128, 0x5a);
    let size = producer.page_size().unwrap();
    assert!(
        matches!(RingWriter::create(&mut producer, size), Err(error) if error.kind() == io::ErrorKind::InvalidInput)
    );
    let (_, frame) = reader.next().unwrap();
    assert_eq!(&*frame, &[0x5a; 128]);
}

fn payload_byte(sequence: u32, offset: usize) -> u8 {
    (sequence.wrapping_mul(37).wrapping_add(offset as u32 * 13) >> (offset % 4)) as u8
}

fn validate(meta: &PacketMeta, frame: &OwnedFrame, sequence: u32) {
    assert_eq!(meta.slot, sequence, "receive order");
    assert_eq!(frame.len(), sequence as usize * 73 % 600 + 1);
    for (offset, &byte) in frame.iter().enumerate() {
        assert_eq!(
            byte,
            payload_byte(sequence, offset),
            "sequence {sequence}, byte {offset}"
        );
    }
}

#[test]
fn cross_thread_stress_preserves_held_frames() {
    const COUNT: u32 = 100_000;
    let name = unique_name("/junction-ring-").unwrap();
    let mut backing = Backing::create_shm(&name).unwrap();
    let size = backing.page_size().unwrap();
    let mut writer = RingWriter::create(&mut backing, size).unwrap();
    let producer = thread::spawn(move || {
        for sequence in 0..COUNT {
            let len = sequence as usize * 73 % 600 + 1;
            loop {
                if let Some(mut record) = writer.reserve(len, meta(sequence)) {
                    for (offset, byte) in record.payload().iter_mut().enumerate() {
                        *byte = payload_byte(sequence, offset);
                    }
                    record.commit();
                    break;
                }
                std::hint::spin_loop();
            }
        }
    });
    let consumer = thread::spawn(move || {
        let backing = Backing::open_shm(&name).unwrap();
        let mut reader = RingReader::open(&backing).unwrap();
        let mut held = Vec::with_capacity(32);
        let mut received = 0;
        let mut random = 0x1234_5678_u32;
        while received != COUNT || !held.is_empty() {
            let mut got_frame = false;
            if received != COUNT && held.len() < 32 {
                if let Some((meta, frame)) = reader.next() {
                    validate(&meta, &frame, received);
                    held.push((received, meta, frame));
                    received += 1;
                    got_frame = true;
                }
            }
            random ^= random << 13;
            random ^= random >> 17;
            random ^= random << 5;
            if !held.is_empty() && (!got_frame || held.len() == 32 || random & 3 == 0) {
                let index = random as usize % held.len();
                let (sequence, meta, frame) = held.swap_remove(index);
                validate(&meta, &frame, sequence);
                drop(frame);
            }
            reader.reclaim();
            std::hint::spin_loop();
        }
    });
    producer.join().unwrap();
    consumer.join().unwrap();
}

#[cfg(target_os = "linux")]
#[test]
fn full_tmpfs_returns_allocation_error_before_mapping() {
    use std::path::PathBuf;
    use std::process::Command;

    struct Tmpfs(PathBuf);
    impl Drop for Tmpfs {
        fn drop(&mut self) {
            assert!(Command::new("umount").arg(&self.0).status().unwrap().success());
            std::fs::remove_dir(&self.0).unwrap();
        }
    }

    let dir = std::env::temp_dir().join(unique_name("junction-full-tmpfs-").unwrap());
    std::fs::create_dir(&dir).unwrap();
    let mount = Command::new("mount")
        .args(["-t", "tmpfs", "-o", "size=4096", "none"])
        .arg(&dir)
        .output();
    match mount {
        Ok(output) if output.status.success() => {}
        result => {
            std::fs::remove_dir(&dir).unwrap();
            panic!("private tmpfs mount unavailable: {:?}", result);
        }
    }
    let mounted = Tmpfs(dir);
    let mut backing = Backing::create_file(&mounted.0.join("ring")).unwrap();
    let page = backing.page_size().unwrap();
    let error = match RingWriter::create(&mut backing, page) {
        Ok(_) => panic!("ring allocation exceeded the tmpfs capacity"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), io::ErrorKind::StorageFull);
}
