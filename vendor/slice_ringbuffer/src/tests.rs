use super::*;
use shared_memory::unique_name;
use std::thread;

fn ring() -> (Backing, Backing, RingWriter, RingReader) {
    let name = unique_name("/junction-ring-").unwrap();
    let producer = Backing::create_shm(&name).unwrap();
    let size = producer.page_size().unwrap();
    let writer = RingWriter::create(&producer, size).unwrap();
    let consumer = Backing::open_shm(&name).unwrap();
    let reader = RingReader::open(&consumer).unwrap();
    (producer, consumer, writer, reader)
}

fn publish(writer: &mut RingWriter, len: usize, value: u8) {
    let mut record = writer.reserve(len).unwrap();
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
    let mut record = writer.reserve(expected.len()).unwrap();
    record.payload().copy_from_slice(&expected);
    record.commit();
    let frame = reader.next().unwrap();
    assert_eq!(frame.payload(), expected.as_slice());
}

#[test]
fn reclaim_only_releases_contiguous_prefix() {
    let (_producer, _consumer, mut writer, mut reader) = ring();
    let quarter = reader.shared.size / 4;
    publish(&mut writer, quarter - HEADER_SIZE, 1);
    publish(&mut writer, quarter - HEADER_SIZE, 2);
    publish(&mut writer, 2 * quarter - HEADER_SIZE, 3);
    let a = reader.next().unwrap();
    let b = reader.next().unwrap();
    let c = reader.next().unwrap();
    drop(b);
    drop(c);
    reader.reclaim();
    assert!(writer.reserve(0).is_none(), "held A must block all reuse");
    drop(a);
    reader.reclaim();
    assert!(writer.reserve(quarter - HEADER_SIZE).is_some());
}

#[test]
fn dropping_frame_after_wrap_frees_exact_stride() {
    let (_producer, _consumer, mut writer, mut reader) = ring();
    let size = reader.shared.size;
    publish(&mut writer, size - 64 - HEADER_SIZE, 0);
    drop(reader.next().unwrap());
    reader.reclaim();
    publish(&mut writer, 128 - HEADER_SIZE, 1);
    let held = reader.next().unwrap();
    publish(&mut writer, size - 128 - HEADER_SIZE, 2);
    assert!(writer.reserve(0).is_none());
    drop(held);
    reader.reclaim();
    assert!(
        writer.reserve(128 + 8 - HEADER_SIZE).is_none(),
        "not eight bytes more"
    );
    let reservation = writer
        .reserve(128 - HEADER_SIZE)
        .expect("exact stride is free");
    reservation.commit();
    assert!(
        writer.reserve(0).is_none(),
        "only the held frame's stride was freed"
    );
}

#[test]
fn frame_outlives_reader_writer_and_backings() {
    let frame = {
        let (producer, consumer, mut writer, mut reader) = ring();
        publish(&mut writer, 256, 0xa5);
        let frame = reader.next().unwrap();
        drop(reader);
        drop(writer);
        drop(consumer);
        drop(producer);
        frame
    };
    assert_eq!(frame.payload(), &[0xa5; 256]);
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

fn payload_byte(sequence: u32, offset: usize) -> u8 {
    (sequence.wrapping_mul(37).wrapping_add(offset as u32 * 13) >> (offset % 4)) as u8
}

fn validate(frame: &RxFrame, sequence: u32) {
    assert_eq!(frame.header().slot, sequence, "receive order");
    assert_eq!(frame.payload().len(), sequence as usize * 73 % 600 + 1);
    for (offset, &byte) in frame.payload().iter().enumerate() {
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
    let (_producer, _consumer, mut writer, mut reader) = ring();
    let producer = thread::spawn(move || {
        for sequence in 0..COUNT {
            let len = sequence as usize * 73 % 600 + 1;
            loop {
                if let Some(mut record) = writer.reserve(len) {
                    record.header().slot = sequence;
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
    let mut held = Vec::with_capacity(32);
    let mut received = 0;
    let mut random = 0x1234_5678_u32;
    while received != COUNT || !held.is_empty() {
        let mut got_frame = false;
        if received != COUNT && held.len() < 32 {
            if let Some(frame) = reader.next() {
                validate(&frame, received);
                held.push((received, frame));
                received += 1;
                got_frame = true;
            }
        }
        random ^= random << 13;
        random ^= random >> 17;
        random ^= random << 5;
        if !held.is_empty() && (!got_frame || held.len() == 32 || random & 3 == 0) {
            let index = random as usize % held.len();
            let (sequence, frame) = held.swap_remove(index);
            validate(&frame, sequence);
            drop(frame);
        }
        reader.reclaim();
        std::hint::spin_loop();
    }
    producer.join().unwrap();
}
