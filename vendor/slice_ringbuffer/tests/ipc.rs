#[cfg(not(loom))]
mod native {
    use std::io::{self, BufRead, BufReader, Write};
    use std::process::{Child, Command, Stdio};
    use std::sync::mpsc::{self, Receiver};
    use std::thread;
    use std::time::{Duration, Instant};

    use anyhow::{bail, ensure, Context, Result};
    use slice_ringbuffer::{
        create_writer, load_reader_from, BufferReadError, BufferWriteError, SliceRingBufferReader,
        SliceRingBufferWriter,
    };

    const TIMEOUT: Duration = Duration::from_secs(10);

    struct ReaderChild(Child);

    impl Drop for ReaderChild {
        fn drop(&mut self) {
            if !matches!(self.0.try_wait(), Ok(Some(_))) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
    }

    fn lines_from(input: impl io::Read + Send + 'static) -> Receiver<io::Result<String>> {
        let (send, receive) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(input).lines() {
                let failed = line.is_err();
                if send.send(line).is_err() || failed {
                    break;
                }
            }
        });
        receive
    }

    fn receive(lines: &Receiver<io::Result<String>>) -> Result<String> {
        lines
            .recv_timeout(TIMEOUT)
            .context("Timed out or lost IPC peer")?
            .context("Could not read IPC peer response")
    }

    fn send(output: &mut impl Write, command: &str) -> Result<()> {
        writeln!(output, "{}", command)?;
        output.flush()?;
        Ok(())
    }

    fn expected_byte(position: usize) -> u8 {
        (position.wrapping_mul(37) ^ (position >> 8)) as u8
    }

    fn write_bytes(writer: &mut SliceRingBufferWriter, position: &mut usize, len: usize) -> Result<()> {
        let bytes: Vec<_> = (*position..*position + len).map(expected_byte).collect();
        writer.try_write(&bytes)?;
        *position += len;
        Ok(())
    }

    fn check_bytes(bytes: &[u8], position: &mut usize) -> Result<()> {
        for (offset, byte) in bytes.iter().enumerate() {
            let expected = expected_byte(*position + offset);
            ensure!(
                *byte == expected,
                "Byte {} was {}, expected {}",
                *position + offset,
                byte,
                expected
            );
        }
        *position += bytes.len();
        Ok(())
    }

    fn read_bytes(
        reader: &mut SliceRingBufferReader,
        position: &mut usize,
        count: usize,
    ) -> Result<()> {
        let end = *position + count;
        let deadline = Instant::now() + TIMEOUT;
        while *position < end {
            ensure!(Instant::now() < deadline, "Timed out waiting for queue bytes");
            match reader.view_bytes() {
                Ok(view) => {
                    let len = view.data().len().min(end - *position);
                    check_bytes(&view.data()[..len], position)?;
                    view.commit(len);
                }
                Err(BufferReadError::NoData) => thread::sleep(Duration::from_millis(1)),
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    fn reader(queue_name: &str, meta_name: &str) -> Result<()> {
        let mut reader = load_reader_from(queue_name, meta_name)?;
        let commands = lines_from(io::stdin());
        let mut output = io::stdout();
        send(&mut output, "attached")?;
        let mut position = 0;
        loop {
            let command = receive(&commands)?;
            let (operation, count) = command.split_once(' ').context("Missing IPC command count")?;
            let count: usize = count.parse()?;
            match operation {
                "read" => {
                    read_bytes(&mut reader, &mut position, count)?;
                    send(&mut output, &format!("read {}", position))?;
                }
                "finish" => {
                    ensure!(count == position, "Writer closed before the stream was drained");
                    let deadline = Instant::now() + TIMEOUT;
                    loop {
                        ensure!(Instant::now() < deadline, "Timed out waiting for writer close");
                        match reader.view_bytes() {
                            Err(BufferReadError::Closed) => break,
                            Err(BufferReadError::NoData) => {
                                thread::sleep(Duration::from_millis(1));
                            }
                            Ok(_) => bail!("Queue contained bytes beyond the expected stream"),
                        }
                    }
                    send(&mut output, &format!("closed {}", position))?;
                    return Ok(());
                }
                _ => bail!("Unknown IPC operation {}", operation),
            }
        }
    }

    fn parent() -> Result<()> {
        let mut writer = create_writer(16 * 1024)?;
        let mut child = ReaderChild(
            Command::new(std::env::current_exe()?)
                .arg("--reader")
                .arg(writer.queue_buffer())
                .arg(writer.meta_buffer())
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()?,
        );
        let mut commands = child.0.stdin.take().context("Missing reader stdin")?;
        let responses = lines_from(child.0.stdout.take().context("Missing reader stdout")?);
        ensure!(receive(&responses)? == "attached", "Reader did not attach");

        let capacity = writer.write_view()?.bytes().len();
        let mut written = 0;
        let mut read = 0;
        write_bytes(&mut writer, &mut written, capacity)?;
        ensure!(
            writer.try_write(&[0]) == Err(BufferWriteError::NoRoom),
            "Full queue did not apply backpressure"
        );

        // Leave bytes at the physical end, then append bytes at the start.
        let count = capacity - 257;
        send(&mut commands, &format!("read {}", count))?;
        read += count;
        ensure!(receive(&responses)? == format!("read {}", read));
        write_bytes(&mut writer, &mut written, capacity - 513)?;
        let count = written - read;
        send(&mut commands, &format!("read {}", count))?;
        read += count;
        ensure!(receive(&responses)? == format!("read {}", read));

        // Both full-capacity views now cross the physical ring boundary.
        for _ in 0..8 {
            write_bytes(&mut writer, &mut written, capacity)?;
            ensure!(
                writer.try_write(&[0]) == Err(BufferWriteError::NoRoom),
                "Wrapped full queue did not apply backpressure"
            );
            send(&mut commands, &format!("read {}", capacity))?;
            read += capacity;
            ensure!(receive(&responses)? == format!("read {}", read));
        }

        write_bytes(&mut writer, &mut written, capacity / 2 + 99)?;
        send(&mut commands, &format!("read {}", written - read))?;
        ensure!(receive(&responses)? == format!("read {}", written));
        // Confirm the complete stream was consumed before checking writer close.
        drop(writer);
        send(&mut commands, &format!("finish {}", written))?;
        ensure!(receive(&responses)? == format!("closed {}", written));
        let deadline = Instant::now() + TIMEOUT;
        loop {
            if let Some(status) = child.0.try_wait()? {
                ensure!(status.success(), "Reader exited with {}", status);
                return Ok(());
            }
            ensure!(Instant::now() < deadline, "Timed out reaping reader");
            thread::sleep(Duration::from_millis(1));
        }
    }

    pub fn run() -> Result<()> {
        let mut arguments = std::env::args().skip(1);
        if arguments.next().as_deref() == Some("--reader") {
            let queue_name = arguments.next().context("Missing queue name")?;
            let meta_name = arguments.next().context("Missing metadata name")?;
            reader(&queue_name, &meta_name)
        } else {
            parent()
        }
    }
}

#[cfg(not(loom))]
fn main() -> anyhow::Result<()> {
    native::run()
}

#[cfg(loom)]
fn main() {
    eprintln!("The two-process IPC regression is excluded from the loom model");
}
