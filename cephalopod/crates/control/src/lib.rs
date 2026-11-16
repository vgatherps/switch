//! A Unix-socket service that gives each connecting cephalopod producer its stream id and the
//! path of the shared ring file, and tells the consumer's `ceph_fifo::Fifo` when streams open
//! and close. For standalone cephalopod programs; the junction daemon assigns sessions over
//! its own control socket and does not use this crate.

use ceph_proto::{Control, Retire, pack};
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};

/// The consumer's side: accepts producers and tracks their streams.
pub struct Listener {
    socket: UnixListener,
    path: Vec<u8>,
    free: Vec<u32>,
    live: Vec<Option<(u32, UnixStream)>>,
    next_uid: u32,
    control: Sender<Control>,
    retire: Receiver<Retire>,
}

impl Listener {
    /// Binds the Unix socket at `socket_path`, first removing any file already there.
    /// Producers are told `path`, the shared ring file (`ceph_ring::Reader::name`), which must
    /// fit in a `u16` length. `max_streams` must equal the value passed to `Fifo::new`.
    /// `control` and `retire` are the `Fifo`'s channels.
    pub fn bind(
        socket_path: &Path,
        path: &str,
        max_streams: u32,
        control: Sender<Control>,
        retire: Receiver<Retire>,
    ) -> io::Result<Listener> {
        let path = path.as_bytes().to_vec();
        if u16::try_from(path.len()).is_err() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "shared-file path exceeds u16 wire length",
            ));
        }
        match std::fs::remove_file(socket_path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        let socket = UnixListener::bind(socket_path)?;
        socket.set_nonblocking(true)?;
        let free = (0..max_streams).rev().collect();
        let live = (0..max_streams).map(|_| None).collect();
        Ok(Listener {
            socket,
            path,
            free,
            live,
            next_uid: 1,
            control,
            retire,
        })
    }

    /// Serves producers until the `Fifo`'s channels close; blocks, so run it on its own thread.
    ///
    /// Each accepted producer gets a free slot and a new unique id, announced to the `Fifo`
    /// with `Control::Open` before the reply. A producer that connects while every slot is in
    /// use has its connection closed without a reply. A producer whose connection closes, or
    /// becomes readable (producers send nothing after the handshake), is gone: its slot is
    /// freed and `Control::Close` sent, so its undelivered records are discarded. On
    /// `Retire::Finished` or `Retire::Kicked` the listener closes that producer's connection.
    ///
    /// Returns `Ok` when either channel disconnects, and `Err` when polling or accepting fails
    /// or the unique ids run out.
    pub fn run(mut self) -> io::Result<()> {
        loop {
            loop {
                match self.retire.try_recv() {
                    Ok(retire) => {
                        let (slot, uid) = match retire {
                            Retire::Kicked { slot, uid } | Retire::Finished { slot, uid } => {
                                (slot, uid)
                            }
                        };
                        let entry = &mut self.live[slot as usize];
                        if entry.as_ref().is_some_and(|(live_uid, _)| *live_uid == uid) {
                            *entry = None;
                            self.free.push(slot);
                            if self
                                .control
                                .send(Control::Close { slot, uid })
                                .is_err()
                            {
                                return Ok(());
                            }
                        }
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return Ok(()),
                }
            }

            let mut slots = Vec::with_capacity(self.live.len());
            let mut pollfds = Vec::with_capacity(self.live.len() + 1);
            pollfds.push(libc::pollfd {
                fd: self.socket.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            });
            for (slot, entry) in self.live.iter().enumerate() {
                if let Some((_, socket)) = entry {
                    slots.push(slot as u32);
                    pollfds.push(libc::pollfd {
                        fd: socket.as_raw_fd(),
                        events: libc::POLLIN,
                        revents: 0,
                    });
                }
            }

            let result = unsafe {
                libc::poll(pollfds.as_mut_ptr(), pollfds.len() as libc::nfds_t, 10)
            };
            if result < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
            }

            if pollfds[0].revents & libc::POLLIN != 0 && !self.accept_all()? {
                return Ok(());
            }

            for (index, slot) in slots.into_iter().enumerate() {
                let revents = pollfds[index + 1].revents;
                if revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) != 0 {
                    // Players send no data after connecting. A readable event means the player is gone.
                    let (uid, socket) = self.live[slot as usize]
                        .take()
                        .expect("polled slot must still be live");
                    drop(socket);
                    self.free.push(slot);
                    if self.control.send(Control::Close { slot, uid }).is_err() {
                        return Ok(());
                    }
                }
            }
        }
    }

    fn accept_all(&mut self) -> io::Result<bool> {
        for _ in 0..64 {
            let (mut socket, _) = match self.socket.accept() {
                Ok(pair) => pair,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(true),
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::Interrupted | io::ErrorKind::ConnectionAborted
                    ) =>
                {
                    continue;
                }
                Err(error) => return Err(error),
            };
            let Some(slot) = self.free.pop() else {
                continue;
            };
            let uid = self.next_uid;
            self.next_uid = self
                .next_uid
                .checked_add(1)
                .ok_or_else(|| io::Error::other("stream uid space exhausted"))?;
            // Send `Open` before the reply so the player's first record cannot precede it.
            // A failed reply sends the matching `Close`.
            if self.control.send(Control::Open { slot, uid }).is_err() {
                return Ok(false);
            }
            let reply = socket
                .set_write_timeout(Some(std::time::Duration::from_millis(100)))
                .and_then(|()| socket.write_all(&pack(slot, uid).to_le_bytes()))
                .and_then(|()| socket.write_all(&(self.path.len() as u16).to_le_bytes()))
                .and_then(|()| socket.write_all(&self.path));
            if reply.is_err() {
                drop(socket);
                let sent = self.control.send(Control::Close { slot, uid }).is_ok();
                self.free.push(slot);
                if !sent {
                    return Ok(false);
                }
                continue;
            }
            self.live[slot as usize] = Some((uid, socket));
        }
        Ok(true)
    }
}

/// A producer's connection to a [`Listener`]. Keep it until [`closed`](Self::closed) is true:
/// closing it earlier closes the stream, and its undelivered records are discarded.
pub struct Session {
    /// The stream id to push with: the slot in the high 32 bits, the unique id in the low 32.
    pub stream: u64,
    /// The shared ring file, for `ceph_ring::Writer::attach`.
    pub path: String,
    socket: UnixStream,
}

/// Connects to a [`Listener`]'s socket at `path` and reads the stream id and ring path. Fails
/// with `UnexpectedEof` when the listener has no free slot and closes the connection.
pub fn connect(path: &Path) -> io::Result<Session> {
    let mut socket = UnixStream::connect(path)?;
    let mut stream = [0_u8; 8];
    let mut length = [0_u8; 2];
    socket.read_exact(&mut stream)?;
    socket.read_exact(&mut length)?;
    let mut shared_path = vec![0_u8; u16::from_le_bytes(length) as usize];
    socket.read_exact(&mut shared_path)?;
    let shared_path = String::from_utf8(shared_path)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok(Session {
        stream: u64::from_le_bytes(stream),
        path: shared_path,
        socket,
    })
}

impl Session {
    /// Whether the listener closed the connection: after the stream's end record was
    /// delivered, or after the stream was kicked. Never waits.
    pub fn closed(&self) -> bool {
        let mut pollfd = libc::pollfd {
            fd: self.socket.as_raw_fd(),
            events: libc::POLLIN | libc::POLLHUP,
            revents: 0,
        };
        let result = unsafe { libc::poll(&mut pollfd, 1, 0) };
        result > 0 && pollfd.revents & (libc::POLLIN | libc::POLLHUP) != 0
    }
}
