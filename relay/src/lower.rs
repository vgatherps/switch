//! The interface relay consumes from the layer below it: raw TCP frames
//! ([`Raw`](crate::Raw)) or a TLS layer handing out plaintext.

/// A received TCP frame: one segment's payload plus its hardware RX stamp.
pub trait RxFrame {
    fn payload(&self) -> &[u8];
    fn payload_mut(&mut self) -> &mut [u8];
    fn rx_timestamp(&self) -> u64;
}

/// One contiguous run of received bytes, in stream order.
pub struct Chunk<'a> {
    /// Never empty. Mutable so the server role can unmask in place.
    pub bytes: &'a mut [u8],
    /// Stamp of the frame holding the run's first byte (TLS: the record's first byte).
    pub rx_timestamp: u64,
}

/// A byte stream delivered as frames.
///
/// Bytes are handed out as runs: raw TCP gives the unconsumed rest of the
/// current frame, TLS one record's plaintext.
pub trait PlainRx {
    type Frame: RxFrame;
    /// The send half the receive path needs per call (the TLS connection lives there).
    type Tx;
    type Error;

    /// Hand over the next frame. Called only after `chunk` returned `Ok(None)`.
    fn push(&mut self, f: Self::Frame, tx: &mut Self::Tx) -> Result<(), Self::Error>;
    /// The unconsumed rest of the current run; `None` when a `push` is needed,
    /// by which point every fully consumed frame has been dropped.
    /// Without an intervening `consume`, a second call returns the same bytes,
    /// including changes the caller made to them.
    fn chunk(&mut self, tx: &mut Self::Tx) -> Result<Option<Chunk<'_>>, Self::Error>;
    /// The unconsumed bytes of the run the last `chunk` returned; empty if none.
    fn current(&self) -> &[u8];
    /// Mark the first `n` bytes of `current()` consumed. Frees nothing.
    fn consume(&mut self, n: usize);
    /// If the current run lies inside a frame: consume its first `len` bytes and
    /// move the frame out, with the offset of those bytes in `payload()`. Bytes
    /// after them stay readable through later chunks. `None`, with nothing
    /// consumed, when the run is not inside a frame.
    fn take_frame(&mut self, len: usize) -> Option<(Self::Frame, usize)>;
}

/// A frame buffer to fill and send.
pub trait TxFrame {
    /// The whole writable room of the frame.
    fn room_mut(&mut self) -> &mut [u8];
}

/// The send half below WebSocket: frames, record sealing (TLS), a wait for
/// room, and a synchronous send.
pub trait PlainTx {
    type Frame: TxFrame;
    /// A frame whose bytes are final. Only `seal` and `take_pending` make one
    /// and only `try_send` takes one, so an unsealed frame cannot be sent.
    type Sealed;
    type Error;

    /// A new, empty frame, once one is free.
    fn alloc(&mut self) -> impl Future<Output = Result<Self::Frame, Self::Error>>;
    /// Resolves once a sealed frame carrying `len` plaintext bytes can be
    /// sent: `try_send` of it then succeeds until the transport runs again.
    /// `Err` once the transport can no longer send.
    fn room(&mut self, len: usize) -> impl Future<Output = Result<(), Self::Error>>;
    /// `(head, room)`: plaintext starts at `room_mut()[head]`, and one frame
    /// carries at most `room` plaintext bytes.
    fn plain(&self) -> (usize, usize);
    /// Turn the plaintext `room_mut()[head..head + len]` into the bytes to send
    /// (TLS: one record; raw: set the length). Under TLS this consumes a
    /// sequence number, so the sealed frame must then be sent, never dropped.
    fn seal(&mut self, f: Self::Frame, len: usize) -> Result<Self::Sealed, Self::Error>;
    /// The lower layer has records to send on its own (TLS: an alert).
    fn has_pending(&self) -> bool;
    /// Write those records into `f`; `None`, dropping `f`, when there are none.
    /// Called only when `has_pending()`.
    fn take_pending(&mut self, f: Self::Frame) -> Result<Option<Self::Sealed>, Self::Error>;
    /// Seal the lower layer's own end-of-stream record into `f` (TLS:
    /// close_notify); `None`, dropping `f`, when it has none (raw TCP ends
    /// with FIN, which is not a frame).
    fn seal_close(&mut self, f: Self::Frame) -> Result<Option<Self::Sealed>, Self::Error>;
    /// Send now, or hand the sealed frame back unchanged when there is no room.
    fn try_send(&mut self, s: Self::Sealed) -> Result<(), Self::Sealed>;
    /// End the transport after the frames sent (TCP FIN). Receiving goes on.
    fn shutdown(&mut self);
}
