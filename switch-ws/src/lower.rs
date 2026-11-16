//! What switch-ws needs from the layer below it, implemented for both lower layers: TLS through
//! switch-tls ([`TlsRx`]/[`TlsTx`]) and raw TCP ([`Raw`](crate::raw::Raw) receiving, switch-tls's
//! frame sender sending). Crate-private: switch-ws is tied to these two, and the traits exist so
//! the core runs on test frames too.

use switch_tls::{Chunk, FrameTx, RxFrame, Sealed, TlsRx, TlsTx};

/// A byte stream delivered as frames.
///
/// Bytes are handed out as runs: raw TCP gives the unconsumed rest of the current frame, TLS
/// one record's plaintext.
pub(crate) trait PlainRx {
    type Frame: RxFrame;
    /// The send half the receive path needs per call (the TLS connection lives there).
    type Tx;
    type Error;

    /// Hand over the next frame. Called only after `chunk` returned `Ok(None)`.
    fn push(&mut self, f: Self::Frame);
    /// The unconsumed rest of the current run; `None` when a `push` is needed, by which point
    /// every fully consumed frame has been dropped. Without an intervening `consume`, a second
    /// call returns the same bytes, including changes the caller made to them.
    fn chunk(&mut self, tx: &mut Self::Tx) -> Result<Option<Chunk<'_>>, Self::Error>;
    /// The unconsumed bytes of the run the last `chunk` returned; empty if none. Exactly those
    /// bytes, unchanged, until the next `chunk` or `push`: text validated at receipt is read
    /// back through this without validating again.
    fn current(&self) -> &[u8];
    /// Mark the first `n` bytes of `current()` consumed. Frees nothing.
    fn consume(&mut self, n: usize);
    /// Drop everything held (frame, run, buffered bytes); nothing is read or pushed after.
    fn discard(&mut self);
    /// The stream ended inside the lower layer (TLS close_notify); `chunk` returns `None` for
    /// good. Raw TCP ends only with FIN, which `fin` reports.
    fn eof(&self) -> bool;
    /// TCP FIN arrived while reading, after `chunk` returned `None`: `Ok` if the lower layer
    /// had ended its stream first (raw TCP has nothing to end), else why not (TLS truncation).
    fn fin(&mut self, tx: &mut Self::Tx) -> Result<(), Self::Error>;
}

/// The send half below WebSocket: switch-tls's frame sender, plus the records the lower layer
/// sends on its own.
pub(crate) trait PlainTx: FrameTx {
    /// `seal_close` makes a record (TLS close_notify); raw TCP ends with FIN alone, which
    /// needs no frame.
    const HAS_CLOSE_RECORD: bool;
    /// The lower layer has records to send on its own (TLS: an alert).
    fn has_pending(&self) -> bool;
    /// Write those records into `f`; `None`, dropping `f`, when there are none. Called only
    /// when `has_pending()`.
    fn take_pending(&mut self, f: Self::Frame) -> Result<Option<Self::Sealed>, Self::Error>;
    /// Seal the lower layer's own end-of-stream record into `f` (TLS: close_notify); `None`,
    /// dropping `f`, when it has none (raw TCP ends with FIN, which is not a frame).
    fn seal_close(&mut self, f: Self::Frame) -> Result<Option<Self::Sealed>, Self::Error>;
}

impl<F: RxFrame, L: FrameTx> PlainRx for TlsRx<F, L> {
    type Frame = F;
    type Tx = TlsTx<L>;
    type Error = switch_tls::Error;

    fn push(&mut self, f: F) {
        TlsRx::push(self, f)
    }

    fn chunk(&mut self, tx: &mut TlsTx<L>) -> Result<Option<Chunk<'_>>, switch_tls::Error> {
        TlsRx::chunk(self, tx)
    }

    fn current(&self) -> &[u8] {
        TlsRx::current(self)
    }

    fn consume(&mut self, n: usize) {
        TlsRx::consume(self, n)
    }

    fn discard(&mut self) {
        TlsRx::discard(self)
    }

    fn eof(&self) -> bool {
        TlsRx::eof(self)
    }

    fn fin(&mut self, tx: &mut TlsTx<L>) -> Result<(), switch_tls::Error> {
        TlsRx::fin(self, tx)
    }
}

impl<L: FrameTx> PlainTx for TlsTx<L> {
    const HAS_CLOSE_RECORD: bool = true;

    fn has_pending(&self) -> bool {
        TlsTx::has_pending(self)
    }

    fn take_pending(&mut self, f: L::Frame) -> Result<Option<Sealed<L::Sealed>>, Self::Error> {
        TlsTx::take_pending(self, f)
    }

    fn seal_close(&mut self, f: L::Frame) -> Result<Option<Sealed<L::Sealed>>, Self::Error> {
        self.close_notify(f).map(Some)
    }
}
