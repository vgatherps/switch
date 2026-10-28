//! What relay needs from the layer below it, implemented for both lower layers: TLS through
//! sheath ([`TlsRx`]/[`TlsTx`]) and raw TCP ([`Raw`](crate::raw::Raw) receiving, sheath's
//! frame sender sending). Crate-private: relay is tied to these two, and the traits exist so
//! the core runs on test frames too.

use sheath::{Chunk, FrameTx, RxFrame, Sealed, TlsRx, TlsTx};

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
    fn push(&mut self, f: Self::Frame, tx: &mut Self::Tx) -> Result<(), Self::Error>;
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
}

/// The send half below WebSocket: sheath's frame sender, plus the records the lower layer
/// sends on its own.
pub(crate) trait PlainTx: FrameTx {
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
    type Error = sheath::Error;

    fn push(&mut self, f: F, _: &mut TlsTx<L>) -> Result<(), sheath::Error> {
        TlsRx::push(self, f);
        Ok(())
    }

    fn chunk(&mut self, tx: &mut TlsTx<L>) -> Result<Option<Chunk<'_>>, sheath::Error> {
        TlsRx::chunk(self, tx)
    }

    fn current(&self) -> &[u8] {
        TlsRx::current(self)
    }

    fn consume(&mut self, n: usize) {
        TlsRx::consume(self, n)
    }
}

impl<L: FrameTx> PlainTx for TlsTx<L> {
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
