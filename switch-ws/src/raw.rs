//! ws:// lower layer: frames pass through untouched.

use core::convert::Infallible;
use core::marker::PhantomData;

use switch_tls::{Chunk, RxFrame};

use crate::lower::PlainRx;

/// Holds at most one frame. `T` is the send half it is paired with, which
/// receiving does not use.
pub struct Raw<F, T = ()> {
    frame: Option<F>,
    pos: usize,
    _tx: PhantomData<fn(&mut T)>,
}

impl<F, T> Raw<F, T> {
    pub fn new() -> Self {
        Raw { frame: None, pos: 0, _tx: PhantomData }
    }
}

impl<F, T> Default for Raw<F, T> {
    fn default() -> Self {
        Raw::new()
    }
}

impl<F: RxFrame, T> PlainRx for Raw<F, T> {
    type Frame = F;
    type Tx = T;
    type Error = Infallible;

    fn push(&mut self, f: F) {
        assert!(self.current().is_empty(), "push with unconsumed bytes");
        self.frame = Some(f);
        self.pos = 0;
    }

    fn chunk(&mut self, _: &mut T) -> Result<Option<Chunk<'_>>, Infallible> {
        if self.frame.as_ref().is_some_and(|f| self.pos == f.payload().len()) {
            // Fully consumed: release it now, not at the next push (a TcpFrame holds receive window).
            self.frame = None;
        }
        Ok(self.frame.as_mut().map(|f| {
            let stamp = f.rx_timestamp();
            Chunk { bytes: &mut f.payload_mut()[self.pos..], rx_timestamp: stamp }
        }))
    }

    fn current(&self) -> &[u8] {
        match &self.frame {
            Some(f) => &f.payload()[self.pos..],
            None => &[],
        }
    }

    fn consume(&mut self, n: usize) {
        assert!(n <= self.current().len(), "consume past the current run");
        self.pos += n;
    }

    fn discard(&mut self) {
        self.frame = None;
        self.pos = 0;
    }

    fn eof(&self) -> bool {
        false
    }

    fn fin(&mut self, _: &mut T) -> Result<(), Infallible> {
        Ok(())
    }
}
