//! ws:// lower layer: frames pass through untouched.

use core::convert::Infallible;
use core::marker::PhantomData;

use sheath::{Chunk, RxFrame};

use crate::lower::PlainRx;

/// Holds at most one frame. After `take_frame` the frame's unconsumed bytes
/// move to `carry`, which is served before the next frame. `T` is the send half
/// it is paired with, which receiving does not use.
pub struct Raw<F, T = ()> {
    frame: Option<F>,
    pos: usize,
    carry: Vec<u8>,
    carry_pos: usize,
    carry_stamp: u64,
    _tx: PhantomData<fn(&mut T)>,
}

impl<F, T> Raw<F, T> {
    pub fn new() -> Self {
        Raw { frame: None, pos: 0, carry: Vec::new(), carry_pos: 0, carry_stamp: 0, _tx: PhantomData }
    }

    fn in_carry(&self) -> bool {
        self.carry_pos < self.carry.len()
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

    fn push(&mut self, f: F, _: &mut T) -> Result<(), Infallible> {
        assert!(self.current().is_empty(), "push with unconsumed bytes");
        self.frame = Some(f);
        self.pos = 0;
        Ok(())
    }

    fn chunk(&mut self, _: &mut T) -> Result<Option<Chunk<'_>>, Infallible> {
        if self.in_carry() {
            let stamp = self.carry_stamp;
            return Ok(Some(Chunk { bytes: &mut self.carry[self.carry_pos..], rx_timestamp: stamp }));
        }
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
        if self.in_carry() {
            return &self.carry[self.carry_pos..];
        }
        match &self.frame {
            Some(f) => &f.payload()[self.pos..],
            None => &[],
        }
    }

    fn consume(&mut self, n: usize) {
        assert!(n <= self.current().len(), "consume past the current run");
        if self.in_carry() {
            self.carry_pos += n;
            if self.carry_pos == self.carry.len() {
                self.carry.clear();
                self.carry_pos = 0;
            }
        } else {
            self.pos += n;
        }
    }

    fn take_frame(&mut self, len: usize) -> Option<(F, usize)> {
        if self.in_carry() {
            return None;
        }
        let f = self.frame.take()?;
        let start = self.pos;
        let rest = &f.payload()[start..];
        assert!(len <= rest.len(), "take_frame past the current run");
        self.carry.extend_from_slice(&rest[len..]);
        self.carry_stamp = f.rx_timestamp();
        self.pos = 0;
        Some((f, start))
    }
}
