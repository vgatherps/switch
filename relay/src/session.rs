//! One connection: the receive core, the send core and the input that feeds them. State
//! changes only in the cores' synchronous methods and in [`Session::rx_step`] and
//! [`Session::take_input`]; the async methods await input and the send core's next job and
//! call those.
//!
//! The input's end: the peer's FIN while reading fails the connection (`PeerEof`, or the TLS
//! truncation) and is the normal end after its close. A reset is the socket's death; a send
//! wait that sees it only stops sending (the call that saw it fails with `Failed(Dead)`). Once
//! the death is known and the input ended (by FIN or reset, after the frames received before
//! it are read), `settle` decides the outcome: before the peer's close it is the connection's
//! failure (`Failed(Dead)`), after it a clean end: reading had stopped there anyway, and what
//! we still owed the peer (our echo, close_notify) is moot. A failure stored before stays.

use core::future::{Future, poll_fn};
use core::pin::pin;
use core::task::Poll;

use crate::lower::{PlainRx, PlainTx};
use crate::rx::{Message, WsError, WsRx};
use crate::tx::{End, Failure, Job, TxCore, TxError};

/// How the input ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InputEnd {
    /// The peer's FIN.
    Fin,
    /// A reset, or a socket that can no longer receive.
    Reset,
}

/// The frames the peer sends, in order (TCP's receive half).
pub(crate) trait Input {
    type Frame;
    /// The next frame. Dropping the future loses nothing; its wake registration goes with it.
    async fn recv(&mut self) -> Result<Self::Frame, InputEnd>;
}

/// What happens to input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RxIn {
    /// Frames go to the receive core.
    Reading,
    /// Receiving is over (the peer's close, a failure, `linger`); frames are dropped unread.
    Stopped,
    /// The input ended; it is not polled again.
    Eof,
}

pub(crate) struct Session<R: PlainRx, T: PlainTx, I> {
    pub(crate) rx: WsRx<R>,
    pub(crate) tx: TxCore<T>,
    input: I,
    rx_in: RxIn,
}

impl<R, T, I> Session<R, T, I>
where
    R: PlainRx<Tx = T>,
    T: PlainTx,
    I: Input<Frame = R::Frame>,
    Failure: From<R::Error>,
{
    pub(crate) fn new(rx: WsRx<R>, tx: TxCore<T>, input: I) -> Self {
        Session { rx, tx, input, rx_in: RxIn::Reading }
    }

    #[cfg(test)]
    pub(crate) fn rx_in(&self) -> RxIn {
        self.rx_in
    }

    /// The next message, or `None` once the close handshake is done and our FIN delivered (or
    /// the socket died after the peer's close); after a failure, the first failure once the
    /// drain is done (or the socket died).
    /// With a message in hand, queued output gets one bounded pass (`TxCore::service`) and
    /// the message returns without waiting on it. Without one, input and the next send job
    /// are awaited together; whichever is ready first is handled, both if both are.
    pub(crate) async fn recv(&mut self) -> Result<Option<Message<'_>>, TxError> {
        // Parsing stops only where it needs input, so it runs again only once input came.
        let mut parse = true;
        loop {
            self.settle();
            if parse && self.rx_in == RxIn::Reading && self.rx_step() {
                self.tx.service().await;
                return Ok(self.rx.take_message());
            }
            let over = match self.tx.end() {
                End::Delivered => true,
                // the outcome is known once the input ended, or a failure or the peer's close
                // already decided it
                End::Dead => self.rx_in == RxIn::Eof || self.tx.failed().is_some() || self.tx.peer_closed(),
                End::Open | End::Notify | End::Fin => false,
            };
            if over {
                return self.tx.result().map(|()| None);
            }
            parse = self.wait().await;
        }
    }

    pub(crate) async fn send(&mut self, kind: crate::rx::Kind, payload: &[u8]) -> Result<(), TxError> {
        self.tx.send(kind, payload).await
    }

    pub(crate) async fn close(&mut self, code: u16, reason: &str) -> Result<(), TxError> {
        self.tx.close(code, reason).await
    }

    /// Sends no close frame (one already sealed still goes out), stops receiving, drains to
    /// FIN, and reads and drops input until the peer's FIN: dropping the connection then sends
    /// no RST. `Err(Failed(Dead))` if the socket reset before the peer's close (after it, a
    /// reset is a clean end); a failure before is not reported.
    pub(crate) async fn linger(&mut self) -> Result<(), TxError> {
        self.tx.abandon();
        if self.rx_in == RxIn::Reading {
            self.stop_rx();
        }
        while !(self.rx_in == RxIn::Eof && matches!(self.tx.end(), End::Fin | End::Delivered | End::Dead)) {
            self.wait().await;
        }
        self.settle();
        match (self.tx.end(), self.tx.failed()) {
            (End::Dead, Some(_)) => Err(TxError::Failed(Failure::Dead)),
            (End::Dead, None) | (End::Open | End::Notify | End::Fin | End::Delivered, _) => Ok(()),
        }
    }

    /// Parses what the receive core has; `true` when a message is ready. A close, a protocol
    /// or lower-layer error, or the lower layer's end stops receiving.
    fn rx_step(&mut self) -> bool {
        let found = self.rx.has_message(self.tx.lower_mut());
        if let Some(p) = self.rx.take_pong() {
            self.tx.ping(p);
        }
        match found {
            Ok(true) => return true,
            Ok(false) if self.rx.eof() => self.tx.fail(Failure::PeerEof),
            Ok(false) => return false,
            Err(WsError::Closed) => {
                let echo = self.rx.peer_close().expect("the receive core keeps the close it returned");
                self.tx.peer_close(echo);
            }
            Err(WsError::Protocol(e)) => self.tx.fail(Failure::Protocol(e)),
            Err(WsError::Lower(e)) => self.tx.fail(e.into()),
        }
        self.stop_rx();
        false
    }

    fn stop_rx(&mut self) {
        self.rx.discard();
        self.rx_in = RxIn::Stopped;
    }

    /// One input result: a frame is pushed while reading, dropped unread after; the peer's
    /// FIN while reading fails the connection (PeerEof, or the TLS truncation); a reset (the
    /// input reports one after a FIN too) is the socket's death, whose outcome is `settle`d.
    fn take_input(&mut self, input: Result<R::Frame, InputEnd>) {
        match input {
            Ok(frame) => match self.rx_in {
                RxIn::Reading => self.rx.push(frame),
                RxIn::Stopped | RxIn::Eof => {}
            },
            Err(end) => {
                match (end, self.rx_in) {
                    (InputEnd::Fin, RxIn::Reading) => match self.rx.fin(self.tx.lower_mut()) {
                        Ok(()) => self.tx.fail(Failure::PeerEof),
                        Err(e) => self.tx.fail(e.into()),
                    },
                    // receiving was over: the peer's close came first, or a failure did
                    (InputEnd::Fin, RxIn::Stopped | RxIn::Eof) => {}
                    (InputEnd::Reset, _) => self.tx.stop(),
                }
                self.rx.discard();
                self.rx_in = RxIn::Eof;
                self.settle();
            }
        }
    }

    /// Once the socket's death is known and the input ended (every frame before it read): the
    /// death is the connection's failure unless the peer's close came first or a failure did.
    fn settle(&mut self) {
        if self.tx.end() == End::Dead && self.rx_in == RxIn::Eof {
            self.tx.reset();
        }
    }

    /// Awaits the input (unless it ended) and the send core's next job (unless there is
    /// none), both polled on every poll; handles what either produced. Both futures live
    /// across pending polls (their wake registrations with them) and end before anything is
    /// parsed. `true` if input (a frame or its end) was taken.
    async fn wait(&mut self) -> bool {
        let reading = self.rx_in != RxIn::Eof;
        let job = self.tx.next_job() != Job::Idle;
        assert!(reading || job, "input ended with no job left, but the connection is not over");
        let (input, _) = either(reading.then(|| self.input.recv()), job.then(|| self.tx.try_step())).await;
        let took = input.is_some();
        if let Some(input) = input {
            self.take_input(input);
        }
        self.settle();
        took
    }
}

/// Polls each present future on every poll until at least one is ready; what each produced in
/// that poll (a ready output is never lost or polled again).
async fn either<A: Future, B: Future>(a: Option<A>, b: Option<B>) -> (Option<A::Output>, Option<B::Output>) {
    let mut a = pin!(a);
    let mut b = pin!(b);
    poll_fn(|cx| {
        let ra = a.as_mut().as_pin_mut().and_then(|f| match f.poll(cx) {
            Poll::Ready(out) => Some(out),
            Poll::Pending => None,
        });
        let rb = b.as_mut().as_pin_mut().and_then(|f| match f.poll(cx) {
            Poll::Ready(out) => Some(out),
            Poll::Pending => None,
        });
        if ra.is_none() && rb.is_none() { Poll::Pending } else { Poll::Ready((ra, rb)) }
    })
    .await
}
