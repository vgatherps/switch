//! Running a Net's tasks: the loop owner's round (turn, poll, idle sleep), any future as a
//! switch task, and the races tasks bound their waits with.

use std::future::{Future, poll_fn};
use std::pin::{Pin, pin};
use std::task::Poll;
use std::time::Duration;

use switch::{Runtime, RuntimeError, Spawner};

use crate::{Instant, Net, StackError};

/// Longest idle sleep of `round`: a timer the Stack does not report (a Runtime sleep) is late by
/// at most this.
const MAX_IDLE_SLEEP: Duration = Duration::from_millis(1);

#[derive(Debug)]
pub enum RoundError {
    Stack(StackError),
    Runtime(RuntimeError),
}

/// One `net.turn(now)` and one `rt.poll(tick)`. When neither moved anything (nothing received,
/// processed, sent, staged or pending; no task polled), sleeps until the Stack's next timer, at
/// most 1 ms.
pub fn round(net: &mut Net, rt: &mut Runtime, now: Instant, tick: u64) -> Result<(), RoundError> {
    let turn = net.turn(now).map_err(RoundError::Stack)?;
    let polled = rt.poll(tick).map_err(RoundError::Runtime)?;
    if turn.moved() || turn.tx_pending > 0 || turn.rx_staged > 0 || polled.tasks_polled > 0 {
        return Ok(());
    }
    let sleep = match net.poll_at() {
        None => MAX_IDLE_SLEEP,
        Some(at) if at <= now => return Ok(()),
        Some(at) => Duration::from_micros((at - now).total_micros()).min(MAX_IDLE_SLEEP),
    };
    std::thread::sleep(sleep);
    Ok(())
}

/// Any future as a switch task.
#[switch::task]
pub fn boxed(future: Pin<Box<dyn Future<Output = ()>>>) -> impl Future<Output = ()> {
    future
}

/// `a`'s output, or `None` if `b` finishes first; `a` is polled first. Dropping `a` drops what
/// it holds.
pub async fn first<A: Future, B: Future>(a: A, b: B) -> Option<A::Output> {
    let (mut a, mut b) = (pin!(a), pin!(b));
    poll_fn(|cx| match a.as_mut().poll(cx) {
        Poll::Ready(out) => Poll::Ready(Some(out)),
        Poll::Pending => b.as_mut().poll(cx).map(|_| None),
    })
    .await
}

/// `work`'s output, or `None` once `limit` passed first, counted in Runtime ticks as
/// milliseconds (the loop passes milliseconds to `round`). Called from a task of `spawner`'s
/// Runtime, which is therefore open.
pub async fn within<F: Future>(spawner: &Spawner, limit: Duration, work: F) -> Option<F::Output> {
    let ticks = u64::try_from(limit.as_millis()).expect("limit fits u64 milliseconds");
    let sleep = spawner.sleep(ticks).expect("the Runtime polling this task is open");
    let expired = async { sleep.await.expect("a sleep in the task's own open Runtime") };
    first(work, expired).await
}
