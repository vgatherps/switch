//! Running a Net's tasks: the loop owner's round (turn, poll) and the races tasks bound their
//! waits with.

use std::future::{Future, poll_fn};
use std::pin::pin;
use std::task::Poll;
use std::time::Duration;

use switch::{Runtime, RuntimeError, Spawner};

use crate::{Instant, Net, TurnError};

#[derive(Debug)]
pub enum RoundError {
    Turn(TurnError),
    Runtime(RuntimeError),
}

/// One `net.turn(now)` and one `rt.poll(tick)`; never sleeps. Returns whether anything moved:
/// something received, processed, sent, staged or still pending, or a task polled. A loop that
/// may sleep when nothing moved does so itself, until `net.poll_at()` at the latest.
pub fn round(net: &mut Net, rt: &mut Runtime, now: Instant, tick: u64) -> Result<bool, RoundError> {
    let turn = net.turn(now).map_err(RoundError::Turn)?;
    let polled = rt.poll(tick).map_err(RoundError::Runtime)?;
    Ok(turn.moved() || turn.tx_pending > 0 || turn.rx_staged > 0 || polled.tasks_polled > 0)
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

/// `work`'s output, or `None` once `limit` passes from the Runtime's latest accepted tick.
/// The Runtime's initial and poll ticks, and all relative sleeps on it, must use monotonic
/// nanoseconds. Called from a task of `spawner`'s Runtime, which is therefore open.
///
/// Panics if `limit` does not fit in `u64` nanoseconds.
pub async fn within<F: Future>(spawner: &Spawner, limit: Duration, work: F) -> Option<F::Output> {
    let ticks = u64::try_from(limit.as_nanos()).expect("limit fits u64 nanoseconds");
    let sleep = spawner.sleep(ticks).expect("the Runtime polling this task is open");
    let expired = async { sleep.await.expect("a sleep in the task's own open Runtime") };
    first(work, expired).await
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::future::pending;
    use std::rc::Rc;

    use super::*;

    #[switch::task]
    async fn timeout(spawner: Spawner, completed: Rc<Cell<Option<bool>>>) {
        let result = within(&spawner, Duration::from_nanos(123_456), pending::<()>()).await;
        completed.set(Some(result.is_none()));
    }

    #[test]
    fn sub_millisecond_timeout_expires_at_exact_deadline() {
        let start = 1_000_037;
        let deadline = start + 123_456;
        let mut runtime = Runtime::new(start);
        let spawner = runtime.spawner();
        let completed = Rc::new(Cell::new(None));
        spawner.spawn(timeout(spawner.clone(), completed.clone())).unwrap();

        runtime.poll(start).unwrap();
        assert_eq!(completed.get(), None);
        runtime.poll(start).unwrap();
        assert_eq!(completed.get(), None);
        runtime.poll(deadline - 1).unwrap();
        assert_eq!(completed.get(), None);
        runtime.poll(deadline).unwrap();
        assert_eq!(completed.get(), Some(true));
    }

    #[test]
    #[should_panic(expected = "limit fits u64 nanoseconds")]
    fn timeout_rejects_duration_larger_than_u64_nanoseconds() {
        let runtime = Runtime::new(0);
        let spawner = runtime.spawner();
        let mut future = pin!(within(&spawner, Duration::from_secs(u64::MAX), pending::<()>()));
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        let _ = future.as_mut().poll(&mut context);
    }
}
