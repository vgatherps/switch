//! Running a Net's tasks: the loop owner's round (receive, poll, send) and the races tasks
//! bound their waits with.

use std::future::{Future, poll_fn};
use std::pin::pin;
use std::task::Poll;
use std::time::Duration;

use switch_runtime::{Runtime, RuntimeError, Spawner};

use crate::{Instant, Net, TurnError};

/// Error from [`round`]. Implements only `Debug`; print it with `{:?}`.
#[derive(Debug)]
pub enum RoundError {
    /// The receive or send step failed. When the receive step fails, the round stops there:
    /// no runtime poll and no send step.
    Turn(TurnError),
    /// The runtime poll failed; the send step did not run.
    Runtime(RuntimeError),
}

/// One round: [`net.receive(now)`](Net::receive), [`rt.poll(tick)`](Runtime::poll), then
/// [`net.send()`](Net::send). Because the send step comes after the poll, a reply a woken task
/// queues leaves in the same round as the packet that woke it.
///
/// `now` is the Stack's time in microseconds and `tick` the runtime's tick; take both from one
/// clock reading. Ticks must be nanoseconds if any task uses [`within`].
///
/// Never sleeps. Returns `true` if anything moved: the receive step received, rejected or
/// processed packets or had the NIC accept some, packets are still staged, the poll ran a task,
/// or the send step had packets accepted or left some pending. A loop that sleeps when nothing
/// moved should wake by [`Net::poll_at`] at the latest.
pub fn round(net: &mut Net, rt: &mut Runtime, now: Instant, tick: u64) -> Result<bool, RoundError> {
    let received = net.receive(now).map_err(RoundError::Turn)?;
    let polled = rt.poll(tick).map_err(RoundError::Runtime)?;
    let sent = net.send().map_err(RoundError::Turn)?;
    Ok(received.moved()
        || received.rx_staged > 0
        || polled.tasks_polled > 0
        || sent.tx_accepted > 0
        || sent.tx_pending > 0)
}

/// Races two futures: `Some` with `a`'s output if `a` finishes, `None` if `b` finishes first
/// (its output is discarded). Each poll polls `a` before `b`, so `a` wins when both are ready.
/// The future that did not finish is dropped, and with it whatever it held; to keep an `Unpin`
/// `b` for another race, pass `&mut b`.
pub async fn first<A: Future, B: Future>(a: A, b: B) -> Option<A::Output> {
    let (mut a, mut b) = (pin!(a), pin!(b));
    poll_fn(|cx| match a.as_mut().poll(cx) {
        Poll::Ready(out) => Poll::Ready(Some(out)),
        Poll::Pending => b.as_mut().poll(cx).map(|_| None),
    })
    .await
}

/// Bounds `work` by a time limit: `Some` with its output if it finishes in time, `None` once
/// `limit` has passed, in which case `work` is dropped with whatever it held.
///
/// The limit is a runtime sleep of `limit` in nanoseconds, counted from the runtime's latest
/// accepted tick at the first poll. It has passed once a poll's tick reaches the deadline.
/// `work` is polled first, so work that is ready wins even after the deadline. The runtime's
/// ticks (initial, every poll, every relative sleep) must be monotonic nanoseconds; in any other
/// unit every limit is off by the ratio. `spawner` must belong to the runtime that polls this
/// task.
///
/// # Panics
///
/// If `limit` does not fit in `u64` nanoseconds, if the deadline overflows, or, when the sleep
/// is polled, if `spawner` belongs to another runtime.
pub async fn within<F: Future>(spawner: &Spawner, limit: Duration, work: F) -> Option<F::Output> {
    let ticks = u64::try_from(limit.as_nanos()).expect("limit fits u64 nanoseconds");
    let sleep = spawner.sleep(ticks).expect("within deadline overflowed");
    let expired = async { sleep.await.expect("a sleep in the task's own open Runtime") };
    first(work, expired).await
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::future::pending;
    use std::rc::Rc;

    use super::*;

    // ANCHOR: exact_deadline
    #[switch_runtime::task]
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
    // ANCHOR_END: exact_deadline

    #[test]
    #[should_panic(expected = "limit fits u64 nanoseconds")]
    fn timeout_rejects_duration_larger_than_u64_nanoseconds() {
        let runtime = Runtime::new(0);
        let spawner = runtime.spawner();
        let mut future = pin!(within(&spawner, Duration::from_secs(u64::MAX), pending::<()>()));
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        let _ = future.as_mut().poll(&mut context);
    }

    #[test]
    #[should_panic(expected = "within deadline overflowed")]
    fn timeout_reports_deadline_overflow() {
        let runtime = Runtime::new(u64::MAX);
        let spawner = runtime.spawner();
        let mut future = pin!(within(&spawner, Duration::from_nanos(1), pending::<()>()));
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        let _ = future.as_mut().poll(&mut context);
    }
}
