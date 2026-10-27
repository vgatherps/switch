#[path = "support/runtime_workload.rs"]
mod workload;
use std::cell::Cell;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use switch::{Runtime, RuntimeError, Sleep};

#[test]
fn deterministic_public_mixed_turns_have_exact_reports_output_and_drops() {
    let mut mixed = workload::Mixed::new();
    let reports = mixed.run();
    mixed.validate(reports);
    mixed.runtime.shutdown();
    assert_eq!(mixed.audit.drops.get(), 7);
}
struct Retain {
    sleep: Sleep,
    registered: bool,
    drops: Rc<Cell<usize>>,
}
impl Future for Retain {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if !self.registered {
            assert!(Pin::new(&mut self.sleep).poll(cx).is_pending());
            self.registered = true;
        }
        // Expiry may poll us again; leave Sleep unobserved for task retirement.
        Poll::Pending
    }
}
impl Drop for Retain {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}
struct Counted {
    mode: u8,
    polls: Rc<Cell<usize>>,
    drops: Rc<Cell<usize>>,
}
impl Future for Counted {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        self.polls.set(self.polls.get() + 1);
        match self.mode {
            0 => Poll::Ready(()),
            1 => panic_any(98765usize),
            _ => Poll::Pending,
        }
    }
}
impl Drop for Counted {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}
#[test]
fn middle_batch_panic_preserves_payload_and_retires_pending_expired_and_unprocessed_tasks() {
    let mut runtime = Runtime::new(0);
    let spawner = runtime.spawner();
    let timer_drops = Rc::new(Cell::new(0));
    for deadline in [5, 10] {
        spawner
            .spawn(Retain {
                sleep: spawner.sleep_until(deadline).unwrap(),
                registered: false,
                drops: timer_drops.clone(),
            })
            .unwrap();
    }
    assert_eq!(runtime.poll(0).unwrap().tasks_polled, 2);
    let polls: [Rc<Cell<usize>>; 3] = std::array::from_fn(|_| Rc::new(Cell::new(0)));
    let drops: [Rc<Cell<usize>>; 3] = std::array::from_fn(|_| Rc::new(Cell::new(0)));
    // AssertUnwindSafe wraps a reviewed shutdown-only runtime; no scheduling resumes.
    for mode in 0..3 {
        spawner
            .spawn(Counted {
                mode,
                polls: polls[mode as usize].clone(),
                drops: drops[mode as usize].clone(),
            })
            .unwrap();
    }
    let payload = catch_unwind(AssertUnwindSafe(|| runtime.poll(5)))
    .unwrap_err();
    assert_eq!(payload.downcast_ref::<usize>(), Some(&98765));
    assert_eq!(polls.each_ref().map(|p| p.get()), [1, 1, 0]);
    assert_eq!(drops.each_ref().map(|p| p.get()), [1, 0, 0]);
    assert_eq!(timer_drops.get(), 0);
    assert_eq!(
        runtime.poll(5),
        Err(RuntimeError::Panicked)
    );
    assert_eq!(
        spawner.spawn(async {}).unwrap_err().reason,
        RuntimeError::Panicked
    );
    runtime.shutdown();
    runtime.shutdown();
    assert_eq!(timer_drops.get(), 2);
    assert_eq!(drops.each_ref().map(|p| p.get()), [1, 1, 1]);
}

#[cfg(not(miri))]
#[test]
fn ordinary_double_panic_fails_only_the_child_process() {
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "double_panic_child", "--ignored", "--nocapture"])
        .env("SWITCH_DOUBLE_PANIC_CHILD", "1")
        .env("RUST_BACKTRACE", "0")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut pipe = child.stderr.take().unwrap();
    let reader = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        std::io::Read::read_to_end(&mut pipe, &mut bytes).unwrap();
        bytes
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while child.try_wait().unwrap().is_none() {
        if std::time::Instant::now() >= deadline {
            child.kill().unwrap();
            let _ = child.wait();
            let _ = reader.join();
            panic!("double-panic child did not terminate within probe deadline");
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    let status = child.wait().unwrap();
    let bytes = reader.join().unwrap();
    assert!(!status.success(), "child unexpectedly survived");
    let stderr = String::from_utf8_lossy(&bytes);
    println!("double-panic child status={status}\nstderr:\n{stderr}");
    assert!(stderr.contains("original-unwind-marker"), "{stderr}");
    assert!(stderr.contains("future-destructor-marker"), "{stderr}");
}
#[cfg(not(miri))]
#[test]
#[ignore = "invoked only by the supervised double-panic subprocess test"]
fn double_panic_child() {
    assert_eq!(std::env::var("SWITCH_DOUBLE_PANIC_CHILD").unwrap(), "1");
    struct PanicDrop;
    impl Future for PanicDrop {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            Poll::Pending
        }
    }
    impl Drop for PanicDrop {
        fn drop(&mut self) {
            panic!("future-destructor-marker");
        }
    }
    let runtime = Runtime::new(0);
    runtime.spawner().spawn(PanicDrop).unwrap();
    panic!("original-unwind-marker");
}

#[test]
fn actual_benchmark_pending_and_burst_controls_finish_exact_work() {
    let mut pending = workload::PendingSleep::new();
    for _ in 0..3 {
        assert_eq!(workload::counts(pending.step()), (1, 0, 0, 1, 1));
    }
    assert_eq!(pending.polls.get(), 4);
    assert_eq!(workload::counts(pending.finish()), (1, 1, 0, 0, 0));
    assert_eq!(pending.drops.get(), 1);
    let mut burst = workload::Mixed::new();
    assert_eq!(workload::counts(burst.step(0)), workload::EXPECTED[0]);
    assert_eq!(workload::counts(burst.step(1)), workload::EXPECTED[1]);
    assert_eq!(workload::counts(burst.step(3)), (1, 1, 32, 0, 0));
    assert_eq!(burst.audit.checksum.get(), 569);
    assert_eq!(burst.audit.drops.get(), 3);
    burst.runtime.shutdown();
}
