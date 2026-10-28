use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use switch::{Capacity, PollReport, Runtime, Sleep, Spawner};

pub const TICKS: [u64; 6] = [100, 100, 101, 102, 1 << 60, u64::MAX];
pub const EXPECTED: [(usize, usize); 6] = [(3, 2), (1, 1), (2, 1), (1, 0), (1, 1), (2, 0)];
pub fn counts(r: PollReport) -> (usize, usize) {
    (r.tasks_polled, r.live_tasks)
}
#[derive(Default)]
pub struct Audit {
    pub drops: Cell<usize>,
    pub checksum: Cell<u64>,
    pub output: RefCell<Vec<u64>>,
}
impl Audit {
    fn complete(&self, value: u64) {
        self.checksum.set(self.checksum.get() + value);
        self.output.borrow_mut().push(value);
    }
}
pub struct Task {
    pub sleeps: Vec<Sleep>,
    pub yields: usize,
    pub value: u64,
    pub audit: Rc<Audit>,
}
impl Future for Task {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut all_ready = true;
        for sleep in &mut self.sleeps {
            match Pin::new(sleep).poll(cx) {
                Poll::Ready(result) => result.unwrap(),
                Poll::Pending => all_ready = false,
            }
        }
        if !all_ready {
            return Poll::Pending;
        }
        if self.yields != 0 {
            self.yields -= 1;
            // Deferred: the yield takes the task to the next turn.
            switch::StoredWaker::new(cx).wake_deferred();
            return Poll::Pending;
        }
        self.audit.complete(self.value);
        Poll::Ready(())
    }
}
impl Drop for Task {
    fn drop(&mut self) {
        self.audit.drops.set(self.audit.drops.get() + 1);
    }
}
#[switch::task]
fn mixed_task(
    sleeps: Vec<Sleep>,
    yields: usize,
    value: u64,
    audit: Rc<Audit>,
) -> impl Future<Output = ()> {
    Task {
        sleeps,
        yields,
        value,
        audit,
    }
}
pub struct Mixed {
    pub runtime: Runtime,
    pub audit: Rc<Audit>,
    spawner: Spawner,
    events: VecDeque<u64>,
}
impl Mixed {
    pub fn new() -> Self {
        let runtime = Runtime::with_capacity(
            100,
            Capacity {
                tasks: 40,
                timers: 32,
            },
        );
        let spawner = runtime.spawner();
        let audit = Rc::new(Audit {
            output: RefCell::new(Vec::with_capacity(7)),
            ..Audit::default()
        });
        let sleeps = (0..32).map(|_| spawner.sleep_until(102).unwrap()).collect();
        spawner
            .spawn(mixed_task(sleeps, 0, 528, audit.clone()))
            .unwrap();
        spawner
            .spawn(mixed_task(Vec::new(), 1, 40, audit.clone()))
            .unwrap();
        Self {
            runtime,
            spawner,
            audit,
            events: VecDeque::with_capacity(2),
        }
    }
    pub fn step(&mut self, index: usize) -> PollReport {
        // Offered events are queued by the outside driver before polling.
        match index {
            0 => self.events.push_back(1),
            2 => {
                self.events.push_back(2);
                self.events.push_back(3);
            }
            4 => self.events.push_back(4),
            5 => self.events.push_back(5),
            _ => {}
        }
        let events = &mut self.events;
        let spawner = &self.spawner;
        let audit = &self.audit;
        while let Some(value) = events.pop_front() {
            let sleeps = if value == 4 {
                vec![spawner.sleep_until(u64::MAX).unwrap()]
            } else {
                Vec::new()
            };
            spawner
                .spawn(mixed_task(sleeps, 0, value, audit.clone()))
                .unwrap();
        }
        self.runtime.poll(TICKS[index]).unwrap()
    }
    pub fn run(&mut self) -> [PollReport; 6] {
        std::array::from_fn(|index| self.step(index))
    }
    pub fn validate(&self, reports: [PollReport; 6]) {
        assert_eq!(reports.map(counts), EXPECTED);
        assert_eq!(self.audit.checksum.get(), 583);
        assert_eq!(*self.audit.output.borrow(), [1, 40, 2, 3, 528, 5, 4]);
        assert_eq!(self.audit.drops.get(), 7);
        assert!(self.events.is_empty());
    }
}

pub struct PendingSleep {
    pub runtime: Runtime,
    pub polls: Rc<Cell<usize>>,
    pub drops: Rc<Cell<usize>>,
    stop: Rc<Cell<bool>>,
}
struct Repeated {
    sleep: Sleep,
    stop: Rc<Cell<bool>>,
    polls: Rc<Cell<usize>>,
    drops: Rc<Cell<usize>>,
}
impl Future for Repeated {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.polls.set(self.polls.get() + 1);
        if self.stop.get() {
            return Poll::Ready(());
        }
        assert!(Pin::new(&mut self.sleep).poll(cx).is_pending());
        // Deferred: one poll per turn.
        switch::StoredWaker::new(cx).wake_deferred();
        Poll::Pending
    }
}
impl Drop for Repeated {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}
#[switch::task]
fn repeated(
    sleep: Sleep,
    stop: Rc<Cell<bool>>,
    polls: Rc<Cell<usize>>,
    drops: Rc<Cell<usize>>,
) -> impl Future<Output = ()> {
    Repeated {
        sleep,
        stop,
        polls,
        drops,
    }
}
impl PendingSleep {
    pub fn new() -> Self {
        let mut runtime = Runtime::with_capacity(
            0,
            Capacity {
                tasks: 1,
                timers: 1,
            },
        );
        let spawner = runtime.spawner();
        let polls = Rc::new(Cell::new(0));
        let drops = Rc::new(Cell::new(0));
        let stop = Rc::new(Cell::new(false));
        spawner
            .spawn(repeated(
                spawner.sleep_until(u64::MAX).unwrap(),
                stop.clone(),
                polls.clone(),
                drops.clone(),
            ))
            .unwrap();
        runtime.poll(0).unwrap();
        Self {
            runtime,
            polls,
            drops,
            stop,
        }
    }
    pub fn step(&mut self) -> PollReport {
        self.runtime.poll(0).unwrap()
    }
    pub fn finish(&mut self) -> PollReport {
        self.stop.set(true);
        self.step()
    }
}
