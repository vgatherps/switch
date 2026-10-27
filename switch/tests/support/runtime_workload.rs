use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use switch::{Capacity, PollReport, Runtime, Sleep, Spawner};

pub const TICKS: [u64; 6] = [100, 100, 101, 102, 1 << 60, u64::MAX];
pub const EXPECTED: [(usize, usize, usize, usize, usize); 6] = [
    (3, 1, 0, 1, 2),
    (1, 1, 0, 0, 1),
    (2, 2, 0, 0, 1),
    (1, 1, 32, 0, 0),
    (1, 0, 0, 0, 1),
    (2, 2, 1, 0, 0),
];
pub fn counts(r: PollReport) -> (usize, usize, usize, usize, usize) {
    (
        r.tasks_polled,
        r.tasks_completed,
        r.timers_expired,
        r.ready_tasks,
        r.live_tasks,
    )
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
            cx.waker().wake_by_ref();
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
            .spawn(Task {
                sleeps,
                yields: 0,
                value: 528,
                audit: audit.clone(),
            })
            .unwrap();
        spawner
            .spawn(Task {
                sleeps: Vec::new(),
                yields: 1,
                value: 40,
                audit: audit.clone(),
            })
            .unwrap();
        Self {
            runtime,
            spawner,
            audit,
            events: VecDeque::with_capacity(2),
        }
    }
    pub fn step(&mut self, index: usize) -> PollReport {
        // Offered events are queued by the outside driver before the callback.
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
        self.runtime
            .poll(TICKS[index], || {
                while let Some(value) = events.pop_front() {
                    let sleeps = if value == 4 {
                        vec![spawner.sleep_until(u64::MAX).unwrap()]
                    } else {
                        Vec::new()
                    };
                    spawner
                        .spawn(Task {
                            sleeps,
                            yields: 0,
                            value,
                            audit: audit.clone(),
                        })
                        .unwrap();
                }
            })
            .unwrap()
    }
    pub fn run(&mut self) -> [PollReport; 6] {
        std::array::from_fn(|index| self.step(index))
    }
    pub fn validate(&self, reports: [PollReport; 6]) {
        assert_eq!(reports.map(counts), EXPECTED);
        assert_eq!(self.audit.checksum.get(), 583);
        assert_eq!(*self.audit.output.borrow(), [1, 40, 2, 3, 528, 4, 5]);
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
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}
impl Drop for Repeated {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
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
            .spawn(Repeated {
                sleep: spawner.sleep_until(u64::MAX).unwrap(),
                stop: stop.clone(),
                polls: polls.clone(),
                drops: drops.clone(),
            })
            .unwrap();
        runtime.poll(0, || {}).unwrap();
        Self {
            runtime,
            polls,
            drops,
            stop,
        }
    }
    pub fn step(&mut self) -> PollReport {
        self.runtime.poll(0, || {}).unwrap()
    }
    pub fn finish(&mut self) -> PollReport {
        self.stop.set(true);
        self.step()
    }
}
