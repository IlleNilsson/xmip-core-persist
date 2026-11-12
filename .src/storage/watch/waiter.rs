//! One who waits on queues (`super`), one wait at a time: woken by every
//! write that moves a queue it waits on, but one whose every Message there
//! its own holder claimed in that write, and stopped from any thread.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use xcore::lock::Locked;
use xcore::wake::Wake;

use super::Position;
use crate::PersistError;

/// What ends a wait asked over the wire at its Storage node, as its waiter
/// stops.
pub(crate) type Ending = Box<dyn FnOnce() + Send>;

/// One who waits on queues, one wait at a time: woken by every write that
/// moves a queue it waits on, and stopped from any thread, which ends its
/// wait now and every wait after it. One that waits for a node is that
/// node's ([`Waiter::of`]): a write whose every Message in a queue that
/// node claimed in the same write tells it where the queue stands without
/// waking it, since what it would read there it holds already.
#[derive(Debug, Default)]
pub struct Waiter {
    told: Mutex<Told>,
    wake: Wake,
    /// The holder whose claims are its own, as a claim names it.
    holder: Option<String>,
}

/// What a waiter was told since its wait began, and whether it is stopped.
#[derive(Default)]
struct Told {
    /// By queue, where the writes since the wait began left it.
    moved: HashMap<u128, u64>,
    /// By queue, where the writes since the wait began that put only what
    /// its holder claimed there left it: said with the next answer, waking
    /// nothing.
    quiet: HashMap<u128, u64>,
    stopped: bool,
    /// Why its Storage node refused the wait under way: it is the primary
    /// no more.
    refused: Option<String>,
    /// What ends the wait under way at its Storage node, where it was asked
    /// over the wire.
    ending: Option<Ending>,
}

impl fmt::Debug for Told {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Told")
            .field("moved", &self.moved)
            .field("quiet", &self.quiet)
            .field("stopped", &self.stopped)
            .field("refused", &self.refused)
            .finish_non_exhaustive()
    }
}

impl Waiter {
    /// One that waits on nothing yet, for no holder: woken by every write
    /// to a queue it waits on.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// One that waits for the node whose claims name `holder`.
    #[must_use]
    pub fn of(holder: &str) -> Self {
        Self {
            holder: Some(holder.to_string()),
            ..Self::default()
        }
    }

    /// The holder it waits for, where it waits for one.
    #[must_use]
    pub fn holder(&self) -> Option<&str> {
        self.holder.as_deref()
    }

    /// Stop it: its wait now ends at once — at its Storage node too, where
    /// it was asked over the wire — and every wait after it does not wait.
    pub fn stop(&self) {
        let ending = {
            let mut told = self.lock();
            told.stopped = true;
            told.ending.take()
        };
        self.wake.notify_all();
        if let Some(end) = ending {
            end();
        }
    }

    /// Whether it is stopped.
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.lock().stopped
    }

    /// Rest for `bound`, or until it is stopped, whichever is first: what a
    /// waiter does where Xmip Storage did not answer its wait, so it never
    /// asks again at once.
    pub fn rest(&self, bound: Duration) {
        drop(
            self.wake
                .wait_timeout_while(&self.told, self.lock(), bound, |told| !told.stopped),
        );
    }

    /// `end` left with it while a wait asked over the wire is under way,
    /// called once as it stops: what ends that wait at its Storage node.
    /// `false`, and `end` dropped uncalled, where it is stopped already.
    pub(crate) fn ending(&self, end: Ending) -> bool {
        let mut told = self.lock();
        if told.stopped {
            return false;
        }
        told.ending = Some(end);
        true
    }

    /// The wait asked over the wire answered: nothing is left to end.
    pub(crate) fn ended(&self) {
        drop(self.lock().ending.take());
    }

    fn lock(&self) -> MutexGuard<'_, Told> {
        self.told.locked()
    }

    /// A wait begins: nothing told of it yet.
    pub(super) fn begin(&self) {
        let mut told = self.lock();
        told.moved.clear();
        told.quiet.clear();
        told.refused = None;
    }

    /// `queue` moved to `next` by a write: woken, unless every Message the
    /// write put there `own` claimed and `own` is its holder.
    pub(super) fn tell(&self, queue: u128, next: u64, own: Option<&str>) {
        let quiet = own.is_some() && own == self.holder();
        let mut told = self.lock();
        let kept = if quiet {
            told.quiet.entry(queue).or_insert(next)
        } else {
            told.moved.entry(queue).or_insert(next)
        };
        *kept = (*kept).max(next);
        drop(told);
        if !quiet {
            self.wake.notify_all();
        }
    }

    /// Its wait under way refused, `why`: woken.
    pub(super) fn refuse(&self, why: &str) {
        self.lock().refused = Some(why.to_string());
        self.wake.notify_all();
    }

    /// Its wait on `seen`, where `now` says each queue stood as the wait
    /// began, until `until` — a bound too long to count waits until told
    /// or stopped: every queue that stands elsewhere than `seen` says, as
    /// it stands, those told quietly among them; none once `until` came or
    /// it was stopped with none.
    ///
    /// # Errors
    ///
    /// Where its Storage node refused the wait as it waited.
    pub(super) fn waited(
        &self,
        seen: &[Position],
        mut now: Vec<Position>,
        until: Option<Instant>,
    ) -> Result<Vec<Position>, PersistError> {
        let mut told = self.lock();
        loop {
            if let Some(reason) = told.refused.take() {
                return Err(PersistError::Standby { reason });
            }
            raised(&mut now, &told.moved);
            let came = until.is_some_and(|until| Instant::now() >= until);
            if !elsewhere(&now, seen).is_empty() || told.stopped || came {
                raised(&mut now, &told.quiet);
                return Ok(elsewhere(&now, seen));
            }
            told = match until {
                Some(until) => self.wake.wait_until(&self.told, told, until).0,
                None => self.wake.wait(&self.told, told),
            };
        }
    }
}

/// Each of `now` raised to where `moved` says its queue stands, where that
/// is later.
fn raised(now: &mut [Position], moved: &HashMap<u128, u64>) {
    for position in now {
        if let Some(next) = moved.get(&position.queue) {
            position.next = position.next.max(*next);
        }
    }
}

/// Each of `now` that stands elsewhere than `seen` says.
fn elsewhere(now: &[Position], seen: &[Position]) -> Vec<Position> {
    now.iter()
        .zip(seen)
        .filter(|(now, seen)| now.next != seen.next)
        .map(|(now, _)| *now)
        .collect()
}
