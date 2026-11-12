//! One who waits on what a wait watches (`super`), one wait at a time:
//! woken by every write that moves what it waits on, but one whose every
//! Message in a queue its own holder claimed in that write; stopped from
//! any thread, and renewed from any thread where its caller has more to
//! wait on.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use xcore::lock::Locked;
use xcore::wake::Wake;

use super::{Position, Watched};
use crate::PersistError;

/// What ends a wait asked over the wire at its Storage node, as its waiter
/// stops or is renewed.
pub(crate) type Ending = Box<dyn FnOnce() + Send>;

/// One who waits on what a wait watches, one wait at a time: woken by
/// every write that moves what it waits on, stopped from any thread —
/// which ends its wait now and every wait after it — and renewed from any
/// thread, which ends its waits until its caller asks anew, naming more
/// ([`Waiter::asking`]). One that waits for a node
/// is that node's ([`Waiter::of`]): a write whose every Message in a queue
/// that node claimed in the same write tells it where the queue stands
/// without waking it, since what it would read there it holds already.
#[derive(Debug, Default)]
pub struct Waiter {
    told: Mutex<Told>,
    wake: Wake,
    /// The holder whose claims are its own, as a claim names it.
    holder: Option<String>,
}

/// What a waiter was told since its wait began, and whether it is stopped
/// or renewed.
#[derive(Default)]
struct Told {
    /// Where the writes since the wait began left each.
    moved: HashMap<Watched, u64>,
    /// Where the writes since the wait began that put only what its holder
    /// claimed in a queue left it: said with the next answer, waking
    /// nothing.
    quiet: HashMap<Watched, u64>,
    stopped: bool,
    /// How many times it was renewed: its caller had more to wait on.
    renewals: u64,
    /// How many of them its caller's latest wait holds
    /// ([`Waiter::asking`]): one more ends its waits until it asks anew.
    asked: u64,
    /// Why its Storage node refused the wait under way: it is the primary
    /// no more.
    refused: Option<String>,
    /// What ends the wait under way at its Storage node, where it was asked
    /// over the wire.
    ending: Option<Ending>,
}

impl Told {
    /// Whether it was renewed since its caller asked.
    const fn renewed(&self) -> bool {
        self.renewals > self.asked
    }
}

impl fmt::Debug for Told {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Told")
            .field("moved", &self.moved)
            .field("quiet", &self.quiet)
            .field("stopped", &self.stopped)
            .field("renewals", &self.renewals)
            .field("asked", &self.asked)
            .field("refused", &self.refused)
            .finish_non_exhaustive()
    }
}

impl Waiter {
    /// One that waits on nothing yet, for no holder: woken by every write
    /// that moves what it waits on.
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
        self.end(ending);
    }

    /// Renew it: its wait under way ends at once with what moved so far, at
    /// its Storage node too where it was asked over the wire, and so does
    /// every wait and every rest after it until its caller asks anew
    /// ([`Waiter::asking`]): what its caller does once it has more to wait
    /// on.
    pub fn renew(&self) {
        let ending = {
            let mut told = self.lock();
            told.renewals += 1;
            told.ending.take()
        };
        self.end(ending);
    }

    /// A wait is about to be asked of what its caller names now: a renewal
    /// from here on ends it, and one before it — which what it names holds
    /// already — does not.
    pub fn asking(&self) {
        let mut told = self.lock();
        told.asked = told.renewals;
    }

    fn end(&self, ending: Option<Ending>) {
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

    /// Rest for `bound`, or until it is stopped or renewed, whichever is
    /// first: what a waiter does where Xmip Storage did not answer its
    /// wait, so it never asks again at once.
    pub fn rest(&self, bound: Duration) {
        drop(
            self.wake
                .wait_timeout_while(&self.told, self.lock(), bound, |told| {
                    !told.stopped && !told.renewed()
                }),
        );
    }

    /// `end` left with it while a wait asked over the wire is under way,
    /// called once as it stops or is renewed: what ends that wait at its
    /// Storage node. `false`, and `end` dropped uncalled, where it is
    /// stopped already, or renewed since its caller asked.
    pub(crate) fn ending(&self, end: Ending) -> bool {
        let mut told = self.lock();
        if told.stopped || told.renewed() {
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

    /// A wait begins: nothing told of it yet. A renewal since its caller
    /// asked stays, and ends it.
    pub(super) fn begin(&self) {
        let mut told = self.lock();
        told.moved.clear();
        told.quiet.clear();
        told.refused = None;
    }

    /// `watched` moved to `at` by a write: woken, unless every Message the
    /// write put in a queue `own` claimed and `own` is its holder.
    pub(super) fn tell(&self, watched: Watched, at: u64, own: Option<&str>) {
        let quiet = own.is_some() && own == self.holder();
        let mut told = self.lock();
        // Kept where it was told last: a quiet tell is said after the rest.
        if quiet {
            told.quiet.insert(watched, at);
        } else {
            told.quiet.remove(&watched);
            told.moved.insert(watched, at);
        }
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

    /// Its wait on `seen`, where `now` says each stood as the wait began,
    /// until `until` — a bound too long to count waits until told, stopped
    /// or renewed: every one that stands elsewhere than `seen` says, as it
    /// stands, those told quietly among them; none once `until` came or it
    /// was stopped or renewed with none.
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
            if !elsewhere(&now, seen).is_empty() || told.stopped || told.renewed() || came {
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

/// Each of `now` where `moved` says it stands: told after `now` was read,
/// its writer telling in the order it writes, so the latest.
fn raised(now: &mut [Position], moved: &HashMap<Watched, u64>) {
    for position in now {
        if let Some(at) = moved.get(&position.watched) {
            position.at = *at;
        }
    }
}

/// Each of `now` that stands elsewhere than `seen` says.
fn elsewhere(now: &[Position], seen: &[Position]) -> Vec<Position> {
    now.iter()
        .zip(seen)
        .filter(|(now, seen)| now.at != seen.at)
        .map(|(now, _)| *now)
        .collect()
}
