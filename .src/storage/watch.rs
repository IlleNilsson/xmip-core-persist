//! A wait asked of Xmip Storage (open problem 46; ADR-0018, amendments
//! 2026-11-12): a node's send scan, and its Work Process step's, waits on
//! the queues it takes from, and is answered as soon as a Publication, a
//! hand-on, a turn or a Replay puts a Message in one of them — on whichever
//! node wrote it — instead of finding it at its next scan; and a Journey it
//! passed over waits on what holds it back, answered as soon as that moves.
//!
//! **What is waited on** ([`Watched`]), each by a count only a write moves:
//! a queue's tail, the place its next entry takes, which a write putting an
//! entry in it moves — what a step takes from; a queue's head, how many
//! entries were ever taken out of it, which a write taking one out moves —
//! a sequence, whose head moving lets the next of it go on; a Work Process
//! Instance, how many turns it took, which a turn written over it moves;
//! and a key of the Instance Key table, one where an Instance holds it and
//! none where none does, which a write giving it to an Instance or letting
//! it go moves. The last three are what a Journey behind another Stream of
//! its sequence, and a turn its Instance did not take, wait on: until
//! 2026-11-12 the write that moved them put nothing in the waiting
//! Journey's queue, and it was taken up at the next scan.
//!
//! **Asked with what was seen last.** A wait names each with where its
//! waiter last saw it stand ([`Position`]), and is answered with every one
//! that stands elsewhere now: at once where one already does, so a write
//! that lands between a read and the wait after it is never waited past.
//! One no write moved within the wait's bound is answered with none —
//! empty, never an error.
//!
//! **Answered with what is due** ([`Moved`]): each queue's tail that moved
//! comes back with the first of what is due in it, read by the Storage
//! node as it answers, up to as many as the wait asked for, so the woken
//! node claims without a read of its own first — until 2026-11-12 a round
//! trip of 0.14 ms over loopback after every wake.
//!
//! **Told by the one writer** ([`Watch`]): the embedded Storage node's
//! writer tells every waiter on what a batch moved, as soon as the batch is
//! in its engine and readable, each waiter by a wake of its own, so one
//! write wakes every node waiting on it and no other — and not the node
//! whose own claims in that write hold every Message it put in a queue,
//! told quietly. What a woken node finds is sent only under a claim, and a
//! claim is answered once its own batch is durable, which the syncs of the
//! batches before it come first in: no far end sees a Message before the
//! write that queued it is durable. The watch keeps where its writer left
//! each waited on now, and the latest of what none waits on now, at most
//! [`UNWAITED`] (`[tuning] storage_unwaited`), the longest unwaited let go
//! first (`known`): a wait reads the store only for what is not kept, and
//! what is kept follows the waits under way, never every Instance, key and
//! sequence ever waited on. Its writer tells in the order it writes, so
//! what was told last is where each stands — a key's count goes back to
//! none where it is let go, so the last told is kept, never the highest.
//!
//! **Stopped at once, renewed at once.** A [`Waiter`] stopped ends its wait
//! in this process at once, and every wait after; one renewed ends the
//! wait under way, and its caller asks again with more to wait on. One
//! asked over the wire is ended at its Storage node, asked over another
//! connection by what the client left with the waiter ([`Waiter::ending`]):
//! no node looks whether its waiter stopped.
//!
//! **Refused by a primary no more** ([`Watch::refuse`]): a Storage node that
//! steps down or stops ends every wait on its queues refused
//! ([`PersistError::Standby`]), so the node waiting asks the next Storage
//! node at once rather than wait out its bound on one no write reaches.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use xcore::lock::Locked;

use super::commit::Batch;
use super::due::{self, DuePart, DueQueue};
use super::hold::KINDS;
use super::{instance, queue};
use crate::{EncryptedStore, Engine, PersistError};

mod form;
mod known;
mod moved;
mod waiter;

use known::Known;
pub use known::UNWAITED;
pub(crate) use moved::{Change, moved};
pub(crate) use waiter::Ending;
pub use waiter::Waiter;

/// What a wait watches, by its identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Watched {
    /// A queue ([`super::named`]) by its tail: the place its next entry
    /// takes, which only a write that puts an entry in it moves.
    Queue(u128),
    /// A queue by its head: how many entries were ever taken out of it,
    /// which only a write that takes one out moves — a sequence's head
    /// moving on.
    Head(u128),
    /// A Work Process Instance by how many turns it took, which only a turn
    /// written over it moves.
    Instance(u128),
    /// A key of the Instance Key table: one where an Instance holds it,
    /// none where none does.
    Key(u128),
    /// The operator's acts asked of a node, by the name-based identifier
    /// of its scope (`super::act`): the place of the last, which only an
    /// act asked of it moves — what a node's act watch waits on.
    Acts(u128),
    /// One operator's act by its identifier: none while it is asked, one
    /// once its node decided it, which only its decision moves — what the
    /// door that asked it waits on.
    Act(u128),
    /// The administration database's `membership` table by the number of
    /// its latest write, which only a node's membership row written moves
    /// (`super::membership`): what a node following the members waits on.
    Membership,
}

/// Where what a wait watches stands: its count, as [`Watched`] says what is
/// counted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Position {
    pub watched: Watched,
    pub at: u64,
}

impl Position {
    /// `queue` by its tail, its next entry taking the place `next`.
    #[must_use]
    pub const fn queue(queue: u128, next: u64) -> Self {
        Self {
            watched: Watched::Queue(queue),
            at: next,
        }
    }
}

/// What a wait answers for each it watches that stands elsewhere than its
/// waiter saw: where it stands, and for a queue's tail, the first of what
/// is due in it as the Storage node read it answering — up to as many as
/// the wait asked for, none where it asked for none or the read failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Moved {
    pub position: Position,
    pub due: Option<DueQueue>,
}

/// Who waits on what of one embedded Storage node now, and where its one
/// writer left each waited on.
#[derive(Debug, Default)]
pub(crate) struct Watch {
    waiting: Mutex<Waiting>,
}

#[derive(Debug, Default)]
struct Waiting {
    /// Who waits on each now.
    on: HashMap<Watched, Vec<Arc<Waiter>>>,
    /// Where its writer left each waited on now, and the latest none waits
    /// on now (`known`).
    known: Known,
    /// Why every wait is refused: its node is the primary no more.
    refused: Option<String>,
}

impl Watch {
    /// Each of `changes` kept where the watch keeps it (`known`), and every
    /// waiter on it told — quietly, the one whose holder's claims hold all the
    /// batch put in a queue; one whose count could not be read (`None`) is
    /// read again by the next wait on it, and its waiters find it at their
    /// bound.
    pub(crate) fn moved(&self, changes: &[Change]) {
        if changes.is_empty() {
            return;
        }
        let mut waiting = self.waiting.locked();
        for Change { watched, at, own } in changes {
            waiting.known.told(*watched, *at);
            let Some(at) = at else {
                continue;
            };
            for waiter in waiting.on.get(watched).into_iter().flatten() {
                waiter.tell(*watched, *at, own.as_deref());
            }
        }
    }

    /// Every wait refused from now on, `why`, the one under way among them:
    /// its node is the primary no more.
    pub(crate) fn refuse(&self, why: &str) {
        let mut waiting = self.waiting.locked();
        waiting.refused = Some(why.to_string());
        for waiter in waiting.on.values().flatten() {
            waiter.refuse(why);
        }
    }

    /// At most `most` of what none waits on now kept where its writer left
    /// it, the longest unwaited let go first: the Storage node's `[tuning]
    /// storage_unwaited` ([`UNWAITED`] where it does not say).
    pub(crate) fn keep_unwaited(&self, most: usize) {
        self.waiting.locked().known.keep(most);
    }

    /// `seen` waited on by `waiter` for at most `bound`, each count not
    /// kept read by `stands` — from whichever database keeps it: every one
    /// that stands elsewhere than `seen` says, as it stands — at once where
    /// one does as the wait begins — or none where no write moved one
    /// within `bound`, or `waiter` was stopped.
    ///
    /// # Errors
    ///
    /// Where a count cannot be read, or the wait is refused
    /// ([`Watch::refuse`]).
    pub(crate) fn wait(
        &self,
        stands: &dyn Fn(Watched) -> Result<u64, PersistError>,
        (seen, bound): (&[Position], Duration),
        waiter: &Arc<Waiter>,
    ) -> Result<Vec<Position>, PersistError> {
        let until = Instant::now().checked_add(bound);
        waiter.begin();
        // Listed before any count is read, so a write between the two is
        // either read or told.
        let (_listening, unknown) = self.listening(seen, waiter)?;
        let read = unknown
            .into_iter()
            .map(|watched| Ok((watched, stands(watched)?)))
            .collect::<Result<Vec<_>, PersistError>>()?;
        let now = self.standing(seen, &read);
        waiter.waited(seen, now, until)
    }

    /// `waiter` listed for each of `seen` until what this returns is
    /// dropped, and those of `seen` none waited on before, to be read.
    fn listening<'a>(
        &'a self,
        seen: &[Position],
        waiter: &Arc<Waiter>,
    ) -> Result<(Listening<'a>, Vec<Watched>), PersistError> {
        let mut waiting = self.waiting.locked();
        if let Some(reason) = &waiting.refused {
            return Err(PersistError::Standby {
                reason: reason.clone(),
            });
        }
        let mut unknown = Vec::new();
        for position in seen {
            waiting
                .on
                .entry(position.watched)
                .or_default()
                .push(Arc::clone(waiter));
            if waiting.known.listed(position.watched) && !unknown.contains(&position.watched) {
                unknown.push(position.watched);
            }
        }
        let listening = Listening {
            watch: self,
            watched: seen.iter().map(|position| position.watched).collect(),
            waiter: Arc::clone(waiter),
        };
        Ok((listening, unknown))
    }

    /// Where each of `seen` stands as the wait begins: what its writer told
    /// since it was listed, else what the store `read`; one neither says —
    /// its count unreadable since — as `seen` says, found at the bound.
    fn standing(&self, seen: &[Position], read: &[(Watched, u64)]) -> Vec<Position> {
        let mut waiting = self.waiting.locked();
        for &(watched, at) in read {
            // A write told since the read is later than it; one the read
            // saw untold yet is told to this waiter, listed before it read.
            waiting.known.read(watched, at);
        }
        seen.iter()
            .map(|seen| Position {
                watched: seen.watched,
                at: waiting.known.at(seen.watched).unwrap_or(seen.at),
            })
            .collect()
    }
}

/// A waiter listed for what it waits on, taken off as it is dropped.
struct Listening<'a> {
    watch: &'a Watch,
    watched: Vec<Watched>,
    waiter: Arc<Waiter>,
}

impl Drop for Listening<'_> {
    fn drop(&mut self) {
        let mut waiting = self.watch.waiting.locked();
        for watched in &self.watched {
            if let Some(waiters) = waiting.on.get_mut(watched) {
                if let Some(at) = waiters.iter().position(|w| Arc::ptr_eq(w, &self.waiter)) {
                    waiters.swap_remove(at);
                }
                if waiters.is_empty() {
                    waiting.on.remove(watched);
                    waiting.known.unwaited(*watched);
                }
            }
        }
    }
}

/// Where `watched` stands in `store`, the runtime database, for what it
/// keeps; what another database keeps is read there (`Embedded::stands`).
///
/// # Errors
///
/// Where a count cannot be read, or the runtime database keeps none of it.
pub(crate) fn stands<R: Engine>(
    store: &EncryptedStore<R>,
    watched: Watched,
) -> Result<u64, PersistError> {
    let places = |queue| queue::places(store, &Batch::empty(), KINDS, queue);
    Ok(match watched {
        Watched::Queue(queue) => places(queue)?.next,
        Watched::Head(queue) => places(queue)?.taken(),
        Watched::Instance(id) => instance::read_instance(store, id)?.map_or(0, |found| found.turns),
        Watched::Key(key) => u64::from(instance::read_instance_key(store, key)?.is_some()),
        other => {
            return Err(super::record::malformed(format!(
                "the runtime database keeps no count of {other:?}"
            )));
        }
    })
}

/// What a wait answers: each of `moved`, a queue's tail with the first
/// `most` of what is due in it read from `store` at `now` on its clock.
pub(crate) fn answered<R: Engine>(
    store: &EncryptedStore<R>,
    moved: Vec<Position>,
    (most, now): (u32, i128),
) -> Vec<Moved> {
    moved
        .into_iter()
        .map(|position| {
            let due = match position.watched {
                Watched::Queue(queue) if most > 0 => {
                    due::read(store, (queue, DuePart::Due), ((0, 0), most), now).ok()
                }
                _ => None,
            };
            Moved { position, due }
        })
        .collect()
}

#[cfg(any(test, feature = "test-support"))]
impl Watch {
    /// How many wait on `watched` now.
    pub(crate) fn waiting_on(&self, watched: Watched) -> usize {
        self.waiting.locked().on.get(&watched).map_or(0, Vec::len)
    }

    /// How many counts it keeps, and of them how many none waits on now.
    pub(crate) fn kept(&self) -> (usize, usize) {
        self.waiting.locked().known.kept()
    }
}

#[cfg(test)]
mod tests;
