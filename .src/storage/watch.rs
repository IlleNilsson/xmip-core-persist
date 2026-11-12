//! A wait on queues, asked of Xmip Storage (open problem 46; ADR-0018,
//! amendment 2026-11-12): a node's send scan, and its Work Process step's,
//! waits on the queues it takes from, and is answered as soon as a
//! Publication, a hand-on, a turn or a Replay puts a Message in one of them
//! — on whichever node wrote it — instead of finding it at its next scan.
//!
//! **Asked with what was seen last.** Where a queue stands is the place its
//! next entry takes ([`Position`]), which only a write that puts an entry in
//! it moves. A wait names each queue with where its waiter last saw it stand,
//! and is answered with every one that stands elsewhere now: at once where
//! one already does, so a Message that lands between a scan and the wait
//! after it is never waited past. One no write moved within the wait's bound
//! is answered with none — empty, never an error.
//!
//! **Told by the one writer** ([`Watch`]): the embedded Storage node's
//! writer tells every waiter on a queue a batch moved, as soon as the batch
//! is in its engine and readable, each waiter by a wake of its own, so one
//! write wakes every node waiting on its queue and no other — and not the
//! node whose own claims in that write hold every Message it put there,
//! told quietly (`moved`). What a woken node finds is sent only under a
//! claim, and a claim is answered once its own batch is durable, which the
//! syncs of the batches before it come first in: no far end sees a Message
//! before the write that queued it is durable. The watch keeps where its
//! writer left each queue a node waited on, so a wait reads the store only
//! for a queue none waited on before; until 2026-11-12 every wait read
//! every queue it named, and one write cost the Storage node as many reads
//! as nodes waiting times their queues.
//!
//! **Stopped at once.** A [`Waiter`] stopped ends its wait in this process
//! at once, and every wait after. One asked over the wire is ended at its
//! Storage node, asked over another connection by what the client left with
//! the waiter ([`Waiter::ending`]): no node looks whether its waiter stopped.
//!
//! **Refused by a primary no more** ([`Watch::refuse`]): a Storage node that
//! steps down or stops ends every wait on its queues refused
//! ([`PersistError::Standby`]), so the node waiting asks the next Storage
//! node at once rather than wait out its bound on one no write reaches.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use codec::cursor::Cursor;
use codec::form::Form;
use codec::writer::ByteWriter;
use xcore::lock::Locked;

use super::commit::Batch;
use super::hold::KINDS;
use super::queue;
use crate::{EncryptedStore, Engine, PersistError};

mod moved;
mod waiter;

pub(crate) use moved::{Moved, moved};
pub(crate) use waiter::Ending;
pub use waiter::Waiter;

/// Where a queue stands: the place the next entry it takes is given, which
/// only a write that puts an entry in it moves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Position {
    /// The queue ([`super::named`]).
    pub queue: u128,
    /// The place its next entry takes.
    pub next: u64,
}

/// Who waits on which queue of one embedded Storage node now, and where its
/// one writer left each queue waited on.
#[derive(Debug, Default)]
pub(crate) struct Watch {
    waiting: Mutex<Waiting>,
}

#[derive(Debug, Default)]
struct Waiting {
    /// By queue, who waits on it now.
    on: HashMap<u128, Vec<Arc<Waiter>>>,
    /// By queue waited on since the watch opened, where its writer last
    /// left it or a wait read it, whichever is later; `None` while it is to
    /// be read.
    known: HashMap<u128, Option<u64>>,
    /// Why every wait is refused: its node is the primary no more.
    refused: Option<String>,
}

impl Watch {
    /// Each queue of `moved` where a batch left it, kept where a node waited
    /// on it, and every waiter on it told — quietly, the one whose holder's
    /// claims hold all the batch put there; one whose place could not be
    /// read (`None`) is read again by the next wait on it, and its waiters
    /// find it at their bound.
    pub(crate) fn moved(&self, moved: &[Moved]) {
        if moved.is_empty() {
            return;
        }
        let mut waiting = self.waiting.locked();
        for Moved { queue, next, own } in moved {
            if let Some(known) = waiting.known.get_mut(queue) {
                *known = next.map(|next| known.map_or(next, |was| was.max(next)));
            }
            let Some(next) = next else {
                continue;
            };
            for waiter in waiting.on.get(queue).into_iter().flatten() {
                waiter.tell(*queue, *next, own.as_deref());
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

    /// `seen` waited on by `waiter` in `store` for at most `bound`: every
    /// queue that stands elsewhere than `seen` says, as it stands — at once
    /// where one does as the wait begins — or none where no write moved one
    /// within `bound`, or `waiter` was stopped.
    ///
    /// # Errors
    ///
    /// Where a queue's place cannot be read, or the wait is refused
    /// ([`Watch::refuse`]).
    pub(crate) fn wait<R: Engine>(
        &self,
        store: &EncryptedStore<R>,
        (seen, bound): (&[Position], Duration),
        waiter: &Arc<Waiter>,
    ) -> Result<Vec<Position>, PersistError> {
        let until = Instant::now().checked_add(bound);
        waiter.begin();
        // Listed before any place is read, so a write between the two is
        // either read or told.
        let (_listening, unknown) = self.listening(seen, waiter)?;
        let read = unknown
            .into_iter()
            .map(|queue| Ok((queue, stands(store, queue)?)))
            .collect::<Result<Vec<_>, PersistError>>()?;
        let now = self.standing(seen, &read);
        waiter.waited(seen, now, until)
    }

    /// `waiter` listed for each queue of `seen` until what this returns is
    /// dropped, and the queues of `seen` none waited on before, to be read.
    fn listening<'a>(
        &'a self,
        seen: &[Position],
        waiter: &Arc<Waiter>,
    ) -> Result<(Listening<'a>, Vec<u128>), PersistError> {
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
                .entry(position.queue)
                .or_default()
                .push(Arc::clone(waiter));
            let known = waiting.known.entry(position.queue).or_insert(None);
            if known.is_none() && !unknown.contains(&position.queue) {
                unknown.push(position.queue);
            }
        }
        let listening = Listening {
            watch: self,
            queues: seen.iter().map(|position| position.queue).collect(),
            waiter: Arc::clone(waiter),
        };
        Ok((listening, unknown))
    }

    /// Where each queue of `seen` stands as the wait begins: what its writer
    /// last told, or the store `read` where that is later; one neither says
    /// — its place unreadable since — as `seen` says, found at the bound.
    fn standing(&self, seen: &[Position], read: &[(u128, u64)]) -> Vec<Position> {
        let mut waiting = self.waiting.locked();
        for &(queue, next) in read {
            let known = waiting.known.entry(queue).or_insert(None);
            *known = Some(known.map_or(next, |told| told.max(next)));
        }
        seen.iter()
            .map(|seen| Position {
                queue: seen.queue,
                next: waiting
                    .known
                    .get(&seen.queue)
                    .copied()
                    .flatten()
                    .unwrap_or(seen.next),
            })
            .collect()
    }
}

/// A waiter listed for its queues, taken off them as it is dropped.
struct Listening<'a> {
    watch: &'a Watch,
    queues: Vec<u128>,
    waiter: Arc<Waiter>,
}

impl Drop for Listening<'_> {
    fn drop(&mut self) {
        let mut waiting = self.watch.waiting.locked();
        for queue in &self.queues {
            if let Some(waiters) = waiting.on.get_mut(queue) {
                if let Some(at) = waiters.iter().position(|w| Arc::ptr_eq(w, &self.waiter)) {
                    waiters.swap_remove(at);
                }
                if waiters.is_empty() {
                    waiting.on.remove(queue);
                }
            }
        }
    }
}

/// Where `queue` stands in `store`.
fn stands<R: Engine>(store: &EncryptedStore<R>, queue: u128) -> Result<u64, PersistError> {
    Ok(queue::places(store, &Batch::empty(), KINDS, queue)?.next)
}

#[cfg(any(test, feature = "test-support"))]
impl Watch {
    /// How many wait on `queue` now.
    pub(crate) fn waiting_on(&self, queue: u128) -> usize {
        self.waiting.locked().on.get(&queue).map_or(0, Vec::len)
    }
}

impl Form for Position {
    fn write(&self, out: &mut Vec<u8>) {
        out.u128_be(self.queue);
        out.u64_be(self.next);
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        Ok(Self {
            queue: cursor.u128_be()?,
            next: cursor.u64_be()?,
        })
    }
}

#[cfg(test)]
mod tests;
