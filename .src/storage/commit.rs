//! The embedded Storage node's one writer to its runtime database: every
//! write goes through it, and every write waiting when it is free goes in
//! the same batch, under one sync — group commit (`deployment-model.md`
//! section 7: *once it is synced to disk; group commit shares one sync
//! among concurrent writes*). A batch is decided on the thread of the
//! caller that leads it, never handed to a thread of its own
//! ([`waiting`]; ADR-0018, amendment 2026-11-01).
//!
//! One writer is also what makes a claim a conditional update on one
//! node: each condition is read and its write decided in order, against
//! what the batch has written so far, so two claimants of one Message in
//! one batch are decided one after the other, and the first wins (the
//! owner, 2026-10-01: *All DB records have to be central and clusterable,
//! if one node fails another one should be able to pick up*;
//! `deployment-model.md` section 7, *A claim … the same condition, in
//! `RocksDB` on the one node*).
//!
//! **A batch is answered once written, and synced after.** Each write is
//! answered with the batch's [`Durable`] as soon as the batch is in the
//! engine — decided and readable — and the batch is synced then, on a
//! thread of its own ([`syncer`]), while the next is decided: a
//! caller waiting for its write to be durable waits on it
//! ([`Committer::submit`]), and a Publication's send is prepared meanwhile
//! ([`Committer::ahead`]; ADR-0018, amendments 2026-10-20 and 2026-10-21).
//! A batch that writes nothing — a request asked again, found written —
//! is durable with the latest batch handed to the sync, which holds what
//! it found, and says what it worked itself (`Durable::beside`;
//! 2026-11-15). Each sequence's head a batch moves is known with the
//! batch's [`Durable`] before the batch is readable ([`moving`]), so a read
//! of the head says when what it read is durable (`XmipStorage::read_head`;
//! 2026-11-15). **A failed sync stops the Storage node** (`EncryptedStore::sync`),
//! as a batch that panics once in the engine does: nothing is read or
//! written again until the node is opened anew over its log (fail-stop).

mod batch;
mod kept;
mod kinds;
mod moving;
mod op;
mod syncer;
mod waiting;

use batch::again;
pub(crate) use batch::{Batch, sequence};
pub(crate) use kinds::{
    AUDIT, CHUNK, CLAIM, DEAD, DEAD_MESSAGE, DEAD_PLACES, DUE_PLACES, HELD, HELD_JOURNEY, JOURNEY,
    KEPT, MESSAGE, NEXT, PLACES, PUBLICATION, SEQUENCE, STREAM,
};
pub(crate) use op::{Done, Op};

use kept::Kept;
use moving::Moving;
use syncer::Syncer;
use waiting::{Answer, Decides, Reply, Waiting};

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use codec::form::Form;
use std::time::Duration;
use xcore::Clock;
use xcore::spent::Spent;

use super::claim::{self, Standing, Stored, end, hold};
use super::columns::Columns;
use super::dead::{self, Replayed};
use super::durable::Durable;
use super::publication::{self, Decided};
use super::record::{Claim, malformed};
use super::row;
use super::watch::{self, Watch};
use crate::{EncryptedStore, Engine, PersistError, RecordChange};

/// The most writes one batch takes.
const GROUP: usize = 1024;

/// The one writer, and the writes waiting for its next batch.
pub(crate) struct Committer {
    waiting: Waiting,
    /// Each sequence's head a batch under way moved, with when it is
    /// durable, shared with the writer.
    moving: Arc<Moving>,
}

impl Committer {
    /// The writer over `store`, telling time by `clock`, keeping `columns`,
    /// fencing the claims it takes by the Storage `epoch`, telling `watch`
    /// of every queue a batch moves, and keeping at most `most` bytes of
    /// the records it wrote ([`kept`]).
    ///
    /// # Errors
    ///
    /// When the audit sequence cannot be read, or the sync thread not
    /// started.
    pub(crate) fn start<R: Engine + 'static>(
        store: Arc<EncryptedStore<R>>,
        told: (Arc<dyn Clock>, Arc<Columns>, Arc<AtomicU64>, Arc<Watch>),
        most: Arc<AtomicUsize>,
    ) -> Result<Self, PersistError> {
        let moving = Arc::new(Moving::default());
        let writer = Writer::start(store, told, (most, Arc::clone(&moving)))?;
        Ok(Self {
            waiting: Waiting::new(Box::new(writer)),
            moving,
        })
    }

    /// When the write that last moved the head of the sequence `queue` is
    /// durable, where a batch under way moved it: none where none did.
    pub(crate) fn moving(&self, queue: u128) -> Option<Durable> {
        self.moving.head(queue)
    }

    /// `op` decided and written, durably, or refused.
    ///
    /// # Errors
    ///
    /// The write's own, its sync's, or the writer having stopped.
    pub(crate) fn submit(&self, op: Op) -> Result<Done, PersistError> {
        let (done, durable) = self.ahead(op)?;
        durable.wait()?;
        Ok(done)
    }

    /// `op` decided and written, or refused, answered before its batch is
    /// synced: what it decided, and when it is durable.
    ///
    /// # Errors
    ///
    /// The write's own, or the batch it was in having failed.
    pub(crate) fn ahead(&self, op: Op) -> Result<(Done, Durable), PersistError> {
        self.waiting.decided(op)
    }

    /// Each record its writer keeps of what it wrote, by its place, as it
    /// keeps it: what a test holds against the store ([`kept`]).
    #[cfg(test)]
    pub(crate) fn kept(&self) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        self.waiting.kept()
    }
}

struct Writer<R> {
    store: Arc<EncryptedStore<R>>,
    clock: Arc<dyn Clock>,
    next: u64,
    columns: Arc<Columns>,
    /// Where each batch written is synced, while the next is decided; a
    /// batch handed it is synced before the writer is let go of.
    syncer: Syncer,
    /// The Storage epoch its node is the primary in: a claim's fence.
    epoch: Arc<AtomicU64>,
    /// Who waits on which queue (`super::watch`).
    watch: Arc<Watch>,
    /// The records it wrote, as it left them ([`kept`]); lent to each
    /// batch while it is decided.
    kept: Kept,
    /// The most bytes of them it keeps, as its Storage node says now.
    most: Arc<AtomicUsize>,
    /// The latest batch handed to the sync: what a batch that writes
    /// nothing is durable with.
    last: Durable,
    /// The batch in the engine and not yet handed to the sync.
    under_way: Option<Durable>,
    /// Each sequence's head a batch under way moved ([`moving`]).
    moving: Arc<Moving>,
}

impl<R: Engine> Decides for Writer<R> {
    fn commit(&mut self, group: Vec<(Op, Reply)>) {
        self.committed(group);
    }

    fn recover(&mut self) {
        // What it kept is read again; the batch it panicked in held it.
        self.kept.forget();
        // Written, and answered to some of its callers or none: nothing
        // tells what any was told, so the node stops (fail-stop).
        if let Some(durable) = self.under_way.take() {
            let why = "the writer panicked after a batch was written";
            durable.tell(Err(why.to_string()));
            self.store.stop(why.to_string());
            return;
        }
        if let Ok(next) = sequence(&self.store, NEXT) {
            self.next = next;
        }
    }

    #[cfg(test)]
    fn kept(&self) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        let each = self.kept.each();
        each.map(|(at, value)| (at.to_vec(), value.clone()))
            .collect()
    }
}

impl<R: Engine> Writer<R> {
    /// The writer over `store`, as [`Committer::start`] starts it, keeping
    /// nothing yet, telling `moving` of each head a batch moves.
    fn start(
        store: Arc<EncryptedStore<R>>,
        (clock, columns, epoch, watch): (Arc<dyn Clock>, Arc<Columns>, Arc<AtomicU64>, Arc<Watch>),
        (most, moving): (Arc<AtomicUsize>, Arc<Moving>),
    ) -> Result<Self, PersistError>
    where
        R: 'static,
    {
        Ok(Self {
            next: sequence(&store, NEXT)?,
            syncer: Syncer::start(Arc::clone(&store))?,
            store,
            clock,
            columns,
            epoch,
            watch,
            kept: Kept::default(),
            most,
            last: Durable::kept(),
            under_way: None,
            moving,
        })
    }

    fn committed(&mut self, group: Vec<(Op, Reply)>) {
        let (spent, writes) = (
            Spent::begin(),
            u32::try_from(group.len()).unwrap_or(u32::MAX),
        );
        let entered = spent.entered();
        let now = self.clock.unix_timestamp_nanos();
        let before = self.next;
        // What it keeps lent to the batch: a batch that panics takes it.
        let mut batch = Batch::over(row::nanos(now), std::mem::take(&mut self.kept));
        let mut decided = Vec::with_capacity(group.len());
        for (op, reply) in group {
            // Each operation decided on a stage of its own: all of it
            // taken into the batch, or none of it.
            let next = self.next;
            let mut staged = batch.staged();
            let done = self.decide(op, now, &mut staged);
            let Batch {
                written,
                changes,
                laid,
                ..
            } = staged;
            if done.is_ok() {
                batch.taken((written, changes, laid));
            } else {
                self.next = next;
            }
            decided.push((reply, done));
        }
        // Each queue's first due found again, as the batch leaves it.
        let settled = super::due::settle(&self.store, &mut batch);
        // Each queue it puts a Message in, as it found them.
        let moved = watch::moved(&self.store, &batch);
        if self.next != before {
            batch.put(
                SEQUENCE,
                NEXT.to_vec(),
                Some(self.next.to_be_bytes().to_vec()),
            );
        }
        let empty = batch.changes.is_empty();
        // Nothing of its own to sync: durable once the latest batch is,
        // its figures its own.
        let durable = if empty {
            self.last.beside()
        } else {
            Durable::pending()
        };
        let written = match settled {
            _ if empty => Ok(Vec::new()),
            Err(failed) => Err(failed),
            Ok(()) => {
                // Each head it moves known before it is readable: a read
                // that finds one moved finds this batch (`moving`).
                self.moving.entered(&moved, &durable);
                self.written(&mut batch, now, &durable)
            }
        };
        self.kept = batch.lent();
        let written = match written {
            Ok(changes) => {
                // In the engine: what it wrote kept as it leaves it.
                self.kept
                    .written(changes, self.most.load(Ordering::Relaxed));
                // Readable now: every node waiting on a queue it moved woken.
                self.watch.moved(&moved);
                Ok(())
            }
            Err(refused) => {
                // Refused whole: nothing of it is in the engine, and no
                // head it was to move waits for it.
                self.moving.refused(&durable);
                self.next = before;
                self.under_way = None;
                Err(refused)
            }
        };
        // What the batch took outside its I/O, shared by its writes.
        drop(entered);
        durable.worked_by(spent.own(Duration::ZERO), writes);
        for (reply, done) in decided {
            let answer = match (&written, done) {
                (Ok(()), done) => done.map(|done| (done, durable.clone())),
                (Err(error), _) => Err(again(error)),
            };
            let _ = reply.send(Answer::Decided(answer));
        }
        if let Some(durable) = self.under_way.take() {
            self.last = durable.clone();
            self.syncer.written(durable);
        }
    }

    /// `batch`, decided at `now`, in the engine, unsynced, `durable` under
    /// way from here on, whatever stops it: each searchable record stamped
    /// on this clock, and its index entries in the same write
    /// (`super::columns`); it is answered first, and synced after. Each
    /// record a change replaces is the one the batch read of it, if it did,
    /// and one it made new is laid out as it was made. What it wrote, each
    /// record as the store holds it now.
    fn written(
        &mut self,
        batch: &mut Batch<'_>,
        now: i128,
        durable: &Durable,
    ) -> Result<Vec<RecordChange<'static>>, PersistError> {
        self.under_way = Some(durable.clone());
        let made = (
            std::mem::take(&mut batch.changes),
            std::mem::take(&mut batch.laid),
        );
        let kept = |kind, key: &[u8]| batch.before(&self.store, kind, key);
        let (changes, entries) = self.columns.written_over(made, row::nanos(now), kept)?;
        self.store.apply_indexed_deferred(&changes, &entries)?;
        Ok(changes)
    }

    fn stored(&self, batch: &Batch<'_>, claim: &Claim) -> Result<Option<Stored>, PersistError> {
        claim::stored(&self.store, batch, claim)
    }

    fn decide(&mut self, op: Op, now: i128, batch: &mut Batch<'_>) -> Result<Done, PersistError> {
        match op {
            Op::Put(kind, key, value) => {
                batch.put(kind, key, Some(value));
                Ok(Done::Written)
            }
            Op::Claim { claim, lease_nanos } => {
                let until = now.saturating_add(lease_nanos);
                claim::take(
                    &self.store,
                    batch,
                    claim,
                    (now, until, self.epoch.load(Ordering::Acquire)),
                )
                .map(Done::Claim)
            }
            Op::Renew {
                claims,
                lease_nanos,
            } => {
                let mut held = Vec::new();
                for claim in claims {
                    if let Some(stored) = self.stored(batch, &claim)?
                        && stored.held_by(&claim)
                    {
                        // A later deadline — a retry's backoff — is kept.
                        let until = now.saturating_add(lease_nanos);
                        let until = until.max(stored.claim.until_unix_nanos);
                        held.push(hold(batch, stored.claim, until));
                    }
                }
                Ok(Done::Claims(held))
            }
            Op::Release(claim) => match self.stored(batch, &claim)? {
                Some(stored) if stored.held_by(&claim) => {
                    end(batch, stored.claim, Standing::Released);
                    Ok(Done::Yes(true))
                }
                _ => Ok(Done::Yes(false)),
            },
            Op::HandOn(hand_on) => claim::hand_on(&self.store, batch, *hand_on, now).map(Done::Yes),
            Op::Turn(turn) => super::instance::turn(&self.store, batch, *turn, now).map(Done::Yes),
            Op::Publish(asked) => match publication::decide(
                &self.store,
                batch,
                *asked,
                (now, self.epoch.load(Ordering::Acquire)),
            )? {
                Decided::Before(held) => Ok(Done::Claims(held)),
                Decided::Now(held, audit) => self
                    .decide(Op::Audit(audit), now, batch)
                    .map(|_| Done::Claims(held)),
            },
            Op::Replay(replay) => match dead::replay(&self.store, batch, &replay)? {
                Replayed::Now => self
                    .decide(Op::Audit(Box::new(replay.audit)), now, batch)
                    .map(|_| Done::Replayed(Replayed::Now)),
                other => Ok(Done::Replayed(other)),
            },
            Op::Audit(entry) => {
                let number = self.next;
                self.next += 1;
                batch.put(AUDIT, number.to_be_bytes().to_vec(), Some(entry.bytes()));
                Ok(Done::Written)
            }
            Op::Kept(number) => {
                let kept = match batch.read(&self.store, SEQUENCE, KEPT)? {
                    Some(bytes) => u64::from_be_bytes(
                        bytes
                            .try_into()
                            .map_err(|_| malformed("an audit sequence that is not eight bytes"))?,
                    ),
                    None => 0,
                };
                if kept != number {
                    return Ok(Done::Yes(false));
                }
                batch.put(AUDIT, number.to_be_bytes().to_vec(), None);
                batch.put(
                    SEQUENCE,
                    KEPT.to_vec(),
                    Some((number + 1).to_be_bytes().to_vec()),
                );
                Ok(Done::Yes(true))
            }
        }
    }
}

#[cfg(test)]
mod tests;
