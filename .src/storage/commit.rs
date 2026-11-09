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
//! it found. **A failed sync stops the Storage node** (`EncryptedStore::sync`),
//! as a batch that panics once in the engine does: nothing is read or
//! written again until the node is opened anew over its log (fail-stop).

mod batch;
mod syncer;
mod waiting;

use batch::again;
pub(crate) use batch::{Batch, sequence};

use syncer::Syncer;
use waiting::{Answer, Decides, Reply, Waiting};

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use codec::form::Form;
use std::time::Duration;
use xcore::Clock;
use xcore::spent::Spent;

use super::audit_entry::AuditEntry;
use super::claim::{self, Standing, Stored, end, hold};
use super::columns::Columns;
use super::dead::{self, Replay, Replayed};
use super::durable::Durable;
use super::hand_on::HandOn;
use super::publication::{self, Decided, Publication};
use super::record::{Claim, malformed};
use super::row;
use crate::{EncryptedStore, Engine, PersistError};

/// The record kinds of the runtime database.
pub(crate) const CHUNK: &str = "chunk";
/// A Stream's own record (`super::stream`).
pub(crate) const STREAM: &str = "stream";
pub(crate) const MESSAGE: &str = "message";
pub(crate) const JOURNEY: &str = "journey";
pub(crate) const CLAIM: &str = "claim";
pub(crate) const AUDIT: &str = "audit";
pub(crate) const HELD: &str = "held";
/// A held Message's place, found by its queue and its Journey.
pub(crate) const HELD_JOURNEY: &str = "held-journey";
pub(crate) const PLACES: &str = "held-places";
/// Where a queue's first entries by when they are due are (`super::due`).
pub(crate) const DUE_PLACES: &str = "held-due-places";
/// A node's Dead Message Queue: its entries, each one's place by its
/// Message, and its places.
pub(crate) const DEAD: &str = "dead-message";
pub(crate) const DEAD_MESSAGE: &str = "dead-message-by-message";
pub(crate) const DEAD_PLACES: &str = "dead-message-places";
/// That a Publication was written, by its Message: the digest of what was
/// asked (`super::publication`).
pub(crate) const PUBLICATION: &str = "publication";
pub(crate) const SEQUENCE: &str = "audit-sequence";
/// The sequence's two places: the next audit record's number, and the
/// first the keeper has not moved.
pub(crate) const NEXT: &[u8] = b"next";
pub(crate) const KEPT: &[u8] = b"kept";

/// The most writes one batch takes.
const GROUP: usize = 1024;

/// One write the committer decides.
pub(crate) enum Op {
    /// A record as it is, replacing the last.
    Put(&'static str, Vec<u8>, Vec<u8>),
    Claim {
        claim: Claim,
        lease_nanos: i128,
    },
    Renew {
        claims: Vec<Claim>,
        lease_nanos: i128,
    },
    Release(Claim),
    HandOn(Box<HandOn>),
    /// A Work Process's turn: its hand-on with its Instances (`super::instance`).
    Turn(Box<super::instance::Turn>),
    /// A Message, its Journeys, the ones held, its Dead Message Queue entry
    /// and its audit record, in one batch, once by its Message.
    Publish(Box<Publication>),
    /// An entry's Journeys written and the entry taken out, audited.
    Replay(Box<Replay>),
    Audit(Box<AuditEntry>),
    /// The keeper moved audit record `sequence`: forget it here, where the
    /// keeper has not moved past it already.
    Kept(u64),
}

/// What a write decided.
pub(crate) enum Done {
    Written,
    Claim(Option<Claim>),
    /// The claims a Publication's node holds of those it asked for.
    Claims(Vec<Claim>),
    Yes(bool),
    Replayed(Replayed),
}

/// The one writer, and the writes waiting for its next batch.
pub(crate) struct Committer {
    waiting: Waiting,
}

impl Committer {
    /// The writer over `store`, telling time by `clock`, keeping `columns`,
    /// fencing the claims it takes by the Storage `epoch`.
    ///
    /// # Errors
    ///
    /// When the audit sequence cannot be read, or the sync thread not
    /// started.
    pub(crate) fn start<R: Engine + 'static>(
        store: Arc<EncryptedStore<R>>,
        (clock, columns, epoch): (Arc<dyn Clock>, Arc<Columns>, Arc<AtomicU64>),
    ) -> Result<Self, PersistError> {
        let next = sequence(&store, NEXT)?;
        let syncer = Syncer::start(Arc::clone(&store))?;
        let writer = Writer {
            store,
            clock,
            next,
            columns,
            syncer,
            epoch,
            last: Durable::kept(),
            under_way: None,
        };
        Ok(Self {
            waiting: Waiting::new(Box::new(writer)),
        })
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
    /// The latest batch handed to the sync: what a batch that writes
    /// nothing is durable with.
    last: Durable,
    /// The batch in the engine and not yet handed to the sync.
    under_way: Option<Durable>,
}

impl<R: Engine> Decides for Writer<R> {
    fn commit(&mut self, group: Vec<(Op, Reply)>) {
        self.committed(group);
    }

    fn recover(&mut self) {
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
}

impl<R: Engine> Writer<R> {
    fn committed(&mut self, group: Vec<(Op, Reply)>) {
        let (spent, writes) = (
            Spent::begin(),
            u32::try_from(group.len()).unwrap_or(u32::MAX),
        );
        let entered = spent.entered();
        let now = self.clock.unix_timestamp_nanos();
        let before = self.next;
        let mut batch = Batch::empty();
        let mut decided = Vec::with_capacity(group.len());
        for (op, reply) in group {
            // Each operation decided on a stage of its own: all of it
            // taken into the batch, or none of it.
            let next = self.next;
            let mut staged = batch.staged();
            let done = self.decide(op, now, &mut staged);
            let Batch {
                written, changes, ..
            } = staged;
            if done.is_ok() {
                batch.written.extend(written);
                batch.changes.extend(changes);
            } else {
                self.next = next;
            }
            decided.push((reply, done));
        }
        // Each queue's first due found again, as the batch leaves it.
        let settled = super::due::settle(&self.store, &mut batch);
        if self.next != before {
            batch.put(
                SEQUENCE,
                NEXT.to_vec(),
                Some(self.next.to_be_bytes().to_vec()),
            );
        }
        let empty = batch.changes.is_empty();
        // Nothing of its own to sync: durable once the latest batch is.
        let durable = if empty {
            self.last.clone()
        } else {
            Durable::pending()
        };
        let written = if empty {
            Ok(())
        } else if let Err(failed) = settled {
            Err(failed)
        } else {
            // From here on the batch may be in the engine, whatever stops it.
            self.under_way = Some(durable.clone());
            // Each searchable record stamped on this clock, and its index
            // entries in the same write (`super::columns`), unsynced: it
            // is answered first, and synced after. Each record a change
            // replaces is the one the batch read of it, if it did.
            let changes = std::mem::take(&mut batch.changes);
            let kept = |kind, key: &[u8]| batch.before(&self.store, kind, key);
            self.columns
                .written_over(changes, row::nanos(now), kept)
                .and_then(|(changes, entries)| {
                    self.store.apply_indexed_deferred(&changes, &entries)
                })
        };
        if written.is_err() {
            // Refused whole: nothing of it is in the engine.
            self.next = before;
            self.under_way = None;
        }
        // What the batch took outside its I/O, shared by its writes.
        drop(entered);
        if !empty {
            durable.worked_by(spent.own(Duration::ZERO), writes);
        }
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
            Op::HandOn(hand_on) => claim::hand_on(&self.store, batch, &hand_on, now).map(Done::Yes),
            Op::Turn(turn) => super::instance::turn(&self.store, batch, &turn, now).map(Done::Yes),
            Op::Publish(asked) => match publication::decide(
                &self.store,
                batch,
                &asked,
                (now, self.epoch.load(Ordering::Acquire)),
            )? {
                Decided::Before(held) => Ok(Done::Claims(held)),
                Decided::Now(held) => self
                    .decide(Op::Audit(Box::new(asked.audit)), now, batch)
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
