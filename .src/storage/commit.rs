//! The embedded Storage node's one writer to its runtime database: every
//! write goes through it, and every write waiting when it is free goes in
//! the same batch, under one sync — group commit (`deployment-model.md`
//! section 7: *once it is synced to disk; group commit shares one sync
//! among concurrent writes*).
//!
//! One writer is also what makes a claim a conditional update on one
//! node: each condition is read and its write decided in order, against
//! what the batch has written so far, so two claimants of one Journey in
//! one batch are decided one after the other, and the first wins (the
//! owner, 2026-10-01: *All DB records have to be central and clusterable,
//! if one node fails another one should be able to pick up*;
//! `deployment-model.md` section 7, *A claim … the same condition, in
//! `RocksDB` on the one node*).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender, SyncSender};
use std::thread::JoinHandle;

use xcore::Clock;

use super::claim::{self, Standing, Stored, end, hold};
use super::columns::Columns;
use super::dead::{self, Replay, Replayed};
use super::hand_on::HandOn;
use super::publication::{self, Decided, Publication};
use super::record::{AuditEntry, Claim, Form, malformed};
use super::row;
use crate::{EncryptedStore, Engine, PersistError, RecordChange};

/// The record kinds of the runtime database.
pub(crate) const CHUNK: &str = "chunk";
pub(crate) const MESSAGE: &str = "message";
pub(crate) const JOURNEY: &str = "journey";
pub(crate) const CLAIM: &str = "claim";
pub(crate) const AUDIT: &str = "audit";
pub(crate) const HELD: &str = "held";
/// A held Journey's place, found by its queue and its Journey.
pub(crate) const HELD_JOURNEY: &str = "held-journey";
pub(crate) const PLACES: &str = "held-places";
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

type Reply = SyncSender<Result<Done, PersistError>>;

/// The writer's thread, and the way to it.
pub(crate) struct Committer {
    work: Option<Sender<(Op, Reply)>>,
    thread: Option<JoinHandle<()>>,
}

impl Committer {
    /// The writer over `store`, telling time by `clock`, keeping `columns`.
    ///
    /// # Errors
    ///
    /// When the audit sequence cannot be read, or the thread not started.
    pub(crate) fn start<R: Engine + 'static>(
        store: Arc<EncryptedStore<R>>,
        clock: Arc<dyn Clock>,
        columns: Arc<Columns>,
    ) -> Result<Self, PersistError> {
        let next = sequence(&store, NEXT)?;
        let (work, inbox) = mpsc::channel();
        let thread = std::thread::Builder::new()
            .name("xmip-storage-commit".to_string())
            .spawn(move || {
                let mut writer = Writer {
                    store,
                    clock,
                    next,
                    columns,
                };
                writer.run(&inbox);
            })
            .map_err(|error| PersistError::engine("xmip-storage", error))?;
        Ok(Self {
            work: Some(work),
            thread: Some(thread),
        })
    }

    /// `op` decided and written, durably, or refused.
    ///
    /// # Errors
    ///
    /// The write's own, or the writer having stopped.
    pub(crate) fn submit(&self, op: Op) -> Result<Done, PersistError> {
        let (reply, answer) = mpsc::sync_channel(1);
        let stopped = || PersistError::engine("xmip-storage", "the writer has stopped");
        self.work
            .as_ref()
            .ok_or_else(stopped)?
            .send((op, reply))
            .map_err(|_| stopped())?;
        answer.recv().map_err(|_| stopped())?
    }
}

impl Drop for Committer {
    fn drop(&mut self) {
        self.work = None;
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// A sequence number as kept, zero where none is.
pub(crate) fn sequence<R: Engine>(
    store: &EncryptedStore<R>,
    which: &[u8],
) -> Result<u64, PersistError> {
    let Some(bytes) = store.get(SEQUENCE, which)? else {
        return Ok(0);
    };
    let array: [u8; 8] = bytes
        .try_into()
        .map_err(|_| malformed("an audit sequence that is not eight bytes"))?;
    Ok(u64::from_be_bytes(array))
}

struct Writer<R> {
    store: Arc<EncryptedStore<R>>,
    clock: Arc<dyn Clock>,
    next: u64,
    columns: Arc<Columns>,
}

/// One batch being decided: what it has written so far, by place — or one
/// operation's part of it, staged over the batch below, which takes it only
/// once the whole operation has been decided. An operation that fails
/// half-way is dropped with its stage, so nothing of it is written, while
/// the others in its batch are.
pub(crate) struct Batch<'a> {
    below: Option<&'a Batch<'a>>,
    written: HashMap<(&'static str, Vec<u8>), Option<Vec<u8>>>,
    changes: Vec<RecordChange<'static>>,
}

impl Batch<'_> {
    /// One operation's stage over this batch.
    fn staged(&self) -> Batch<'_> {
        Batch {
            below: Some(self),
            written: HashMap::new(),
            changes: Vec::new(),
        }
    }

    /// The value `kind` `key` holds in `store`, as this batch, and every
    /// batch below it, has left it.
    pub(crate) fn read<R: Engine>(
        &self,
        store: &EncryptedStore<R>,
        kind: &'static str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, PersistError> {
        match (self.written.get(&(kind, key.to_vec())), self.below) {
            (Some(value), _) => Ok(value.clone()),
            (None, Some(below)) => below.read(store, kind, key),
            (None, None) => store.get(kind, key),
        }
    }

    pub(crate) fn put(&mut self, kind: &'static str, key: Vec<u8>, value: Option<Vec<u8>>) {
        self.written.insert((kind, key.clone()), value.clone());
        self.changes.push((kind, key, value));
    }

    /// `record` under its identifier `id`, replacing the last.
    pub(crate) fn record(&mut self, kind: &'static str, id: u128, record: &impl Form) {
        self.put(kind, id.to_be_bytes().to_vec(), Some(record.bytes()));
    }
}

impl<R: Engine> Writer<R> {
    fn run(&mut self, inbox: &Receiver<(Op, Reply)>) {
        while let Ok(first) = inbox.recv() {
            let mut group = vec![first];
            while group.len() < GROUP {
                match inbox.try_recv() {
                    Ok(more) => group.push(more),
                    Err(_) => break,
                }
            }
            self.commit(group);
        }
    }

    fn commit(&mut self, group: Vec<(Op, Reply)>) {
        let now = self.clock.unix_timestamp_nanos();
        let before = self.next;
        let mut batch = Batch {
            below: None,
            written: HashMap::new(),
            changes: Vec::new(),
        };
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
        if self.next != before {
            batch.put(
                SEQUENCE,
                NEXT.to_vec(),
                Some(self.next.to_be_bytes().to_vec()),
            );
        }
        let written = if batch.changes.is_empty() {
            Ok(())
        } else {
            // Each searchable record stamped on this clock, and its index
            // entries in the same write (`super::columns`).
            self.columns
                .written(&self.store, batch.changes, row::nanos(now))
                .and_then(|(changes, entries)| self.store.apply_indexed(&changes, &entries))
        };
        if written.is_err() {
            self.next = before;
        }
        for (reply, done) in decided {
            let answer = match (&written, done) {
                (Ok(()), done) => done,
                (Err(error), _) => Err(again(error)),
            };
            let _ = reply.send(answer);
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
                claim::take(&self.store, batch, claim, (now, until)).map(Done::Claim)
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
            Op::Publish(asked) => match publication::decide(&self.store, batch, &asked, now)? {
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

/// The failure of a batch, said again to each write that was in it.
fn again(error: &PersistError) -> PersistError {
    match error {
        PersistError::Refused { scope, reason } => PersistError::Refused {
            scope: scope.clone(),
            reason: reason.clone(),
        },
        PersistError::Engine { engine, reason } => PersistError::Engine {
            engine,
            reason: reason.clone(),
        },
        other => malformed(other.to_string()),
    }
}
