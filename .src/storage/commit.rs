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

use super::record::{AuditEntry, Claim, Form, HandOn, malformed, read_byte};
use crate::{EncryptedStore, Engine, PersistError, RecordChange};

/// The record kinds of the runtime database.
pub(crate) const CHUNK: &str = "chunk";
pub(crate) const MESSAGE: &str = "message";
pub(crate) const JOURNEY: &str = "journey";
pub(crate) const CLAIM: &str = "claim";
pub(crate) const AUDIT: &str = "audit";
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
        claim: Claim,
        lease_nanos: i128,
    },
    Release(Claim),
    HandOn(Box<HandOn>),
    Audit(AuditEntry),
    /// The keeper moved audit record `sequence`: forget it here, where the
    /// keeper has not moved past it already.
    Kept(u64),
}

/// What a write decided.
pub(crate) enum Done {
    Written,
    Claim(Option<Claim>),
    Yes(bool),
}

type Reply = SyncSender<Result<Done, PersistError>>;

/// The writer's thread, and the way to it.
pub(crate) struct Committer {
    work: Option<Sender<(Op, Reply)>>,
    thread: Option<JoinHandle<()>>,
}

impl Committer {
    /// The writer over `store`, telling time by `clock`.
    ///
    /// # Errors
    ///
    /// When the audit sequence cannot be read, or the thread not started.
    pub(crate) fn start<R: Engine + 'static>(
        store: Arc<EncryptedStore<R>>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, PersistError> {
        let next = sequence(&store, NEXT)?;
        let (work, inbox) = mpsc::channel();
        let thread = std::thread::Builder::new()
            .name("xmip-storage-commit".to_string())
            .spawn(move || {
                let mut writer = Writer { store, clock, next };
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

/// A claim as stored: the last claim on its Journey, and whether it was
/// given back. A released claim keeps its token, so a hand-on repeated
/// after a lost answer is known for what it is.
struct Stored {
    claim: Claim,
    released: bool,
}

impl Stored {
    fn bytes(&self) -> Vec<u8> {
        let mut out = vec![u8::from(self.released)];
        self.claim.write(&mut out);
        out
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, PersistError> {
        let mut cursor = codec::cursor::Cursor::new(bytes);
        let released = read_byte(&mut cursor)? != 0;
        let claim = Claim::read(&mut cursor)?;
        Ok(Self { claim, released })
    }

    fn held_by(&self, claim: &Claim) -> bool {
        !self.released && self.claim.token == claim.token
    }
}

struct Writer<R> {
    store: Arc<EncryptedStore<R>>,
    clock: Arc<dyn Clock>,
    next: u64,
}

/// One batch being decided: what it has written so far, by place.
struct Batch {
    written: HashMap<(&'static str, Vec<u8>), Option<Vec<u8>>>,
    changes: Vec<RecordChange<'static>>,
}

impl Batch {
    fn put(&mut self, kind: &'static str, key: Vec<u8>, value: Option<Vec<u8>>) {
        self.written.insert((kind, key.clone()), value.clone());
        self.changes.push((kind, key, value));
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
            written: HashMap::new(),
            changes: Vec::new(),
        };
        let decided: Vec<(Reply, Result<Done, PersistError>)> = group
            .into_iter()
            .map(|(op, reply)| (reply, self.decide(op, now, &mut batch)))
            .collect();
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
            self.store.apply(&batch.changes)
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

    /// The value `kind` `key` holds, as this batch has left it.
    fn read(
        &self,
        batch: &Batch,
        kind: &'static str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, PersistError> {
        match batch.written.get(&(kind, key.to_vec())) {
            Some(value) => Ok(value.clone()),
            None => self.store.get(kind, key),
        }
    }

    fn stored(&self, batch: &Batch, claim: &Claim) -> Result<Option<Stored>, PersistError> {
        self.read(batch, CLAIM, &key(claim))?
            .map(|bytes| Stored::from_bytes(&bytes))
            .transpose()
    }

    fn decide(&mut self, op: Op, now: i128, batch: &mut Batch) -> Result<Done, PersistError> {
        match op {
            Op::Put(kind, key, value) => {
                batch.put(kind, key, Some(value));
                Ok(Done::Written)
            }
            Op::Claim { claim, lease_nanos } => {
                let stored = self.stored(batch, &claim)?;
                if let Some(stored) = &stored
                    && !stored.released
                    && stored.claim.until_unix_nanos >= now
                {
                    let ours = stored.claim.token == claim.token;
                    return Ok(Done::Claim(ours.then(|| stored.claim.clone())));
                }
                Ok(Done::Claim(Some(hold(batch, claim, now + lease_nanos))))
            }
            Op::Renew { claim, lease_nanos } => match self.stored(batch, &claim)? {
                Some(stored) if stored.held_by(&claim) => Ok(Done::Claim(Some(hold(
                    batch,
                    stored.claim,
                    now + lease_nanos,
                )))),
                _ => Ok(Done::Claim(None)),
            },
            Op::Release(claim) => match self.stored(batch, &claim)? {
                Some(stored) if stored.held_by(&claim) => {
                    give_back(batch, stored.claim);
                    Ok(Done::Yes(true))
                }
                _ => Ok(Done::Yes(false)),
            },
            Op::HandOn(hand_on) => self.hand_on(&hand_on, batch),
            Op::Audit(entry) => {
                let number = self.next;
                self.next += 1;
                batch.put(AUDIT, number.to_be_bytes().to_vec(), Some(entry.bytes()));
                Ok(Done::Written)
            }
            Op::Kept(number) => {
                let kept = match self.read(batch, SEQUENCE, KEPT)? {
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

    fn hand_on(&self, hand_on: &HandOn, batch: &mut Batch) -> Result<Done, PersistError> {
        let Some(stored) = self.stored(batch, &hand_on.claim)? else {
            return Ok(Done::Yes(false));
        };
        if stored.released {
            // Handed on already under this token: an answer that was lost.
            return Ok(Done::Yes(stored.claim.token == hand_on.claim.token));
        }
        if stored.claim.token != hand_on.claim.token {
            return Ok(Done::Yes(false));
        }
        let result = &hand_on.result;
        batch.put(
            JOURNEY,
            result.journey.value().to_be_bytes().to_vec(),
            Some(result.bytes()),
        );
        for message in &hand_on.messages {
            let key = message.message.value().to_be_bytes().to_vec();
            batch.put(MESSAGE, key, Some(message.bytes()));
        }
        for next in &hand_on.next {
            batch.put(
                JOURNEY,
                next.journey.value().to_be_bytes().to_vec(),
                Some(next.bytes()),
            );
        }
        give_back(batch, stored.claim);
        Ok(Done::Yes(true))
    }
}

/// `claim` held until `until`, written in `batch`.
fn hold(batch: &mut Batch, mut claim: Claim, until: i128) -> Claim {
    claim.until_unix_nanos = until;
    let stored = Stored {
        claim: claim.clone(),
        released: false,
    };
    batch.put(CLAIM, key(&claim), Some(stored.bytes()));
    claim
}

/// `claim` given back, its token kept, written in `batch`.
fn give_back(batch: &mut Batch, claim: Claim) {
    let key = key(&claim);
    let stored = Stored {
        claim,
        released: true,
    };
    batch.put(CLAIM, key, Some(stored.bytes()));
}

/// Where a Journey's claim is kept: under the Journey's identifier.
fn key(claim: &Claim) -> Vec<u8> {
    claim.journey.value().to_be_bytes().to_vec()
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
