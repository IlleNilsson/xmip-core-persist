//! Xmip Storage as whatever a Storage node is now: each operation asked of
//! the storage its standing answers with, decided as it is asked — its own
//! databases while it is the primary; while it is a standby, a refusal to a
//! node asking it (`StorageNode::served`), or the primary, reached over the
//! wire, for its own work (`StorageNode::reaching`).

use std::sync::Arc;
use std::time::Duration;

use xcore::{AuditId, JourneyId, MessageId, StreamId};

use crate::PersistError;
use crate::storage::{
    AdministrationKind, AdministrationRecord, AuditEntry, Claim, DeadEntry, DeadQueue, DuePart,
    DueQueue, Durable, FailedQueue, HandOn, HeldQueue, InstanceRecord, JourneyRecord, KeptAudit,
    MessageRecord, Publication, Query, Replay, Replayed, StreamChunk, StreamRecord, Turn,
    XmipStorage,
};

/// What answers now, or why nothing does.
pub type Now = Box<dyn Fn() -> Result<Lent, PersistError> + Send + Sync>;

/// What answers one operation, and what is held while it does: a primary
/// stopping waits for every operation it lent itself to before it hands on
/// its last writes (`StorageNode::stop`).
pub struct Lent {
    pub storage: Arc<dyn XmipStorage>,
    pub held: Option<Box<dyn Send>>,
}

impl Lent {
    /// `storage`, holding nothing while it answers.
    #[must_use]
    pub fn free(storage: Arc<dyn XmipStorage>) -> Self {
        Self {
            storage,
            held: None,
        }
    }
}

/// Xmip Storage asked of what [`Now`] says answers, operation by operation.
pub struct Delegated(pub Now);

impl Delegated {
    fn now(&self) -> Result<Lent, PersistError> {
        (self.0)()
    }
}

impl XmipStorage for Delegated {
    fn write_chunk(&self, chunk: &StreamChunk) -> Result<(), PersistError> {
        self.now()?.storage.write_chunk(chunk)
    }

    fn read_chunk(&self, s: StreamId, index: u32) -> Result<Option<StreamChunk>, PersistError> {
        self.now()?.storage.read_chunk(s, index)
    }

    fn write_stream(&self, last: &StreamChunk, s: &StreamRecord) -> Result<(), PersistError> {
        self.now()?.storage.write_stream(last, s)
    }

    fn read_stream(&self, s: StreamId) -> Result<Option<StreamRecord>, PersistError> {
        self.now()?.storage.read_stream(s)
    }

    fn write_message(&self, message: &MessageRecord) -> Result<(), PersistError> {
        self.now()?.storage.write_message(message)
    }

    fn read_message(&self, message: MessageId) -> Result<Option<MessageRecord>, PersistError> {
        self.now()?.storage.read_message(message)
    }

    fn write_journey(&self, journey: &JourneyRecord) -> Result<(), PersistError> {
        self.now()?.storage.write_journey(journey)
    }

    fn read_journey(&self, journey: JourneyId) -> Result<Option<JourneyRecord>, PersistError> {
        self.now()?.storage.read_journey(journey)
    }

    fn publish(&self, publication: &Publication) -> Result<(Vec<Claim>, Durable), PersistError> {
        self.now()?.storage.publish(publication)
    }

    fn read_held(&self, queue: u128, from: u64, most: u32) -> Result<HeldQueue, PersistError> {
        self.now()?.storage.read_held(queue, from, most)
    }

    fn read_failed(&self, queue: u128, from: u64, most: u32) -> Result<FailedQueue, PersistError> {
        self.now()?.storage.read_failed(queue, from, most)
    }

    fn read_due(
        &self,
        queue: u128,
        part: DuePart,
        from: (u64, u64),
        most: u32,
    ) -> Result<DueQueue, PersistError> {
        self.now()?.storage.read_due(queue, part, from, most)
    }

    fn read_dead(&self, queue: u128, from: u64, most: u32) -> Result<DeadQueue, PersistError> {
        self.now()?.storage.read_dead(queue, from, most)
    }

    fn read_dead_message(&self, queue: u128, m: MessageId) -> Result<DeadEntry, PersistError> {
        self.now()?.storage.read_dead_message(queue, m)
    }

    fn replay(&self, replay: &Replay) -> Result<Replayed, PersistError> {
        self.now()?.storage.replay(replay)
    }

    fn claim(
        &self,
        journey: JourneyId,
        holder: &str,
        token: u128,
        lease: Duration,
    ) -> Result<Option<Claim>, PersistError> {
        self.now()?.storage.claim(journey, holder, token, lease)
    }

    fn renew(&self, claims: &[Claim], lease: Duration) -> Result<Vec<Claim>, PersistError> {
        self.now()?.storage.renew(claims, lease)
    }

    fn release(&self, claim: &Claim) -> Result<bool, PersistError> {
        self.now()?.storage.release(claim)
    }

    fn hand_on(&self, hand_on: &HandOn) -> Result<bool, PersistError> {
        self.now()?.storage.hand_on(hand_on)
    }

    fn turn(&self, turn: &Turn) -> Result<bool, PersistError> {
        self.now()?.storage.turn(turn)
    }

    fn read_instance(&self, instance: u128) -> Result<Option<InstanceRecord>, PersistError> {
        self.now()?.storage.read_instance(instance)
    }

    fn read_correlation(&self, correlation: u128) -> Result<Option<u128>, PersistError> {
        self.now()?.storage.read_correlation(correlation)
    }

    fn write_audit(&self, entry: &AuditEntry) -> Result<(), PersistError> {
        self.now()?.storage.write_audit(entry)
    }

    fn keep_audit(&self, most: u32, chunk: usize) -> Result<u32, PersistError> {
        self.now()?.storage.keep_audit(most, chunk)
    }

    fn read_kept_audit(&self, id: AuditId) -> Result<Option<KeptAudit>, PersistError> {
        self.now()?.storage.read_kept_audit(id)
    }

    fn read_kept_audit_stream(
        &self,
        id: AuditId,
        stream: StreamId,
    ) -> Result<Option<StreamRecord>, PersistError> {
        self.now()?.storage.read_kept_audit_stream(id, stream)
    }

    fn read_kept_audit_chunk(
        &self,
        id: AuditId,
        stream: Option<StreamId>,
        index: u32,
    ) -> Result<Option<Vec<u8>>, PersistError> {
        self.now()?.storage.read_kept_audit_chunk(id, stream, index)
    }

    fn write_administration(&self, record: &AdministrationRecord) -> Result<(), PersistError> {
        self.now()?.storage.write_administration(record)
    }

    fn read_administration(
        &self,
        kind: AdministrationKind,
        id: u128,
    ) -> Result<Option<AdministrationRecord>, PersistError> {
        self.now()?.storage.read_administration(kind, id)
    }

    fn remove_administration(
        &self,
        kind: AdministrationKind,
        id: u128,
    ) -> Result<(), PersistError> {
        self.now()?.storage.remove_administration(kind, id)
    }

    fn query(&self, query: &Query) -> Result<Vec<u128>, PersistError> {
        self.now()?.storage.query(query)
    }

    /// A statement keeps what answers as it begins: the primary's own
    /// databases, or the one Storage node the client pins.
    fn pinned(&self) -> Option<Arc<dyn XmipStorage>> {
        let now = self.now().ok()?.storage;
        Some(now.pinned().unwrap_or(now))
    }
}
