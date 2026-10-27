//! Xmip Storage: the doorway to all storage.
//!
//! The nodes of the Storage role serve every storage operation, and every
//! other node calls them, never a database (`runtime-model.md` section 3;
//! ADR-0056, amendment 2026-10-01). [`XmipStorage`] is those operations:
//!
//! - **the runtime database**, the Ledger: write and read a Stream chunk, a
//!   Message and a Journey; write a Publication — a Message, its Journeys,
//!   those held and queued, its node's claims, its Dead Message Queue entry,
//!   its audit record — as one; read a queue oldest first, or what is due in
//!   it; read and replay a node's Dead Message Queue; claim, renew and
//!   release a Journey; hand a step on; write an audit record;
//! - **a search** of any of them: the records one index of one table finds,
//!   in the clear (`query`, proposed 2026-10-09);
//! - **the audit keeper**, moving audit records, each with its Streams'
//!   bytes, to the audit database (ADR-0062, ADR-0070);
//! - **the administration database**: what must be shared and kept over
//!   time, never configuration (`deployment-model.md` section 7);
//! - **the audit database**: the audit kept over time, on storage of its own.
//!
//! **Every write counts only once the database has it durably** (the owner,
//! 2026-10-01: *Safe way*): synced on an embedded Storage node, committed
//! behind a database server. A Stream chunk counts with its Publication
//! ([`XmipStorage::write_chunk`]), and a Publication once its [`Durable`]
//! says, so a receive cycle is one statement ([`statement`]).
//!
//! **An operation is all or nothing.** Where one fails half-way, nothing of
//! it is written, though the others sharing its write are.
//!
//! Three things answer to the trait: [`Embedded`], in process on a node of
//! the Storage role; [`StorageClient`], every other node's, round robin over
//! Xmip's TLS; and whatever serves a database server, which follows. Nothing
//! above them knows which it has.

mod audit_entry;
mod audited;
#[cfg(test)]
mod authority;
mod chain;
mod claim;
pub mod client;
mod columns;
mod commit;
pub mod database;
mod dead;
mod due;
mod durable;
mod embedded;
mod facts;
mod failed;
mod hand_on;
mod hold;
mod instance;
mod kept_audit;
mod origin;
mod publication;
mod query;
mod queue;
mod record;
mod row;
pub mod schema;
mod server;
pub mod standby;
mod stream;
mod wire;

pub use audit_entry::{AuditBody, AuditEntry};
pub use audited::{Audited, KeptStream};
pub use chain::{canonical_form, carried_streams, chain_digest};
pub use client::{Reached, StorageClient, statement};
pub use dead::{
    Dead, DeadEntry, DeadMessage, DeadQueue, Named, Replay, Replayed, dead_message_queue,
};
pub use due::{DuePart, DueQueue};
pub use durable::Durable;
pub use embedded::Embedded;
pub use facts::{AuditFacts, JourneyFacts, MessageFacts};
pub use failed::{Failed, FailedQueue, Failure};
pub use hand_on::{HandOn, Wait};
pub use hold::{Held, HeldQueue, Hold, named};
pub use instance::{Correlation, InstanceRecord, InstanceWrite, Turn};
pub use kept_audit::KeptAudit;
pub use publication::Publication;
pub use query::{Ask, Query, Span};
pub use record::{
    AdministrationKind, AdministrationRecord, Claim, Form, JourneyRecord, MessageRecord,
    StreamChunk,
};
pub use server::{ALPN, StorageServer};
pub use standby::{Standing, StorageNode};
pub use stream::{ChunkReader, Chunked, DIGEST, StreamDigest, StreamRecord};

use std::sync::Arc;
use std::time::Duration;

use xcore::{AuditId, JourneyId, MessageId, StreamId};

use crate::PersistError;

/// Xmip Storage's operations, over whichever databases are behind them.
///
/// Every operation answers in the Storage node's own time and on its own
/// clock; a claim's lapse is the Storage node's to decide, so no two
/// claimants' clocks are ever compared. Every error is a [`PersistError`],
/// so a record that fails its authentication tag reaches the caller as
/// [`PersistError::Refused`], to be audited (ADR-0062, ADR-0063).
pub trait XmipStorage: Send + Sync {
    /// Write a Stream chunk, replacing one of the same Stream and number:
    /// in order with every other write, and **durable with the next
    /// durable write, not on return** — on the embedded Storage node its
    /// Publication's sync covers it (`persist::Engine::apply_deferred`).
    /// Nothing is acknowledged before the Publication referring to it
    /// commits, so a receive cycle costs one sync, and a crash before
    /// Publication leaves at most chunks no Message refers to, which the
    /// sender, never acknowledged, sends again.
    ///
    /// # Errors
    ///
    /// When it cannot be written.
    fn write_chunk(&self, chunk: &StreamChunk) -> Result<(), PersistError>;

    /// A Stream's chunk by its number, or `None`.
    ///
    /// # Errors
    ///
    /// When it cannot be read, or fails its tag.
    fn read_chunk(&self, stream: StreamId, index: u32)
    -> Result<Option<StreamChunk>, PersistError>;

    /// Write a Stream's last chunk and the Stream's own record as one write
    /// — durable with the next durable write, as every chunk is
    /// ([`XmipStorage::write_chunk`]) — once, as its writer ends it: a
    /// Stream is written once and never changed, and every Message that
    /// refers to it, the one received and any assigned from it, refers to
    /// this one record (the owner, 2026-10-09: *A new message might refer
    /// to the same stream*). Its time is the Storage node's.
    ///
    /// # Errors
    ///
    /// When it cannot be written.
    fn write_stream(&self, last: &StreamChunk, stream: &StreamRecord) -> Result<(), PersistError>;

    /// A Stream's own record — its length and its chunks — or `None`.
    ///
    /// # Errors
    ///
    /// When it cannot be read, or fails its tag.
    fn read_stream(&self, stream: StreamId) -> Result<Option<StreamRecord>, PersistError>;

    /// Write a Message, replacing the last written under its identifier.
    ///
    /// # Errors
    ///
    /// When it cannot be written.
    fn write_message(&self, message: &MessageRecord) -> Result<(), PersistError>;

    /// A Message as last written, or `None`.
    ///
    /// # Errors
    ///
    /// When it cannot be read, or fails its tag.
    fn read_message(&self, message: MessageId) -> Result<Option<MessageRecord>, PersistError>;

    /// Write a Journey, replacing the last written under its identifier.
    ///
    /// # Errors
    ///
    /// When it cannot be written.
    fn write_journey(&self, journey: &JourneyRecord) -> Result<(), PersistError>;

    /// A Journey as last written, or `None`.
    ///
    /// # Errors
    ///
    /// When it cannot be read, or fails its tag.
    fn read_journey(&self, journey: JourneyId) -> Result<Option<JourneyRecord>, PersistError>;

    /// Write a Publication as one atomic write: its Message, its Journeys,
    /// the ones held, the claims its node takes on them and its audit
    /// record, together, or nothing — once, by its Message. The claims its
    /// node holds of those it asked for, **answered once decided and
    /// readable, before its sync**, and when it is durable ([`Durable`]):
    /// nothing is acknowledged, and no far end sees a Journey of it, before
    /// that. Asked again after a lost answer, it writes nothing and answers
    /// the claims of it its node still holds under their tokens.
    /// # Errors
    /// When it cannot be written, or its Message was published before as
    /// another Publication; nothing of it is written then.
    fn publish(&self, publication: &Publication) -> Result<(Vec<Claim>, Durable), PersistError>;

    /// What `queue` holds: its places, how many, and up to `most` of them
    /// from the place `from` on, oldest first, one range of its index by
    /// place. `most` zero reads no entry: its places and its first.
    ///
    /// # Errors
    ///
    /// When it cannot be read, or fails its tag: the caller tries again,
    /// and takes nothing past what it could not read.
    fn read_held(&self, queue: u128, from: u64, most: u32) -> Result<HeldQueue, PersistError>;

    /// `queue`'s index of failed Journeys ([`Failed`]), read as `read_held` reads a queue.
    ///
    /// # Errors
    /// When it cannot be read, or fails its tag.
    fn read_failed(&self, queue: u128, from: u64, most: u32) -> Result<FailedQueue, PersistError>;

    /// `part` of `queue` by when each is due ([`DueQueue`]): up to `most`
    /// from the due time and place `from` on, a range from where its first is.
    /// # Errors
    /// When it cannot be read, or fails its tag.
    fn read_due(
        &self,
        queue: u128,
        part: DuePart,
        from: (u64, u64),
        most: u32,
    ) -> Result<DueQueue, PersistError>;

    /// A node's Dead Message Queue, `queue` ([`dead_message_queue`]): its
    /// places, how many it holds, and up to `most` entries from the place
    /// `from` on, oldest first, as [`XmipStorage::read_held`] reads a queue.
    ///
    /// # Errors
    ///
    /// When it cannot be read, or fails its tag.
    fn read_dead(&self, queue: u128, from: u64, most: u32) -> Result<DeadQueue, PersistError>;

    /// What `queue` keeps for `message`: its entry, that it was replayed,
    /// or that it never kept one.
    ///
    /// # Errors
    ///
    /// When it cannot be read, or fails its tag.
    fn read_dead_message(&self, queue: u128, message: MessageId)
    -> Result<DeadEntry, PersistError>;

    /// Replay an entry as one atomic write: the Journeys its Message opened
    /// against the Subscriptions of now, the ones held, and its audit
    /// record written, and the entry taken out — or nothing. Asked again
    /// after a lost answer, or once another replayed it, it writes nothing
    /// and says [`Replayed::Before`].
    ///
    /// # Errors
    ///
    /// When it cannot be read or written; nothing of it is written then.
    fn replay(&self, replay: &Replay) -> Result<Replayed, PersistError>;

    /// Claim `journey` for `holder` under `token` for `lease`: a
    /// conditional update — set the holder where there is none or the last
    /// one's claim has lapsed — time-limited (`runtime-model.md` section 3).
    /// The claim taken, or `None` where another holds it. A claimant that
    /// asks again under the token it holds by gets its claim back, so a
    /// request repeated after a lost answer takes nothing twice.
    ///
    /// # Errors
    ///
    /// When it cannot be read or written.
    fn claim(
        &self,
        journey: JourneyId,
        holder: &str,
        token: u128,
        lease: Duration,
    ) -> Result<Option<Claim>, PersistError>;

    /// Renew every one of `claims` for `lease` from now, while the work
    /// runs, in one request: those still their holder's, renewed — a claim
    /// held to a later deadline, a retry's due time and a lease past it,
    /// keeps that deadline, never shortened — and none of those no longer
    /// their holder's, released, or lapsed and taken by another. A node
    /// renews all its claims at once, so one slow answer costs one wait,
    /// never one per claim.
    ///
    /// # Errors
    ///
    /// When it cannot be read or written.
    fn renew(&self, claims: &[Claim], lease: Duration) -> Result<Vec<Claim>, PersistError>;

    /// Give `claim` back, as a draining stop does rather than letting it
    /// lapse (ADR-0018 clause 12): `true` where it was its holder's to give.
    /// A claim given back is not a step handed on: a hand-on under it after
    /// is `false`.
    ///
    /// # Errors
    ///
    /// When it cannot be read or written.
    fn release(&self, claim: &Claim) -> Result<bool, PersistError>;

    /// Hand a step on as one atomic write: its result, the Messages it made,
    /// the Journeys that go on, the queues it leaves and the places it
    /// takes, and the claim released — or kept until a due time, where the
    /// step waits — together; or nothing, and `false`, where the claim is no
    /// longer its holder's. A hand-on repeated after a lost answer is `true`
    /// and writes nothing again; one after its claim was only given back is
    /// `false`.
    ///
    /// # Errors
    ///
    /// When it cannot be read or written; nothing of it is written then.
    fn hand_on(&self, hand_on: &HandOn) -> Result<bool, PersistError>;

    /// Hand a Work Process's turn on as one atomic write: its Journey's
    /// hand-on, the Instances it moved and the correlation keys it set and
    /// let go; or nothing, and `false`, where the claim is no longer its
    /// holder's or an Instance no longer stands at the turn it was read at
    /// (`instance`).
    ///
    /// # Errors
    ///
    /// When it cannot be read or written; nothing of it is written then.
    fn turn(&self, turn: &Turn) -> Result<bool, PersistError>;

    /// The Work Process Instance `instance`, where the Ledger keeps one.
    ///
    /// # Errors
    ///
    /// When it cannot be read.
    fn read_instance(&self, instance: u128) -> Result<Option<InstanceRecord>, PersistError>;

    /// The Instance the correlation key `correlation` belongs to, where one
    /// does.
    ///
    /// # Errors
    ///
    /// When it cannot be read.
    fn read_correlation(&self, correlation: u128) -> Result<Option<u128>, PersistError>;
    /// Write an audit record to the runtime database, where it is first
    /// written (ADR-0062, amendment 2026-10-01).
    ///
    /// # Errors
    ///
    /// When it cannot be written; the caller writes it to the operating
    /// system's log then (ADR-0062 clause 3).
    fn write_audit(&self, entry: &AuditEntry) -> Result<(), PersistError>;

    /// The audit keeper: move up to `most` audit records, oldest first, to
    /// the administration database, each once by its identifier, its body
    /// in chunks of `chunk` bytes — the size the runtime writes a Stream in
    /// — and each Stream it carries in chunks beside it with the Stream's
    /// record (ADR-0070; `audited`, `kept_audit`), chained in its writer's
    /// audit chain (`chain`). How many moved.
    ///
    /// # Errors
    ///
    /// When either database cannot be read or written.
    fn keep_audit(&self, most: u32, chunk: usize) -> Result<u32, PersistError>;

    /// An audit record the keeper moved, its body in chunks beside it.
    ///
    /// # Errors
    ///
    /// When it cannot be read, or fails its tag.
    fn read_kept_audit(&self, id: AuditId) -> Result<Option<KeptAudit>, PersistError>;

    /// The Stream `stream` a kept audit record carries, as the keeper kept
    /// it — its length, its chunks, its digest — or `None` where it carries
    /// no such Stream (ADR-0070; the `audit_stream` table).
    ///
    /// # Errors
    ///
    /// When it cannot be read, or fails its tag.
    fn read_kept_audit_stream(
        &self,
        id: AuditId,
        stream: StreamId,
    ) -> Result<Option<StreamRecord>, PersistError>;

    /// A chunk of what the keeper kept beside a record in chunks — its body
    /// where `stream` is `None`, else the Stream it carries — by its number,
    /// or `None` past its last (ADR-0070). Read them through
    /// [`ChunkReader::audit_body`] and [`ChunkReader::audited`], which hold
    /// them to the length and digest the record keeps.
    ///
    /// # Errors
    ///
    /// When it cannot be read, or fails its tag.
    fn read_kept_audit_chunk(
        &self,
        id: AuditId,
        stream: Option<StreamId>,
        index: u32,
    ) -> Result<Option<Vec<u8>>, PersistError>;

    /// Write an administration record, replacing the last of its kind and
    /// identifier.
    ///
    /// # Errors
    ///
    /// When it cannot be written.
    fn write_administration(&self, record: &AdministrationRecord) -> Result<(), PersistError>;

    /// An administration record, or `None`.
    ///
    /// # Errors
    ///
    /// When it cannot be read, or fails its tag.
    fn read_administration(
        &self,
        kind: AdministrationKind,
        id: u128,
    ) -> Result<Option<AdministrationRecord>, PersistError>;

    /// An administration record gone. Removing what is absent is not an
    /// error.
    ///
    /// # Errors
    ///
    /// When it cannot be written.
    fn remove_administration(&self, kind: AdministrationKind, id: u128)
    -> Result<(), PersistError>;

    /// The identifiers of the records `query` finds, up to its most, in
    /// its index's order or the reverse: one index of one table read, a
    /// name or an identifier found by equality, as its column keeps it
    /// (`schema` names the columns and the indexes). The caller reads each
    /// record as it reads any.
    ///
    /// # Errors
    ///
    /// When it cannot be read, or an entry fails its tag.
    fn query(&self, query: &Query) -> Result<Vec<u128>, PersistError>;

    /// One statement's Xmip Storage: every operation asked through it goes
    /// to one Storage node, however many it is (the owner, 2026-10-03:
    /// *writing chunk 1 to n should be regarded as one statement, one call
    /// against one Storage node*, and the Publication and its Journeys with
    /// them). `None` where every operation already reaches one storage, as
    /// on the embedded Storage node. Take it through [`statement`].
    fn pinned(&self) -> Option<Arc<dyn XmipStorage>> {
        None
    }
}
