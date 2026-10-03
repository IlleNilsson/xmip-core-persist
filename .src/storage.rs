//! Xmip Storage: the doorway to all storage.
//!
//! The nodes declaring the Storage role serve every storage operation, and
//! every other node calls those operations, never a database directly
//! (`runtime-model.md` section 3, *The Ledger*; ADR-0056, amendment
//! 2026-10-01, the Storage role). [`XmipStorage`] is those operations, once:
//!
//! - **the runtime database**, the Ledger: write and read a Stream chunk, a
//!   Message and a Journey; write a Publication — a Message, its Journeys,
//!   the ones a paused Subscription holds, its Dead Message Queue entry
//!   where nothing matched, and its audit record — as one; read what a
//!   Subscription holds, oldest first, and release a held Journey once its
//!   step is done; read a node's Dead Message Queue, oldest first, and
//!   replay an entry; claim a Journey, renew the claim and release it;
//!   hand a step on; write an audit record as it is first written;
//! - **the audit keeper**, moving audit records from the runtime database to
//!   the administration database (ADR-0062, amendment 2026-10-01);
//! - **the administration database**: what must be shared and kept over
//!   time — registration, membership, Modules, Handlers, deployment state,
//!   operator state — and the audit kept there; never configuration
//!   (`deployment-model.md` section 7).
//!
//! **Every write counts only once the database has it durably** (the owner,
//! 2026-10-01: *Safe way*): an operation returns once its write is on disk —
//! synced on an embedded Storage node, committed behind a database server.
//! One write counts later, and says so: a Stream chunk is durable with the
//! next durable write, its Publication's, since nothing is acknowledged
//! before that commits ([`XmipStorage::write_chunk`]), so a receive cycle is
//! one statement, asked of one Storage node ([`statement`]).
//!
//! **An operation is all or nothing.** Where one fails half-way — a record
//! it reads cannot be read, or fails its tag — nothing of it is written,
//! though the others sharing its write are.
//!
//! Three things answer to the trait: [`Embedded`], the embedded Storage
//! node's own databases; [`StorageClient`], a node reaching the Storage nodes
//! round robin over Xmip's TLS; and whatever serves a database server
//! (`xmip-core-persist-postgresql`, which follows). A node that declares the
//! Storage role itself calls its [`Embedded`] in process — not a connection,
//! so no TLS — and every other node calls a [`StorageClient`]; both are the
//! same trait, so nothing above them knows which it has.

mod claim;
pub mod client;
mod commit;
pub mod database;
mod dead;
mod embedded;
mod hold;
mod publication;
mod queue;
mod record;
pub mod schema;
mod server;
mod wire;

pub use client::StorageClient;
pub use dead::{
    Dead, DeadEntry, DeadMessage, DeadQueue, Named, Replay, Replayed, dead_message_queue,
};
pub use embedded::Embedded;
pub use hold::{Held, HeldQueue, Hold, named};
pub use publication::Publication;
pub use record::{
    AdministrationKind, AdministrationRecord, AuditEntry, Claim, Form, HandOn, JourneyRecord,
    MessageRecord, StreamChunk,
};
pub use server::{ALPN, StorageServer};

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
    /// the ones held and its audit record, together, or nothing. Written
    /// again after a lost answer, its records are written again under their
    /// identifiers, each held Journey keeps the one place it has, by its
    /// identifier, and its audit record is kept once, by its identifier, by
    /// the audit keeper.
    ///
    /// # Errors
    ///
    /// When it cannot be written; nothing of it is written then.
    fn publish(&self, publication: &Publication) -> Result<(), PersistError>;

    /// What `queue` holds: its places, how many it holds, and up to `most`
    /// of them from the place `from` on, oldest first. `most` zero reads
    /// the places alone.
    ///
    /// # Errors
    ///
    /// When it cannot be read, or fails its tag: the caller tries again,
    /// and takes nothing past what it could not read.
    fn read_held(&self, queue: u128, from: u64, most: u32) -> Result<HeldQueue, PersistError>;

    /// Release the Journey held at `sequence` in `queue`, writing `journey`
    /// as its step left it, as one write: once it was delivered, or
    /// stopped for good. A Journey whose step failed is written with
    /// [`XmipStorage::write_journey`] and keeps its place. Released again
    /// after a lost answer, nothing is let go twice.
    ///
    /// # Errors
    ///
    /// When it cannot be written; nothing of it is written then.
    fn release_held(
        &self,
        queue: u128,
        sequence: u64,
        journey: &JourneyRecord,
    ) -> Result<(), PersistError>;

    /// A node's Dead Message Queue, `queue` ([`dead_message_queue`]): its
    /// places, how many it holds, and up to `most` entries from the place
    /// `from` on, oldest first. `most` zero reads the places alone.
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

    /// Renew `claim` for `lease` from now, while the work runs: the claim
    /// renewed, or `None` where it is no longer its holder's — released, or
    /// lapsed and taken by another.
    ///
    /// # Errors
    ///
    /// When it cannot be read or written.
    fn renew(&self, claim: &Claim, lease: Duration) -> Result<Option<Claim>, PersistError>;

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
    /// the Journeys that go on, and the claim released, together — or
    /// nothing, and `false`, where the claim is no longer its holder's. A
    /// hand-on repeated after a lost answer is `true` and writes nothing
    /// again; one after its claim was only given back is `false`.
    ///
    /// # Errors
    ///
    /// When it cannot be read or written; nothing of it is written then.
    fn hand_on(&self, hand_on: &HandOn) -> Result<bool, PersistError>;

    /// Write an audit record to the runtime database, where it is first
    /// written (ADR-0062, amendment 2026-10-01).
    ///
    /// # Errors
    ///
    /// When it cannot be written; the caller writes it to the operating
    /// system's log then (ADR-0062 clause 3).
    fn write_audit(&self, entry: &AuditEntry) -> Result<(), PersistError>;

    /// The audit keeper: move up to `most` audit records, oldest first,
    /// from the runtime database to the administration database. Each is
    /// kept exactly once — a move cut short is finished by the next, and a
    /// record written twice is kept once, by its identifier. How many moved.
    ///
    /// # Errors
    ///
    /// When either database cannot be read or written.
    fn keep_audit(&self, most: u32) -> Result<u32, PersistError>;

    /// An audit record the keeper moved, or `None`.
    ///
    /// # Errors
    ///
    /// When it cannot be read, or fails its tag.
    fn read_kept_audit(&self, id: AuditId) -> Result<Option<AuditEntry>, PersistError>;

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

/// `storage` for one statement: a receive cycle's chunks, its Publication
/// and its Journeys asked of one Storage node, so the Publication's one
/// sync covers the chunks before it ([`XmipStorage::write_chunk`]).
#[must_use]
pub fn statement(storage: &Arc<dyn XmipStorage>) -> Arc<dyn XmipStorage> {
    storage.pinned().unwrap_or_else(|| Arc::clone(storage))
}
