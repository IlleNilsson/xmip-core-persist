//! Xmip Storage: the doorway to all storage.
//!
//! The nodes declaring the Storage role serve every storage operation, and
//! every other node calls those operations, never a database directly
//! (`runtime-model.md` section 3, *The Ledger*; ADR-0056, amendment
//! 2026-10-01, the Storage role). [`XmipStorage`] is those operations, once:
//!
//! - **the runtime database**, the Ledger: write and read a Stream chunk, a
//!   Message and a Journey; claim a Journey, renew the claim and release it;
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
//!
//! Three things answer to the trait: [`Embedded`], the embedded Storage
//! node's own databases; [`StorageClient`], a node reaching the Storage nodes
//! round robin over Xmip's TLS; and whatever serves a database server
//! (`xmip-core-persist-postgresql`, which follows). A node that declares the
//! Storage role itself calls its [`Embedded`] in process — not a connection,
//! so no TLS — and every other node calls a [`StorageClient`]; both are the
//! same trait, so nothing above them knows which it has.

mod client;
mod commit;
pub mod database;
mod embedded;
mod record;
pub mod schema;
mod server;
mod wire;

pub use client::StorageClient;
pub use embedded::Embedded;
pub use record::{
    AdministrationKind, AdministrationRecord, AuditEntry, Claim, Form, HandOn, JourneyRecord,
    MessageRecord, StreamChunk,
};
pub use server::{ALPN, StorageServer};

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
    /// Write a Stream chunk, replacing one of the same Stream and number.
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
    ///
    /// # Errors
    ///
    /// When it cannot be read or written.
    fn release(&self, claim: &Claim) -> Result<bool, PersistError>;

    /// Hand a step on as one atomic write: its result, the Messages it made,
    /// the Journeys that go on, and the claim released, together — or
    /// nothing, and `false`, where the claim is no longer its holder's. A
    /// hand-on repeated after a lost answer is `true` and writes nothing
    /// again.
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
}
