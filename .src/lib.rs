//! Durable runtime state: what a node writes down so that a Journey survives
//! the node.
//!
//! The shapes here are persistence's own — a checkpoint, a lease, a
//! deduplication fingerprint. What they are *about* is not: a record is keyed
//! by the estate's identifiers and carries the Journey state `xmip-core-journey`
//! defines. This crate held a raw UUID where `JourneyId` and `MessageId`
//! exist, a name where `ClusterId` and `NodeId` exist, and a six-variant copy
//! of `JourneyState`, until 2026-09-14 (ADR-0044: shared code lives where both
//! already depend). The newtype is what stops a Journey identifier being
//! passed where a Message identifier belongs, which a `Uuid` never could.
//!
//! The times are still text, RFC 3339 in UTC, as they were.
//!
//! **Everything stored is encrypted, here, once** (ADR-0063 clause 2).
//! [`EncryptedStore`] seals every record with AES-256-GCM before an
//! [`Engine`] sees it and authenticates it when it comes back; the key a
//! record is found by is a keyed hash, so an engine's files hold neither
//! what is stored nor what it is stored under. The engines are technologies
//! mounted beside this source — `rocksdb`, the runtime store, and `sqlite`,
//! the management store (ADR-0015, amendment 2026-09-25) — and store
//! ciphertext only. The keys come from the key home, `xmip-core-secret`.
//!
//! [`RuntimeStore`] is what the runtime writes a Journey's durable state
//! through, and [`EncryptedStore`] over any engine is one.

// Each subject in a file of its own, and each reached at one path: the
// crate's root.
mod encrypted_store;
mod engine;
mod error;
#[cfg(any(test, feature = "test-support"))]
pub mod fixture;
mod runtime_store;

pub use encrypted_store::EncryptedStore;
pub use engine::Engine;
pub use error::PersistError;
pub use runtime_store::RuntimeStore;

use journey::Journey;
use serde::{Deserialize, Serialize};
use xcore::{ClusterId, JourneyId, MessageId, NodeId};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableRecordIdentity {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    pub journey_id: JourneyId,
    pub message_id: Option<MessageId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableExecutionCheckpoint {
    pub identity: DurableRecordIdentity,
    pub xmip_process_name: Option<String>,
    pub current_step: String,
    pub generation: u32,
    pub payload_refs: Vec<String>,
    pub waiting_for: Vec<RecoveryWaitCondition>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryWaitCondition {
    pub condition_name: String,
    pub correlation_key: String,
    pub timeout_utc: Option<String>,
}

/// A Journey as last written, and where recovery picks it up.
///
/// The [`Journey`] is stored whole — its state, its chain back to the Journey
/// that caused it, its depth and its entries. Until 2026-09-23 this held a
/// cut-down copy of the Journey's fields, which dropped `previous_journey_id`,
/// `cause`, `depth` and the entries: a Journey recovered from it restarted its
/// depth at zero, and the chain limit (ADR-0026) forgot every link made before
/// the restart (open-problems.md, problem 25, row a). What is here beside it
/// is persistence's own.
///
/// Every state is written, `Dismissed` included: a Journey an operator
/// stopped is a terminal record like any other (ADR-0013).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableJourneyState {
    pub cluster_id: ClusterId,
    pub journey: Journey,
    pub last_known_step: Option<String>,
    /// The generations of [`Journey::messages`] still in flight.
    pub active_message_ids: Vec<MessageId>,
    pub audit_position: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryLease {
    pub cluster_id: ClusterId,
    pub journey_id: JourneyId,
    pub owner_node_id: NodeId,
    pub lease_token: String,
    pub expires_utc: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeduplicationRecord {
    pub journey_id: JourneyId,
    pub message_id: MessageId,
    pub source_fingerprint: String,
}
