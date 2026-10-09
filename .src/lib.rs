//! Durable state: everything Xmip keeps of its own, encrypted, and Xmip
//! Storage, the doorway every node reaches it through.
//!
//! **Everything stored is encrypted, here, once** (ADR-0063 clause 2).
//! [`EncryptedStore`] seals every record with AES-256-GCM before an
//! [`Engine`] sees it and authenticates it when it comes back; the key a
//! record is found by is a keyed hash, so an engine's files hold neither
//! what is stored nor what it is stored under. The engines are technologies
//! mounted beside this source — `rocksdb`, the runtime database, and
//! `sqlite`, the administration database (ADR-0015, amendments 2026-09-25
//! and 2026-10-01) — and store ciphertext only. The keys come from the key
//! home, `xmip-core-secret`.
//!
//! [`storage`] is Xmip Storage: its operations, once (`XmipStorage`), the
//! embedded Storage node that keeps both databases itself, a Storage node
//! serving them over Xmip's TLS, and a node reaching Storage nodes round
//! robin (`runtime-model.md` section 3, *The Ledger*). Every record the
//! runtime keeps — a Stream's chunks, a Message, a Journey, a claim, what a
//! paused Subscription holds, operator state — is written through it.

// Each subject in a file of its own, and each reached at one path: the
// crate's root.
mod encrypted_store;
mod engine;
mod error;
#[cfg(any(test, feature = "test-support"))]
pub mod fixture;
pub mod storage;

pub use encrypted_store::{EncryptedStore, IndexEntry, RecordChange};
pub use engine::{Change, Engine, Entry};
pub use error::PersistError;
