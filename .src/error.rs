//! Why a store could not keep or give back a record.

use secret::SecretError;
use std::error::Error;
use std::fmt;

/// What went wrong in persistence, with the scope a failure is audited
/// under (ADR-0062) where there is one.
#[derive(Debug)]
pub enum PersistError {
    /// A record failed its authentication tag: altered on disk, written
    /// under another key, or moved from another record's place. It is a
    /// failure, never a missing record, and the caller audits it with this
    /// scope and reason (ADR-0063, Consequences).
    Refused { scope: String, reason: String },
    /// The key home could not wrap or unwrap the store's data key: the
    /// key-encryption key is missing, exposed, or not the one that wrapped it.
    Key(SecretError),
    /// The engine beneath could not read or write.
    Engine {
        engine: &'static str,
        reason: String,
    },
    /// A record would not encode or decode.
    Record { reason: String },
}

impl PersistError {
    /// An engine's own failure, in its own words.
    pub fn engine(engine: &'static str, cause: impl fmt::Display) -> Self {
        Self::Engine {
            engine,
            reason: cause.to_string(),
        }
    }
}

impl From<SecretError> for PersistError {
    fn from(error: SecretError) -> Self {
        Self::Key(error)
    }
}

impl fmt::Display for PersistError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refused { scope, reason } => write!(f, "{scope}: refused, {reason}"),
            Self::Key(error) => write!(f, "the store's data key: {error}"),
            Self::Engine { engine, reason } => write!(f, "{engine}: {reason}"),
            Self::Record { reason } => write!(f, "record: {reason}"),
        }
    }
}

impl Error for PersistError {}
