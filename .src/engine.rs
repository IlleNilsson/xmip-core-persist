//! What an engine is to persist: bytes under bytes, and nothing more.

use crate::PersistError;

/// A key/value engine beneath [`crate::EncryptedStore`]: `RocksDB` for the
/// runtime store, `SQLite` for the management store, each a technology under
/// this crate (ADR-0015, amendment 2026-09-25).
///
/// An engine sees only what the encryption above it hands down — a keyed
/// hash as the key and a sealed record as the value — so it neither needs
/// nor gets any idea of what a record is. Nothing is encrypted here; that is
/// the layer's, once (ADR-0063 clause 2).
pub trait Engine: Send + Sync {
    /// Which engine this is, for the scope of a failure.
    fn engine(&self) -> &'static str;

    /// The value under `key`, or `None`.
    ///
    /// # Errors
    ///
    /// [`PersistError::Engine`] when the engine cannot read.
    fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, PersistError>;

    /// `value` under `key`, replacing what was there, durable on return.
    ///
    /// # Errors
    ///
    /// [`PersistError::Engine`] when the engine cannot write.
    fn write(&self, key: &[u8], value: &[u8]) -> Result<(), PersistError>;

    /// `value` under `key` only if nothing is there, as one step: `true`
    /// when written, `false` when the key was taken.
    ///
    /// # Errors
    ///
    /// [`PersistError::Engine`] when the engine cannot write.
    fn write_new(&self, key: &[u8], value: &[u8]) -> Result<bool, PersistError>;

    /// Nothing under `key` any more. Removing what is absent is not an
    /// error.
    ///
    /// # Errors
    ///
    /// [`PersistError::Engine`] when the engine cannot write.
    fn remove(&self, key: &[u8]) -> Result<(), PersistError>;
}

/// An engine lent is an engine: a test opens the layer again over the same
/// records without giving the engine up.
impl<E: Engine + ?Sized> Engine for &E {
    fn engine(&self) -> &'static str {
        (**self).engine()
    }

    fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, PersistError> {
        (**self).read(key)
    }

    fn write(&self, key: &[u8], value: &[u8]) -> Result<(), PersistError> {
        (**self).write(key, value)
    }

    fn write_new(&self, key: &[u8], value: &[u8]) -> Result<bool, PersistError> {
        (**self).write_new(key, value)
    }

    fn remove(&self, key: &[u8]) -> Result<(), PersistError> {
        (**self).remove(key)
    }
}
