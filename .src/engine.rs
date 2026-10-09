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

    /// Every change of `batch` as one write: all of them or none, durable
    /// on return. A change is a key and its new value, `None` removing it.
    ///
    /// It is what makes a hand-on one atomic write — the step's result,
    /// the next Journey and the claim released together (`runtime-model.md`
    /// section 3, *The Ledger*) — and what lets many writes share one sync,
    /// which is group commit (`deployment-model.md` section 7).
    ///
    /// # Errors
    ///
    /// [`PersistError::Engine`] when the engine cannot write; nothing of
    /// the batch is written then.
    fn apply(&self, batch: &[Change]) -> Result<(), PersistError>;

    /// Every change of `batch` as one write, all or none, in order with
    /// every other write and **not synced on return**: it is durable once
    /// a later [`Engine::apply`] — or any write durable on return — has
    /// returned, since that sync covers everything written before it. What
    /// a write is worth before then is what an operating system keeps of a
    /// process that died. An engine without a cheaper write syncs it as
    /// [`Engine::apply`] does, which is what this does unless an engine
    /// says otherwise.
    ///
    /// It is how a Stream's chunks are written: nothing is acknowledged
    /// before the Publication that refers to them commits, and that
    /// commit's sync makes them durable with it, so a receive cycle costs
    /// one sync (`runtime-model.md` section 3, *The Ledger*).
    ///
    /// # Errors
    ///
    /// [`PersistError::Engine`] when the engine cannot write; nothing of
    /// the batch is written then.
    fn apply_deferred(&self, batch: &[Change]) -> Result<(), PersistError> {
        self.apply(batch)
    }

    /// Up to `most` keys and their values from `first` to `last`, both
    /// included, in the order of their bytes — or the reverse, the last
    /// first, where `reverse`.
    ///
    /// It is how an index entry is found
    /// ([`crate::EncryptedStore::scan_index`]): an entry's key is not hashed
    /// again, so it sorts by its index's columns, and a search is one range.
    /// Nothing else reads a range; a keyed hash sorts nowhere.
    ///
    /// # Errors
    ///
    /// [`PersistError::Engine`] when the engine cannot read.
    fn scan(
        &self,
        first: &[u8],
        last: &[u8],
        most: usize,
        reverse: bool,
    ) -> Result<Vec<Entry>, PersistError>;
}

/// One change of a batch: a key and its new value, `None` removing it.
pub type Change = (Vec<u8>, Option<Vec<u8>>);

/// One key and its value, as a range read finds them.
pub type Entry = (Vec<u8>, Vec<u8>);

/// An engine boxed is an engine: a program that links several opens the one
/// its configuration names, whichever it is (ADR-0018, amendment
/// 2026-09-30).
impl<E: Engine + ?Sized> Engine for Box<E> {
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

    fn apply(&self, batch: &[Change]) -> Result<(), PersistError> {
        (**self).apply(batch)
    }

    fn apply_deferred(&self, batch: &[Change]) -> Result<(), PersistError> {
        (**self).apply_deferred(batch)
    }

    fn scan(
        &self,
        first: &[u8],
        last: &[u8],
        most: usize,
        reverse: bool,
    ) -> Result<Vec<Entry>, PersistError> {
        (**self).scan(first, last, most, reverse)
    }
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

    fn apply(&self, batch: &[Change]) -> Result<(), PersistError> {
        (**self).apply(batch)
    }

    fn apply_deferred(&self, batch: &[Change]) -> Result<(), PersistError> {
        (**self).apply_deferred(batch)
    }

    fn scan(
        &self,
        first: &[u8],
        last: &[u8],
        most: usize,
        reverse: bool,
    ) -> Result<Vec<Entry>, PersistError> {
        (**self).scan(first, last, most, reverse)
    }
}
