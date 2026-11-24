//! A Storage node's database as its standbys copy it (`super::copies`):
//! an engine whose every write is numbered and kept as it is written, under
//! the database's own lock, so the numbers are the engine's own order.

use std::sync::Arc;

use super::copies::Copies;
use crate::{BorrowedChange, Change, Engine, Entry, PersistError, owned};

/// An engine whose every write is numbered and kept in `copies` as it is
/// written: a Storage node's database as its standbys copy it.
pub struct Copied<E> {
    engine: E,
    database: usize,
    copies: Arc<Copies>,
}

impl<E: Engine> Copied<E> {
    /// `engine`, the database at `database` of [`DATABASES`], kept in
    /// `copies`.
    pub fn new(engine: E, database: usize, copies: Arc<Copies>) -> Self {
        Self {
            engine,
            database,
            copies,
        }
    }

    /// `write`, durable on return or not, numbered and kept.
    fn written(
        &self,
        durable: bool,
        write: impl FnOnce() -> Result<bool, PersistError>,
        changes: impl FnOnce() -> Vec<Change>,
    ) -> Result<bool, PersistError> {
        self.copies
            .written((self.database, durable), write, changes)
    }
}

impl<E: Engine> Engine for Copied<E> {
    fn engine(&self) -> &'static str {
        self.engine.engine()
    }

    fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, PersistError> {
        self.engine.read(key)
    }

    fn write(&self, key: &[u8], value: &[u8]) -> Result<(), PersistError> {
        let write = || self.engine.write(key, value).map(|()| true);
        self.written(true, write, || vec![(key.to_vec(), Some(value.to_vec()))])
            .map(drop)
    }

    fn write_new(&self, key: &[u8], value: &[u8]) -> Result<bool, PersistError> {
        let write = || self.engine.write_new(key, value);
        self.written(true, write, || vec![(key.to_vec(), Some(value.to_vec()))])
    }

    fn remove(&self, key: &[u8]) -> Result<(), PersistError> {
        let write = || self.engine.remove(key).map(|()| true);
        self.written(true, write, || vec![(key.to_vec(), None)])
            .map(drop)
    }

    fn apply(&self, batch: &[BorrowedChange<'_>]) -> Result<(), PersistError> {
        let write = || self.engine.apply(batch).map(|()| true);
        self.written(true, write, || owned(batch)).map(drop)
    }

    fn apply_deferred(&self, batch: &[BorrowedChange<'_>]) -> Result<(), PersistError> {
        let write = || self.engine.apply_deferred(batch).map(|()| true);
        self.written(false, write, || owned(batch)).map(drop)
    }

    fn sync(&self) -> Result<(), PersistError> {
        self.copies.sync(self.database, &self.engine)
    }

    fn scan(
        &self,
        first: &[u8],
        last: &[u8],
        most: usize,
        reverse: bool,
    ) -> Result<Vec<Entry>, PersistError> {
        self.engine.scan(first, last, most, reverse)
    }

    fn view(&self) -> Result<Box<dyn crate::Reads + '_>, PersistError> {
        self.engine.view()
    }
}
