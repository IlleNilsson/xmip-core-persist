//! A store read in one consistent view ([`crate::Engine::view`]): what a
//! read made of several reads — a queue's page, its index entries and the
//! records they find — needs, so no write lands between them (corrected
//! 2026-10-27: a page read mid-write came back short, and a sequence's
//! head was taken for empty).

use super::EncryptedStore;
use crate::{Engine, Entry, PersistError, Reads};

/// The engine as it stands: what a single read reads through.
pub(super) struct Now<'a, E>(pub(super) &'a E);

impl<E: Engine> Reads for Now<'_, E> {
    fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, PersistError> {
        self.0.read(key)
    }

    fn scan(
        &self,
        first: &[u8],
        last: &[u8],
        most: usize,
        reverse: bool,
    ) -> Result<Vec<Entry>, PersistError> {
        self.0.scan(first, last, most, reverse)
    }
}

/// A store and one consistent view of its engine.
pub struct Viewed<'a, E> {
    pub(super) store: &'a EncryptedStore<E>,
    pub(super) reads: Box<dyn Reads + 'a>,
}

impl<E: Engine> Viewed<'_, E> {
    /// As [`EncryptedStore::get`], in the view.
    ///
    /// # Errors
    ///
    /// As [`EncryptedStore::get`].
    pub fn get(&self, kind: &str, key: &[u8]) -> Result<Option<Vec<u8>>, PersistError> {
        self.store.get_in(self.reads.as_ref(), kind, key)
    }
}

/// What reads a range of an index: a store as it stands, or one view of it.
pub trait IndexScan {
    /// As [`EncryptedStore::scan_index_keyed`].
    ///
    /// # Errors
    ///
    /// As [`EncryptedStore::scan_index_keyed`].
    fn scan_index_keyed(
        &self,
        first: &[u8],
        last: &[u8],
        most: usize,
        reverse: bool,
    ) -> Result<Vec<(Vec<u8>, u128)>, PersistError>;
}

impl<E: Engine> IndexScan for EncryptedStore<E> {
    fn scan_index_keyed(
        &self,
        first: &[u8],
        last: &[u8],
        most: usize,
        reverse: bool,
    ) -> Result<Vec<(Vec<u8>, u128)>, PersistError> {
        EncryptedStore::scan_index_keyed(self, first, last, most, reverse)
    }
}

impl<E: Engine> IndexScan for Viewed<'_, E> {
    fn scan_index_keyed(
        &self,
        first: &[u8],
        last: &[u8],
        most: usize,
        reverse: bool,
    ) -> Result<Vec<(Vec<u8>, u128)>, PersistError> {
        self.store
            .scan_index_in(self.reads.as_ref(), (first, last), most, reverse)
    }
}
