//! A store's index entries: the one engine key not hashed, so it sorts
//! (`EncryptedStore::apply_indexed`, `EncryptedStore::scan_index`). The
//! caller builds it from its values in the clear; its value, the
//! identifier of the record it finds, is sealed with its whole key as
//! associated data.

use super::viewed::Now;
use super::{EncryptedStore, IndexEntry, RecordChange};
use crate::{Change, Engine, PersistError, Reads};
use xcore::spent::on_io;

/// What every index entry's engine key opens with, so the entries sort
/// together, apart from the records: a keyed hash begins with these seven
/// bytes once in 2^56, and is refused by its tag if a range ever meets one.
const INDEX: &[u8] = b"\0index\0";

impl<E: Engine> EncryptedStore<E> {
    /// Every change of `changes` and every index entry of `entries` as one
    /// write through the engine's [`Engine::apply`], all or none, durable
    /// on return, so an index never disagrees with its records. An entry is
    /// its key after the index mark and the identifier of the record it
    /// finds, sealed with its whole key as associated data — or `None`,
    /// removing it.
    ///
    /// # Errors
    ///
    /// As [`EncryptedStore::apply`]; nothing is written then.
    pub fn apply_indexed(
        &self,
        changes: &[RecordChange<'_>],
        entries: &[IndexEntry],
    ) -> Result<(), PersistError> {
        let indexed = self.indexed(changes, entries)?;
        on_io(|| self.engine.apply(&indexed))
    }

    /// As [`EncryptedStore::apply_indexed`], through the engine's
    /// [`Engine::apply_deferred`]: durable with the next write durable on
    /// return, or the next [`EncryptedStore::sync`], not on its own.
    ///
    /// # Errors
    ///
    /// As [`EncryptedStore::apply`]; nothing is written then.
    pub fn apply_indexed_deferred(
        &self,
        changes: &[RecordChange<'_>],
        entries: &[IndexEntry],
    ) -> Result<(), PersistError> {
        let indexed = self.indexed(changes, entries)?;
        on_io(|| self.engine.apply_deferred(&indexed))
    }

    /// `changes` sealed, and every index entry of `entries` sealed with
    /// its whole key as associated data: one batch for the engine.
    fn indexed(
        &self,
        changes: &[RecordChange<'_>],
        entries: &[IndexEntry],
    ) -> Result<Vec<Change>, PersistError> {
        let mut batch = self.sealed(changes)?;
        for (key, record) in entries {
            let key = [INDEX, key.as_slice()].concat();
            let sealed = record
                .map(|record| self.seal(&key, &record.to_be_bytes()))
                .transpose()?;
            batch.push((key, sealed));
        }
        Ok(batch)
    }

    /// The records the index entries from `first` to `last` find, both
    /// included, each key after the index mark, in their order — or the
    /// reverse — up to `most`: one range of the engine, each authenticated.
    ///
    /// # Errors
    ///
    /// [`PersistError::Refused`] when an entry fails its tag, and
    /// [`PersistError::Engine`] when the engine cannot read.
    pub fn scan_index(
        &self,
        first: &[u8],
        last: &[u8],
        most: usize,
        reverse: bool,
    ) -> Result<Vec<u128>, PersistError> {
        let entries = self.scan_index_keyed(first, last, most, reverse)?;
        Ok(entries.into_iter().map(|(_, record)| record).collect())
    }

    /// As [`EncryptedStore::scan_index`], each record with its entry's key
    /// after the index mark: the values the entry was kept by, in the
    /// clear, authenticated with it.
    ///
    /// # Errors
    ///
    /// As [`EncryptedStore::scan_index`].
    pub fn scan_index_keyed(
        &self,
        first: &[u8],
        last: &[u8],
        most: usize,
        reverse: bool,
    ) -> Result<Vec<(Vec<u8>, u128)>, PersistError> {
        self.scan_index_in(&Now(&self.engine), (first, last), most, reverse)
    }

    /// [`EncryptedStore::scan_index_keyed`] through `reads`.
    pub(super) fn scan_index_in(
        &self,
        reads: &dyn Reads,
        (first, last): (&[u8], &[u8]),
        most: usize,
        reverse: bool,
    ) -> Result<Vec<(Vec<u8>, u128)>, PersistError> {
        self.alive()?;
        let (first, last) = ([INDEX, first].concat(), [INDEX, last].concat());
        let entries = reads.scan(&first, &last, most, reverse)?;
        entries
            .into_iter()
            .map(|(key, stored)| {
                let record = self.opened(&key, &stored, &|| "index entry".to_string())?;
                let record: [u8; 16] = record.try_into().map_err(|_| PersistError::Refused {
                    scope: format!("{} index entry", self.engine.engine()),
                    reason: "not a record's identifier".to_string(),
                })?;
                let key = key.get(INDEX.len()..).unwrap_or_default().to_vec();
                Ok((key, u128::from_be_bytes(record)))
            })
            .collect()
    }
}
