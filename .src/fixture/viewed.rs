//! The engines in memory read in one consistent view: their records held
//! for the view's length ([`crate::Engine::view`]).

use std::collections::BTreeMap;
use std::sync::MutexGuard;
use std::sync::atomic::Ordering;

use super::Counted;
use crate::{Entry, PersistError, Reads};

/// The engine in memory, its records held.
pub(super) struct MemoryView<'a>(pub(super) MutexGuard<'a, BTreeMap<Vec<u8>, Vec<u8>>>);

impl Reads for MemoryView<'_> {
    fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, PersistError> {
        Ok(self.0.get(key).cloned())
    }

    fn scan(
        &self,
        first: &[u8],
        last: &[u8],
        most: usize,
        reverse: bool,
    ) -> Result<Vec<Entry>, PersistError> {
        Ok(ranged(&self.0, (first, last), most, reverse))
    }
}

/// The records from `first` to `last` of `records`, both included, in
/// order or the reverse, up to `most`.
pub(super) fn ranged(
    records: &BTreeMap<Vec<u8>, Vec<u8>>,
    (first, last): (&[u8], &[u8]),
    most: usize,
    reverse: bool,
) -> Vec<Entry> {
    if first > last {
        return Vec::new();
    }
    let range = records.range(first.to_vec()..=last.to_vec());
    let copied = |(key, value): (&Vec<u8>, &Vec<u8>)| (key.clone(), value.clone());
    if reverse {
        range.rev().take(most).map(copied).collect()
    } else {
        range.take(most).map(copied).collect()
    }
}

/// The engine that counts, its records held, still counting.
pub(super) struct CountedView<'a> {
    pub(super) view: MemoryView<'a>,
    pub(super) counted: &'a Counted,
}

impl Reads for CountedView<'_> {
    fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, PersistError> {
        self.counted.reads.fetch_add(1, Ordering::SeqCst);
        self.view.read(key)
    }

    fn scan(
        &self,
        first: &[u8],
        last: &[u8],
        most: usize,
        reverse: bool,
    ) -> Result<Vec<Entry>, PersistError> {
        self.counted.ranges.fetch_add(1, Ordering::SeqCst);
        let found = self.view.scan(first, last, most, reverse)?;
        self.counted.found.fetch_add(found.len(), Ordering::SeqCst);
        Ok(found)
    }
}
