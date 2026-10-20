//! One batch the embedded Storage node's writer decides (`super::commit`):
//! what it has written so far, by place, read back by every operation
//! decided after it in the same batch.

use std::collections::HashMap;

use super::super::record::Form;
use crate::{EncryptedStore, Engine, PersistError, RecordChange};

/// One batch being decided: what it has written so far, by place — or one
/// operation's part of it, staged over the batch below, which takes it only
/// once the whole operation has been decided. An operation that fails
/// half-way is dropped with its stage, so nothing of it is written, while
/// the others in its batch are.
pub(crate) struct Batch<'a> {
    pub(super) below: Option<&'a Batch<'a>>,
    pub(super) written: HashMap<(&'static str, Vec<u8>), Option<Vec<u8>>>,
    pub(super) changes: Vec<RecordChange<'static>>,
}

impl Batch<'_> {
    /// A batch with nothing in it yet, over what is written.
    pub(crate) fn empty() -> Batch<'static> {
        Batch {
            below: None,
            written: HashMap::new(),
            changes: Vec::new(),
        }
    }

    /// One operation's stage over this batch.
    pub(super) fn staged(&self) -> Batch<'_> {
        Batch {
            below: Some(self),
            written: HashMap::new(),
            changes: Vec::new(),
        }
    }

    /// The value `kind` `key` holds in `store`, as this batch, and every
    /// batch below it, has left it.
    pub(crate) fn read<R: Engine>(
        &self,
        store: &EncryptedStore<R>,
        kind: &'static str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, PersistError> {
        match (self.written.get(&(kind, key.to_vec())), self.below) {
            (Some(value), _) => Ok(value.clone()),
            (None, Some(below)) => below.read(store, kind, key),
            (None, None) => store.get(kind, key),
        }
    }

    pub(crate) fn put(&mut self, kind: &'static str, key: Vec<u8>, value: Option<Vec<u8>>) {
        self.written.insert((kind, key.clone()), value.clone());
        self.changes.push((kind, key, value));
    }

    /// `record` under its identifier `id`, replacing the last.
    pub(crate) fn record(&mut self, kind: &'static str, id: u128, record: &impl Form) {
        self.put(kind, id.to_be_bytes().to_vec(), Some(record.bytes()));
    }
}
