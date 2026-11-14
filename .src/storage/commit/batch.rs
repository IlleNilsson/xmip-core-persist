//! One batch the embedded Storage node's writer decides (`super::commit`):
//! what it has written so far, by place, read back by every operation
//! decided after it in the same batch; and what the store held of each
//! record it read, read from the store once a batch.
//!
//! **A place the batch makes new is never read** (2026-11-14): a queue's
//! next place, and a Publication's Message and Journeys, whose identifiers
//! are drawn fresh, hold nothing before the batch, so the batch keeps them
//! as found empty ([`Batch::absent`]) and no operation, nor the laying out
//! after them, reads the store for them. A record it makes new is laid out
//! as it is made ([`Batch::created`]): stamped, and its index entries taken
//! from it as it is, never decoded again to be stamped.

use std::cell::RefCell;
use std::collections::HashMap;

use codec::form::Form;

use super::super::columns::{Columns, Laid};
use super::super::row::Columned;
use super::super::schema::Database;
use crate::{EncryptedStore, Engine, PersistError, RecordChange};

/// One batch being decided: what it has written so far, by place — or one
/// operation's part of it, staged over the batch below, which takes it only
/// once the whole operation has been decided. An operation that fails
/// half-way is dropped with its stage, so nothing of it is written, while
/// the others in its batch are.
pub(crate) struct Batch<'a> {
    pub(super) below: Option<&'a Batch<'a>>,
    pub(super) written: ByPlace,
    pub(super) changes: Vec<RecordChange<'static>>,
    /// The index entries of each record the batch made new, laid out as it
    /// was made ([`Batch::created`]), by its place.
    pub(super) laid: Laid<'static>,
    /// Xmip Storage's clock as the batch is decided, as a column keeps it:
    /// what a record it makes new is stamped with.
    now: u64,
    /// What the store held of each record the batch read, as it held it
    /// before the batch: read once, however many of its operations — and
    /// its laying out after them — ask (the bottom batch's alone).
    kept: RefCell<ByPlace>,
}

/// Each record's value by its place, its kind and its key: `None` where
/// there is none.
pub(super) type ByPlace = HashMap<(&'static str, Vec<u8>), Option<Vec<u8>>>;

impl Batch<'_> {
    /// A batch with nothing in it yet, over what is written, decided at
    /// `now`, Xmip Storage's clock as a column keeps it.
    pub(crate) fn empty(now: u64) -> Batch<'static> {
        Batch {
            below: None,
            written: HashMap::new(),
            changes: Vec::new(),
            laid: HashMap::new(),
            now,
            kept: RefCell::default(),
        }
    }

    /// One operation's stage over this batch.
    pub(super) fn staged(&self) -> Batch<'_> {
        Batch {
            below: Some(self),
            written: HashMap::new(),
            changes: Vec::new(),
            laid: HashMap::new(),
            now: self.now,
            kept: RefCell::default(),
        }
    }

    /// `kind` `key`, a place the batch makes new, held nothing before it:
    /// kept so, and never read from the store. A place read already keeps
    /// what the read found.
    pub(crate) fn absent(&self, kind: &'static str, key: &[u8]) {
        if let Some(below) = self.below {
            return below.absent(kind, key);
        }
        self.kept
            .borrow_mut()
            .entry((kind, key.to_vec()))
            .or_insert(None);
    }

    /// Whether this batch, or one below it, has written `kind` `key`.
    fn wrote(&self, kind: &'static str, key: &[u8]) -> bool {
        let here = !self.written.is_empty() && self.written.contains_key(&(kind, key.to_vec()));
        here || self.below.is_some_and(|below| below.wrote(kind, key))
    }

    /// `record` made new at `kind` `key`, a place nothing was at before the
    /// batch: kept as found empty ([`Batch::absent`]), stamped at the
    /// batch's now and laid out as it is (`super::super::columns`), so the
    /// laying out after the batch neither reads what it replaces nor
    /// decodes it to stamp it. Where the batch wrote the place already, it
    /// is written as any record is, and laid out with the rest.
    ///
    /// # Errors
    ///
    /// Where its table is not one the schema knows.
    pub(crate) fn created<T: Columned>(
        &mut self,
        kind: &'static str,
        key: Vec<u8>,
        mut record: T,
    ) -> Result<(), PersistError> {
        self.absent(kind, &key);
        if self.wrote(kind, &key) {
            self.put(kind, key, Some(record.bytes()));
            return Ok(());
        }
        let columns = Columns::of(Database::Runtime);
        let (bytes, entries) = columns.laid(kind, &mut record, self.now)?;
        self.laid.insert((kind, key.clone()), entries);
        self.put(kind, key, Some(bytes));
        Ok(())
    }

    /// The value `kind` `key` holds in `store`, as this batch, and every
    /// batch below it, has left it.
    pub(crate) fn read<R: Engine>(
        &self,
        store: &EncryptedStore<R>,
        kind: &'static str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, PersistError> {
        // A stage that wrote nothing yet is not asked: its place's key is
        // never made.
        let written = if self.written.is_empty() {
            None
        } else {
            self.written.get(&(kind, key.to_vec()))
        };
        match (written, self.below) {
            (Some(value), _) => Ok(value.clone()),
            (None, Some(below)) => below.read(store, kind, key),
            (None, None) => self.before(store, kind, key),
        }
    }

    /// The value `kind` `key` held in `store` before this batch, whatever
    /// the batch has written of it since: read from the store the first
    /// time it is asked, and from what that read found after.
    pub(crate) fn before<R: Engine>(
        &self,
        store: &EncryptedStore<R>,
        kind: &'static str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, PersistError> {
        if let Some(below) = self.below {
            return below.before(store, kind, key);
        }
        let place = (kind, key.to_vec());
        if let Some(kept) = self.kept.borrow().get(&place) {
            return Ok(kept.clone());
        }
        let kept = store.get(kind, key)?;
        self.kept.borrow_mut().insert(place, kept.clone());
        Ok(kept)
    }

    /// What this batch itself has written of `kind`, by key: the value it
    /// leaves there, or `None` where it takes it out.
    pub(crate) fn written_of(
        &self,
        kind: &'static str,
    ) -> impl Iterator<Item = (&[u8], Option<&[u8]>)> {
        self.written
            .iter()
            .filter(move |((written, _), _)| *written == kind)
            .map(|((_, key), value)| (key.as_slice(), value.as_deref()))
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

/// A sequence number as kept, zero where none is.
pub(crate) fn sequence<R: Engine>(
    store: &EncryptedStore<R>,
    which: &[u8],
) -> Result<u64, PersistError> {
    let Some(bytes) = store.get(super::SEQUENCE, which)? else {
        return Ok(0);
    };
    let array: [u8; 8] = bytes
        .try_into()
        .map_err(|_| super::malformed("an audit sequence that is not eight bytes"))?;
    Ok(u64::from_be_bytes(array))
}

/// The failure of a batch, said again to each write that was in it.
pub(super) fn again(error: &PersistError) -> PersistError {
    match error {
        PersistError::Refused { scope, reason } => PersistError::Refused {
            scope: scope.clone(),
            reason: reason.clone(),
        },
        PersistError::Engine { engine, reason } => PersistError::Engine {
            engine,
            reason: reason.clone(),
        },
        other => super::malformed(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::Memory;

    #[test]
    fn a_record_is_read_from_the_store_once_a_batch_and_as_the_batch_left_it() {
        let keys = secret::Held::new(secret::fixture::Memory::default());
        let kek = secret::KekName::new("runtime").expect("a name");
        let store = EncryptedStore::open(Memory::default(), &keys, &kek).expect("open");
        store.put("journey", b"7", b"before").expect("put");
        let mut batch = Batch::empty(0);
        assert_eq!(
            batch.read(&store, "journey", b"7").expect("read"),
            Some(b"before".to_vec())
        );
        // Changed beneath it: the batch reads what it read.
        store.put("journey", b"7", b"since").expect("put");
        assert_eq!(
            batch.before(&store, "journey", b"7").expect("read"),
            Some(b"before".to_vec())
        );
        batch.put("journey", b"7".to_vec(), Some(b"written".to_vec()));
        let staged = batch.staged();
        assert_eq!(
            staged.read(&store, "journey", b"7").expect("read"),
            Some(b"written".to_vec()),
            "as the batch left it"
        );
        assert_eq!(
            staged.before(&store, "journey", b"7").expect("read"),
            Some(b"before".to_vec()),
            "and what was there before it"
        );
        assert_eq!(staged.before(&store, "journey", b"8").expect("read"), None);
    }
}
