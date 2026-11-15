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
//!
//! **A record its writer keeps is never read** (2026-11-15): what the
//! writer's earlier batches wrote of it, as they left it, is what the batch
//! finds there before it ([`super::kept`]), read neither from the engine
//! nor decrypted again. And a record it replaces is laid out once, over
//! what the batch read of it ([`Batch::replaced`]): stamped, and only the
//! index entries the two differ in written.

use std::cell::RefCell;
use std::collections::HashMap;

use codec::form::Form;

use super::super::columns::{Columns, Laid};
use super::super::row::Columned;
use super::super::schema::Database;
use super::kept::Kept;
use crate::encrypted_store::place::place;
use crate::{EncryptedStore, Engine, PersistError, RecordChange};

/// One batch being decided: what it has written so far, by place — or one
/// operation's part of it, staged over the batch below, which takes it only
/// once the whole operation has been decided. An operation that fails
/// half-way is dropped with its stage, so nothing of it is written, while
/// the others in its batch are.
///
/// Every place is found by its bytes, as the store seals it for
/// (`EncryptedStore`'s place), made once a lookup and on the stack; and
/// each value written is kept once, in `changes`, where `written` finds
/// it (2026-11-15).
pub(crate) struct Batch<'a> {
    pub(super) below: Option<&'a Batch<'a>>,
    /// Where in `changes` the last write of each place this batch wrote is,
    /// by its place.
    pub(super) written: HashMap<Vec<u8>, usize>,
    pub(super) changes: Vec<RecordChange<'static>>,
    /// The changes to the index entries of each record the batch laid out
    /// as it made it ([`Batch::replaced`]), by its place.
    pub(super) laid: Laid,
    /// Xmip Storage's clock as the batch is decided, as a column keeps it:
    /// what a record it makes new is stamped with.
    now: u64,
    /// What the store held of each record the batch read, as it held it
    /// before the batch: read once, however many of its operations — and
    /// its laying out after them — ask (the bottom batch's alone).
    kept: RefCell<ByPlace>,
    /// What its writer keeps of the records it wrote, lent to the batch
    /// while it is decided (the bottom batch's alone).
    writer: Kept,
}

/// Each record's value by its place: `None` where there is none.
type ByPlace = HashMap<Vec<u8>, Option<Vec<u8>>>;

impl Batch<'_> {
    /// A batch with nothing in it yet, over what is written, decided at
    /// `now`, Xmip Storage's clock as a column keeps it: every record it
    /// reads read from the store.
    pub(crate) fn empty(now: u64) -> Batch<'static> {
        Batch::over(now, Kept::default())
    }

    /// As [`Batch::empty`], each record `writer` keeps read from it
    /// ([`super::kept`]), lent until [`Batch::lent`] gives it back.
    pub(super) fn over(now: u64, writer: Kept) -> Batch<'static> {
        Batch {
            below: None,
            written: HashMap::new(),
            changes: Vec::new(),
            laid: HashMap::new(),
            now,
            kept: RefCell::default(),
            writer,
        }
    }

    /// What its writer keeps, given back.
    pub(super) fn lent(&mut self) -> Kept {
        std::mem::take(&mut self.writer)
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
            writer: Kept::default(),
        }
    }

    /// `kind` `key`, a place the batch makes new, held nothing before it:
    /// kept so, and never read from the store. A place read already keeps
    /// what the read found.
    pub(crate) fn absent(&self, kind: &'static str, key: &[u8]) {
        if let Some(below) = self.below {
            return below.absent(kind, key);
        }
        let at = place(kind, key);
        let mut kept = self.kept.borrow_mut();
        if !kept.contains_key(&*at) {
            kept.insert(at.to_vec(), None);
        }
    }

    /// Whether this batch, or one below it, has written the place `at`.
    fn wrote(&self, at: &[u8]) -> bool {
        let here = !self.written.is_empty() && self.written.contains_key(at);
        here || self.below.is_some_and(|below| below.wrote(at))
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
        record: T,
    ) -> Result<(), PersistError> {
        self.absent(kind, &key);
        self.replaced(kind, key, record, None)
    }

    /// `record` written at `kind` `key` over `before`, what this batch read
    /// there ([`Batch::read`]) — the record it replaces, or none: stamped
    /// over it and laid out once, its index entries those `before` has and
    /// it has not taken out and those it has and `before` has not put in
    /// (`super::super::columns`), so the laying out after the batch neither
    /// reads `before` again nor decodes either to stamp it (2026-11-15).
    /// Where the batch wrote the place already, `before` is not what the
    /// store holds: it is written as any record is, and laid out with the
    /// rest.
    ///
    /// # Errors
    ///
    /// Where its table is not one the schema knows.
    pub(crate) fn replaced<T: Columned>(
        &mut self,
        kind: &'static str,
        key: Vec<u8>,
        mut record: T,
        before: Option<&T>,
    ) -> Result<(), PersistError> {
        let at = place(kind, &key);
        if self.wrote(&at) {
            self.put(kind, key, Some(record.bytes()));
            return Ok(());
        }
        let columns = Columns::of(Database::Runtime);
        let (bytes, entries) = columns.laid(kind, &mut record, before, self.now)?;
        self.laid.insert(at.to_vec(), entries);
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
        self.read_at(store, (kind, key), &place(kind, key))
    }

    /// [`Batch::read`] of the place `at`, `kind` `key`'s.
    fn read_at<R: Engine>(
        &self,
        store: &EncryptedStore<R>,
        (kind, key): (&'static str, &[u8]),
        at: &[u8],
    ) -> Result<Option<Vec<u8>>, PersistError> {
        // A stage that wrote nothing yet is not asked.
        let written = if self.written.is_empty() {
            None
        } else {
            self.written.get(at)
        };
        match (written, self.below) {
            (Some(&change), _) => Ok(self.changes[change].2.clone()),
            (None, Some(below)) => below.read_at(store, (kind, key), at),
            (None, None) => self.before_at(store, (kind, key), at),
        }
    }

    /// The value `kind` `key` held in `store` before this batch, whatever
    /// the batch has written of it since: as its writer keeps it, where it
    /// does; else read from the store the first time it is asked, and from
    /// what that read found after.
    pub(crate) fn before<R: Engine>(
        &self,
        store: &EncryptedStore<R>,
        kind: &'static str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, PersistError> {
        self.before_at(store, (kind, key), &place(kind, key))
    }

    /// [`Batch::before`] of the place `at`, `kind` `key`'s.
    fn before_at<R: Engine>(
        &self,
        store: &EncryptedStore<R>,
        (kind, key): (&'static str, &[u8]),
        at: &[u8],
    ) -> Result<Option<Vec<u8>>, PersistError> {
        if let Some(below) = self.below {
            return below.before_at(store, (kind, key), at);
        }
        if let Some(kept) = self.kept.borrow().get(at) {
            return Ok(kept.clone());
        }
        if let Some(kept) = self.writer.get(at) {
            return Ok(kept.clone());
        }
        let kept = store.get(kind, key)?;
        self.kept.borrow_mut().insert(at.to_vec(), kept.clone());
        Ok(kept)
    }

    /// What this batch itself has written of `kind`, by key: the value it
    /// leaves there, or `None` where it takes it out.
    pub(crate) fn written_of(
        &self,
        kind: &'static str,
    ) -> impl Iterator<Item = (&[u8], Option<&[u8]>)> {
        self.written
            .values()
            .map(|&change| &self.changes[change])
            .filter(move |(written, _, _)| *written == kind)
            .map(|(_, key, value)| (key.as_slice(), value.as_deref()))
    }

    pub(crate) fn put(&mut self, kind: &'static str, key: Vec<u8>, value: Option<Vec<u8>>) {
        let change = self.changes.len();
        let at = place(kind, &key);
        match self.written.get_mut(&*at) {
            Some(last) => *last = change,
            None => {
                self.written.insert(at.to_vec(), change);
            }
        }
        self.changes.push((kind, key, value));
    }

    /// What one operation's stage over this batch wrote — where in its
    /// `changes` each place's last write is, the changes, and what it laid
    /// out — taken into it once the operation is decided whole: its changes
    /// after this batch's, in their order, each place found where it is
    /// among them.
    pub(super) fn taken(
        &mut self,
        (written, changes, laid): (HashMap<Vec<u8>, usize>, Vec<RecordChange<'static>>, Laid),
    ) {
        let first = self.changes.len();
        self.changes.extend(changes);
        let moved = written.into_iter().map(|(at, change)| (at, first + change));
        self.written.extend(moved);
        self.laid.extend(laid);
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
