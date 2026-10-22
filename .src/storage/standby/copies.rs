//! Every write a Storage node's databases take, numbered in the order its
//! engines took it and kept for its standbys, each database on its own:
//! what a primary hands on, and what a standby took from it.
//!
//! A write is numbered and kept under the same lock as it is written, so
//! the numbers are the engine's own order: the writer's batches, a
//! Stream's chunks and the audit keeper's moves interleave as they were
//! written, and a standby that writes them in that order holds what the
//! primary held. What is kept is the engine's change — a keyed hash and a
//! sealed record — so a standby keeping the same data key holds the same
//! records without opening one (`super`). A store's own keys, its data key
//! among them, are never copied ([`OWN`]).

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use crate::encrypted_store::OWN;
use crate::{Change, Engine, Entry, PersistError};

/// The databases a Storage node keeps, in every list here: the runtime,
/// the administration and the audit database.
pub const DATABASES: usize = 3;

/// Where a standby keeps, in each database, how far it has copied: the
/// history it follows and the number of the next write it takes, written in
/// the same batch as every write it copies.
pub(crate) const POSITION: &[u8] = b"\0xmip-core-persist/position\0";

/// One write as kept: its number, and its changes.
pub type Copy = (u64, Arc<[Change]>);

/// One database's writes as kept, oldest first.
#[derive(Default)]
struct Kept {
    /// The number the next write takes.
    next: u64,
    writes: VecDeque<Copy>,
    bytes: usize,
}

impl Kept {
    fn keep(&mut self, changes: Arc<[Change]>, most: usize) {
        self.bytes += size(&changes);
        self.writes.push_back((self.next, changes));
        self.next += 1;
        while self.bytes > most && self.writes.len() > 1 {
            if let Some((_, oldest)) = self.writes.pop_front() {
                self.bytes -= size(&oldest);
            }
        }
    }
}

fn size(changes: &[Change]) -> usize {
    changes
        .iter()
        .map(|(key, value)| key.len() + value.as_ref().map_or(0, Vec::len))
        .sum()
}

/// Why a standby cannot take its primary's writes on from where it is.
#[derive(Debug, PartialEq, Eq)]
pub enum Behind {
    /// It follows another history: it was copied from another primary, or
    /// from this one before it started again.
    Another,
    /// What it lacks is no longer kept here.
    Past,
}

/// A Storage node's writes, numbered and kept.
pub struct Copies {
    /// The history the numbers count: the same on a primary and on every
    /// standby following it, new when a node starts one of its own.
    history: Mutex<u128>,
    kept: [Mutex<Kept>; DATABASES],
    /// The most bytes of writes each database keeps for a standby that is
    /// behind; one further behind is copied whole again.
    most: usize,
    /// How many writes were kept, all databases together: what a standby's
    /// shipping waits on.
    told: Mutex<u64>,
    changed: Condvar,
}

impl Copies {
    /// A history `history` whose next write in each database is `next`,
    /// keeping up to `most` bytes of each database's writes.
    #[must_use]
    pub fn new(history: u128, next: [u64; DATABASES], most: usize) -> Self {
        Self {
            history: Mutex::new(history),
            kept: next.map(|next| {
                Mutex::new(Kept {
                    next,
                    ..Kept::default()
                })
            }),
            most,
            told: Mutex::new(0),
            changed: Condvar::new(),
        }
    }

    fn kept(&self, database: usize) -> MutexGuard<'_, Kept> {
        self.kept[database]
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The history the numbers count.
    #[must_use]
    pub fn history(&self) -> u128 {
        *self.history.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The number the next write of each database takes.
    #[must_use]
    pub fn next(&self) -> [u64; DATABASES] {
        [0, 1, 2].map(|database| self.kept(database).next)
    }

    /// Start `history` afresh at `next`, keeping nothing: a standby copied
    /// whole from its primary.
    pub fn restart(&self, history: u128, next: [u64; DATABASES]) {
        let mut kept: Vec<MutexGuard<'_, Kept>> = (0..DATABASES).map(|d| self.kept(d)).collect();
        *self.history.lock().unwrap_or_else(PoisonError::into_inner) = history;
        for (kept, next) in kept.iter_mut().zip(next) {
            **kept = Kept {
                next,
                ..Kept::default()
            };
        }
    }

    fn tell(&self) {
        let mut told = self.told.lock().unwrap_or_else(PoisonError::into_inner);
        *told += 1;
        self.changed.notify_all();
    }

    /// `write` done to `database`, and where it wrote, the changes
    /// `changes` makes kept under the next number — both under the
    /// database's lock, so the numbers are the order the engine took them.
    ///
    /// # Errors
    ///
    /// The write's own; nothing is kept then.
    pub(crate) fn written(
        &self,
        database: usize,
        write: impl FnOnce() -> Result<bool, PersistError>,
        changes: impl FnOnce() -> Vec<Change>,
    ) -> Result<bool, PersistError> {
        let mut kept = self.kept(database);
        let done = write()?;
        if !done {
            return Ok(false);
        }
        let copied: Vec<Change> = changes()
            .into_iter()
            .filter(|(key, _)| !key.starts_with(OWN))
            .collect();
        if !copied.is_empty() {
            kept.keep(copied.into(), self.most);
            drop(kept);
            self.tell();
        }
        Ok(true)
    }

    /// The write `number` of `database`, its primary's, written to `engine`
    /// with the position it reaches in the same batch, and kept under its
    /// number — a standby's copy, in order.
    ///
    /// # Errors
    ///
    /// Where `number` is not the next this database takes, or the engine
    /// refuses; nothing is written then.
    pub(crate) fn copied(
        &self,
        engine: &dyn Engine,
        (database, number): (usize, u64),
        changes: Arc<[Change]>,
    ) -> Result<(), PersistError> {
        let mut kept = self.kept(database);
        if number != kept.next {
            return Err(PersistError::Failed {
                reason: format!(
                    "a copy numbered {number} where the next is {}: one was lost on the way",
                    kept.next
                ),
            });
        }
        let mut batch: Vec<Change> = changes.to_vec();
        batch.push(position(self.history(), number + 1));
        engine.apply_deferred(&batch)?;
        kept.keep(changes, self.most);
        drop(kept);
        self.tell();
        Ok(())
    }

    /// The writes of `database` from the number `from` on, up to `most`
    /// bytes but at least one where there is one.
    ///
    /// # Errors
    ///
    /// [`Behind::Past`] where what follows `from` is no longer kept.
    pub fn since(&self, database: usize, from: u64, most: usize) -> Result<Vec<Copy>, Behind> {
        let kept = self.kept(database);
        if from >= kept.next {
            return Ok(Vec::new());
        }
        let first = kept.writes.front().map_or(kept.next, |(number, _)| *number);
        if from < first {
            return Err(Behind::Past);
        }
        let mut bytes = 0;
        let mut taken = Vec::new();
        for (number, changes) in kept.writes.iter().skip_while(|(n, _)| *n < from) {
            if bytes >= most && !taken.is_empty() {
                break;
            }
            bytes += size(changes);
            taken.push((*number, Arc::clone(changes)));
        }
        Ok(taken)
    }

    /// Whether a standby of `history` whose next writes are `next` can take
    /// this node's writes on from there.
    ///
    /// # Errors
    ///
    /// Why not, where it cannot.
    pub fn follows(&self, history: u128, next: [u64; DATABASES]) -> Result<(), Behind> {
        if history != self.history() {
            return Err(Behind::Another);
        }
        for (database, from) in next.into_iter().enumerate() {
            let kept = self.kept(database);
            let first = kept.writes.front().map_or(kept.next, |(number, _)| *number);
            if from > kept.next {
                return Err(Behind::Another);
            }
            if from < first {
                return Err(Behind::Past);
            }
        }
        Ok(())
    }

    /// How many writes were kept so far, all databases together.
    #[must_use]
    pub fn count(&self) -> u64 {
        *self.told.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Wait until a write is kept after the `seen`-th, or `most` passed.
    pub fn wait(&self, seen: u64, most: Duration) {
        let told = self.told.lock().unwrap_or_else(PoisonError::into_inner);
        if *told == seen {
            let _ = self
                .changed
                .wait_timeout(told, most)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }
}

/// The position record: `history`, and `next`, the number of the next
/// write a standby takes.
pub(crate) fn position(history: u128, next: u64) -> Change {
    let mut value = history.to_be_bytes().to_vec();
    value.extend_from_slice(&next.to_be_bytes());
    (POSITION.to_vec(), Some(value))
}

/// The history and the next number `engine`'s position record holds, or
/// `None` where it holds none: it never copied, or it was a primary since.
///
/// # Errors
///
/// Where the engine cannot be read.
pub(crate) fn positioned(engine: &dyn Engine) -> Result<Option<(u128, u64)>, PersistError> {
    let Some(value) = engine.read(POSITION)? else {
        return Ok(None);
    };
    let (history, next) = value.split_at_checked(16).unwrap_or((&[], &[]));
    let (Ok(history), Ok(next)) = (<[u8; 16]>::try_from(history), <[u8; 8]>::try_from(next)) else {
        return Ok(None);
    };
    Ok(Some((
        u128::from_be_bytes(history),
        u64::from_be_bytes(next),
    )))
}

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

    fn written(
        &self,
        write: impl FnOnce() -> Result<bool, PersistError>,
        changes: impl FnOnce() -> Vec<Change>,
    ) -> Result<bool, PersistError> {
        self.copies.written(self.database, write, changes)
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
        self.written(write, || vec![(key.to_vec(), Some(value.to_vec()))])
            .map(drop)
    }

    fn write_new(&self, key: &[u8], value: &[u8]) -> Result<bool, PersistError> {
        let write = || self.engine.write_new(key, value);
        self.written(write, || vec![(key.to_vec(), Some(value.to_vec()))])
    }

    fn remove(&self, key: &[u8]) -> Result<(), PersistError> {
        let write = || self.engine.remove(key).map(|()| true);
        self.written(write, || vec![(key.to_vec(), None)]).map(drop)
    }

    fn apply(&self, batch: &[Change]) -> Result<(), PersistError> {
        let write = || self.engine.apply(batch).map(|()| true);
        self.written(write, || batch.to_vec()).map(drop)
    }

    fn apply_deferred(&self, batch: &[Change]) -> Result<(), PersistError> {
        let write = || self.engine.apply_deferred(batch).map(|()| true);
        self.written(write, || batch.to_vec()).map(drop)
    }

    fn sync(&self) -> Result<(), PersistError> {
        self.engine.sync()
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::Memory;

    #[test]
    fn writes_are_numbered_in_the_order_written_and_a_store_s_own_keys_stay_home() {
        let copies = Arc::new(Copies::new(7, [1, 1, 1], 1 << 20));
        let engine = Copied::new(Memory::default(), 0, Arc::clone(&copies));
        engine.write(b"a", b"1").expect("written");
        assert!(
            engine
                .write_new(&[OWN, b"key"].concat(), b"k")
                .expect("new")
        );
        assert!(!engine.write_new(b"a", b"2").expect("taken"));
        engine
            .apply_deferred(&[(b"b".to_vec(), Some(b"2".to_vec()))])
            .expect("applied");
        engine.remove(b"a").expect("removed");

        let kept = copies.since(0, 1, usize::MAX).expect("kept");
        let numbers: Vec<u64> = kept.iter().map(|(number, _)| *number).collect();
        assert_eq!(numbers, [1, 2, 3]);
        assert_eq!(kept[2].1.as_ref(), [(b"a".to_vec(), None)]);
        assert_eq!(copies.next(), [4, 1, 1]);
        assert_eq!(copies.since(0, 4, usize::MAX), Ok(Vec::new()));
    }

    #[test]
    fn a_standby_takes_each_copy_in_order_with_its_position_and_refuses_a_gap() {
        let copies = Copies::new(9, [1, 1, 1], 1 << 20);
        let engine = Memory::default();
        let changes: Arc<[Change]> = vec![(b"a".to_vec(), Some(b"1".to_vec()))].into();
        copies
            .copied(&engine, (1, 1), Arc::clone(&changes))
            .expect("the first");
        assert!(copies.copied(&engine, (1, 3), changes).is_err(), "a gap");
        assert_eq!(engine.read(b"a").expect("read"), Some(b"1".to_vec()));
        assert_eq!(positioned(&engine).expect("read"), Some((9, 2)));
        assert_eq!(copies.next(), [1, 2, 1]);
    }

    #[test]
    fn a_standby_too_far_behind_or_of_another_history_cannot_follow() {
        let copies = Arc::new(Copies::new(3, [1, 1, 1], 8));
        let engine = Copied::new(Memory::default(), 2, Arc::clone(&copies));
        for value in [b"aaaa", b"bbbb", b"cccc"] {
            engine.write(b"k", value).expect("written");
        }
        assert_eq!(copies.follows(3, [1, 1, 1]), Err(Behind::Past));
        assert_eq!(copies.follows(4, [1, 1, 4]), Err(Behind::Another));
        assert_eq!(copies.follows(3, [1, 1, 9]), Err(Behind::Another));
        assert_eq!(copies.follows(3, [1, 1, 3]), Ok(()));
        assert_eq!(copies.since(2, 1, 8), Err(Behind::Past));
    }
}
