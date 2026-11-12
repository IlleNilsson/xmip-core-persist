//! A Work Process's Instances and keys, as Xmip Storage keeps them:
//! two tables of their own in the runtime database, the Ledger
//! (`runtime-model.md` section 22, *Process State belongs to the
//! cluster*; the owner, 2026-10-14: every type of data its own table).
//!
//! - **Process Instance**: an Instance's state in the form
//!   `xmip-core-process` writes, kept whole, by the Instance's identifier,
//!   with how many turns it has taken and whether it has ended.
//! - **Instance Key**: each value an Instance is correlated by — its Work
//!   Process, Correlation and key, named — and the Instance it belongs to.
//!
//! Both are written in a hand-on and in nothing else, so an Instance moves
//! on exactly when the Message that moved it does ([`super::HandOn`]). An
//! Instance is written only over the turn it was read at: two nodes taking
//! two turns of one Instance from the same state never both write, and the
//! later hand-on writes nothing, its claim kept for its holder to release.
//! A key is written only where it belongs to no Instance or to the one
//! writing it, in the same write: two first arrivals of one key that each
//! start an Instance never both do, and the later one, taken again, finds
//! the key and joins the first. A key is let go only by the Instance it
//! belongs to.

use codec::cursor::Cursor;
use codec::field;
use codec::form::Form;
use codec::writer::ByteWriter;

use super::commit::Batch;
use super::hand_on::HandOn;
use crate::{EncryptedStore, Engine, PersistError};

/// Where the Process Instance table keeps an Instance, by its identifier.
pub(crate) const INSTANCE: &str = "process-instance";

/// Where the Instance Key table keeps a key, by its identifier.
pub(crate) const INSTANCE_KEY: &str = "process-instance-key";

/// One Instance as the Ledger keeps it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstanceRecord {
    pub instance: u128,
    /// The Work Process it is an Instance of.
    pub work_process: String,
    /// How many turns it has taken.
    pub turns: u64,
    pub ended: bool,
    /// Its state, in `xmip-core-process`'s form.
    pub body: Vec<u8>,
}

/// An Instance written by a hand-on: the record, and the turns the one it
/// replaces had — zero for a new Instance, which no record may stand at.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstanceWrite {
    pub record: InstanceRecord,
    pub over: u64,
}

/// A key an Instance is correlated by, and the Instance it belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InstanceKey {
    /// The key's identifier: its Work Process, Correlation and key, named
    /// (`super::named`).
    pub key: u128,
    pub instance: u128,
}

impl Form for InstanceRecord {
    fn write(&self, out: &mut Vec<u8>) {
        out.u128_be(self.instance);
        field::text(out, &self.work_process);
        out.u64_be(self.turns);
        out.byte(u8::from(self.ended));
        field::counted(out, &self.body);
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        Ok(Self {
            instance: cursor.u128_be()?,
            work_process: field::read_text(cursor)?,
            turns: cursor.u64_be()?,
            ended: cursor.byte()? != 0,
            body: field::read_counted(cursor)?.to_vec(),
        })
    }
}

/// How many turns the Instance kept as `bytes` has taken, read without its
/// state: what the one writer tells a waiter on it (`super::watch`).
pub(crate) fn turns_of(bytes: &[u8]) -> Option<u64> {
    let mut cursor = Cursor::new(bytes);
    cursor.u128_be().ok()?;
    field::read_text(&mut cursor).ok()?;
    cursor.u64_be().ok()
}

impl Form for InstanceWrite {
    fn write(&self, out: &mut Vec<u8>) {
        self.record.write(out);
        out.u64_be(self.over);
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        Ok(Self {
            record: InstanceRecord::read(cursor)?,
            over: cursor.u64_be()?,
        })
    }
}

impl Form for InstanceKey {
    fn write(&self, out: &mut Vec<u8>) {
        out.u128_be(self.key);
        out.u128_be(self.instance);
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        Ok(Self {
            key: cursor.u128_be()?,
            instance: cursor.u128_be()?,
        })
    }
}

/// Whether every Instance of `writes` stands, as `batch` has left it, at
/// the turn its write is over, and every key of `keys` belongs to no
/// Instance or to the one it names: nothing is written unless all do, so
/// two first arrivals of one key never both start an Instance by it.
pub(crate) fn current<R: Engine>(
    store: &EncryptedStore<R>,
    batch: &Batch<'_>,
    writes: &[InstanceWrite],
    keys: &[InstanceKey],
) -> Result<bool, PersistError> {
    for write in writes {
        let key = write.record.instance.to_be_bytes();
        let stands = match batch.read(store, INSTANCE, &key)? {
            Some(bytes) => InstanceRecord::from_bytes(&bytes)?.turns,
            None => 0,
        };
        if stands != write.over {
            return Ok(false);
        }
    }
    for key in keys {
        let owner = owner(store, batch, key.key)?;
        if owner.is_some_and(|owner| owner != key.instance) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The Instance the key `key` belongs to, as `batch` has left it.
fn owner<R: Engine>(
    store: &EncryptedStore<R>,
    batch: &Batch<'_>,
    key: u128,
) -> Result<Option<u128>, PersistError> {
    batch
        .read(store, INSTANCE_KEY, &key.to_be_bytes())?
        .map(|bytes| InstanceKey::from_bytes(&bytes).map(|found| found.instance))
        .transpose()
        .map_err(PersistError::from)
}

/// `writes` and `keys` written in `batch`, and each of `unkeyed` taken out
/// where it still belongs to the Instance it names: a key another Instance
/// took since is that one's.
pub(crate) fn write<R: Engine>(
    store: &EncryptedStore<R>,
    batch: &mut Batch<'_>,
    (writes, keys): (&[InstanceWrite], &[InstanceKey]),
    unkeyed: &[InstanceKey],
) -> Result<(), PersistError> {
    for write in writes {
        batch.record(INSTANCE, write.record.instance, &write.record);
    }
    for key in keys {
        batch.record(INSTANCE_KEY, key.key, key);
    }
    for gone in unkeyed {
        if owner(store, batch, gone.key)? == Some(gone.instance) {
            batch.put(INSTANCE_KEY, gone.key.to_be_bytes().to_vec(), None);
        }
    }
    Ok(())
}

/// A Work Process's turn, handed on as one write: the Message's hand-on,
/// the Instances it moved, the keys it correlated and the keys it let go,
/// each named with the Instance it belongs to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Turn {
    pub hand_on: HandOn,
    pub instances: Vec<InstanceWrite>,
    pub keys: Vec<InstanceKey>,
    pub unkeyed: Vec<InstanceKey>,
}

impl Form for Turn {
    fn write(&self, out: &mut Vec<u8>) {
        self.hand_on.write(out);
        self.instances.write(out);
        self.keys.write(out);
        self.unkeyed.write(out);
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        Ok(Self {
            hand_on: HandOn::read(cursor)?,
            instances: Vec::read(cursor)?,
            keys: Vec::read(cursor)?,
            unkeyed: Vec::read(cursor)?,
        })
    }
}

/// `turn` decided in `batch` at `now`: nothing where an Instance it moves
/// no longer stands where its turn read it, a key it correlates is another
/// Instance's, or the claim is not its holder's; else its hand-on, its
/// Instances and its keys, all in the one write.
pub(crate) fn turn<R: Engine>(
    store: &EncryptedStore<R>,
    batch: &mut Batch<'_>,
    turn: &Turn,
    now: i128,
) -> Result<bool, PersistError> {
    if !current(store, batch, &turn.instances, &turn.keys)? {
        return Ok(false);
    }
    if !super::claim::hand_on(store, batch, &turn.hand_on, now)? {
        return Ok(false);
    }
    write(store, batch, (&turn.instances, &turn.keys), &turn.unkeyed)?;
    Ok(true)
}

/// The Instance `instance`, where the Ledger keeps one.
pub(crate) fn read_instance<R: Engine>(
    store: &EncryptedStore<R>,
    instance: u128,
) -> Result<Option<InstanceRecord>, PersistError> {
    store
        .get(INSTANCE, &instance.to_be_bytes())?
        .map(|bytes| InstanceRecord::from_bytes(&bytes))
        .transpose()
        .map_err(PersistError::from)
}

/// The Instance the key `key` belongs to, where one does.
pub(crate) fn read_instance_key<R: Engine>(
    store: &EncryptedStore<R>,
    key: u128,
) -> Result<Option<u128>, PersistError> {
    store
        .get(INSTANCE_KEY, &key.to_be_bytes())?
        .map(|bytes| InstanceKey::from_bytes(&bytes).map(|found| found.instance))
        .transpose()
        .map_err(PersistError::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_instance_write_and_a_key_come_back_from_their_bytes() {
        let write = InstanceWrite {
            record: InstanceRecord {
                instance: 7,
                work_process: "OrderApproval".into(),
                turns: 3,
                ended: false,
                body: vec![1, 2, 3],
            },
            over: 2,
        };
        assert_eq!(
            InstanceWrite::from_bytes(&write.bytes()).expect("reads"),
            write
        );
        let key = InstanceKey {
            key: 9,
            instance: 7,
        };
        assert_eq!(InstanceKey::from_bytes(&key.bytes()).expect("reads"), key);
    }
}
