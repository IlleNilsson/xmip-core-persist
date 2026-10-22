//! A Work Process's Instances and correlations, as Xmip Storage keeps them:
//! two tables of their own in the runtime database, the Ledger
//! (`runtime-model.md` section 22, *Process State belongs to the
//! cluster*; the owner, 2026-10-14: every type of data its own table).
//!
//! - **Process Instance**: an Instance's state in the form
//!   `xmip-core-process` writes, kept whole, by the Instance's identifier,
//!   with how many turns it has taken and whether it has ended.
//! - **Correlation**: a correlation set's key, by the identifier its
//!   Work Process, set and key name, and the Instance it belongs to.
//!
//! Both are written in a hand-on and in nothing else, so an Instance moves
//! on exactly when the Journey that moved it does ([`super::HandOn`]). An
//! Instance is written only over the turn it was read at: two nodes taking
//! two turns of one Instance from the same state never both write, and the
//! later hand-on writes nothing, its claim kept for its holder to release.

use codec::cursor::Cursor;

use super::commit::Batch;
use super::hand_on::HandOn;
use super::record::{
    Form, read_byte, read_bytes, read_text, read_u32, read_u64, read_u128, write_byte, write_bytes,
    write_text, write_u32, write_u64, write_u128,
};
use crate::{EncryptedStore, Engine, PersistError};

/// Where the Process Instance table keeps an Instance, by its identifier.
pub(crate) const INSTANCE: &str = "process-instance";

/// Where the Correlation table keeps a key, by its identifier.
pub(crate) const CORRELATION: &str = "process-correlation";

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

/// A correlation set's key and the Instance it belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Correlation {
    /// The key's identifier: its Work Process, set and key, named
    /// (`super::named`).
    pub correlation: u128,
    pub instance: u128,
}

impl Form for InstanceRecord {
    fn write(&self, out: &mut Vec<u8>) {
        write_u128(out, self.instance);
        write_text(out, &self.work_process);
        write_u64(out, self.turns);
        write_byte(out, u8::from(self.ended));
        write_bytes(out, &self.body);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            instance: read_u128(cursor)?,
            work_process: read_text(cursor)?,
            turns: read_u64(cursor)?,
            ended: read_byte(cursor)? != 0,
            body: read_bytes(cursor)?,
        })
    }
}

impl Form for InstanceWrite {
    fn write(&self, out: &mut Vec<u8>) {
        self.record.write(out);
        write_u64(out, self.over);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            record: InstanceRecord::read(cursor)?,
            over: read_u64(cursor)?,
        })
    }
}

impl Form for Correlation {
    fn write(&self, out: &mut Vec<u8>) {
        write_u128(out, self.correlation);
        write_u128(out, self.instance);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            correlation: read_u128(cursor)?,
            instance: read_u128(cursor)?,
        })
    }
}

/// Whether every Instance of `writes` stands, as `batch` has left it, at
/// the turn its write is over: nothing is written unless all do.
pub(crate) fn current<R: Engine>(
    store: &EncryptedStore<R>,
    batch: &Batch<'_>,
    writes: &[InstanceWrite],
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
    Ok(true)
}

/// `writes` and `correlations` written in `batch`, and `uncorrelated`
/// taken out.
pub(crate) fn write(
    batch: &mut Batch<'_>,
    writes: &[InstanceWrite],
    correlations: &[Correlation],
    uncorrelated: &[u128],
) {
    for write in writes {
        batch.record(INSTANCE, write.record.instance, &write.record);
    }
    for correlation in correlations {
        batch.record(CORRELATION, correlation.correlation, correlation);
    }
    for gone in uncorrelated {
        batch.put(CORRELATION, gone.to_be_bytes().to_vec(), None);
    }
}

/// A Work Process's turn, handed on as one write: the Journey's hand-on,
/// the Instances it moved, the keys it correlated and the keys it let go.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Turn {
    pub hand_on: HandOn,
    pub instances: Vec<InstanceWrite>,
    pub correlations: Vec<Correlation>,
    pub uncorrelated: Vec<u128>,
}

impl Form for Turn {
    fn write(&self, out: &mut Vec<u8>) {
        self.hand_on.write(out);
        self.instances.write(out);
        self.correlations.write(out);
        write_u32(
            out,
            u32::try_from(self.uncorrelated.len()).unwrap_or(u32::MAX),
        );
        for gone in &self.uncorrelated {
            write_u128(out, *gone);
        }
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            hand_on: HandOn::read(cursor)?,
            instances: Vec::read(cursor)?,
            correlations: Vec::read(cursor)?,
            uncorrelated: (0..read_u32(cursor)?)
                .map(|_| read_u128(cursor))
                .collect::<Result<_, _>>()?,
        })
    }
}

/// `turn` decided in `batch` at `now`: nothing where an Instance it moves
/// no longer stands where its turn read it, or where the claim is not its
/// holder's; else its hand-on, its Instances and its correlations, all in
/// the one write.
pub(crate) fn turn<R: Engine>(
    store: &EncryptedStore<R>,
    batch: &mut Batch<'_>,
    turn: &Turn,
    now: i128,
) -> Result<bool, PersistError> {
    if !current(store, batch, &turn.instances)? {
        return Ok(false);
    }
    if !super::claim::hand_on(store, batch, &turn.hand_on, now)? {
        return Ok(false);
    }
    write(
        batch,
        &turn.instances,
        &turn.correlations,
        &turn.uncorrelated,
    );
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
}

/// The Instance the key `correlation` belongs to, where one does.
pub(crate) fn read_correlation<R: Engine>(
    store: &EncryptedStore<R>,
    correlation: u128,
) -> Result<Option<u128>, PersistError> {
    store
        .get(CORRELATION, &correlation.to_be_bytes())?
        .map(|bytes| Correlation::from_bytes(&bytes).map(|found| found.instance))
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_instance_write_and_a_correlation_come_back_from_their_bytes() {
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
        let correlation = Correlation {
            correlation: 9,
            instance: 7,
        };
        assert_eq!(
            Correlation::from_bytes(&correlation.bytes()).expect("reads"),
            correlation
        );
    }
}
