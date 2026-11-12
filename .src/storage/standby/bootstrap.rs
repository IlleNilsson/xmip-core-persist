//! How far a standby's bootstrap by a whole copy is (ADR-0056, amendment
//! 2026-11-12): **copying**, its primary's pages coming in; **catching
//! up**, every page in and the writes its primary took while it scanned
//! being replayed; **ready**, holding one state of its primary's Ledger.
//!
//! A primary scans each database page by page while it goes on writing
//! (`super::ship`), so the pages alone can reflect a write in part — what it
//! changed on a page read after it, and not on one read before. They are one
//! state of the primary's only once the standby has replayed every write
//! numbered before where the scan of each database ended, which the primary
//! says with each database's end (`Shipped::Copied`). Until then the
//! standby is never promoted, and says so in its standing. The state is
//! kept, durable, in its runtime database, so a standby started again goes
//! on in the state it was in.

use codec::CodecError;
use codec::cursor::Cursor;
use codec::form::Form;
use codec::writer::ByteWriter;
use xcore::lock::Locked;

use super::StorageNode;
use super::copies::DATABASES;
use crate::{Change, Engine, PersistError};

/// Where a Storage node keeps, in its runtime database, how far its
/// bootstrap is: nothing while it is ready.
pub(crate) const BOOTSTRAP: &[u8] = b"\0xmip-core-persist/bootstrap\0";

/// How far a Storage node's bootstrap by a whole copy is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bootstrap {
    /// It holds one state of its primary's Ledger, or never took a whole
    /// copy: it may be promoted.
    Ready,
    /// A whole copy into it is under way, or was cut short: it holds part
    /// of a Ledger.
    Copying,
    /// Every page of a whole copy is in, and the writes its primary took
    /// while it scanned are being replayed: one state of the primary's once
    /// each database's copies reach `through`.
    CatchingUp {
        /// The epoch of the primary the copy came from: the one whose
        /// writes are replayed onto its pages.
        epoch: u64,
        /// The number of the first write of each database the copy's pages
        /// cannot reflect.
        through: [u64; DATABASES],
    },
}

impl Bootstrap {
    /// Whether it may be promoted: only once ready.
    #[must_use]
    pub fn is_ready(self) -> bool {
        matches!(self, Self::Ready)
    }

    /// What it is, in words, for a standby whose next writes are `next`:
    /// what its standing says, and why it is not promoted.
    #[must_use]
    pub fn said(self, next: [u64; DATABASES]) -> String {
        match self {
            Self::Ready => "ready".to_string(),
            Self::Copying => "copying, not promoted until ready".to_string(),
            Self::CatchingUp { through, .. } => {
                let left: u64 = through
                    .iter()
                    .zip(next)
                    .map(|(through, next)| through.saturating_sub(next))
                    .sum();
                format!("catching up, {left} writes to go, not promoted until ready")
            }
        }
    }

    /// Its record in the runtime database: none once ready.
    fn record(self) -> Change {
        match self {
            Self::Ready => (BOOTSTRAP.to_vec(), None),
            _ => (BOOTSTRAP.to_vec(), Some(self.bytes())),
        }
    }
}

impl Form for Bootstrap {
    fn write(&self, out: &mut Vec<u8>) {
        match self {
            Self::Ready => {
                out.byte(0);
            }
            Self::Copying => {
                out.byte(1);
            }
            Self::CatchingUp { epoch, through } => {
                out.byte(2);
                out.u64_be(*epoch);
                for through in through {
                    out.u64_be(*through);
                }
            }
        }
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        Ok(match cursor.byte()? {
            0 => Self::Ready,
            1 => Self::Copying,
            2 => Self::CatchingUp {
                epoch: cursor.u64_be()?,
                through: [cursor.u64_be()?, cursor.u64_be()?, cursor.u64_be()?],
            },
            other => {
                return Err(CodecError::new(format!("no bootstrap is numbered {other}")));
            }
        })
    }
}

/// The bootstrap `engine`, a runtime database, keeps: ready where it keeps
/// none, and copying where what it keeps says no bootstrap, so a record it
/// cannot read is never taken for ready.
///
/// # Errors
///
/// Where the engine cannot be read.
pub(super) fn kept(engine: &dyn Engine) -> Result<Bootstrap, PersistError> {
    Ok(engine.read(BOOTSTRAP)?.map_or(Bootstrap::Ready, |bytes| {
        Bootstrap::from_bytes(&bytes).unwrap_or(Bootstrap::Copying)
    }))
}

impl StorageNode {
    /// How far its bootstrap by a whole copy is: it is promoted only once
    /// ready.
    #[must_use]
    pub fn bootstrap(&self) -> Bootstrap {
        *self.bootstrap.locked()
    }

    /// `bootstrap` written, durable, with `with` in one write of its runtime
    /// database, and held from then; nothing is held where it could not be
    /// written.
    ///
    /// # Errors
    ///
    /// The write's own.
    pub(super) fn bootstrapped(
        &self,
        bootstrap: Bootstrap,
        with: &[Change],
    ) -> Result<(), PersistError> {
        let mut batch = with.to_vec();
        batch.push(bootstrap.record());
        self.engines[0].apply(&batch)?;
        *self.bootstrap.locked() = bootstrap;
        Ok(())
    }

    /// Ready, where it is catching up and every database's copies have
    /// reached where its copy is whole: called only once each database is
    /// synced as far as its copies go, so a standby started again never
    /// holds less than ready said.
    ///
    /// # Errors
    ///
    /// Where ready could not be written; it is still catching up then.
    pub(super) fn caught_up(&self) -> Result<(), PersistError> {
        let Bootstrap::CatchingUp { through, .. } = self.bootstrap() else {
            return Ok(());
        };
        let next = self.copies.next();
        if next
            .iter()
            .zip(through)
            .all(|(next, through)| *next >= through)
        {
            self.bootstrapped(Bootstrap::Ready, &[])?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::Memory;

    #[test]
    fn each_state_is_kept_as_written_and_one_unread_is_never_ready() {
        let engine = Memory::default();
        assert_eq!(kept(&engine).expect("read"), Bootstrap::Ready);
        let catching_up = Bootstrap::CatchingUp {
            epoch: 4,
            through: [12, 3, 7],
        };
        for bootstrap in [Bootstrap::Copying, catching_up, Bootstrap::Ready] {
            engine.apply(&[bootstrap.record()]).expect("written");
            assert_eq!(kept(&engine).expect("read"), bootstrap);
            assert_eq!(
                Bootstrap::from_bytes(&bootstrap.bytes()).expect("read"),
                bootstrap
            );
        }
        assert_eq!(
            engine.read(BOOTSTRAP).expect("read"),
            None,
            "none once ready"
        );
        engine.write(BOOTSTRAP, &[9]).expect("written");
        assert_eq!(kept(&engine).expect("read"), Bootstrap::Copying);
        assert_eq!(
            catching_up.said([10, 3, 8]),
            "catching up, 2 writes to go, not promoted until ready"
        );
    }
}
