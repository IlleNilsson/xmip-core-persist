//! The searchable tables, each the record it keeps (`super`): which kind of
//! record in which database is one, and its row read from it, or stamped as
//! it is written.

use super::super::act::{ACT, ActRecord};
use super::super::audited::{KEPT_AUDIT_STREAMS, KeptStream};
use super::super::commit::{DEAD, HELD, JOURNEY, MESSAGE};
use super::super::dead::Dead;
use super::super::failed::{FAILED, Failed};
use super::super::hold::Held;
use super::super::instance::{INSTANCE, InstanceRecord};
use super::super::kept_audit::KeptAudit;
use super::super::membership::{MEMBERSHIP, MemberRecord};
use super::super::record::{AdministrationRecord, JourneyRecord, MessageRecord};
use super::super::row::{Columned, Row};
use super::super::schema::Database;
use super::super::secret::{KeptSecret, SECRET};
use super::{ADMINISTRATION, KEPT_AUDIT};
use crate::PersistError;

/// The searchable tables, each the record it keeps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Searched {
    Journey,
    Message,
    Held,
    Dead,
    Failed,
    Audit,
    AuditStream,
    Administration,
    Secret,
    Membership,
    Act,
    Instance,
}

impl Searched {
    /// The table records of `kind` in `database` are, where it is one.
    pub(super) fn of(database: Database, kind: &str) -> Option<Self> {
        Some(match (database, kind) {
            (Database::Runtime, INSTANCE) => Self::Instance,
            (Database::Runtime, JOURNEY) => Self::Journey,
            (Database::Runtime, MESSAGE) => Self::Message,
            (Database::Runtime, HELD) => Self::Held,
            (Database::Runtime, DEAD) => Self::Dead,
            (Database::Runtime, FAILED) => Self::Failed,
            (Database::Audit, KEPT_AUDIT) => Self::Audit,
            (Database::Audit, KEPT_AUDIT_STREAMS) => Self::AuditStream,
            (Database::Administration, kind) if kind.starts_with(ADMINISTRATION) => {
                Self::Administration
            }
            (Database::Administration, SECRET) => Self::Secret,
            (Database::Administration, MEMBERSHIP) => Self::Membership,
            (Database::Administration, ACT) => Self::Act,
            _ => return None,
        })
    }

    pub(super) const fn table(self) -> &'static str {
        match self {
            Self::Journey => "journey",
            Self::Message => "message",
            Self::Held => "held",
            Self::Dead => "dead_message",
            Self::Failed => "failed_message",
            Self::Audit => "audit",
            Self::AuditStream => "audit_stream",
            Self::Administration => "administration",
            Self::Secret => "secret",
            Self::Membership => MEMBERSHIP,
            Self::Act => ACT,
            Self::Instance => "process_instance",
        }
    }

    /// The row and the identifier of the record `bytes` hold, as it was
    /// kept.
    pub(super) fn row(self, bytes: &[u8]) -> Result<(Row, u128), PersistError> {
        match self {
            Self::Journey => row::<JourneyRecord>(bytes),
            Self::Message => row::<MessageRecord>(bytes),
            Self::Held => row::<Held>(bytes),
            Self::Dead => row::<Dead>(bytes),
            Self::Failed => row::<Failed>(bytes),
            Self::Audit => row::<KeptAudit>(bytes),
            Self::AuditStream => row::<KeptStream>(bytes),
            Self::Administration => row::<AdministrationRecord>(bytes),
            Self::Secret => row::<KeptSecret>(bytes),
            Self::Membership => row::<MemberRecord>(bytes),
            Self::Act => row::<ActRecord>(bytes),
            Self::Instance => row::<InstanceRecord>(bytes),
        }
    }

    /// `bytes` stamped at `now` over `before`: the bytes to keep, the row,
    /// and the identifier.
    pub(super) fn stamped(
        self,
        bytes: &[u8],
        before: Option<&[u8]>,
        now: u64,
    ) -> Result<(Vec<u8>, Row, u128), PersistError> {
        match self {
            Self::Journey => stamp::<JourneyRecord>(bytes, before, now),
            Self::Message => stamp::<MessageRecord>(bytes, before, now),
            Self::Held => stamp::<Held>(bytes, before, now),
            Self::Dead => stamp::<Dead>(bytes, before, now),
            Self::Failed => stamp::<Failed>(bytes, before, now),
            Self::Audit => stamp::<KeptAudit>(bytes, before, now),
            Self::AuditStream => stamp::<KeptStream>(bytes, before, now),
            Self::Administration => stamp::<AdministrationRecord>(bytes, before, now),
            Self::Secret => stamp::<KeptSecret>(bytes, before, now),
            Self::Membership => stamp::<MemberRecord>(bytes, before, now),
            Self::Act => stamp::<ActRecord>(bytes, before, now),
            Self::Instance => stamp::<InstanceRecord>(bytes, before, now),
        }
    }
}

fn row<T: Columned>(bytes: &[u8]) -> Result<(Row, u128), PersistError> {
    let record = T::from_bytes(bytes)?;
    Ok((record.row(), record.id()))
}

fn stamp<T: Columned>(
    bytes: &[u8],
    before: Option<&[u8]>,
    now: u64,
) -> Result<(Vec<u8>, Row, u128), PersistError> {
    let mut record = T::from_bytes(bytes)?;
    let before = before.map(T::from_bytes).transpose()?;
    Ok(lay(&mut record, before.as_ref(), now))
}

/// `record` stamped at `now` over `before`, the record it replaces: the
/// bytes to keep, the row, and the identifier — once, whether it was read
/// from its bytes or made new as it is (`super::Columns::laid`).
pub(super) fn lay<T: Columned>(
    record: &mut T,
    before: Option<&T>,
    now: u64,
) -> (Vec<u8>, Row, u128) {
    record.stamp(now, before);
    (record.bytes(), record.row(), record.id())
}
