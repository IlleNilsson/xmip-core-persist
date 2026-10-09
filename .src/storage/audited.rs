//! What an audit record of an act on a Message carries of it (ADR-0070,
//! clauses 1 and 2 as amended 2026-10-09; the owner: *In an Audit you
//! can't have references, it should be spelled out*): the Message in full,
//! as it was at the audited event, and its Stream's bytes with their
//! SHA-256 digest and their length.
//!
//! **The Message travels in the record.** Its one binary form, as the
//! Message record keeps it (`xmip-core-message`, `Message::record`) — its
//! lineage, generation, treatment, Sections and context, promoted
//! properties among them — sealed with the rest of the audit record.
//!
//! **The Stream is kept beside it, in chunks of its own.** A Stream may be
//! large, and is never whole in memory: the audit keeper, moving the record
//! to the administration database, copies the Stream's chunks one at a
//! time beside it, by the record's identifier and their number, each
//! unsynced, and keeps the record last, synced, so the record is there only
//! once its chunks are; a move cut short is finished by the next, which
//! writes the same chunks again. Their digest and length are taken from
//! the Stream's own record, their one home ([`super::StreamRecord`]), into
//! the kept record's columns. A read of the copy is held to both
//! ([`super::ChunkReader::audited`]).

use codec::cursor::Cursor;
use xcore::{AuditId, StreamId};

use super::record::{Form, read_bytes, read_u128, write_bytes, write_u128};
use crate::PersistError;

/// Where the administration database keeps the chunks of the Stream a
/// kept audit record carries.
pub(crate) const KEPT_AUDIT_STREAM: &str = "audit-stream-chunk";

/// The Message an audited act was on, and the Stream it is over.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Audited {
    /// The Message in its one binary form, as it was at the audited event.
    pub message: Vec<u8>,
    /// The Stream whose bytes the audit keeper keeps beside the record:
    /// the Message's first Section's.
    pub stream: StreamId,
}

impl Form for Audited {
    fn write(&self, out: &mut Vec<u8>) {
        write_bytes(out, &self.message);
        write_u128(out, self.stream.value());
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            message: read_bytes(cursor)?,
            stream: StreamId::new(read_u128(cursor)?),
        })
    }
}

/// A kept chunk's key: the audit record's identifier and its number.
pub(crate) fn chunk_key(audit: AuditId, index: u32) -> Vec<u8> {
    [audit.value().to_be_bytes().as_slice(), &index.to_be_bytes()].concat()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn what_a_record_carries_comes_back_from_its_bytes_as_it_was() {
        let audited = Audited {
            message: b"the Message".to_vec(),
            stream: StreamId::new(4),
        };
        assert_eq!(
            Audited::from_bytes(&audited.bytes()).expect("read"),
            audited
        );
    }
}
