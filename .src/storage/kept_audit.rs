//! An audit record as the audit keeper keeps it in the administration
//! database: its row in the `audit` table — its identifier, the Streams it
//! carries and every field in a column of its own, its place in its
//! writer's chain among them — and its body beside it in chunks of its own,
//! as a Stream is kept (ADR-0070, amendment 2026-10-10; the owner: *The
//! audit body has to be like the stream, in chunks*): the `audit_body_chunk`
//! table, by the record's identifier and the chunk's number, read back a
//! chunk at a time (`super::ChunkReader::audit_body`), never whole.

use codec::cursor::Cursor;
use xcore::{AuditId, StreamId};

use super::facts::AuditFacts;
use super::record::{Form, read_u32, read_u128, write_u32, write_u128};
use crate::PersistError;

/// Where the audit database keeps the chunks of each kept audit
/// record's body: the `audit_body_chunk` table.
pub(crate) const KEPT_AUDIT_BODY: &str = "audit-body-chunk";

/// A kept audit record's row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeptAudit {
    pub id: AuditId,
    /// The Streams it carries, in its order, each once: each a row of the
    /// `audit_stream` table beside it.
    pub streams: Vec<StreamId>,
    pub facts: AuditFacts,
}

impl Form for KeptAudit {
    fn write(&self, out: &mut Vec<u8>) {
        write_u128(out, self.id.value());
        write_u32(out, u32::try_from(self.streams.len()).unwrap_or(u32::MAX));
        for stream in &self.streams {
            write_u128(out, stream.value());
        }
        self.facts.write(out);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        let id = AuditId::new(read_u128(cursor)?);
        let streams = (0..read_u32(cursor)?)
            .map(|_| read_u128(cursor).map(StreamId::new))
            .collect::<Result<_, _>>()?;
        Ok(Self {
            id,
            streams,
            facts: AuditFacts::read(cursor)?,
        })
    }
}

/// A body chunk's key: the record's identifier and the chunk's number.
pub(crate) fn body_key(audit: AuditId, index: u32) -> Vec<u8> {
    [audit.value().to_be_bytes().as_slice(), &index.to_be_bytes()].concat()
}
