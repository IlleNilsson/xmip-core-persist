//! An audit record as the audit keeper keeps it in the administration
//! database: its row in the `audit` table — its identifier, the Streams it
//! carries and every field in a column of its own, its place in its
//! writer's chain among them — and its body beside it in chunks of its own,
//! as a Stream is kept (ADR-0070, amendment 2026-10-10; the owner: *The
//! audit body has to be like the stream, in chunks*): the `audit_body_chunk`
//! table, by the record's identifier and the chunk's number, read back a
//! chunk at a time (`super::ChunkReader::audit_body`), never whole.

use codec::cursor::Cursor;
use codec::form::Form;
use codec::writer::ByteWriter;
use xcore::{AuditId, StreamId};

use super::facts::AuditFacts;

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
        out.u128_be(self.id.value());
        out.u32_be(u32::try_from(self.streams.len()).unwrap_or(u32::MAX));
        for stream in &self.streams {
            out.u128_be(stream.value());
        }
        self.facts.write(out);
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        let id = AuditId::new(cursor.u128_be()?);
        let streams = (0..cursor.u32_be()?)
            .map(|_| cursor.u128_be().map(StreamId::new))
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
