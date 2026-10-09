//! What an audit record of an act on a Message carries of it (ADR-0070,
//! clauses 1 and 2 as amended 2026-10-09; the owner: *In an Audit you
//! can't have references, it should be spelled out*): the Message in full,
//! as it was at the audited event, and the bytes of every Stream its
//! Sections are over, each with its SHA-256 digest and its length.
//!
//! **The Message travels in the record.** Its one binary form, as the
//! Message record keeps it (`xmip-core-message`, `Message::record`) — its
//! lineage, generation, treatment, Sections and context, promoted
//! properties among them — sealed with the rest of the audit record.
//!
//! **Each Stream is kept beside it, in chunks of its own.** A Stream may be
//! large, and is never whole in memory: the audit keeper, moving the record
//! to the administration database, copies each Stream's chunks one at a
//! time beside it, by the record's identifier, the Stream's and their
//! number, each unsynced, and keeps the record last, synced, so the record
//! is there only once its chunks are; a move cut short is finished by the
//! next, which writes the same chunks again. A Stream two Sections share is
//! copied once.
//!
//! **Each Stream's digest and length are in the record, not in columns.**
//! The keeper takes them from the Stream's own record, their one home
//! ([`super::StreamRecord`]), into [`Audited::kept`]: a Message has as
//! many Streams as it has Sections, and a column holds one value, while a
//! list stays in the body (`super::schema::searchable`). A read of a copy is
//! held to both ([`super::ChunkReader::audited`]).

use codec::cursor::Cursor;
use xcore::{AuditId, StreamId};

use super::record::{Form, read_bytes, read_u32, read_u128, write_bytes, write_u32, write_u128};
use super::stream::StreamRecord;
use crate::PersistError;

/// Where the administration database keeps the chunks of the Streams a
/// kept audit record carries.
pub(crate) const KEPT_AUDIT_STREAM: &str = "audit-stream-chunk";

/// The Message an audited act was on, and the Streams it is over.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Audited {
    /// The Message in its one binary form, as it was at the audited event.
    pub message: Vec<u8>,
    /// The Streams its Sections are over, in the order of its Sections,
    /// each once: those whose bytes the audit keeper keeps beside the
    /// record.
    pub streams: Vec<StreamId>,
    /// Each Stream's own record as the keeper kept its bytes — its length,
    /// its chunks and its SHA-256 digest: Xmip Storage's to set, empty
    /// until the record is kept.
    pub kept: Vec<StreamRecord>,
}

impl Audited {
    /// The kept record of `stream`, where the keeper kept it.
    #[must_use]
    pub fn kept(&self, stream: StreamId) -> Option<&StreamRecord> {
        self.kept.iter().find(|record| record.stream == stream)
    }
}

impl Form for Audited {
    fn write(&self, out: &mut Vec<u8>) {
        write_bytes(out, &self.message);
        write_u32(out, u32::try_from(self.streams.len()).unwrap_or(u32::MAX));
        for stream in &self.streams {
            write_u128(out, stream.value());
        }
        self.kept.write(out);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        let message = read_bytes(cursor)?;
        let streams = (0..read_u32(cursor)?)
            .map(|_| read_u128(cursor).map(StreamId::new))
            .collect::<Result<_, _>>()?;
        Ok(Self {
            message,
            streams,
            kept: Vec::read(cursor)?,
        })
    }
}

/// A kept chunk's key: the audit record's identifier, the Stream's and
/// the chunk's number.
pub(crate) fn chunk_key(audit: AuditId, stream: StreamId, index: u32) -> Vec<u8> {
    [
        audit.value().to_be_bytes().as_slice(),
        &stream.value().to_be_bytes(),
        &index.to_be_bytes(),
    ]
    .concat()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn what_a_record_carries_comes_back_from_its_bytes_as_it_was() {
        let audited = Audited {
            message: b"the Message".to_vec(),
            streams: vec![StreamId::new(4), StreamId::new(5)],
            kept: vec![StreamRecord {
                stream: StreamId::new(4),
                length: 3,
                chunks: 1,
                digest: [2; super::super::DIGEST],
                written_unix_nanos: 1,
            }],
        };
        assert_eq!(
            Audited::from_bytes(&audited.bytes()).expect("read"),
            audited
        );
    }
}
