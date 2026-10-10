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
//! to the audit database, copies each Stream's chunks one at a
//! time beside it, by the record's identifier, the Stream's and their
//! number, each unsynced, and keeps the record last, synced, so the record
//! is there only once its chunks are; a move cut short is finished by the
//! next, which writes the same chunks again. A Stream two Sections share is
//! copied once.
//!
//! **Each Stream is a row of its own** (the owner, 2026-10-09: *when the
//! Xmip Core or its Providers uses the Audit functionality the whole
//! shebang goes to audit*): the keeper writes, in the record's own write, a
//! [`KeptStream`] for each — the record's identifier, and the Stream's own
//! record ([`super::StreamRecord`], the one home of its length, its chunks,
//! its digest and when it was written) — which the `audit_stream` table
//! lays out in columns, in the clear, found by the record and by the
//! Stream. A read of a copy is held to its length and its digest
//! ([`super::ChunkReader::audited`]).

use codec::cursor::Cursor;
use xcore::{AuditId, StreamId};

use super::record::{Form, read_bytes, read_u32, read_u128, write_bytes, write_u32, write_u128};
use super::stream::StreamRecord;
use crate::PersistError;

/// Where the audit database keeps the chunks of the Streams a
/// kept audit record carries.
pub(crate) const KEPT_AUDIT_STREAM: &str = "audit-stream-chunk";

/// Where the audit database keeps each Stream a kept audit record
/// carries: the `audit_stream` table.
pub(crate) const KEPT_AUDIT_STREAMS: &str = "audit_stream";

/// The Message an audited act was on, and the Streams it is over.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Audited {
    /// The Message in its one binary form, as it was at the audited event.
    pub message: Vec<u8>,
    /// The Streams its Sections are over, in the order of its Sections,
    /// each once: those the audit keeper keeps beside the record.
    pub streams: Vec<StreamId>,
}

/// A Stream a kept audit record carries, as the keeper kept it: a row of
/// the `audit_stream` table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeptStream {
    pub audit: AuditId,
    /// The Stream's own record as the Ledger kept it.
    pub stream: StreamRecord,
}

impl Form for KeptStream {
    fn write(&self, out: &mut Vec<u8>) {
        write_u128(out, self.audit.value());
        self.stream.write(out);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            audit: AuditId::new(read_u128(cursor)?),
            stream: StreamRecord::read(cursor)?,
        })
    }
}

/// A kept Stream's key: the audit record's identifier and the Stream's.
pub(crate) fn stream_key(audit: AuditId, stream: StreamId) -> Vec<u8> {
    [
        audit.value().to_be_bytes().as_slice(),
        &stream.value().to_be_bytes(),
    ]
    .concat()
}

impl Form for Audited {
    fn write(&self, out: &mut Vec<u8>) {
        write_bytes(out, &self.message);
        write_u32(out, u32::try_from(self.streams.len()).unwrap_or(u32::MAX));
        for stream in &self.streams {
            write_u128(out, stream.value());
        }
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        let message = read_bytes(cursor)?;
        let streams = (0..read_u32(cursor)?)
            .map(|_| read_u128(cursor).map(StreamId::new))
            .collect::<Result<_, _>>()?;
        Ok(Self { message, streams })
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
        };
        assert_eq!(
            Audited::from_bytes(&audited.bytes()).expect("read"),
            audited
        );
        let kept = KeptStream {
            audit: AuditId::new(1),
            stream: StreamRecord {
                stream: StreamId::new(4),
                length: 3,
                chunks: 1,
                digest: [2; super::super::DIGEST],
                written_unix_nanos: 1,
            },
        };
        assert_eq!(KeptStream::from_bytes(&kept.bytes()).expect("read"), kept);
    }
}
