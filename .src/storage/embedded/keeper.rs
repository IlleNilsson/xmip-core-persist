//! The embedded Storage node's audit keeper (ADR-0062, amendment
//! 2026-10-01): each audit record moved from the runtime database to the
//! audit database (ADR-0070, amendment 2026-10-10) exactly once, by its identifier, and a record
//! of an act on a Message with the bytes of each of its Streams kept beside
//! it, a chunk at a time, and each Stream's row of the `audit_stream` table
//! — its length, its chunks, its digest — in the record's own write
//! (ADR-0070, `super::super::audited`) — and its body, its record and the
//! Message it carries, in chunks of its own beside its row, as a Stream is
//! kept (ADR-0070, amendment 2026-10-10; `super::super::kept_audit`).

use std::sync::PoisonError;

use xcore::{AuditId, StreamId};

use super::super::audit_entry::AuditEntry;
use super::super::audited::{
    KEPT_AUDIT_STREAM, KEPT_AUDIT_STREAMS, KeptStream, chunk_key as kept_chunk_key, stream_key,
};
use super::super::chain::{self, carried_streams};
use super::super::columns::KEPT_AUDIT;
use super::super::commit::{AUDIT, CHUNK, KEPT, NEXT, Op, STREAM, sequence};
use super::super::facts::AuditFacts;
use super::super::kept_audit::{KEPT_AUDIT_BODY, KeptAudit, body_key};
use super::super::record::{Form, StreamChunk, malformed};
use super::super::stream::{StreamDigest, StreamRecord};
use super::{Embedded, chunk_key};
use crate::{Engine, PersistError, RecordChange};

/// The `audit_stream` rows of the Streams a record carries, and their
/// records.
type Kept = (Vec<RecordChange<'static>>, Vec<StreamRecord>);

impl<R: Engine + 'static, A: Engine> Embedded<R, A> {
    /// Move up to `most` audit records, oldest first, each body in chunks
    /// of `chunk` bytes: `keep_audit`.
    pub(super) fn keep(&self, most: u32, chunk: usize) -> Result<u32, PersistError> {
        let _keeping = self.keeping.lock().unwrap_or_else(PoisonError::into_inner);
        let kept = sequence(&self.runtime, KEPT)?;
        let next = sequence(&self.runtime, NEXT)?;
        let mut moved = 0;
        for number in kept..next.min(kept.saturating_add(u64::from(most))) {
            let entry: AuditEntry = self
                .read(AUDIT, &number.to_be_bytes())?
                .ok_or_else(|| malformed(format!("audit record {number} is gone")))?;
            // Kept by its identifier, once, with its index entries: a
            // record already there — this move cut short before, or the
            // same record written twice — is not kept again.
            // Chained as it is kept, after the last record of its writer's
            // chain, the chain's head moved in the same write (`chain`).
            let id = entry.id.value().to_be_bytes();
            if self.audit.get(KEPT_AUDIT, &id)?.is_none() {
                let (mut changes, streams) = self.streams_kept(&entry)?;
                let mut kept = KeptAudit {
                    id: entry.id,
                    streams: carried_streams(&entry),
                    facts: entry.facts.clone(),
                };
                self.body_kept(&entry, chunk, &mut kept.facts)?;
                changes.push(chain::link(&self.audit, &mut kept, &streams)?);
                changes.push((KEPT_AUDIT, id.to_vec(), Some(kept.bytes())));
                self.indexed((&self.audit, &self.audit_columns), changes)?;
            }
            if !self.yes(Op::Kept(number))? {
                break;
            }
            moved += 1;
        }
        Ok(moved)
    }

    /// Each Stream `entry` carries, once, kept beside it in the audit
    /// database a chunk at a time, each unsynced — the
    /// record's own write, synced, after them makes them durable with it —
    /// and its `audit_stream` row, to go in that write: Xmip Storage's to
    /// set, from the Stream's own record — with those records, in order.
    fn streams_kept(&self, entry: &AuditEntry) -> Result<Kept, PersistError> {
        let (mut rows, mut records) = (Vec::new(), Vec::new());
        for stream in carried_streams(entry) {
            let row = KeptStream {
                audit: entry.id,
                stream: self.stream_kept(entry.id, stream)?,
            };
            let key = stream_key(entry.id, stream);
            rows.push((KEPT_AUDIT_STREAMS, key, Some(row.bytes())));
            records.push(row.stream);
        }
        Ok((rows, records))
    }

    /// The Stream `stream` the audit record `id` carries, its chunks kept
    /// beside the record, and its own record.
    fn stream_kept(&self, id: AuditId, stream: StreamId) -> Result<StreamRecord, PersistError> {
        let missing = |what: String| {
            malformed(format!(
                "the audit record {id} carries the Stream {stream}, and the Ledger holds {what}"
            ))
        };
        let record: StreamRecord = self
            .read(STREAM, &stream.value().to_be_bytes())?
            .ok_or_else(|| missing("no record of it".to_string()))?;
        for index in 0..record.chunks {
            let chunk: StreamChunk = self
                .read(CHUNK, &chunk_key(stream, index))?
                .ok_or_else(|| missing(format!("no chunk {index} of it")))?;
            let key = kept_chunk_key(id, stream, index);
            self.audit
                .apply_deferred(&[(KEPT_AUDIT_STREAM, key, Some(chunk.bytes()))])?;
        }
        Ok(record)
    }

    /// The body of `entry` kept beside it in chunks of `chunk` bytes, each
    /// unsynced as a Stream's copy is — the record's own write, synced,
    /// makes them durable with it — and its length, its chunks and the
    /// SHA-256 of its bytes, taken as they pass, set in `facts`.
    fn body_kept(
        &self,
        entry: &AuditEntry,
        chunk: usize,
        facts: &mut AuditFacts,
    ) -> Result<(), PersistError> {
        let body = entry.body_bytes();
        let mut digest = StreamDigest::default();
        let mut index = 0;
        for piece in body.chunks(chunk.max(1)) {
            digest.update(piece);
            let key = body_key(entry.id, index);
            self.audit
                .apply_deferred(&[(KEPT_AUDIT_BODY, key, Some(piece.to_vec()))])?;
            index += 1;
        }
        facts.body_length = body.len() as u64;
        facts.body_chunks = index;
        facts.body_digest = digest.finish();
        Ok(())
    }

    /// An audit record the keeper moved: `read_kept_audit`.
    pub(super) fn kept(&self, id: AuditId) -> Result<Option<KeptAudit>, PersistError> {
        self.audit
            .get(KEPT_AUDIT, &id.value().to_be_bytes())?
            .map(|bytes| KeptAudit::from_bytes(&bytes))
            .transpose()
    }

    /// A chunk of a kept record's body: `read_kept_audit_body_chunk`.
    pub(super) fn kept_body_chunk(
        &self,
        id: AuditId,
        index: u32,
    ) -> Result<Option<Vec<u8>>, PersistError> {
        self.audit.get(KEPT_AUDIT_BODY, &body_key(id, index))
    }

    /// A Stream a kept record carries: `read_kept_audit_stream`.
    pub(super) fn kept_stream(
        &self,
        id: AuditId,
        stream: StreamId,
    ) -> Result<Option<StreamRecord>, PersistError> {
        self.audit
            .get(KEPT_AUDIT_STREAMS, &stream_key(id, stream))?
            .map(|bytes| KeptStream::from_bytes(&bytes).map(|kept| kept.stream))
            .transpose()
    }

    /// A chunk of a Stream a kept record carries: `read_kept_audit_chunk`.
    pub(super) fn kept_chunk(
        &self,
        id: AuditId,
        stream: StreamId,
        index: u32,
    ) -> Result<Option<StreamChunk>, PersistError> {
        self.audit
            .get(KEPT_AUDIT_STREAM, &kept_chunk_key(id, stream, index))?
            .map(|bytes| StreamChunk::from_bytes(&bytes))
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::sync::Arc;

    use secret::{Held, KekName};
    use xcore::{AuditId, StreamId};

    use super::*;
    use crate::fixture::Memory;
    use crate::storage::{Ask, AuditFacts, Audited, ChunkReader, Query, StreamDigest, XmipStorage};
    use crate::{EncryptedStore, storage::Embedded as Node};

    fn node() -> Node<Memory, Memory> {
        let keys = Held::new(secret::fixture::Memory::default());
        let kek = KekName::new("storage").expect("name");
        Node::over(
            EncryptedStore::open(Memory::default(), &keys, &kek).expect("runtime"),
            EncryptedStore::open(Memory::default(), &keys, &kek).expect("administration"),
            EncryptedStore::open(Memory::default(), &keys, &kek).expect("audit"),
            Arc::new(xcore::SystemClock),
        )
        .expect("node")
    }

    /// `content` written as the Stream `stream` in chunks of `chunk` bytes,
    /// its record with them, as a writer writes one.
    fn written(node: &Node<Memory, Memory>, stream: StreamId, content: &[u8], chunk: usize) {
        let pieces: Vec<&[u8]> = content.chunks(chunk).collect();
        let mut digest = StreamDigest::default();
        let last = u32::try_from(pieces.len() - 1).expect("chunks");
        for (index, bytes) in (0..).zip(&pieces) {
            digest.update(bytes);
            let piece = StreamChunk {
                stream,
                index,
                bytes: bytes.to_vec(),
            };
            if index < last {
                node.write_chunk(&piece).expect("chunk");
                continue;
            }
            let record = StreamRecord {
                stream,
                length: content.len() as u64,
                chunks: last + 1,
                digest: digest.clone().finish(),
                written_unix_nanos: 0,
            };
            node.write_stream(&piece, &record).expect("stream");
        }
    }

    fn carrying(streams: &[u128]) -> Audited {
        Audited {
            message: b"the Message, in its one form".to_vec(),
            streams: streams.iter().map(|id| StreamId::new(*id)).collect(),
        }
    }

    /// The audit record `id`, carrying `audited`, written and kept.
    fn kept(node: &Node<Memory, Memory>, id: u128, audited: Option<Audited>) -> AuditEntry {
        let entry = AuditEntry {
            id: AuditId::new(id),
            body: b"[[record]]\naction = \"publish\"\n\n".to_vec(),
            audited,
            facts: AuditFacts::default(),
        };
        node.write_audit(&entry).expect("written");
        node.keep_audit(10, 4096).expect("kept");
        crate::fixture::kept_as_written(node, entry.id).expect("kept")
    }

    fn content(length: usize, seed: usize) -> Vec<u8> {
        (0..length)
            .map(|at| u8::try_from((at + seed) % 251).expect("below 251"))
            .collect()
    }

    fn verified(node: &Node<Memory, Memory>, entry: &AuditEntry, stream: u128) -> Vec<u8> {
        let mut read = Vec::new();
        ChunkReader::audited(node, entry.id, StreamId::new(stream))
            .expect("read")
            .expect("it carries it")
            .read_to_end(&mut read)
            .expect("verified");
        read
    }

    #[test]
    fn a_kept_record_carries_its_message_and_every_stream_in_chunks_of_their_own() {
        let node = node();
        let (first, second) = (content(10_000, 0), content(5_000, 7));
        written(&node, StreamId::new(7), &first, 4096);
        written(&node, StreamId::new(8), &second, 4096);
        // Two Sections over the first Stream, one over the second.
        let kept = kept(&node, 1, Some(carrying(&[7, 8, 7])));

        let audited = kept.audited.as_ref().expect("carried");
        assert_eq!(audited.message, b"the Message, in its one form");
        let carrying = Query {
            ask: Ask::AuditOfStream { stream: 7 },
            most: 10,
            newest_first: false,
        };
        let found = node.query(&carrying).expect("asked");
        assert_eq!(found, [kept.id.value()], "a shared Stream kept once");
        for (stream, bytes) in [(7, &first), (8, &second)] {
            let ledger = node.read_stream(StreamId::new(stream)).expect("read");
            let row = node.read_kept_audit_stream(kept.id, StreamId::new(stream));
            assert_eq!(row.expect("read"), ledger, "its own row, in the clear");
            assert_eq!(verified(&node, &kept, stream), *bytes);
        }
        let third = node.read_kept_audit_chunk(kept.id, Some(StreamId::new(7)), 2);
        assert_eq!(third.expect("read").expect("there"), first[8192..]);
        let past = node.read_kept_audit_chunk(kept.id, Some(StreamId::new(8)), 2);
        assert_eq!(past.expect("read"), None);
    }

    #[test]
    fn a_large_stream_is_kept_and_read_a_chunk_at_a_time() {
        let node = node();
        let bytes = content(64_240 * 40 + 17, 0);
        written(&node, StreamId::new(8), &bytes, 64_240);
        let kept = kept(&node, 2, Some(carrying(&[8])));

        let reader = ChunkReader::audited(&node, kept.id, StreamId::new(8)).expect("read");
        let mut reader = reader.expect("carried");
        let mut buffer = vec![0; 8192];
        let mut digest = StreamDigest::default();
        let mut length = 0;
        loop {
            let taken = reader.read(&mut buffer).expect("verified");
            if taken == 0 {
                break;
            }
            digest.update(&buffer[..taken]);
            length += taken;
        }
        assert_eq!(length, bytes.len());
        let row = node.read_kept_audit_stream(kept.id, StreamId::new(8));
        let row = row.expect("read").expect("its row");
        assert_eq!(digest.finish(), row.digest);
        let last = node.read_kept_audit_chunk(kept.id, Some(StreamId::new(8)), 40);
        assert!(last.expect("read").is_some(), "41 chunks of their own");
    }

    #[test]
    fn a_changed_byte_in_a_kept_copy_is_refused_on_read_in_words() {
        let node = node();
        written(&node, StreamId::new(9), &content(9_000, 0), 4096);
        let kept = kept(&node, 3, Some(carrying(&[9])));
        let (id, stream) = (kept.id, StreamId::new(9));
        let changed = node.kept_chunk(id, stream, 1).expect("read");
        let mut changed = changed.expect("a second chunk");
        changed.bytes[0] ^= 1;
        let key = kept_chunk_key(id, stream, 1);
        node.audit()
            .put(KEPT_AUDIT_STREAM, &key, &changed.bytes())
            .expect("changed");

        let mut read = Vec::new();
        let refused = ChunkReader::audited(&node, id, stream)
            .expect("read")
            .expect("it carries it")
            .read_to_end(&mut read)
            .expect_err("not the audited Stream");
        let said = refused.to_string();
        assert!(said.starts_with("REFUSED: "), "{said}");
        assert!(said.contains("is not the audited one"), "{said}");
    }

    #[test]
    fn a_record_of_no_message_keeps_no_stream() {
        let node = node();
        let kept = kept(&node, 4, None);
        assert_eq!(kept.audited, None);
        let reader = ChunkReader::audited(&node, kept.id, StreamId::new(1)).expect("read");
        assert!(reader.is_none());
    }

    #[test]
    fn a_record_whose_stream_the_ledger_does_not_hold_is_not_kept() {
        let node = node();
        let entry = AuditEntry {
            id: AuditId::new(5),
            body: Vec::new(),
            audited: Some(carrying(&[99])),
            facts: AuditFacts::default(),
        };
        node.write_audit(&entry).expect("written");
        let failed = node.keep_audit(10, 4096).expect_err("no Stream");
        assert!(failed.to_string().contains("holds no record"), "{failed}");
        assert_eq!(node.read_kept_audit(AuditId::new(5)).expect("read"), None);
    }

    #[test]
    fn a_kept_body_is_in_chunks_of_its_own_read_one_at_a_time_and_held_to_its_digest() {
        let node = node();
        let entry = AuditEntry {
            id: AuditId::new(6),
            body: content(700, 3),
            audited: Some(Audited {
                message: content(900, 5),
                streams: Vec::new(),
            }),
            facts: AuditFacts::default(),
        };
        node.write_audit(&entry).expect("written");
        assert_eq!(node.keep_audit(10, 256).expect("kept"), 1);

        let row = node.read_kept_audit(entry.id).expect("read").expect("kept");
        let length = entry.body_bytes().len() as u64;
        assert_eq!(row.facts.body_length, length);
        assert_eq!(u64::from(row.facts.body_chunks), length.div_ceil(256));
        let whole = crate::fixture::kept_as_written(&node, entry.id);
        assert_eq!(
            whole,
            Some(entry.clone()),
            "the body read back from its chunks"
        );

        let key = body_key(entry.id, 2);
        let mut changed = node
            .kept_body_chunk(entry.id, 2)
            .expect("read")
            .expect("there");
        changed[0] ^= 1;
        node.audit()
            .put(KEPT_AUDIT_BODY, &key, &changed)
            .expect("changed");
        let mut read = Vec::new();
        let refused = ChunkReader::audit_body(&node, entry.id)
            .expect("read")
            .expect("kept")
            .read_to_end(&mut read)
            .expect_err("not the audited body");
        assert!(refused.to_string().starts_with("REFUSED: "), "{refused}");
    }
}
