//! The embedded Storage node's audit keeper (ADR-0062, amendment
//! 2026-10-01): each audit record moved from the runtime database to the
//! administration database exactly once, by its identifier, and a record
//! of an act on a Message with the bytes of each of its Streams kept beside
//! it, a chunk at a time, and each Stream's row of the `audit_stream` table
//! — its length, its chunks, its digest — in the record's own write
//! (ADR-0070, `super::super::audited`).

use std::sync::PoisonError;

use xcore::{AuditId, StreamId};

use super::super::audited::{
    KEPT_AUDIT_STREAM, KEPT_AUDIT_STREAMS, KeptStream, chunk_key as kept_chunk_key, stream_key,
};
use super::super::columns::KEPT_AUDIT;
use super::super::commit::{AUDIT, CHUNK, KEPT, NEXT, Op, STREAM, sequence};
use super::super::record::{AuditEntry, Form, StreamChunk, malformed};
use super::super::stream::StreamRecord;
use super::{Embedded, chunk_key};
use crate::{Engine, PersistError, RecordChange};

impl<R: Engine + 'static, A: Engine> Embedded<R, A> {
    /// Move up to `most` audit records, oldest first: `keep_audit`.
    pub(super) fn keep(&self, most: u32) -> Result<u32, PersistError> {
        let _keeping = self
            .administering
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
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
            let id = entry.id.value().to_be_bytes();
            if self.administration.get(KEPT_AUDIT, &id)?.is_none() {
                let mut changes = self.streams_kept(&entry)?;
                changes.push((KEPT_AUDIT, id.to_vec(), Some(entry.bytes())));
                self.administered(changes)?;
            }
            if !self.yes(Op::Kept(number))? {
                break;
            }
            moved += 1;
        }
        Ok(moved)
    }

    /// Each Stream `entry` carries, once, kept beside it in the
    /// administration database a chunk at a time, each unsynced — the
    /// record's own write, synced, after them makes them durable with it —
    /// and its `audit_stream` row, to go in that write: Xmip Storage's to
    /// set, from the Stream's own record.
    fn streams_kept(&self, entry: &AuditEntry) -> Result<Vec<RecordChange<'static>>, PersistError> {
        let Some(audited) = &entry.audited else {
            return Ok(Vec::new());
        };
        let mut rows = Vec::new();
        let mut kept: Vec<StreamId> = Vec::new();
        for stream in audited.streams.iter().copied() {
            if kept.contains(&stream) {
                continue;
            }
            kept.push(stream);
            let row = KeptStream {
                audit: entry.id,
                stream: self.stream_kept(entry.id, stream)?,
            };
            let key = stream_key(entry.id, stream);
            rows.push((KEPT_AUDIT_STREAMS, key, Some(row.bytes())));
        }
        Ok(rows)
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
            self.administration
                .apply_deferred(&[(KEPT_AUDIT_STREAM, key, Some(chunk.bytes()))])?;
        }
        Ok(record)
    }

    /// An audit record the keeper moved: `read_kept_audit`.
    pub(super) fn kept(&self, id: AuditId) -> Result<Option<AuditEntry>, PersistError> {
        self.administration
            .get(KEPT_AUDIT, &id.value().to_be_bytes())?
            .map(|bytes| AuditEntry::from_bytes(&bytes))
            .transpose()
    }

    /// A Stream a kept record carries: `read_kept_audit_stream`.
    pub(super) fn kept_stream(
        &self,
        id: AuditId,
        stream: StreamId,
    ) -> Result<Option<StreamRecord>, PersistError> {
        self.administration
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
        self.administration
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
        node.keep_audit(10).expect("kept");
        node.read_kept_audit(entry.id).expect("read").expect("kept")
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
        let third = node.read_kept_audit_chunk(kept.id, StreamId::new(7), 2);
        assert_eq!(third.expect("read").expect("there").bytes, first[8192..]);
        let past = node.read_kept_audit_chunk(kept.id, StreamId::new(8), 2);
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
        let last = node.read_kept_audit_chunk(kept.id, StreamId::new(8), 40);
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
        node.administration()
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
        let failed = node.keep_audit(10).expect_err("no Stream");
        assert!(failed.to_string().contains("holds no record"), "{failed}");
        assert_eq!(node.read_kept_audit(AuditId::new(5)).expect("read"), None);
    }
}
