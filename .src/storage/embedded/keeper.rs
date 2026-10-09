//! The embedded Storage node's audit keeper (ADR-0062, amendment
//! 2026-10-01): each audit record moved from the runtime database to the
//! administration database exactly once, by its identifier, and a record
//! of an act on a Message with its Stream's bytes kept beside it, a chunk
//! at a time, their digest and length from the Stream's own record
//! (ADR-0070, `super::super::audited`).

use std::sync::PoisonError;

use xcore::AuditId;

use super::super::audited::{KEPT_AUDIT_STREAM, chunk_key as kept_chunk_key};
use super::super::columns::KEPT_AUDIT;
use super::super::commit::{AUDIT, CHUNK, KEPT, NEXT, Op, STREAM, sequence};
use super::super::record::{AuditEntry, Form, StreamChunk, malformed};
use super::super::stream::StreamRecord;
use super::{Embedded, chunk_key};
use crate::{Engine, PersistError};

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
            let mut entry: AuditEntry = self
                .read(AUDIT, &number.to_be_bytes())?
                .ok_or_else(|| malformed(format!("audit record {number} is gone")))?;
            // Kept by its identifier, once, with its index entries: a
            // record already there — this move cut short before, or the
            // same record written twice — is not kept again.
            let id = entry.id.value().to_be_bytes();
            if self.administration.get(KEPT_AUDIT, &id)?.is_none() {
                self.stream_kept(&mut entry)?;
                self.administered(vec![(KEPT_AUDIT, id.to_vec(), Some(entry.bytes()))])?;
            }
            if !self.yes(Op::Kept(number))? {
                break;
            }
            moved += 1;
        }
        Ok(moved)
    }

    /// The Stream `entry` carries, kept beside it in the administration
    /// database a chunk at a time, each unsynced — the record's own write,
    /// synced, after them makes them durable with it — and its digest and
    /// length set from the Stream's record, Xmip Storage's to set: none
    /// where it carries no Message.
    fn stream_kept(&self, entry: &mut AuditEntry) -> Result<(), PersistError> {
        entry.facts.stream_digest = None;
        entry.facts.stream_length = None;
        let Some(audited) = &entry.audited else {
            return Ok(());
        };
        let (id, stream) = (entry.id, audited.stream);
        let record: StreamRecord = self
            .read(STREAM, &stream.value().to_be_bytes())?
            .ok_or_else(|| {
                malformed(format!(
                    "the audit record {id} carries the Stream {stream}, and the Ledger holds \
                     no record of it"
                ))
            })?;
        for index in 0..record.chunks {
            let chunk: StreamChunk =
                self.read(CHUNK, &chunk_key(stream, index))?
                    .ok_or_else(|| {
                        malformed(format!(
                            "the audit record {id} carries the Stream {stream}, and the Ledger \
                             holds no chunk {index} of it"
                        ))
                    })?;
            let key = kept_chunk_key(id, index);
            self.administration
                .apply_deferred(&[(KEPT_AUDIT_STREAM, key, Some(chunk.bytes()))])?;
        }
        entry.facts.stream_digest = Some(record.digest);
        entry.facts.stream_length = Some(record.length);
        Ok(())
    }

    /// An audit record the keeper moved: `read_kept_audit`.
    pub(super) fn kept(&self, id: AuditId) -> Result<Option<AuditEntry>, PersistError> {
        self.administration
            .get(KEPT_AUDIT, &id.value().to_be_bytes())?
            .map(|bytes| AuditEntry::from_bytes(&bytes))
            .transpose()
    }

    /// A chunk of the Stream a kept record carries: `read_kept_audit_chunk`.
    pub(super) fn kept_chunk(
        &self,
        id: AuditId,
        index: u32,
    ) -> Result<Option<StreamChunk>, PersistError> {
        self.administration
            .get(KEPT_AUDIT_STREAM, &kept_chunk_key(id, index))?
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
    use crate::storage::{
        AuditFacts, Audited, ChunkReader, StreamDigest, StreamRecord, XmipStorage,
    };
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

    fn audit(id: u128, audited: Option<Audited>) -> AuditEntry {
        AuditEntry {
            id: AuditId::new(id),
            body: b"[[record]]\naction = \"publish\"\n\n".to_vec(),
            audited,
            facts: AuditFacts {
                stream_digest: Some([9; 32]),
                ..AuditFacts::default()
            },
        }
    }

    fn content(length: usize) -> Vec<u8> {
        (0..length)
            .map(|at| u8::try_from(at % 251).expect("below 251"))
            .collect()
    }

    #[test]
    fn a_kept_record_carries_its_message_and_its_stream_in_chunks_of_its_own() {
        let node = node();
        let stream = StreamId::new(7);
        let bytes = content(10_000);
        written(&node, stream, &bytes, 4096);
        let carried = Audited {
            message: b"the Message, in its one form".to_vec(),
            stream,
        };
        node.write_audit(&audit(1, Some(carried.clone())))
            .expect("written");
        assert_eq!(node.keep_audit(10).expect("kept"), 1);

        let kept = node.read_kept_audit(AuditId::new(1)).expect("read");
        let kept = kept.expect("kept");
        assert_eq!(kept.audited, Some(carried), "the Message spelled out");
        let ledger = node.read_stream(stream).expect("read").expect("a record");
        assert_eq!(
            kept.facts.stream_digest,
            Some(ledger.digest),
            "from its home"
        );
        assert_eq!(kept.facts.stream_length, Some(10_000));
        let third = node
            .read_kept_audit_chunk(AuditId::new(1), 2)
            .expect("read");
        assert_eq!(third.expect("a third chunk").bytes, bytes[8192..]);
        assert_eq!(
            node.read_kept_audit_chunk(AuditId::new(1), 3)
                .expect("read"),
            None
        );

        let mut read = Vec::new();
        ChunkReader::audited(&node, &kept)
            .expect("it carries one")
            .read_to_end(&mut read)
            .expect("verified");
        assert_eq!(read, bytes);
    }

    #[test]
    fn a_large_stream_is_kept_and_read_a_chunk_at_a_time() {
        let node = node();
        let stream = StreamId::new(8);
        let bytes = content(64_240 * 40 + 17);
        written(&node, stream, &bytes, 64_240);
        let carried = Audited {
            message: Vec::new(),
            stream,
        };
        node.write_audit(&audit(2, Some(carried))).expect("written");
        node.keep_audit(10).expect("kept");
        let kept = node.read_kept_audit(AuditId::new(2)).expect("read");
        let kept = kept.expect("kept");

        let mut reader = ChunkReader::audited(&node, &kept).expect("it carries one");
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
        assert_eq!(Some(digest.finish()), kept.facts.stream_digest);
        assert!(
            node.read_kept_audit_chunk(AuditId::new(2), 40)
                .expect("read")
                .is_some(),
            "41 chunks of their own"
        );
    }

    #[test]
    fn a_changed_byte_in_the_kept_copy_is_refused_on_read_in_words() {
        let node = node();
        let stream = StreamId::new(9);
        written(&node, stream, &content(9_000), 4096);
        let carried = Audited {
            message: Vec::new(),
            stream,
        };
        node.write_audit(&audit(3, Some(carried))).expect("written");
        node.keep_audit(10).expect("kept");
        let kept = node.read_kept_audit(AuditId::new(3)).expect("read");
        let kept = kept.expect("kept");
        let key = kept_chunk_key(AuditId::new(3), 1);
        let mut changed = node.kept_chunk(AuditId::new(3), 1).expect("read");
        let mut changed = changed.take().expect("a second chunk");
        changed.bytes[0] ^= 1;
        node.administration()
            .put(KEPT_AUDIT_STREAM, &key, &changed.bytes())
            .expect("changed");

        let mut read = Vec::new();
        let refused = ChunkReader::audited(&node, &kept)
            .expect("it carries one")
            .read_to_end(&mut read)
            .expect_err("not the audited Stream");
        let said = refused.to_string();
        assert!(said.starts_with("REFUSED: "), "{said}");
        assert!(said.contains("is not the audited one"), "{said}");
    }

    #[test]
    fn a_record_of_no_message_keeps_no_stream_and_its_writer_sets_none() {
        let node = node();
        node.write_audit(&audit(4, None)).expect("written");
        node.keep_audit(10).expect("kept");
        let kept = node.read_kept_audit(AuditId::new(4)).expect("read");
        let kept = kept.expect("kept");
        assert_eq!(kept.facts.stream_digest, None, "Xmip Storage's to set");
        assert!(ChunkReader::audited(&node, &kept).is_none());
    }

    #[test]
    fn a_record_whose_stream_the_ledger_does_not_hold_is_not_kept() {
        let node = node();
        let carried = Audited {
            message: Vec::new(),
            stream: StreamId::new(99),
        };
        node.write_audit(&audit(5, Some(carried))).expect("written");
        let failed = node.keep_audit(10).expect_err("no Stream");
        assert!(failed.to_string().contains("holds no record"), "{failed}");
        assert_eq!(node.read_kept_audit(AuditId::new(5)).expect("read"), None);
    }
}
