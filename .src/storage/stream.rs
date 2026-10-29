//! A Stream as Xmip Storage keeps it beside its chunks (proposed
//! 2026-10-09; the owner, the same day: *a Message refers to a stream, a
//! stream is stored in chunks*): its own record, one per Stream, keyed by
//! its identifier, which a chunk and a Message refer to by it.
//!
//! **The one home of a Stream's length and its digest.** A Stream ends where
//! it has no further chunk; how long it is, in how many chunks, and the
//! SHA-256 of its bytes are its record's, and a reader holds what it reads
//! to them. The Message keeps only which Stream it refers to. The digest is
//! computed by the writer from the bytes as they pass once
//! ([`StreamDigest`]), and an audit record of an act on a Message takes it
//! from here when the audit keeper keeps the Stream's bytes beside it
//! (ADR-0070).
//!
//! **Written with its last chunk.** The writer knows the length, the chunks
//! and the digest only once it has read the last; the record goes in the
//! same write as that chunk ([`super::XmipStorage::write_stream`]), unsynced
//! as every chunk is, and durable with the Publication after it — so a
//! receive cycle still costs one sync, and a crash before the Publication
//! leaves the Stream with no Message referring to it, as its chunks always
//! did.
//!
//! **Read back a chunk at a time** ([`ChunkReader`]): a Stream in the
//! Ledger, or the copy a kept audit record carries, one chunk held at a
//! time, up to the first there is not, and held to the length its record
//! keeps — and an audit record's copy to the digest the record carries too:
//! one that does not match is refused in words, never read as the audited
//! Stream (ADR-0070 clause 4).

use std::io::{self, Read};
use std::sync::Arc;

use codec::cursor::Cursor;
use codec::hex;
use sha2::{Digest as _, Sha256};
use xcore::{AuditId, StreamId};

use super::XmipStorage;
use super::record::{
    Form, StreamChunk, malformed, read_u32, read_u64, read_u128, write_u32, write_u64, write_u128,
};
use crate::PersistError;

/// How long a SHA-256 digest is, in bytes.
pub const DIGEST: usize = 32;

/// A Stream's own record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamRecord {
    pub stream: StreamId,
    /// Its length, in bytes.
    pub length: u64,
    /// How many chunks it is kept in; an empty Stream is one empty chunk.
    pub chunks: u32,
    /// The SHA-256 of its bytes, as its writer computed it from them
    /// passing once ([`StreamDigest`]).
    pub digest: [u8; DIGEST],
    /// When Xmip Storage wrote it, in nanoseconds since the Unix epoch:
    /// Xmip Storage's to set.
    pub written_unix_nanos: u64,
}

impl Form for StreamRecord {
    fn write(&self, out: &mut Vec<u8>) {
        write_u128(out, self.stream.value());
        write_u64(out, self.length);
        write_u32(out, self.chunks);
        write_digest(out, &self.digest);
        write_u64(out, self.written_unix_nanos);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            stream: StreamId::new(read_u128(cursor)?),
            length: read_u64(cursor)?,
            chunks: read_u32(cursor)?,
            digest: read_digest(cursor)?,
            written_unix_nanos: read_u64(cursor)?,
        })
    }
}

/// Where a Stream's chunk is kept: its Stream and its number in it.
pub(crate) fn chunk_key(stream: StreamId, index: u32) -> Vec<u8> {
    [
        stream.value().to_be_bytes().as_slice(),
        &index.to_be_bytes(),
    ]
    .concat()
}

pub(crate) fn write_digest(out: &mut Vec<u8>, digest: &[u8; DIGEST]) {
    out.extend_from_slice(digest);
}

pub(crate) fn read_digest(cursor: &mut Cursor<'_>) -> Result<[u8; DIGEST], PersistError> {
    let bytes = cursor
        .take(DIGEST)
        .map_err(|error| malformed(error.to_string()))?;
    <[u8; DIGEST]>::try_from(bytes).map_err(|_| malformed("a digest cut short"))
}

/// The SHA-256 of a Stream's bytes, taken a chunk at a time as they pass:
/// what its writer keeps in its record, and what a read of an audit
/// record's copy is held to.
#[derive(Clone, Default)]
pub struct StreamDigest(Sha256);

impl StreamDigest {
    /// `bytes` taken in, after those before.
    pub fn update(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    /// The digest of everything taken in.
    #[must_use]
    pub fn finish(self) -> [u8; DIGEST] {
        self.0.finalize().into()
    }
}

/// Where a [`ChunkReader`] reads a Stream's chunks from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Chunked {
    /// The Ledger: the Stream's own chunks.
    Ledger(StreamId),
    /// The copy of a Stream a kept audit record carries, and the digest the
    /// record says its bytes have.
    Audit(AuditId, StreamId, [u8; DIGEST]),
    /// A kept audit record's body, kept in chunks as a Stream is (ADR-0070,
    /// amendment 2026-10-10), and the digest the record says it has.
    AuditBody(AuditId, [u8; DIGEST]),
}

/// A Stream's chunks read in order, one held at a time, up to the first
/// there is not — where the Stream ends — and held to its length: a chunk
/// lost, damaged or deleted is refused in words, never read as a shorter
/// Stream. An audit record's copy is held to its digest too, at its end,
/// so a reader takes nothing it read as the audited Stream until the read
/// has ended without an error.
///
/// A read that fails says which of two things went wrong by its error's
/// kind: [`io::ErrorKind::InvalidData`] where the bytes are not the ones
/// kept — a chunk missing, failing its tag, or the whole not of its length
/// or digest — which is evidence; any other kind where Xmip Storage could
/// not be asked, which is no verdict at all. The [`PersistError`] it came
/// from is the error's inner one.
///
/// A chunk is taken whole by [`ChunkReader::next_chunk`], as kept, or a
/// piece at a time as a [`Read`]; a reader over Xmip Storage it shares
/// ([`ChunkReader::shared`]) outlives the call that made it, as one
/// Stream's read lent to several readers does.
pub struct ChunkReader<'a> {
    storage: Asking<'a>,
    from: Chunked,
    length: u64,
    next: u32,
    held: Vec<u8>,
    at: usize,
    /// How many bytes the chunks taken so far hold.
    read: u64,
    ended: bool,
    digest: Option<StreamDigest>,
}

/// Where a [`ChunkReader`] asks Xmip Storage: lent for the reader's life,
/// or shared with it.
enum Asking<'a> {
    Lent(&'a dyn XmipStorage),
    Shared(Arc<dyn XmipStorage>),
}

impl Asking<'_> {
    fn storage(&self) -> &dyn XmipStorage {
        match self {
            Self::Lent(storage) => *storage,
            Self::Shared(storage) => storage.as_ref(),
        }
    }
}

impl ChunkReader<'static> {
    /// The chunks `from` names behind `storage`, which the reader shares,
    /// `length` bytes together.
    #[must_use]
    pub fn shared(storage: Arc<dyn XmipStorage>, from: Chunked, length: u64) -> Self {
        Self::asking(Asking::Shared(storage), from, length)
    }

    /// The Ledger's Stream `stream` behind `storage`, `length` bytes
    /// together, read on from its chunk `chunk`, the chunks before it —
    /// `before` bytes — already taken: what a reader fallen behind a shared
    /// read of the Stream reads on from, asking Xmip Storage for the chunks
    /// it missed and never for one it took (open problem 38, 2026-10-20).
    /// Held to its length as a read from the start is: `before` and the
    /// chunks read on make the whole Stream, or it is refused.
    #[must_use]
    pub fn shared_from(
        storage: Arc<dyn XmipStorage>,
        stream: StreamId,
        length: u64,
        (chunk, before): (u32, u64),
    ) -> Self {
        let mut reader = Self::shared(storage, Chunked::Ledger(stream), length);
        (reader.next, reader.read) = (chunk, before);
        reader
    }
}

impl<'a> ChunkReader<'a> {
    /// The chunks `from` names behind `storage`, `length` bytes together.
    #[must_use]
    pub fn new(storage: &'a dyn XmipStorage, from: Chunked, length: u64) -> Self {
        Self::asking(Asking::Lent(storage), from, length)
    }

    fn asking(storage: Asking<'a>, from: Chunked, length: u64) -> Self {
        Self {
            storage,
            from,
            length,
            next: 0,
            held: Vec::new(),
            at: 0,
            read: 0,
            ended: false,
            digest: matches!(from, Chunked::Audit(..) | Chunked::AuditBody(..))
                .then(StreamDigest::default),
        }
    }

    /// The Stream `stream` that the kept audit record `audit` carries, read
    /// as the audit keeper kept it beside the record, held to the length
    /// and the digest its `audit_stream` row keeps; `None` where the record
    /// carries no such Stream, or was not kept yet.
    ///
    /// # Errors
    ///
    /// Where its row cannot be read, or fails its tag.
    pub fn audited(
        storage: &'a dyn XmipStorage,
        audit: AuditId,
        stream: StreamId,
    ) -> Result<Option<Self>, PersistError> {
        let kept = storage.read_kept_audit_stream(audit, stream)?;
        Ok(kept.map(|kept| {
            let from = Chunked::Audit(audit, stream, kept.digest);
            Self::new(storage, from, kept.length)
        }))
    }

    /// The body of the kept audit record `audit`, read a chunk at a time
    /// as the keeper kept it, held to the length and the digest its row
    /// keeps; `None` where no such record was kept.
    ///
    /// # Errors
    ///
    /// Where its row cannot be read, or fails its tag.
    pub fn audit_body(
        storage: &'a dyn XmipStorage,
        audit: AuditId,
    ) -> Result<Option<Self>, PersistError> {
        let kept = storage.read_kept_audit(audit)?;
        Ok(kept.map(|kept| {
            let from = Chunked::AuditBody(audit, kept.facts.body_digest);
            Self::new(storage, from, kept.facts.body_length)
        }))
    }

    /// The next chunk whole, as kept — what a [`Read`] took part of, the
    /// rest of it — and `None` at the Stream's end, held to its length, and
    /// an audit record's copy to its digest, as a read is: what a reader
    /// lending each chunk to others takes, rather than a copy of it.
    ///
    /// # Errors
    ///
    /// As a read fails, and of the same kind.
    pub fn next_chunk(&mut self) -> io::Result<Option<Vec<u8>>> {
        if self.at < self.held.len() {
            let rest = self.held.split_off(self.at);
            (self.held, self.at) = (Vec::new(), 0);
            return Ok(Some(rest));
        }
        if self.ended {
            return Ok(None);
        }
        let Some(chunk) = self.chunk().map_err(unread)? else {
            self.end()?;
            return Ok(None);
        };
        self.read += chunk.len() as u64;
        if let Some(digest) = self.digest.as_mut() {
            digest.update(&chunk);
        }
        self.next += 1;
        (self.held, self.at) = (Vec::new(), 0);
        Ok(Some(chunk))
    }

    fn chunk(&self) -> Result<Option<Vec<u8>>, PersistError> {
        let chunk = |chunk: Option<StreamChunk>| chunk.map(|chunk| chunk.bytes);
        let storage = self.storage.storage();
        match self.from {
            Chunked::Ledger(stream) => storage.read_chunk(stream, self.next).map(chunk),
            Chunked::Audit(audit, stream, _) => {
                storage.read_kept_audit_chunk(audit, Some(stream), self.next)
            }
            Chunked::AuditBody(audit, _) => storage.read_kept_audit_chunk(audit, None, self.next),
        }
    }

    /// The Stream ended at its first missing chunk: what was read before it
    /// is the whole Stream — of its length, and an audit's of its digest —
    /// or it is refused.
    fn end(&mut self) -> io::Result<()> {
        let (read, chunks, length) = (self.read, self.next, self.length);
        let (what, said) = match self.from {
            Chunked::Ledger(stream) if read != length => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "the Stream {stream} holds {read} bytes in {chunks} chunk(s) in the Ledger, \
                     and its record says {length}: a chunk was lost or damaged after it was \
                     published"
                    ),
                ));
            }
            Chunked::Ledger(_) => {
                self.ended = true;
                return Ok(());
            }
            Chunked::Audit(audit, stream, said) => (
                format!("the Stream {stream} the audit record {audit} carries"),
                said,
            ),
            Chunked::AuditBody(audit, said) => {
                (format!("the body of the audit record {audit}"), said)
            }
        };
        // A kept copy, held to its length and to its digest.
        if read != length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "REFUSED: {what} is {read} bytes in {chunks} chunk(s), and its record says \
                 {length}: it is not the audited one"
                ),
            ));
        }
        let digest = self.digest.take().unwrap_or_default().finish();
        if digest != said {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "REFUSED: {what} is not the audited one: its bytes' SHA-256 is {}, and the \
                 record says {}",
                    hex::encode(&digest),
                    hex::encode(&said)
                ),
            ));
        }
        self.ended = true;
        Ok(())
    }
}

/// A chunk's read that failed, as [`ChunkReader`] says it: a chunk that
/// failed its tag or would not decode is not the one kept; anything else is
/// Xmip Storage not answering.
fn unread(error: PersistError) -> io::Error {
    let kind = match error {
        PersistError::Refused { .. } | PersistError::Record { .. } => io::ErrorKind::InvalidData,
        _ => io::ErrorKind::Other,
    };
    io::Error::new(kind, error)
}

impl Read for ChunkReader<'_> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        while self.at == self.held.len() {
            let Some(chunk) = self.next_chunk()? else {
                return Ok(0);
            };
            (self.held, self.at) = (chunk, 0);
        }
        let taken = out.len().min(self.held.len() - self.at);
        out[..taken].copy_from_slice(&self.held[self.at..self.at + taken]);
        self.at += taken;
        Ok(taken)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_chunk_not_as_kept_is_evidence_and_storage_not_answering_is_not() {
        let refused = PersistError::Refused {
            scope: "chunk".to_string(),
            reason: "its tag".to_string(),
        };
        assert_eq!(unread(refused).kind(), io::ErrorKind::InvalidData);
        let unreachable = PersistError::Unreachable {
            reason: "no Storage node answered".to_string(),
        };
        let error = unread(unreachable);
        assert_eq!(error.kind(), io::ErrorKind::Other);
        let inner = error.into_inner().expect("its cause");
        assert!(matches!(
            inner.downcast::<PersistError>().as_deref(),
            Ok(PersistError::Unreachable { .. })
        ));
    }

    #[test]
    fn a_stream_s_record_comes_back_from_its_bytes_as_it_was() {
        let record = StreamRecord {
            stream: StreamId::new(9),
            length: 10_000,
            chunks: 3,
            digest: [7; DIGEST],
            written_unix_nanos: 42,
        };
        assert_eq!(
            StreamRecord::from_bytes(&record.bytes()).expect("read"),
            record
        );
    }

    #[test]
    fn the_digest_taken_a_chunk_at_a_time_is_the_sha_256_of_the_whole() {
        let mut digest = StreamDigest::default();
        digest.update(b"a");
        digest.update(b"bc");
        assert_eq!(
            hex::encode(&digest.finish()),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
