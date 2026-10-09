//! A Stream as Xmip Storage keeps it beside its chunks (proposed
//! 2026-10-09; the owner, the same day: *a Message refers to a stream, a
//! stream is stored in chunks*): its own record, one per Stream, keyed by
//! its identifier, which a chunk and a Message refer to by it.
//!
//! **The one home of a Stream's length.** A Stream ends where it has no
//! further chunk; how long it is, and in how many chunks, is its record's,
//! and a reader holds what it reads to it. The Message keeps only which
//! Stream it refers to.
//!
//! **Written with its last chunk.** The writer knows the length and the
//! chunks only once it has read the last; the record goes in the same write
//! as that chunk ([`super::XmipStorage::write_stream`]), unsynced as every
//! chunk is, and durable with the Publication after it — so a receive cycle
//! still costs one sync, and a crash before the Publication leaves the
//! Stream with no Message referring to it, as its chunks always did.

use codec::cursor::Cursor;
use xcore::StreamId;

use super::record::{Form, read_u32, read_u64, read_u128, write_u32, write_u64, write_u128};
use crate::PersistError;

/// A Stream's own record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamRecord {
    pub stream: StreamId,
    /// Its length, in bytes.
    pub length: u64,
    /// How many chunks it is kept in; an empty Stream is one empty chunk.
    pub chunks: u32,
    /// When Xmip Storage wrote it, in nanoseconds since the Unix epoch:
    /// Xmip Storage's to set.
    pub written_unix_nanos: u64,
}

impl Form for StreamRecord {
    fn write(&self, out: &mut Vec<u8>) {
        write_u128(out, self.stream.value());
        write_u64(out, self.length);
        write_u32(out, self.chunks);
        write_u64(out, self.written_unix_nanos);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            stream: StreamId::new(read_u128(cursor)?),
            length: read_u64(cursor)?,
            chunks: read_u32(cursor)?,
            written_unix_nanos: read_u64(cursor)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stream_s_record_comes_back_from_its_bytes_as_it_was() {
        let record = StreamRecord {
            stream: StreamId::new(9),
            length: 10_000,
            chunks: 3,
            written_unix_nanos: 42,
        };
        assert_eq!(
            StreamRecord::from_bytes(&record.bytes()).expect("read"),
            record
        );
    }
}
