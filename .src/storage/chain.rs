//! A node's audit records in Xmip Storage are a chain, one per writer
//! (ADR-0070 clause 5, amended 2026-10-10: *one chain per writer* — the
//! node's, or the program's where no node writes it), formed where the
//! audit log is written: by the audit keeper, as it keeps each record in
//! the administration database (`super::embedded::keeper`). It keeps them
//! one at a time, in the order they were written, each once by its
//! identifier — a record asked twice after a lost answer is chained once —
//! and it reads the record of every Stream a record carries to keep it
//! beside it. Each record is given the next number in its writer's chain
//! and the digest of the record before it there, and its own digest over
//! its canonical form ([`canonical_form`]); the head of the chain — the
//! number and the digest of its last record — is kept in the record's own
//! write, so the chain goes on from it after a restart, of the node or of
//! Xmip Storage.
//!
//! **What is digested.** The record's canonical form: its kept row — its
//! identifier, the Streams it carries, every field it keeps in a column,
//! its writer, its number, the digest before it and its body's length,
//! chunks and SHA-256 among them; its own digest and the keeper's time
//! left out — and the record of every Stream it carries, in its order,
//! each once: identifier, length, chunks, the SHA-256 of its bytes and when
//! it was written, as its `audit_stream` row keeps it. The body — its
//! record and the Message in full — is inside the chain through the digest
//! the keeper took as its chunks passed, and a Stream's bytes through
//! theirs; a read of either is held to it (ADR-0070 clause 4).
//!
//! The walk that says where a chain breaks is the audit capability's, one
//! for every log (`xmip-core-audit`, `audit_chain::walk`); what it walks
//! here is found by the kept table's index on the writer and the number
//! ([`super::Ask::AuditChain`]) and digested by [`chain_digest`].

use sha2::{Digest as _, Sha256};
use xcore::StreamId;

use super::audit_entry::AuditEntry;
use super::facts::AuditFacts;
use super::kept_audit::KeptAudit;
use super::record::{Form, malformed, write_u32};
use super::stream::{DIGEST, StreamRecord};
use crate::{EncryptedStore, Engine, PersistError, RecordChange};

/// Where the administration database keeps the head of each writer's
/// chain, by the writer: the number of its last record and its digest.
pub(crate) const CHAIN: &str = "audit-chain-head";

/// The canonical form of the kept record `kept`, which carries `streams`
/// (ADR-0070 clause 5): what its digest is taken over.
#[must_use]
pub fn canonical_form(kept: &KeptAudit, streams: &[StreamRecord]) -> Vec<u8> {
    let unsealed = KeptAudit {
        facts: AuditFacts {
            digest: [0; DIGEST],
            kept_unix_nanos: 0,
            ..kept.facts.clone()
        },
        ..kept.clone()
    };
    let mut out = unsealed.bytes();
    write_u32(&mut out, u32::try_from(streams.len()).unwrap_or(u32::MAX));
    for stream in streams {
        stream.write(&mut out);
    }
    out
}

/// The SHA-256 of the canonical form of `kept`, which carries `streams`.
#[must_use]
pub fn chain_digest(kept: &KeptAudit, streams: &[StreamRecord]) -> [u8; DIGEST] {
    Sha256::digest(canonical_form(kept, streams)).into()
}

/// The Streams `entry` carries, in its order, each once.
#[must_use]
pub fn carried_streams(entry: &AuditEntry) -> Vec<StreamId> {
    let mut carried: Vec<StreamId> = Vec::new();
    for stream in entry.audited.iter().flat_map(|audited| &audited.streams) {
        if !carried.contains(stream) {
            carried.push(*stream);
        }
    }
    carried
}

/// `kept`, which carries `streams`, given its place in its writer's
/// chain after the head `store` holds; and the head moved to it, to go in
/// the record's own write.
///
/// # Errors
///
/// A head that cannot be read, or is not one.
pub(crate) fn link<A: Engine>(
    store: &EncryptedStore<A>,
    kept: &mut KeptAudit,
    streams: &[StreamRecord],
) -> Result<RecordChange<'static>, PersistError> {
    let writer = kept.facts.writer.as_bytes().to_vec();
    let (position, previous) = match store.get(CHAIN, &writer)? {
        Some(head) => {
            let (number, digest) = head.split_at_checked(8).ok_or_else(cut)?;
            let number = u64::from_be_bytes(number.try_into().map_err(|_| cut())?);
            (number, digest.try_into().map_err(|_| cut())?)
        }
        None => (0, [0; DIGEST]),
    };
    kept.facts.position = position + 1;
    kept.facts.previous = previous;
    kept.facts.digest = chain_digest(kept, streams);
    let head = [&kept.facts.position.to_be_bytes()[..], &kept.facts.digest].concat();
    Ok((CHAIN, writer, Some(head)))
}

fn cut() -> PersistError {
    malformed("an audit chain's head that is not a number and a digest")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use secret::{Held, KekName};
    use xcore::AuditId;

    use super::*;
    use crate::EncryptedStore;
    use crate::fixture::Memory;
    use crate::storage::{Ask, AuditFacts, Embedded, Query, XmipStorage};

    /// A chunk a test's bodies are kept in.
    const CHUNK: usize = 16;

    type Node = Embedded<&'static Memory, &'static Memory>;
    type Keys = Held<secret::fixture::Memory>;

    /// Two engines that outlive every node over them: the committer's
    /// thread holds its store for as long as the process.
    fn stores() -> &'static (Memory, Memory) {
        Box::leak(Box::new((Memory::default(), Memory::default())))
    }

    fn node(keys: &Keys, (runtime, administration): (&'static Memory, &'static Memory)) -> Node {
        let kek = KekName::new("storage").expect("name");
        Embedded::over(
            EncryptedStore::open(runtime, keys, &kek).expect("runtime"),
            EncryptedStore::open(administration, keys, &kek).expect("administration"),
            Arc::new(xcore::SystemClock),
        )
        .expect("node")
    }

    fn write(node: &Node, id: u128, writer: &str) {
        let entry = AuditEntry {
            id: AuditId::new(id),
            body: format!("[[record]]\naudit_id = \"{id}\"\n\n").into_bytes(),
            audited: None,
            facts: AuditFacts {
                writer: writer.to_string(),
                ..AuditFacts::default()
            },
        };
        node.write_audit(&entry).expect("written");
    }

    /// `writer`'s chain as kept, in its order.
    fn kept(node: &Node, writer: &str) -> Vec<KeptAudit> {
        node.keep_audit(100, CHUNK).expect("kept");
        let asked = Query {
            ask: Ask::AuditChain {
                writer: writer.to_string(),
                from: 1,
            },
            most: 100,
            newest_first: false,
        };
        let ids = node.query(&asked).expect("asked");
        ids.into_iter()
            .map(|id| node.read_kept_audit(AuditId::new(id)).expect("read"))
            .map(|kept| kept.expect("kept"))
            .collect()
    }

    /// Each record numbered from 1, chained to the one before it, its
    /// digest its canonical form's.
    fn chained(chain: &[KeptAudit]) {
        let mut previous = [0; DIGEST];
        for (number, entry) in (1..).zip(chain) {
            assert_eq!(entry.facts.position, number);
            assert_eq!(entry.facts.previous, previous, "number {number}");
            assert_eq!(entry.facts.digest, chain_digest(entry, &[]));
            previous = entry.facts.digest;
        }
    }

    #[test]
    fn each_writer_s_records_are_numbered_and_chained_on_their_own() {
        let keys = Held::new(secret::fixture::Memory::default());
        let stores = stores();
        let node = node(&keys, (&stores.0, &stores.1));
        let cluster = configure::fixture::test_cluster();
        let (first, second) = (cluster.node_scope(0), cluster.node_scope(1));
        for id in 1..=5 {
            write(&node, id, if id % 2 == 1 { &first } else { &second });
        }

        let (one, two) = (kept(&node, &first), kept(&node, &second));
        assert_eq!(one.len(), 3);
        assert_eq!(two.len(), 2);
        chained(&one);
        chained(&two);
    }

    #[test]
    fn a_writer_s_chain_goes_on_from_its_head_after_a_restart() {
        let keys = Held::new(secret::fixture::Memory::default());
        let stores = stores();
        let writer = configure::fixture::test_cluster().node_scope(0);
        {
            let before = node(&keys, (&stores.0, &stores.1));
            write(&before, 1, &writer);
            write(&before, 2, &writer);
            assert_eq!(before.keep_audit(10, CHUNK).expect("kept"), 2);
        }
        let after = node(&keys, (&stores.0, &stores.1));
        write(&after, 3, &writer);

        let chain = kept(&after, &writer);
        assert_eq!(chain.len(), 3);
        chained(&chain);
    }

    #[test]
    fn a_stream_s_record_is_inside_the_canonical_form() {
        let entry = KeptAudit {
            id: AuditId::new(1),
            streams: vec![StreamId::new(2)],
            facts: AuditFacts::default(),
        };
        let mut stream = StreamRecord {
            stream: StreamId::new(2),
            length: 3,
            chunks: 1,
            digest: [4; DIGEST],
            written_unix_nanos: 5,
        };
        let sealed = chain_digest(&entry, &[stream]);
        stream.digest[0] ^= 1;
        assert_ne!(chain_digest(&entry, &[stream]), sealed);
        let mut kept = entry.clone();
        kept.facts.kept_unix_nanos = 9;
        kept.facts.digest = sealed;
        assert_eq!(
            canonical_form(&kept, &[]),
            canonical_form(&entry, &[]),
            "its own digest and the keeper's time are left out"
        );
    }
}
