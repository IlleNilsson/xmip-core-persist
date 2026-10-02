//! The embedded Storage node: its runtime database and its administration
//! database kept by the node itself, for a single machine and an edge
//! site, with no failover (`deployment-model.md` section 7).
//!
//! Both are persist's [`EncryptedStore`], so Xmip encrypts its own files at
//! rest itself, test nodes included (ADR-0063, amendment 2026-10-01). Which
//! engines are beneath is the program's: `RocksDB` for the runtime database,
//! always, and `SQLite` for the administration database (ADR-0015,
//! amendment 2026-10-01) — on disk for a node, in memory for a Storage node
//! under test. Every runtime write goes through one writer that shares one
//! sync among the writes waiting ([`super::commit`]); an administration
//! write is the administration engine's own, durable on return.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use secret::{KekName, KeyStore};
use xcore::{AuditId, Clock, JourneyId, MessageId, StreamId, SystemClock};

use super::XmipStorage;
use super::commit::{AUDIT, CHUNK, Committer, Done, JOURNEY, KEPT, MESSAGE, NEXT, Op, sequence};
use super::record::{
    AdministrationKind, AdministrationRecord, AuditEntry, Claim, Form, HandOn, JourneyRecord,
    MessageRecord, StreamChunk,
};
use crate::{EncryptedStore, Engine, PersistError};

/// Where the administration database keeps an audit record the keeper
/// moved, by its identifier.
const KEPT_AUDIT: &str = "audit";

/// The embedded Storage node's two databases, and the one writer to the
/// first.
pub struct Embedded<R: Engine + 'static, A: Engine> {
    runtime: Arc<EncryptedStore<R>>,
    administration: EncryptedStore<A>,
    committer: Committer,
    /// One audit keeper at a time in this process.
    keeping: Mutex<()>,
}

impl<R: Engine + 'static, A: Engine> Embedded<R, A> {
    /// The node's databases over `runtime` and `administration`, each
    /// sealed under its own data key wrapped by `keys` under `kek`.
    ///
    /// # Errors
    ///
    /// Where either store does not open — its key does not unwrap, its
    /// engine refuses — or the writer cannot start.
    pub fn open(
        runtime: R,
        administration: A,
        keys: &dyn KeyStore,
        kek: &KekName,
    ) -> Result<Self, PersistError> {
        Self::over(
            EncryptedStore::open(runtime, keys, kek)?,
            EncryptedStore::open(administration, keys, kek)?,
            Arc::new(SystemClock),
        )
    }

    /// The node over two stores already open, telling time by `clock`.
    ///
    /// # Errors
    ///
    /// Where the writer cannot start.
    pub fn over(
        runtime: EncryptedStore<R>,
        administration: EncryptedStore<A>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, PersistError> {
        let runtime = Arc::new(runtime);
        let committer = Committer::start(Arc::clone(&runtime), clock)?;
        Ok(Self {
            runtime,
            administration,
            committer,
            keeping: Mutex::new(()),
        })
    }

    /// The runtime database's store, for a test that reaches past the
    /// operations to the engine.
    #[must_use]
    pub fn runtime(&self) -> &EncryptedStore<R> {
        &self.runtime
    }

    /// The administration database's store, likewise.
    #[must_use]
    pub fn administration(&self) -> &EncryptedStore<A> {
        &self.administration
    }

    fn written(&self, op: Op) -> Result<(), PersistError> {
        self.committer.submit(op).map(drop)
    }

    fn claimed(&self, op: Op) -> Result<Option<Claim>, PersistError> {
        match self.committer.submit(op)? {
            Done::Claim(claim) => Ok(claim),
            _ => Err(super::record::malformed("the writer answered no claim")),
        }
    }

    fn yes(&self, op: Op) -> Result<bool, PersistError> {
        match self.committer.submit(op)? {
            Done::Yes(yes) => Ok(yes),
            _ => Err(super::record::malformed("the writer answered no verdict")),
        }
    }

    fn read<T: Form>(&self, kind: &str, key: &[u8]) -> Result<Option<T>, PersistError> {
        self.runtime
            .get(kind, key)?
            .map(|bytes| T::from_bytes(&bytes))
            .transpose()
    }
}

fn lease_nanos(lease: Duration) -> i128 {
    i128::try_from(lease.as_nanos()).unwrap_or(i128::MAX)
}

fn chunk_key(stream: StreamId, index: u32) -> Vec<u8> {
    [
        stream.value().to_be_bytes().as_slice(),
        &index.to_be_bytes(),
    ]
    .concat()
}

fn administration_kind(kind: AdministrationKind) -> String {
    format!("administration/{}", kind.word())
}

impl<R: Engine + 'static, A: Engine> XmipStorage for Embedded<R, A> {
    fn write_chunk(&self, chunk: &StreamChunk) -> Result<(), PersistError> {
        let key = chunk_key(chunk.stream, chunk.index);
        self.written(Op::Put(CHUNK, key, chunk.bytes()))
    }

    fn read_chunk(
        &self,
        stream: StreamId,
        index: u32,
    ) -> Result<Option<StreamChunk>, PersistError> {
        self.read(CHUNK, &chunk_key(stream, index))
    }

    fn write_message(&self, message: &MessageRecord) -> Result<(), PersistError> {
        let key = message.message.value().to_be_bytes().to_vec();
        self.written(Op::Put(MESSAGE, key, message.bytes()))
    }

    fn read_message(&self, message: MessageId) -> Result<Option<MessageRecord>, PersistError> {
        self.read(MESSAGE, &message.value().to_be_bytes())
    }

    fn write_journey(&self, journey: &JourneyRecord) -> Result<(), PersistError> {
        let key = journey.journey.value().to_be_bytes().to_vec();
        self.written(Op::Put(JOURNEY, key, journey.bytes()))
    }

    fn read_journey(&self, journey: JourneyId) -> Result<Option<JourneyRecord>, PersistError> {
        self.read(JOURNEY, &journey.value().to_be_bytes())
    }

    fn claim(
        &self,
        journey: JourneyId,
        holder: &str,
        token: u128,
        lease: Duration,
    ) -> Result<Option<Claim>, PersistError> {
        let claim = Claim {
            journey,
            holder: holder.to_string(),
            token,
            until_unix_nanos: 0,
        };
        self.claimed(Op::Claim {
            claim,
            lease_nanos: lease_nanos(lease),
        })
    }

    fn renew(&self, claim: &Claim, lease: Duration) -> Result<Option<Claim>, PersistError> {
        self.claimed(Op::Renew {
            claim: claim.clone(),
            lease_nanos: lease_nanos(lease),
        })
    }

    fn release(&self, claim: &Claim) -> Result<bool, PersistError> {
        self.yes(Op::Release(claim.clone()))
    }

    fn hand_on(&self, hand_on: &HandOn) -> Result<bool, PersistError> {
        self.yes(Op::HandOn(Box::new(hand_on.clone())))
    }

    fn write_audit(&self, entry: &AuditEntry) -> Result<(), PersistError> {
        self.written(Op::Audit(entry.clone()))
    }

    fn keep_audit(&self, most: u32) -> Result<u32, PersistError> {
        let _keeping = self.keeping.lock().unwrap_or_else(PoisonError::into_inner);
        let kept = sequence(&self.runtime, KEPT)?;
        let next = sequence(&self.runtime, NEXT)?;
        let mut moved = 0;
        for number in kept..next.min(kept.saturating_add(u64::from(most))) {
            let entry: AuditEntry = self.read(AUDIT, &number.to_be_bytes())?.ok_or_else(|| {
                super::record::malformed(format!("audit record {number} is gone"))
            })?;
            // Kept by its identifier, once: a record already there — this
            // move cut short before, or the same record written twice — is
            // not kept again.
            let id = entry.id.value().to_be_bytes();
            self.administration
                .put_new(KEPT_AUDIT, &id, &entry.bytes())?;
            if !self.yes(Op::Kept(number))? {
                break;
            }
            moved += 1;
        }
        Ok(moved)
    }

    fn read_kept_audit(&self, id: AuditId) -> Result<Option<AuditEntry>, PersistError> {
        self.administration
            .get(KEPT_AUDIT, &id.value().to_be_bytes())?
            .map(|bytes| AuditEntry::from_bytes(&bytes))
            .transpose()
    }

    fn write_administration(&self, record: &AdministrationRecord) -> Result<(), PersistError> {
        self.administration.put(
            &administration_kind(record.kind),
            &record.id.to_be_bytes(),
            &record.bytes(),
        )
    }

    fn read_administration(
        &self,
        kind: AdministrationKind,
        id: u128,
    ) -> Result<Option<AdministrationRecord>, PersistError> {
        self.administration
            .get(&administration_kind(kind), &id.to_be_bytes())?
            .map(|bytes| AdministrationRecord::from_bytes(&bytes))
            .transpose()
    }

    fn remove_administration(
        &self,
        kind: AdministrationKind,
        id: u128,
    ) -> Result<(), PersistError> {
        self.administration
            .remove(&administration_kind(kind), &id.to_be_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::Memory;
    use secret::Held;

    /// A clock a test moves by hand.
    #[derive(Default)]
    struct Pinned(Mutex<i128>);

    impl Pinned {
        fn pass(&self, nanos: i128) {
            *self.0.lock().expect("clock") += nanos;
        }
    }

    impl Clock for Pinned {
        fn unix_timestamp_nanos(&self) -> i128 {
            *self.0.lock().expect("clock")
        }
    }

    type Node = Embedded<Memory, Memory>;

    fn node() -> (Node, Arc<Pinned>) {
        let keys = Held::new(secret::fixture::Memory::default());
        let kek = KekName::new("storage").expect("name");
        let clock = Arc::new(Pinned::default());
        let node = Embedded::over(
            EncryptedStore::open(Memory::default(), &keys, &kek).expect("runtime"),
            EncryptedStore::open(Memory::default(), &keys, &kek).expect("administration"),
            Arc::clone(&clock) as Arc<dyn Clock>,
        )
        .expect("node");
        (node, clock)
    }

    const LEASE: Duration = Duration::from_secs(30);
    const JOURNEY_ID: JourneyId = JourneyId::new(0x0199_0000_0000_7000_8000_0000_0000_0001);

    fn journey(id: JourneyId, body: &[u8]) -> JourneyRecord {
        JourneyRecord {
            journey: id,
            body: body.to_vec(),
        }
    }

    #[test]
    fn what_is_written_is_read_back_and_what_is_not_is_none() {
        let (node, _) = node();
        let chunk = StreamChunk {
            stream: StreamId::new(4),
            index: 1,
            last: true,
            bytes: b"<Order/>".to_vec(),
        };
        node.write_chunk(&chunk).expect("chunk");
        assert_eq!(
            node.read_chunk(StreamId::new(4), 1).expect("read"),
            Some(chunk)
        );
        assert_eq!(node.read_chunk(StreamId::new(4), 0).expect("read"), None);
        let message = MessageRecord {
            message: MessageId::new(5),
            body: b"context".to_vec(),
        };
        node.write_message(&message).expect("message");
        assert_eq!(
            node.read_message(MessageId::new(5)).expect("read"),
            Some(message)
        );
        node.write_journey(&journey(JOURNEY_ID, b"open"))
            .expect("journey");
        let read = node.read_journey(JOURNEY_ID).expect("read");
        assert_eq!(read, Some(journey(JOURNEY_ID, b"open")));
        let pause = AdministrationRecord {
            kind: AdministrationKind::Operator,
            id: 6,
            body: b"paused by ilian".to_vec(),
        };
        node.write_administration(&pause).expect("administration");
        let kept = node.read_administration(AdministrationKind::Operator, 6);
        assert_eq!(kept.expect("read"), Some(pause));
        let other = node.read_administration(AdministrationKind::Deployment, 6);
        assert_eq!(other.expect("read"), None);
        node.remove_administration(AdministrationKind::Operator, 6)
            .expect("removed");
        let gone = node.read_administration(AdministrationKind::Operator, 6);
        assert_eq!(gone.expect("read"), None);
    }

    #[test]
    fn of_two_claimants_one_wins_and_asking_again_takes_nothing_twice() {
        let (node, _) = node();
        let first = node.claim(JOURNEY_ID, "xmip:///C1/node/alpha", 1, LEASE);
        let first = first.expect("claimed").expect("taken");
        let second = node.claim(JOURNEY_ID, "xmip:///C1/node/beta", 2, LEASE);
        assert_eq!(second.expect("claimed"), None);
        let again = node.claim(JOURNEY_ID, "xmip:///C1/node/alpha", 1, LEASE);
        assert_eq!(again.expect("claimed"), Some(first));
    }

    #[test]
    fn a_lapsed_claim_is_taken_over_and_its_old_holder_can_neither_renew_nor_hand_on() {
        let (node, clock) = node();
        let old = node
            .claim(JOURNEY_ID, "alpha", 1, LEASE)
            .expect("claimed")
            .expect("taken");
        clock.pass(10_000_000_000);
        let renewed = node
            .renew(&old, LEASE)
            .expect("renewed")
            .expect("still held");
        assert_eq!(renewed.until_unix_nanos, 40_000_000_000);
        clock.pass(41_000_000_000);
        let new = node
            .claim(JOURNEY_ID, "beta", 2, LEASE)
            .expect("claimed")
            .expect("lapsed");
        assert_eq!(new.holder, "beta");
        assert_eq!(node.renew(&old, LEASE).expect("renew"), None);
        let late = HandOn {
            claim: old.clone(),
            result: journey(JOURNEY_ID, b"by the old holder"),
            messages: Vec::new(),
            next: Vec::new(),
        };
        assert!(!node.hand_on(&late).expect("hand-on"));
        assert_eq!(node.read_journey(JOURNEY_ID).expect("read"), None);
        assert!(!node.release(&old).expect("release"));
        assert!(node.release(&new).expect("release"));
        let freed = node.claim(JOURNEY_ID, "gamma", 3, LEASE).expect("claimed");
        assert!(freed.is_some(), "a release frees it at once");
    }

    #[test]
    fn a_hand_on_writes_its_result_what_it_made_and_what_follows_and_lets_go_together() {
        let (node, _) = node();
        let claim = node
            .claim(JOURNEY_ID, "alpha", 1, LEASE)
            .expect("claimed")
            .expect("taken");
        let next = JourneyId::new(0x0199_0000_0000_7000_8000_0000_0000_0002);
        let hand_on = HandOn {
            claim,
            result: journey(JOURNEY_ID, b"routed"),
            messages: vec![MessageRecord {
                message: MessageId::new(9),
                body: b"generation 2".to_vec(),
            }],
            next: vec![journey(next, b"to send")],
        };
        assert!(node.hand_on(&hand_on).expect("handed on"));
        assert_eq!(
            node.read_journey(JOURNEY_ID).expect("read"),
            Some(hand_on.result.clone())
        );
        assert_eq!(
            node.read_journey(next).expect("read"),
            Some(journey(next, b"to send"))
        );
        assert!(
            node.read_message(MessageId::new(9))
                .expect("read")
                .is_some()
        );
        assert!(
            node.hand_on(&hand_on).expect("asked again"),
            "a lost answer"
        );
        let taken = node.claim(JOURNEY_ID, "beta", 2, LEASE).expect("claimed");
        assert!(taken.is_some(), "the claim went with the hand-on");
    }

    #[test]
    fn claimants_racing_from_many_threads_leave_one_holder() {
        let (node, _) = node();
        let node = Arc::new(node);
        let winners: usize = (0..16u128)
            .map(|token| {
                let node = Arc::clone(&node);
                std::thread::spawn(move || {
                    node.claim(JOURNEY_ID, &format!("node-{token}"), token, LEASE)
                        .expect("claimed")
                        .is_some()
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|racer| usize::from(racer.join().expect("racer")))
            .sum();
        assert_eq!(winners, 1);
    }

    #[test]
    fn the_audit_keeper_moves_each_record_once_oldest_first() {
        let (node, _) = node();
        let entries: Vec<AuditEntry> = (1..=5u128)
            .map(|id| AuditEntry {
                id: AuditId::new(id),
                body: format!("record {id}").into_bytes(),
            })
            .collect();
        for entry in &entries {
            node.write_audit(entry).expect("written");
        }
        // The same record written twice, as a request asked again after a
        // lost answer writes it.
        node.write_audit(&entries[0]).expect("written again");
        // A move cut short: kept there already, not yet forgotten here.
        let second = &entries[1];
        node.administration()
            .put_new(
                KEPT_AUDIT,
                &second.id.value().to_be_bytes(),
                &second.bytes(),
            )
            .expect("kept before");
        assert_eq!(node.keep_audit(2).expect("kept"), 2);
        assert_eq!(node.keep_audit(100).expect("kept"), 4);
        assert_eq!(node.keep_audit(100).expect("kept"), 0);
        for entry in &entries {
            let kept = node.read_kept_audit(entry.id).expect("read");
            assert_eq!(kept, Some(entry.clone()));
        }
        assert_eq!(sequence(node.runtime(), KEPT).expect("kept"), 6);
        for number in 0..6u64 {
            let left = node
                .runtime()
                .get(AUDIT, &number.to_be_bytes())
                .expect("read");
            assert_eq!(left, None, "record {number} left the runtime database");
        }
    }
}
