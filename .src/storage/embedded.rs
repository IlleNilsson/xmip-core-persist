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
use super::dead::{self, DeadEntry, DeadQueue, Replay, Replayed};
use super::hold::{self, HeldQueue};
use super::publication::Publication;
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
        // Straight to the engine, unsynced and needing no condition: the
        // writer's next batch syncs it with whatever it writes.
        let key = chunk_key(chunk.stream, chunk.index);
        self.runtime
            .apply_deferred(&[(CHUNK, key, Some(chunk.bytes()))])
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

    fn publish(&self, publication: &Publication) -> Result<(), PersistError> {
        self.written(Op::Publish(Box::new(publication.clone())))
    }

    fn read_held(&self, queue: u128, from: u64, most: u32) -> Result<HeldQueue, PersistError> {
        hold::read(&self.runtime, queue, from, most)
    }

    fn release_held(
        &self,
        queue: u128,
        sequence: u64,
        journey: &JourneyRecord,
    ) -> Result<(), PersistError> {
        self.written(Op::ReleaseHeld {
            queue,
            sequence,
            journey: journey.clone(),
        })
    }

    fn read_dead(&self, queue: u128, from: u64, most: u32) -> Result<DeadQueue, PersistError> {
        dead::read(&self.runtime, queue, from, most)
    }

    fn read_dead_message(
        &self,
        queue: u128,
        message: MessageId,
    ) -> Result<DeadEntry, PersistError> {
        dead::read_one(&self.runtime, queue, message)
    }

    fn replay(&self, replay: &Replay) -> Result<Replayed, PersistError> {
        match self
            .committer
            .submit(Op::Replay(Box::new(replay.clone())))?
        {
            Done::Replayed(replayed) => Ok(replayed),
            _ => Err(super::record::malformed("the writer answered no Replay")),
        }
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

    /// The scope of the test cluster's node at `place`: a claim's holder.
    fn holder(place: usize) -> String {
        configure::fixture::test_cluster().node_scope(place)
    }

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
        let first = node.claim(JOURNEY_ID, &holder(0), 1, LEASE);
        let first = first.expect("claimed").expect("taken");
        let second = node.claim(JOURNEY_ID, &holder(1), 2, LEASE);
        assert_eq!(second.expect("claimed"), None);
        let again = node.claim(JOURNEY_ID, &holder(0), 1, LEASE);
        assert_eq!(again.expect("claimed"), Some(first));
    }

    #[test]
    fn a_lapsed_claim_is_taken_over_and_its_old_holder_can_neither_renew_nor_hand_on() {
        let (node, clock) = node();
        let old = node
            .claim(JOURNEY_ID, &holder(0), 1, LEASE)
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
            .claim(JOURNEY_ID, &holder(1), 2, LEASE)
            .expect("claimed")
            .expect("lapsed");
        assert_eq!(new.holder, holder(1));
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
        let freed = node
            .claim(JOURNEY_ID, &holder(2), 3, LEASE)
            .expect("claimed");
        assert!(freed.is_some(), "a release frees it at once");
    }

    #[test]
    fn a_hand_on_writes_its_result_what_it_made_and_what_follows_and_lets_go_together() {
        let (node, _) = node();
        let claim = node
            .claim(JOURNEY_ID, &holder(0), 1, LEASE)
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
        let taken = node
            .claim(JOURNEY_ID, &holder(1), 2, LEASE)
            .expect("claimed");
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
    fn a_publication_writes_its_message_its_journeys_and_its_audit_together() {
        let (node, _) = node();
        let publication = Publication {
            message: MessageRecord {
                message: MessageId::new(7),
                body: b"order".to_vec(),
            },
            journeys: vec![
                journey(JOURNEY_ID, b"to billing"),
                journey(JourneyId::new(8), b"x"),
            ],
            held: Vec::new(),
            dead: None,
            audit: AuditEntry {
                id: AuditId::new(9),
                body: b"published".to_vec(),
            },
        };
        node.publish(&publication).expect("published");
        assert_eq!(
            node.read_message(MessageId::new(7)).expect("read"),
            Some(publication.message.clone())
        );
        for written in &publication.journeys {
            let read = node.read_journey(written.journey).expect("read");
            assert_eq!(read.as_ref(), Some(written));
        }
        assert_eq!(node.keep_audit(10).expect("kept"), 1);
        let kept = node.read_kept_audit(AuditId::new(9)).expect("read");
        assert_eq!(kept, Some(publication.audit));
    }

    /// A Publication holding the Journey `id` in `queue`.
    fn holding(queue: u128, id: u128) -> Publication {
        Publication {
            message: MessageRecord {
                message: MessageId::new(id),
                body: b"order".to_vec(),
            },
            journeys: vec![journey(JourneyId::new(id), b"held")],
            held: vec![super::super::Hold {
                queue,
                journey: JourneyId::new(id),
                body: id.to_be_bytes().to_vec(),
            }],
            dead: None,
            audit: AuditEntry {
                id: AuditId::new(id),
                body: b"published".to_vec(),
            },
        }
    }

    /// A Publication of the Message `id` that matched nothing, kept in the
    /// Dead Message Queue `queue`.
    fn unmatched(queue: u128, id: u128) -> Publication {
        Publication {
            message: MessageRecord {
                message: MessageId::new(id),
                body: b"invoice".to_vec(),
            },
            journeys: Vec::new(),
            held: Vec::new(),
            dead: Some(super::super::DeadMessage {
                queue,
                message: MessageId::new(id),
                stream: StreamId::new(id),
                location: "a Receive Location".to_string(),
                promoted: vec![super::super::Named::new("MessageType", "Invoice")],
                declines: vec![super::super::Named::new("orders", "MessageType is Invoice")],
                body: b"facts".to_vec(),
                ..super::super::DeadMessage::default()
            }),
            audit: AuditEntry {
                id: AuditId::new(id),
                body: b"published".to_vec(),
            },
        }
    }

    /// The Replay of the Message `id` in `queue`, opening one Journey held
    /// in the Subscription queue `held`.
    fn replaying(queue: u128, id: u128, held: u128) -> Replay {
        let opened = JourneyId::new(id + 1000);
        Replay {
            queue,
            message: MessageId::new(id),
            journeys: vec![journey(opened, b"replayed")],
            held: vec![super::super::Hold {
                queue: held,
                journey: opened,
                body: b"facts".to_vec(),
            }],
            audit: AuditEntry {
                id: AuditId::new(id + 1000),
                body: b"replayed".to_vec(),
            },
        }
    }

    #[test]
    fn a_message_nothing_matched_is_kept_with_its_entry_and_listed_oldest_first_in_pages() {
        let (node, _) = node();
        for id in 1..=5u128 {
            node.publish(&unmatched(7, id)).expect("published");
        }
        node.publish(&unmatched(7, 3))
            .expect("asked again: kept once");
        node.publish(&unmatched(8, 9))
            .expect("another node's queue");
        let first = node.read_dead(7, 0, 2).expect("read");
        let places: Vec<u64> = first.dead.iter().map(|dead| dead.sequence).collect();
        assert_eq!((first.first, first.next, first.count), (0, 5, 5));
        assert_eq!(places, [0, 1]);
        let next = node.read_dead(7, places[1] + 1, 2).expect("the next page");
        let messages: Vec<MessageId> = next.dead.iter().map(|dead| dead.message.message).collect();
        assert_eq!(messages, [MessageId::new(3), MessageId::new(4)]);
        let last = node.read_dead(7, 4, 2).expect("the last page");
        assert_eq!(last.dead.len(), 1);
        assert_eq!(node.read_dead(8, 0, 10).expect("read").count, 1);

        let one = node.read_dead_message(7, MessageId::new(2)).expect("read");
        let DeadEntry::Kept(one) = one else {
            panic!("kept: {one:?}");
        };
        assert_eq!(one.message.declines[0].name, "orders");
        assert_eq!(one.message.promoted[0].value, "Invoice");
        assert_eq!(
            node.read_message(MessageId::new(2)).expect("read"),
            Some(unmatched(7, 2).message),
            "the Message with it"
        );
        let stranger = node.read_dead_message(7, MessageId::new(99)).expect("read");
        assert_eq!(stranger, DeadEntry::Never);
    }

    #[test]
    fn a_replay_writes_its_journeys_and_takes_the_entry_out_once() {
        let (node, _) = node();
        node.publish(&unmatched(7, 1)).expect("published");
        node.publish(&unmatched(7, 2)).expect("published");
        assert_eq!(node.keep_audit(10).expect("kept"), 2);
        let replay = replaying(7, 1, 5);
        assert_eq!(node.replay(&replay).expect("replayed"), Replayed::Now);
        assert_eq!(
            node.read_journey(JourneyId::new(1001)).expect("read"),
            Some(journey(JourneyId::new(1001), b"replayed"))
        );
        let held = node.read_held(5, 0, 10).expect("read");
        assert_eq!(held.held[0].hold.journey, JourneyId::new(1001));
        let queue = node.read_dead(7, 0, 10).expect("read");
        assert_eq!((queue.first, queue.count), (1, 1), "taken out");
        let replayed = node.read_dead_message(7, MessageId::new(1)).expect("read");
        assert_eq!(replayed, DeadEntry::Replayed);
        assert_eq!(node.keep_audit(10).expect("kept"), 1, "the Replay audited");

        // Asked again after a lost answer, and again with Journeys of its
        // own: nothing written twice.
        let again = Replay {
            journeys: vec![journey(JourneyId::new(77), b"again")],
            ..replaying(7, 1, 5)
        };
        assert_eq!(node.replay(&again).expect("asked again"), Replayed::Before);
        assert_eq!(node.read_journey(JourneyId::new(77)).expect("read"), None);
        assert_eq!(node.read_held(5, 0, 10).expect("read").count, 1);
        assert_eq!(node.keep_audit(10).expect("kept"), 0, "not audited twice");
        node.publish(&unmatched(7, 1))
            .expect("its Publication asked again");
        assert_eq!(
            node.read_dead(7, 0, 10).expect("read").count,
            1,
            "not kept again"
        );
        let never = replaying(7, 42, 5);
        assert_eq!(node.replay(&never).expect("asked"), Replayed::Absent);
    }

    #[test]
    fn an_unmatched_publication_that_fails_midway_keeps_neither_message_nor_entry() {
        use super::super::commit::DEAD_PLACES;
        use super::super::queue::places_key;
        let (node, _) = node();
        node.runtime()
            .put(DEAD_PLACES, &places_key(7), b"torn")
            .expect("torn");
        assert!(node.publish(&unmatched(7, 1)).is_err());
        assert_eq!(node.read_message(MessageId::new(1)).expect("read"), None);
        assert_eq!(node.keep_audit(10).expect("kept"), 0, "no audit record");
        node.runtime()
            .remove(DEAD_PLACES, &places_key(7))
            .expect("mended");
        node.publish(&unmatched(7, 1)).expect("published");
        node.runtime()
            .put(DEAD_PLACES, &places_key(7), b"torn")
            .expect("torn again");
        assert!(node.replay(&replaying(7, 1, 5)).is_err());
        assert_eq!(node.read_journey(JourneyId::new(1001)).expect("read"), None);
        assert_eq!(node.read_held(5, 0, 10).expect("read").count, 0);
    }

    #[test]
    fn what_a_queue_holds_is_read_oldest_first_and_released_in_any_order() {
        let (node, _) = node();
        for id in 1..=4u128 {
            node.publish(&holding(7, id)).expect("published");
        }
        node.publish(&holding(8, 9)).expect("another queue");
        let read = node.read_held(7, 0, 10).expect("read");
        let places: Vec<u64> = read.held.iter().map(|held| held.sequence).collect();
        assert_eq!((read.first, read.next, read.count), (0, 4, 4));
        assert_eq!(places, [0, 1, 2, 3]);
        assert_eq!(read.held[2].hold.journey, JourneyId::new(3));

        let delivered = journey(JourneyId::new(2), b"delivered");
        node.release_held(7, 1, &delivered).expect("released");
        node.release_held(7, 1, &delivered).expect("asked again");
        assert_eq!(
            node.read_journey(JourneyId::new(2)).expect("read"),
            Some(delivered)
        );
        let read = node.read_held(7, 0, 10).expect("read");
        assert_eq!((read.first, read.count), (0, 3), "the first stays");
        node.release_held(7, 0, &journey(JourneyId::new(1), b"done"))
            .expect("released");
        let read = node.read_held(7, 0, 2).expect("read");
        let places: Vec<u64> = read.held.iter().map(|held| held.sequence).collect();
        assert_eq!((read.first, read.count, places), (2, 2, vec![2, 3]));
        assert_eq!(node.read_held(7, 3, 0).expect("places").held, Vec::new());
        assert_eq!(node.read_held(8, 0, 10).expect("read").count, 1);
    }

    #[test]
    fn an_operation_that_fails_midway_leaves_nothing_of_itself() {
        use super::super::commit::{HELD, PLACES};
        use super::super::queue::{entry_key as held_key, places_key};
        let (node, _) = node();
        let torn = |queue: u128| {
            node.runtime()
                .put(PLACES, &places_key(queue), b"torn")
                .expect("torn");
        };
        // A Publication whose queue cannot be read, after its Message and
        // Journey were decided.
        torn(7);
        assert!(node.publish(&holding(7, 1)).is_err());
        assert_eq!(node.read_message(MessageId::new(1)).expect("read"), None);
        assert_eq!(node.read_journey(JourneyId::new(1)).expect("read"), None);
        let none_held = node.runtime().get(HELD, &held_key(7, 0)).expect("read");
        assert_eq!(none_held, None);
        assert_eq!(node.keep_audit(10).expect("kept"), 0, "no audit record");

        // A release whose queue cannot be read, after its Journey was
        // written and its place let go of.
        node.publish(&holding(8, 2)).expect("published");
        torn(8);
        let delivered = journey(JourneyId::new(2), b"delivered");
        assert!(node.release_held(8, 0, &delivered).is_err());
        let left = node.read_journey(JourneyId::new(2)).expect("read");
        assert_eq!(left, Some(journey(JourneyId::new(2), b"held")));
        let still = node.runtime().get(HELD, &held_key(8, 0)).expect("read");
        assert!(still.is_some(), "its place kept");
    }

    #[test]
    fn a_publication_asked_again_holds_its_journey_once() {
        let (node, _) = node();
        node.publish(&holding(7, 1)).expect("published");
        node.publish(&holding(7, 1))
            .expect("asked again after a lost answer");
        node.publish(&holding(7, 2)).expect("the next");
        let read = node.read_held(7, 0, 10).expect("read");
        let held: Vec<JourneyId> = read.held.iter().map(|held| held.hold.journey).collect();
        assert_eq!((read.next, read.count), (2, 2));
        assert_eq!(held, [JourneyId::new(1), JourneyId::new(2)]);
    }

    #[test]
    fn a_hand_on_after_a_mere_release_is_not_success() {
        let (node, _) = node();
        let claim = node
            .claim(JOURNEY_ID, &holder(0), 1, LEASE)
            .expect("claimed")
            .expect("taken");
        assert!(node.release(&claim).expect("released"));
        let hand_on = HandOn {
            claim,
            result: journey(JOURNEY_ID, b"routed"),
            messages: Vec::new(),
            next: Vec::new(),
        };
        assert!(!node.hand_on(&hand_on).expect("hand-on"), "only released");
        assert_eq!(node.read_journey(JOURNEY_ID).expect("read"), None);
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
