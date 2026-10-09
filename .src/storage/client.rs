//! A node reaching Xmip Storage: the Storage nodes its configuration lists,
//! round robin, over Xmip's own mutual TLS (`deployment-model.md` section 7:
//! *the node's configuration lists their addresses … and the node tries
//! them round robin*; ADR-0063, amendment 2026-10-01).
//!
//! **Round robin.** Each request goes to the next Storage node in the list,
//! so the load is spread over them. **More than one is the safety** (the
//! owner, 2026-10-01): a request the node it went to does not answer — it
//! refused the connection, the connection broke, nothing came back within
//! the time — is asked of the next, and so on round the list once, so a
//! Storage node that stops stops no work while another is left
//! (`deployment-model.md` section 9). One that did not answer is passed
//! over for a while — asked only after every other — so the requests after
//! it do not each wait on it again. A connection kept from before that
//! fails is opened once afresh on the same node before the next is tried,
//! since a Storage node that restarted ends every connection it had.
//!
//! **One statement, one Storage node** (the owner, 2026-10-03). A
//! statement — a receive cycle's chunks 1 to n, its Publication and its
//! Journeys — is asked of one Storage node throughout, chosen round robin
//! as it begins ([`XmipStorage::pinned`]). Its operations do not move on to
//! another node: the chunks are durable with the Publication's sync on the
//! node that took them, so a node that stops answering mid-statement fails
//! the statement, and the sender, never acknowledged, sends again.
//!
//! **Asking again is safe.** A request whose answer was lost may have been
//! done by the node that lost it; every operation is written to bear that.
//! A record is written under its own identifier, so writing it twice leaves
//! one; a Publication asked again writes nothing and answers as it stands,
//! by its Message, so a Journey moved on since is not reset; a claim asked
//! again under the token it holds by is that claim; a hand-on asked again
//! after it was done is `true` and writes nothing, and is told from a claim
//! only given back; an audit record written twice is kept once, by its
//! identifier, by the audit keeper. And an operation that failed wrote
//! nothing of itself, so asking it again starts clean.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use super::server::ALPN;
use super::wire::{self, Answer, Request};
use crate::PersistError;

mod operations;

type Connection = tls::Guarded;

/// How long a Storage node that did not answer is passed over, asked only
/// when every other has failed too, before it is tried in its turn again,
/// where the node's `[tuning] storage_pass_over` does not say.
pub const PASS_OVER: Duration = Duration::from_secs(5);

/// What bounds each connect and each read, where the node's `[tuning]
/// storage_timeout` does not say.
pub const TIMEOUT: Duration = Duration::from_secs(5);

/// One Storage node, the connections to it no request is using, and until
/// when it is passed over.
struct Node {
    address: String,
    host: String,
    idle: Mutex<Vec<Connection>>,
    passed_over: Mutex<Option<Instant>>,
}

impl Node {
    fn passed_over(&self, now: Instant) -> bool {
        let until = *self
            .passed_over
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        until.is_some_and(|until| until > now)
    }

    fn answered(&self, answered: bool, pass_over: Duration) {
        *self
            .passed_over
            .lock()
            .unwrap_or_else(PoisonError::into_inner) =
            (!answered).then(|| Instant::now() + pass_over);
    }
}

/// Xmip Storage as a node reaches it: [`XmipStorage`] over the Storage
/// nodes, round robin, or over one of them for one statement.
pub struct StorageClient {
    nodes: Arc<[Node]>,
    next: Arc<AtomicUsize>,
    config: Arc<tls::ClientConfig>,
    timeout: Duration,
    pass_over: Duration,
    /// The Storage node every operation goes to, for one statement.
    pinned: Option<usize>,
}

impl StorageClient {
    /// A client of the Storage nodes at `nodes` — `host:port` each, as a
    /// node's `[storage] nodes` lists them — presenting `identity` and
    /// trusting its anchors, each connect and each read bounded by
    /// `timeout`, and one that did not answer passed over for `pass_over`
    /// (the node's `[tuning]`; [`TIMEOUT`] and [`PASS_OVER`] where it does
    /// not say).
    ///
    /// # Errors
    ///
    /// Where `nodes` is empty, an address has no port, or `identity` does
    /// not make a client configuration.
    pub fn new(
        nodes: &[String],
        identity: &tls::Identity,
        timeout: Duration,
        pass_over: Duration,
    ) -> Result<Self, PersistError> {
        if nodes.is_empty() {
            return Err(unreachable("no Storage node is listed"));
        }
        let nodes = nodes
            .iter()
            .map(|address| {
                let (host, _) = address
                    .rsplit_once(':')
                    .ok_or_else(|| unreachable(format!("'{address}' names no port")))?;
                Ok(Node {
                    address: address.clone(),
                    host: host
                        .trim_start_matches('[')
                        .trim_end_matches(']')
                        .to_string(),
                    idle: Mutex::new(Vec::new()),
                    passed_over: Mutex::new(None),
                })
            })
            .collect::<Result<Arc<[Node]>, PersistError>>()?;
        let config = identity
            .client()
            .map_err(|error| unreachable(error.message))?;
        Ok(Self {
            nodes,
            next: Arc::new(AtomicUsize::new(0)),
            config: Arc::new(tls::alpn::offering(config, &[ALPN])),
            timeout,
            pass_over,
            pinned: None,
        })
    }

    /// The Storage nodes in the order a request asks them: from the next
    /// round robin, one that did not answer lately last.
    fn turn(&self) -> impl Iterator<Item = usize> {
        let first = self.next.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();
        let count = self.nodes.len();
        let (answering, passed): (Vec<usize>, Vec<usize>) = (0..count)
            .map(|step| (first + step) % count)
            .partition(|&index| !self.nodes[index].passed_over(now));
        answering.into_iter().chain(passed)
    }

    /// `request` asked of the next Storage node, and of each after it in
    /// turn while none answers; one that did not answer lately is asked
    /// last.
    /// For one statement, `request` is asked of its Storage node alone.
    fn ask(&self, request: &Request) -> Result<Answer, PersistError> {
        if let Some(index) = self.pinned {
            let node = &self.nodes[index];
            let asked = self.ask_node(node, request);
            node.answered(asked.is_ok(), self.pass_over);
            return asked.map_err(|failure| {
                unreachable(format!(
                    "the statement's Storage node {} did not answer: {failure}",
                    node.address
                ))
            });
        }
        let mut failures = Vec::new();
        for node in self.turn().map(|index| &self.nodes[index]) {
            let asked = self.ask_node(node, request);
            node.answered(asked.is_ok(), self.pass_over);
            match asked {
                Ok(answer) => return Ok(answer),
                Err(failure) => failures.push(format!("{}: {failure}", node.address)),
            }
        }
        Err(unreachable(format!(
            "no Storage node answered: {}",
            failures.join("; ")
        )))
    }

    fn ask_node(&self, node: &Node, request: &Request) -> Result<Answer, String> {
        let kept = node
            .idle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop();
        if let Some(mut connection) = kept
            && let Ok(answer) = exchange(&mut connection, request)
        {
            node.idle
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(connection);
            return Ok(answer);
        }
        let tcp = net::connect(node.address.as_str(), Some(self.timeout))
            .map_err(|failure| failure.to_string())?;
        let mut connection = tls::client_with(&node.host, tcp, Arc::clone(&self.config))
            .map_err(|failure| failure.message)?;
        let answer = exchange(&mut connection, request).map_err(|failure| failure.to_string())?;
        node.idle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(connection);
        Ok(answer)
    }
}

fn exchange(connection: &mut Connection, request: &Request) -> std::io::Result<Answer> {
    wire::send(connection, request)?;
    wire::receive(connection)?
        .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::UnexpectedEof))
}

fn unreachable(reason: impl Into<String>) -> PersistError {
    PersistError::Unreachable {
        reason: reason.into(),
    }
}

impl StorageClient {
    /// This client over one Storage node, for one statement: the one it is
    /// over already, or the next round robin ([`super::XmipStorage::pinned`]).
    fn pinned_client(&self) -> Option<Self> {
        let index = self.pinned.or_else(|| self.turn().next())?;
        Some(Self {
            nodes: Arc::clone(&self.nodes),
            next: Arc::clone(&self.next),
            config: Arc::clone(&self.config),
            timeout: self.timeout,
            pass_over: self.pass_over,
            pinned: Some(index),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;

    use secret::{Held, KekName};

    use crate::storage::{AuditFacts, JourneyFacts, MessageFacts};

    use xcore::{AuditId, JourneyId, MessageId, StreamId};

    use super::*;
    use crate::fixture::Memory;
    use crate::storage::{
        AdministrationKind, AdministrationRecord, AuditEntry, Claim, DeadEntry, DeadQueue,
        Embedded, HandOn, HeldQueue, Hold, JourneyRecord, MessageRecord, Publication, Replay,
        Replayed, StorageServer, StreamChunk, XmipStorage,
    };

    /// A cluster's certificate authority, and identities it issues.
    struct Authority(rcgen::CertifiedKey);

    impl Authority {
        fn new() -> Self {
            let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("params");
            params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
            let key_pair = rcgen::KeyPair::generate().expect("a key");
            let cert = params.self_signed(&key_pair).expect("signed");
            Self(rcgen::CertifiedKey { cert, key_pair })
        }

        /// An identity for a node at 127.0.0.1, trusting `anchors`.
        fn issue(&self, anchors: &Self) -> tls::Identity {
            let key = rcgen::KeyPair::generate().expect("a key");
            let params =
                rcgen::CertificateParams::new(vec!["127.0.0.1".to_string()]).expect("params");
            let leaf = params
                .signed_by(&key, &self.0.cert, &self.0.key_pair)
                .expect("issued");
            tls::Identity::from_pem(
                leaf.pem().as_bytes(),
                key.serialize_pem().as_bytes(),
                anchors.0.cert.pem().as_bytes(),
            )
            .expect("identity")
        }
    }

    fn storage() -> Arc<dyn XmipStorage> {
        let keys = Held::new(secret::fixture::Memory::default());
        let kek = KekName::new("storage").expect("name");
        let node = Embedded::open(Memory::default(), Memory::default(), &keys, &kek);
        Arc::new(node.expect("node"))
    }

    fn serve(storage: &Arc<dyn XmipStorage>, identity: &tls::Identity) -> StorageServer {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        StorageServer::start(Arc::clone(storage), listener, identity).expect("served")
    }

    fn journey(body: &[u8]) -> JourneyRecord {
        JourneyRecord {
            journey: JourneyId::new(0x0199_0000_0000_7000_8000_0000_0000_0011),
            body: body.to_vec(),
            facts: JourneyFacts::default(),
        }
    }

    const TIMEOUT: Duration = Duration::from_secs(5);

    #[test]
    fn a_node_reaches_storage_over_mutual_tls_and_round_robin_carries_on_past_a_stopped_one() {
        let authority = Authority::new();
        // Two Storage nodes in front of one database, as option A has them.
        let behind = storage();
        let first = serve(&behind, &authority.issue(&authority));
        let second = serve(&behind, &authority.issue(&authority));
        let nodes = [first.address().to_string(), second.address().to_string()];
        let client = StorageClient::new(&nodes, &authority.issue(&authority), TIMEOUT, PASS_OVER)
            .expect("client");

        client.write_journey(&journey(b"written")).expect("written");
        assert_eq!(
            client
                .read_journey(journey(b"").journey)
                .expect("read")
                .map(|r| r.body),
            Some(b"written".to_vec())
        );
        let claim = client
            .claim(
                journey(b"").journey,
                &configure::fixture::test_cluster().node_scope(0),
                7,
                TIMEOUT,
            )
            .expect("claimed")
            .expect("taken");

        first.stop();
        for round in 0..4u8 {
            let body = [b'r', round];
            client.write_journey(&journey(&body)).expect("carried on");
            assert_eq!(
                client
                    .read_journey(journey(b"").journey)
                    .expect("read")
                    .map(|r| r.body),
                Some(body.to_vec())
            );
        }
        assert!(client.release(&claim).expect("released"));
        second.stop();
        let gone = client.read_journey(journey(b"").journey);
        assert!(
            matches!(gone, Err(PersistError::Unreachable { .. })),
            "{gone:?}"
        );
    }

    #[test]
    fn a_statement_is_asked_of_one_storage_node_throughout_and_fails_with_it() {
        let authority = Authority::new();
        // Two Storage nodes, each with a database of its own: only asking
        // one of them throughout finds what the statement wrote.
        let (one, other) = (storage(), storage());
        let first = serve(&one, &authority.issue(&authority));
        let second = serve(&other, &authority.issue(&authority));
        let nodes = [first.address().to_string(), second.address().to_string()];
        let client: Arc<dyn XmipStorage> = Arc::new(
            StorageClient::new(&nodes, &authority.issue(&authority), TIMEOUT, PASS_OVER)
                .expect("client"),
        );

        let statement = super::super::statement(&client);
        for round in 0..4u8 {
            statement
                .write_journey(&journey(&[b'r', round]))
                .expect("written");
            assert_eq!(
                statement
                    .read_journey(journey(b"").journey)
                    .expect("read")
                    .map(|r| r.body),
                Some(vec![b'r', round])
            );
        }
        let wrote_first = one
            .read_journey(journey(b"").journey)
            .expect("read")
            .is_some();
        let wrote_second = other
            .read_journey(journey(b"").journey)
            .expect("read")
            .is_some();
        assert!(wrote_first != wrote_second, "one node took every write");

        if wrote_first {
            first.stop();
        } else {
            second.stop();
        }
        let gone = statement.read_journey(journey(b"").journey);
        assert!(
            matches!(gone, Err(PersistError::Unreachable { .. })),
            "{gone:?}"
        );
        client
            .write_journey(&journey(b"carried on"))
            .expect("the next statement asks the node left");
    }

    /// What another node does while an answer is lost.
    type Meanwhile = Box<dyn FnOnce(&dyn XmipStorage) + Send>;

    /// Xmip Storage whose first Publication is written and never answered:
    /// `meanwhile` runs after the write, and the connection it came on is
    /// dropped, as a Storage node's whose answer was lost on the way back.
    struct LosesAnswer {
        beneath: Arc<dyn XmipStorage>,
        meanwhile: Mutex<Option<Meanwhile>>,
    }

    impl XmipStorage for LosesAnswer {
        fn publish(&self, publication: &Publication) -> Result<Vec<Claim>, PersistError> {
            let held = self.beneath.publish(publication)?;
            let meanwhile = self.meanwhile.lock().expect("meanwhile").take();
            let Some(meanwhile) = meanwhile else {
                return Ok(held);
            };
            meanwhile(self.beneath.as_ref());
            // The answer lost: the connection ends with it unsent.
            std::panic::resume_unwind(Box::new("the answer was lost"));
        }

        fn write_chunk(&self, chunk: &StreamChunk) -> Result<(), PersistError> {
            self.beneath.write_chunk(chunk)
        }

        fn read_chunk(&self, s: StreamId, i: u32) -> Result<Option<StreamChunk>, PersistError> {
            self.beneath.read_chunk(s, i)
        }

        fn write_stream(
            &self,
            l: &StreamChunk,
            s: &super::super::StreamRecord,
        ) -> Result<(), PersistError> {
            self.beneath.write_stream(l, s)
        }

        fn read_stream(
            &self,
            s: StreamId,
        ) -> Result<Option<super::super::StreamRecord>, PersistError> {
            self.beneath.read_stream(s)
        }

        fn write_message(&self, message: &MessageRecord) -> Result<(), PersistError> {
            self.beneath.write_message(message)
        }

        fn read_message(&self, id: MessageId) -> Result<Option<MessageRecord>, PersistError> {
            self.beneath.read_message(id)
        }

        fn write_journey(&self, journey: &JourneyRecord) -> Result<(), PersistError> {
            self.beneath.write_journey(journey)
        }

        fn read_journey(&self, id: JourneyId) -> Result<Option<JourneyRecord>, PersistError> {
            self.beneath.read_journey(id)
        }

        fn read_held(&self, queue: u128, from: u64, most: u32) -> Result<HeldQueue, PersistError> {
            self.beneath.read_held(queue, from, most)
        }

        fn read_dead(&self, queue: u128, from: u64, most: u32) -> Result<DeadQueue, PersistError> {
            self.beneath.read_dead(queue, from, most)
        }

        fn read_dead_message(&self, q: u128, m: MessageId) -> Result<DeadEntry, PersistError> {
            self.beneath.read_dead_message(q, m)
        }

        fn replay(&self, replay: &Replay) -> Result<Replayed, PersistError> {
            self.beneath.replay(replay)
        }

        fn claim(
            &self,
            journey: JourneyId,
            holder: &str,
            token: u128,
            lease: Duration,
        ) -> Result<Option<Claim>, PersistError> {
            self.beneath.claim(journey, holder, token, lease)
        }

        fn renew(&self, claims: &[Claim], lease: Duration) -> Result<Vec<Claim>, PersistError> {
            self.beneath.renew(claims, lease)
        }

        fn release(&self, claim: &Claim) -> Result<bool, PersistError> {
            self.beneath.release(claim)
        }

        fn hand_on(&self, hand_on: &HandOn) -> Result<bool, PersistError> {
            self.beneath.hand_on(hand_on)
        }

        fn write_audit(&self, entry: &AuditEntry) -> Result<(), PersistError> {
            self.beneath.write_audit(entry)
        }

        fn keep_audit(&self, most: u32) -> Result<u32, PersistError> {
            self.beneath.keep_audit(most)
        }

        fn read_kept_audit(&self, id: AuditId) -> Result<Option<AuditEntry>, PersistError> {
            self.beneath.read_kept_audit(id)
        }

        fn read_kept_audit_chunk(
            &self,
            id: AuditId,
            index: u32,
        ) -> Result<Option<StreamChunk>, PersistError> {
            self.beneath.read_kept_audit_chunk(id, index)
        }

        fn write_administration(&self, record: &AdministrationRecord) -> Result<(), PersistError> {
            self.beneath.write_administration(record)
        }

        fn read_administration(
            &self,
            kind: AdministrationKind,
            id: u128,
        ) -> Result<Option<AdministrationRecord>, PersistError> {
            self.beneath.read_administration(kind, id)
        }

        fn remove_administration(
            &self,
            kind: AdministrationKind,
            id: u128,
        ) -> Result<(), PersistError> {
            self.beneath.remove_administration(kind, id)
        }

        fn query(&self, query: &super::super::Query) -> Result<Vec<u128>, PersistError> {
            self.beneath.query(query)
        }
    }

    #[test]
    fn a_publication_whose_answer_was_lost_is_asked_again_and_resets_nothing_done_meanwhile() {
        let authority = Authority::new();
        let behind = storage();
        let id = journey(b"").journey;
        let queue = 7;
        // Between the write and its lost answer, another node sends the
        // Journey and takes it out of its queue.
        let meanwhile: Meanwhile = Box::new(move |storage| {
            let other = configure::fixture::test_cluster().node_scope(1);
            let claim = storage.claim(id, &other, 2, TIMEOUT).expect("asked");
            let hand_on = HandOn {
                claim: claim.expect("free: the Publication claimed nothing"),
                result: journey(b"completed"),
                messages: Vec::new(),
                next: Vec::new(),
                leaves: vec![queue],
                queued: Vec::new(),
                requeued: Vec::new(),
                kept_for_nanos: None,
            };
            assert!(storage.hand_on(&hand_on).expect("sent"));
        });
        let losing: Arc<dyn XmipStorage> = Arc::new(LosesAnswer {
            beneath: Arc::clone(&behind),
            meanwhile: Mutex::new(Some(meanwhile)),
        });
        // Two Storage nodes in front of one database, both losing the first
        // answer, so whichever is asked first loses it.
        let first = serve(&losing, &authority.issue(&authority));
        let second = serve(&losing, &authority.issue(&authority));
        let nodes = [first.address().to_string(), second.address().to_string()];
        let client = StorageClient::new(&nodes, &authority.issue(&authority), TIMEOUT, PASS_OVER)
            .expect("client");
        let publication = Publication {
            message: MessageRecord {
                message: MessageId::new(1),
                body: b"order".to_vec(),
                facts: MessageFacts::default(),
            },
            journeys: vec![journey(b"waiting")],
            held: vec![Hold {
                queue,
                journey: id,
                body: Vec::new(),
            }],
            dead: None,
            audit: AuditEntry {
                id: AuditId::new(3),
                body: b"published".to_vec(),
                audited: None,
                facts: AuditFacts::default(),
            },
            claims: Vec::new(),
            lease_nanos: 0,
        };

        let answered = client.publish(&publication).expect("asked again, answered");

        assert_eq!(answered, Vec::new());
        assert_eq!(
            behind.read_journey(id).expect("read").map(|r| r.body),
            Some(b"completed".to_vec()),
            "what the other node did stands"
        );
        assert_eq!(behind.read_held(queue, 0, 10).expect("read").count, 0);
        assert_eq!(behind.keep_audit(10).expect("kept"), 1, "published once");
        first.stop();
        second.stop();
    }

    #[test]
    fn a_node_whose_certificate_another_authority_issued_is_refused() {
        let ours = Authority::new();
        let theirs = Authority::new();
        let behind = storage();
        let server = serve(&behind, &ours.issue(&ours));
        let nodes = [server.address().to_string()];
        let stranger =
            StorageClient::new(&nodes, &theirs.issue(&ours), TIMEOUT, PASS_OVER).expect("client");
        let refused = stranger.write_journey(&journey(b"from a stranger"));
        assert!(
            matches!(refused, Err(PersistError::Unreachable { .. })),
            "{refused:?}"
        );
        assert_eq!(
            behind.read_journey(journey(b"").journey).expect("read"),
            None
        );
        server.stop();
    }

    #[test]
    fn no_storage_node_or_one_without_a_port_is_refused_before_anything_is_asked() {
        let authority = Authority::new();
        let identity = authority.issue(&authority);
        assert!(StorageClient::new(&[], &identity, TIMEOUT, PASS_OVER).is_err());
        let portless = ["storage-1.example".to_string()];
        assert!(StorageClient::new(&portless, &identity, TIMEOUT, PASS_OVER).is_err());
    }
}
