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
//! **Asking again is safe.** A request whose answer was lost may have been
//! done by the node that lost it; every operation is written to bear that.
//! A record is written under its own identifier, so writing it twice leaves
//! one; a claim asked again under the token it holds by is that claim; a
//! hand-on asked again after it was done is `true` and writes nothing; an
//! audit record written twice is kept once, by its identifier, by the audit
//! keeper.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use xcore::{AuditId, JourneyId, MessageId, StreamId};

use super::XmipStorage;
use super::record::{
    AdministrationKind, AdministrationRecord, AuditEntry, Claim, HandOn, JourneyRecord,
    MessageRecord, StreamChunk,
};
use super::server::ALPN;
use super::wire::{self, Answer, Request};
use crate::PersistError;

type Connection = tls::Guarded;

/// How long a Storage node that did not answer is passed over, asked only
/// when every other has failed too, before it is tried in its turn again.
const PASSED_OVER: Duration = Duration::from_secs(5);

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

    fn answered(&self, answered: bool) {
        *self
            .passed_over
            .lock()
            .unwrap_or_else(PoisonError::into_inner) =
            (!answered).then(|| Instant::now() + PASSED_OVER);
    }
}

/// Xmip Storage as a node reaches it: [`XmipStorage`] over the Storage
/// nodes, round robin.
pub struct StorageClient {
    nodes: Vec<Node>,
    next: AtomicUsize,
    config: Arc<tls::ClientConfig>,
    timeout: Duration,
}

impl StorageClient {
    /// A client of the Storage nodes at `nodes` — `host:port` each, as a
    /// node's `[storage] nodes` lists them — presenting `identity` and
    /// trusting its anchors, each connect and each read bounded by
    /// `timeout`.
    ///
    /// # Errors
    ///
    /// Where `nodes` is empty, an address has no port, or `identity` does
    /// not make a client configuration.
    pub fn new(
        nodes: &[String],
        identity: &tls::Identity,
        timeout: Duration,
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
            .collect::<Result<Vec<_>, PersistError>>()?;
        let config = identity
            .client()
            .map_err(|error| unreachable(error.message))?;
        Ok(Self {
            nodes,
            next: AtomicUsize::new(0),
            config: Arc::new(tls::alpn::offering(config, &[ALPN])),
            timeout,
        })
    }

    /// `request` asked of the next Storage node, and of each after it in
    /// turn while none answers; one that did not answer lately is asked
    /// last.
    fn ask(&self, request: &Request) -> Result<Answer, PersistError> {
        let first = self.next.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();
        let (answering, passed): (Vec<&Node>, Vec<&Node>) = (0..self.nodes.len())
            .map(|step| &self.nodes[(first + step) % self.nodes.len()])
            .partition(|node| !node.passed_over(now));
        let mut failures = Vec::new();
        for node in answering.into_iter().chain(passed) {
            let asked = self.ask_node(node, request);
            node.answered(asked.is_ok());
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

/// The answer an operation expected, or why there was none.
fn expected<T>(
    answer: Answer,
    take: impl FnOnce(Answer) -> Result<T, Answer>,
) -> Result<T, PersistError> {
    match answer {
        Answer::Refused(scope, reason) => Err(PersistError::Refused { scope, reason }),
        Answer::Failed(reason) => Err(PersistError::Failed { reason }),
        other => take(other).map_err(|other| PersistError::Record {
            reason: format!("a Storage node answered {other:?}"),
        }),
    }
}

fn done(answer: Answer) -> Result<(), Answer> {
    match answer {
        Answer::Done => Ok(()),
        other => Err(other),
    }
}

fn claimed(answer: Answer) -> Result<Option<Claim>, Answer> {
    match answer {
        Answer::Claim(claim) => Ok(claim),
        other => Err(other),
    }
}

fn yes(answer: Answer) -> Result<bool, Answer> {
    match answer {
        Answer::Yes(yes) => Ok(yes),
        other => Err(other),
    }
}

impl XmipStorage for StorageClient {
    fn write_chunk(&self, chunk: &StreamChunk) -> Result<(), PersistError> {
        expected(self.ask(&Request::WriteChunk(chunk.clone()))?, done)
    }

    fn read_chunk(
        &self,
        stream: StreamId,
        index: u32,
    ) -> Result<Option<StreamChunk>, PersistError> {
        expected(
            self.ask(&Request::ReadChunk(stream, index))?,
            |answer| match answer {
                Answer::Chunk(chunk) => Ok(chunk),
                other => Err(other),
            },
        )
    }

    fn write_message(&self, message: &MessageRecord) -> Result<(), PersistError> {
        expected(self.ask(&Request::WriteMessage(message.clone()))?, done)
    }

    fn read_message(&self, message: MessageId) -> Result<Option<MessageRecord>, PersistError> {
        expected(
            self.ask(&Request::ReadMessage(message))?,
            |answer| match answer {
                Answer::Message(record) => Ok(record),
                other => Err(other),
            },
        )
    }

    fn write_journey(&self, journey: &JourneyRecord) -> Result<(), PersistError> {
        expected(self.ask(&Request::WriteJourney(journey.clone()))?, done)
    }

    fn read_journey(&self, journey: JourneyId) -> Result<Option<JourneyRecord>, PersistError> {
        expected(
            self.ask(&Request::ReadJourney(journey))?,
            |answer| match answer {
                Answer::Journey(record) => Ok(record),
                other => Err(other),
            },
        )
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
        expected(self.ask(&Request::claim(claim, lease))?, claimed)
    }

    fn renew(&self, claim: &Claim, lease: Duration) -> Result<Option<Claim>, PersistError> {
        expected(self.ask(&Request::renew(claim.clone(), lease))?, claimed)
    }

    fn release(&self, claim: &Claim) -> Result<bool, PersistError> {
        expected(self.ask(&Request::Release(claim.clone()))?, yes)
    }

    fn hand_on(&self, hand_on: &HandOn) -> Result<bool, PersistError> {
        expected(self.ask(&Request::HandOn(hand_on.clone()))?, yes)
    }

    fn write_audit(&self, entry: &AuditEntry) -> Result<(), PersistError> {
        expected(self.ask(&Request::WriteAudit(entry.clone()))?, done)
    }

    fn keep_audit(&self, most: u32) -> Result<u32, PersistError> {
        expected(
            self.ask(&Request::KeepAudit(most))?,
            |answer| match answer {
                Answer::Count(count) => Ok(count),
                other => Err(other),
            },
        )
    }

    fn read_kept_audit(&self, id: AuditId) -> Result<Option<AuditEntry>, PersistError> {
        expected(
            self.ask(&Request::ReadKeptAudit(id))?,
            |answer| match answer {
                Answer::Audit(entry) => Ok(entry),
                other => Err(other),
            },
        )
    }

    fn write_administration(&self, record: &AdministrationRecord) -> Result<(), PersistError> {
        expected(
            self.ask(&Request::WriteAdministration(record.clone()))?,
            done,
        )
    }

    fn read_administration(
        &self,
        kind: AdministrationKind,
        id: u128,
    ) -> Result<Option<AdministrationRecord>, PersistError> {
        expected(
            self.ask(&Request::ReadAdministration(kind, id))?,
            |answer| match answer {
                Answer::Administration(record) => Ok(record),
                other => Err(other),
            },
        )
    }

    fn remove_administration(
        &self,
        kind: AdministrationKind,
        id: u128,
    ) -> Result<(), PersistError> {
        expected(self.ask(&Request::RemoveAdministration(kind, id))?, done)
    }
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;

    use secret::{Held, KekName};

    use super::*;
    use crate::fixture::Memory;
    use crate::storage::{Embedded, StorageServer};

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
        let client =
            StorageClient::new(&nodes, &authority.issue(&authority), TIMEOUT).expect("client");

        client.write_journey(&journey(b"written")).expect("written");
        assert_eq!(
            client.read_journey(journey(b"").journey).expect("read"),
            Some(journey(b"written"))
        );
        let claim = client
            .claim(journey(b"").journey, "xmip:///C1/node/alpha", 7, TIMEOUT)
            .expect("claimed")
            .expect("taken");

        first.stop();
        for round in 0..4u8 {
            let body = [b'r', round];
            client.write_journey(&journey(&body)).expect("carried on");
            assert_eq!(
                client.read_journey(journey(b"").journey).expect("read"),
                Some(journey(&body))
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
    fn a_node_whose_certificate_another_authority_issued_is_refused() {
        let ours = Authority::new();
        let theirs = Authority::new();
        let behind = storage();
        let server = serve(&behind, &ours.issue(&ours));
        let nodes = [server.address().to_string()];
        let stranger = StorageClient::new(&nodes, &theirs.issue(&ours), TIMEOUT).expect("client");
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
        assert!(StorageClient::new(&[], &identity, TIMEOUT).is_err());
        let portless = ["storage-1.example".to_string()];
        assert!(StorageClient::new(&portless, &identity, TIMEOUT).is_err());
    }
}
