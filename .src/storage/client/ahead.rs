//! A claim asked of a Storage node ahead of its sync
//! (`XmipStorage::claim_ahead`; ADR-0018, amendment 2026-11-12): one
//! request, wire operation 40, and two frames — the claim once readable
//! there (answer 4) and, where it was taken, once it is durable there or why
//! it is not (answer 28). Asked round robin as every request is, a standby
//! or a Storage node that does not answer passed over for the next.
//!
//! **A connection that carried the first frame is asked nothing before its
//! second is read.** It is kept out of the connections waiting for a
//! request and handed to the claim's [`Durable`], whose first wait reads
//! the second frame, bounded by the client's `storage_timeout`, and only
//! then puts it back. A Durable never waited on drops it with its last
//! clone, closing it, so a second frame is never read as another request's
//! answer; a read that fails or times out drops it too.

use std::sync::Arc;
use std::time::Duration;
use xcore::lock::Locked;

use super::super::durable::Durable;
use super::super::record::Claim;
use super::super::wire::{self, Answer, Request};
use super::expected::{claimed, expected};
use super::{Connection, StorageClient, exchange, standby};
use crate::PersistError;

impl StorageClient {
    /// `claim` asked for `lease`, answered once readable at a Storage node,
    /// with when it is durable there.
    pub(super) fn claimed_ahead(
        &self,
        claim: Claim,
        lease: Duration,
    ) -> Result<(Option<Claim>, Durable), PersistError> {
        let request = Request::claim_ahead(claim, lease);
        let (answer, held) = self.ask_each(|index| {
            let node = &self.nodes[index];
            let (answer, connection) = self.ask_node_held(node, &request, &exchange)?;
            if matches!(answer, Answer::Claim(Some(_))) {
                // Its second frame comes on it: held for that alone.
                return Ok((answer, Some((index, connection))));
            }
            node.idle.locked().push(connection);
            standby(answer).map(|answer| (answer, None))
        })?;
        let claim = expected(answer, claimed)?;
        let durable = match held {
            Some((index, connection)) => self.notice(index, connection),
            None => Durable::kept(),
        };
        Ok((claim, durable))
    }

    /// When the claim `connection` was answered on is durable at the Storage
    /// node listed at `index`: its second frame, read by the first wait, the
    /// connection put back once it is read and closed where it is not.
    fn notice(&self, index: usize, mut connection: Connection) -> Durable {
        let nodes = Arc::clone(&self.nodes);
        Durable::fetching(move || {
            let kept = match wire::receive::<Answer>(&mut connection) {
                Ok(Some(Answer::Kept(None))) => Ok(()),
                Ok(Some(Answer::Kept(Some(why)))) => Err(format!(
                    "the Storage node did not make the claim durable: {why}"
                )),
                Ok(Some(other)) => {
                    return Err(format!(
                        "the Storage node answered {other:?} where the claim's durability was due"
                    ));
                }
                Ok(None) => {
                    return Err("the Storage node closed before the claim was durable".to_string());
                }
                Err(failure) => {
                    return Err(format!("no word of the claim's durability: {failure}"));
                }
            };
            // Its second frame read: free for the next request.
            nodes[index].idle.locked().push(connection);
            kept
        })
    }
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::time::Duration;

    use secret::{Held, KekName};
    use xcore::lock::Locked;
    use xcore::{JourneyId, MessageId};

    use crate::fixture::{Hooked, Hooks, Memory};
    use crate::storage::authority::Authority;
    use crate::storage::client::PASS_OVER;
    use crate::storage::{
        Embedded, JourneyFacts, JourneyRecord, StorageClient, StorageServer, XmipStorage,
    };

    const JOURNEY: JourneyId = JourneyId::new(0x0199_0000_0000_7000_8000_0000_0000_00b1);
    const LEASE: Duration = Duration::from_secs(30);

    fn holder() -> String {
        configure::fixture::test_cluster().node_scope(0)
    }

    /// A Storage node whose runtime database's syncs `hooks` steer, served
    /// on loopback, and a client of it whose reads are bounded by `timeout`.
    fn served(timeout: Duration) -> (StorageServer, StorageClient, Arc<Hooks>) {
        let hooks = Arc::new(Hooks::default());
        let keys = Held::new(secret::fixture::Memory::default());
        let kek = KekName::new("storage").expect("a name");
        let runtime = Hooked::new(Memory::default(), &hooks);
        let node = Embedded::open(runtime, Memory::default(), Memory::default(), &keys, &kek);
        let node: Arc<dyn XmipStorage> = Arc::new(node.expect("opened"));
        let authority = Authority::new();
        let listener = TcpListener::bind("127.0.0.1:0").expect("bound");
        let server = StorageServer::start(node, listener, &authority.issue(&authority));
        let server = server.expect("served");
        let at = [server.address().to_string()];
        let identity = authority.issue(&authority);
        let client = StorageClient::new(&at, &identity, timeout, PASS_OVER).expect("a client");
        (server, client, hooks)
    }

    fn idle(client: &StorageClient) -> usize {
        client.nodes[0].idle.locked().len()
    }

    #[test]
    fn a_claim_over_the_wire_is_answered_readable_and_its_notice_read_once_synced() {
        let (server, client, hooks) = served(Duration::from_secs(10));
        hooks.hold_syncs();
        let (taken, durable) = client
            .claim_ahead(JOURNEY, &holder(), 7, LEASE)
            .expect("answered while its sync is held");
        assert!(taken.is_some(), "taken, readable");
        assert!(hooks.wait_held(Duration::from_secs(10)), "its sync is held");
        assert_eq!(idle(&client), 0, "its connection held for the notice");
        let waiting = durable.clone();
        let waiter = std::thread::spawn(move || waiting.wait());
        std::thread::sleep(Duration::from_millis(50));
        assert!(!waiter.is_finished(), "no notice while the sync is held");
        hooks.release_syncs();
        waiter.join().expect("the waiter ends").expect("durable");
        assert_eq!(durable.told(), Some(true));
        assert_eq!(idle(&client), 1, "back for the next request once read");
        // Taken by another holder: one frame, the connection free at once.
        let other = configure::fixture::test_cluster().node_scope(1);
        let (none, kept) = client
            .claim_ahead(JOURNEY, &other, 8, LEASE)
            .expect("asked");
        assert_eq!(none, None);
        assert_eq!(kept.told(), Some(true), "nothing to wait for");
        assert_eq!(idle(&client), 1);
        server.stop();
    }

    #[test]
    fn a_connection_whose_notice_was_never_read_is_never_asked_again() {
        let (server, client, hooks) = served(Duration::from_secs(10));
        hooks.hold_syncs();
        let (taken, durable) = client
            .claim_ahead(JOURNEY, &holder(), 7, LEASE)
            .expect("answered");
        assert!(taken.is_some());
        drop(durable);
        assert_eq!(idle(&client), 0, "closed, not kept");
        // The notice is sent now, onto a connection the client closed: kept
        // and asked again, its next request would read the notice instead
        // of its answer.
        hooks.release_syncs();
        let journey = JourneyRecord {
            journey: JOURNEY,
            body: b"to send".to_vec(),
            facts: JourneyFacts::default(),
        };
        client.write_journey(&journey).expect("its own answer");
        let read = client.read_journey(JOURNEY).expect("its own answer");
        assert_eq!(read.map(|record| record.body), Some(b"to send".to_vec()));
        assert_eq!(client.read_message(MessageId::new(1)).expect("read"), None);
        server.stop();
    }

    #[test]
    fn a_claim_whose_sync_fails_or_whose_notice_never_comes_is_never_durable() {
        let (server, client, hooks) = served(Duration::from_secs(1));
        hooks.hold_syncs();
        let (_, late) = client
            .claim_ahead(JOURNEY, &holder(), 7, LEASE)
            .expect("answered");
        let waited = late.wait();
        assert!(
            waited.is_err_and(|why| why.to_string().contains("no word")),
            "timed out"
        );
        assert_eq!(idle(&client), 0, "a read that timed out is never kept");
        // Asked again under its token while still held: the same claim.
        let (again, failing) = client
            .claim_ahead(JOURNEY, &holder(), 7, LEASE)
            .expect("answered");
        assert!(again.is_some());
        hooks.fail_syncs(true);
        hooks.release_syncs();
        let failed = failing.wait();
        assert!(
            failed.is_err_and(|why| why.to_string().contains("did not make the claim durable")),
            "the Storage node's own word"
        );
        drop(server);
    }
}
