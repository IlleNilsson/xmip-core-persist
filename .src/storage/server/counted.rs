//! What crossed a Storage node, per node that called it: the Publications it
//! wrote and the Journeys it took on, each counted against the node its
//! certificate was issued for (ADR-0052 and ADR-0056, amendments
//! 2026-11-11). Only a connection of node standing is counted — a person's
//! never asks Xmip Storage anything (`admitted`).
//!
//! A request is counted from its answer, once answered, never as it is
//! asked: a Publication the Storage node wrote (a `Publish` answered with
//! its claims, a Work Process's `Turn` answered yes) and a Journey a node
//! took and handed on (a `HandOn` answered yes, and the Journey a `Turn`
//! took). A claim is not traffic: one another holder won, and the claim a
//! send scan takes and gives back on each pass over a Journey not yet due,
//! move nothing; nor does a request refused, failed, or answered by a
//! standby. Each connection resolves its counter once, as its caller is
//! proven, and adds to it atomically: nothing on the path waits for it.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use xcore::lock::Locked;

use super::super::wire::{Answer, Request};
use super::admitted::NODE;

/// What a request asks that could be counted, read before it is answered.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Asked {
    Publish,
    Turn,
    HandOn,
    Other,
}

impl Asked {
    /// What `request` asks.
    pub(super) const fn of(request: &Request) -> Self {
        match request {
            Request::Publish(_) => Self::Publish,
            Request::Turn(_) => Self::Turn,
            Request::HandOn(_) => Self::HandOn,
            _ => Self::Other,
        }
    }
}

/// What one node asked of a Storage node, counted.
#[derive(Default)]
pub(super) struct Crossed {
    published: AtomicU64,
    taken: AtomicU64,
}

impl Crossed {
    /// Count what `answer` says came of a request that asked `asked`.
    pub(super) fn count(&self, asked: Asked, answer: &Answer) {
        let add = |counter: &AtomicU64| {
            counter.fetch_add(1, Ordering::Relaxed);
        };
        match (asked, answer) {
            (Asked::Publish, Answer::Claims(_)) => add(&self.published),
            (Asked::Turn, Answer::Yes(true)) => {
                add(&self.taken);
                add(&self.published);
            }
            (Asked::HandOn, Answer::Yes(true)) => add(&self.taken),
            _ => {}
        }
    }
}

/// What one node asked of a Storage node: the node its certificate was
/// issued for, how many Publications it wrote and how many Journeys it took
/// on.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Peer {
    pub node: String,
    pub published: u64,
    pub taken: u64,
}

/// Every node's counter, by node, kept across its connections.
#[derive(Default)]
pub(super) struct Peers {
    by: Mutex<BTreeMap<String, Arc<Crossed>>>,
}

impl Peers {
    /// The counter of the caller whose certificate's subject is `who`, of
    /// `standing`: the node its common name names (`tls::Name`). None for a
    /// person, or a connection that proved no subject naming one.
    pub(super) fn of(&self, who: Option<&str>, standing: u8) -> Option<Arc<Crossed>> {
        let who = tls::Name::parse(who.filter(|_| standing == NODE)?);
        let node = who.common_name()?;
        let mut by = self.by.locked();
        Some(Arc::clone(by.entry(node.to_string()).or_default()))
    }

    /// Every node counted so far, by node.
    pub(super) fn counted(&self) -> Vec<Peer> {
        let read = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        self.by
            .locked()
            .iter()
            .map(|(node, crossed)| Peer {
                node: node.clone(),
                published: read(&crossed.published),
                taken: read(&crossed.taken),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::super::admitted::PERSON;
    use super::*;
    use crate::storage::record::Claim;
    use xcore::JourneyId;

    fn claim() -> Claim {
        Claim {
            journey: JourneyId::new(5),
            holder: "edge-01".into(),
            token: 9,
            until_unix_nanos: 0,
            fence: 0,
        }
    }

    fn counted(peers: &Peers) -> (u64, u64) {
        peers
            .counted()
            .first()
            .map_or((0, 0), |peer| (peer.published, peer.taken))
    }

    #[test]
    fn a_nodes_traffic_is_counted_against_its_node_and_a_persons_never() {
        let peers = Peers::default();
        let node = peers
            .of(Some("O=Xmip,CN=edge-01"), NODE)
            .expect("a node is counted");
        assert!(
            peers.of(Some("CN=a-person"), PERSON).is_none(),
            "a person never"
        );
        assert!(peers.of(None, NODE).is_none(), "no subject, no count");
        assert!(peers.of(Some("O=Xmip"), NODE).is_none(), "no node named");
        node.count(Asked::Publish, &Answer::Claims(Vec::new()));
        node.count(Asked::HandOn, &Answer::Yes(true));
        let again = peers.of(Some("CN=edge-01"), NODE).expect("the same node");
        again.count(Asked::Turn, &Answer::Yes(true));
        assert_eq!(
            peers.counted(),
            [Peer {
                node: "edge-01".into(),
                published: 2,
                taken: 2,
            }],
            "kept across its connections, by the node its certificate names"
        );
    }

    #[test]
    fn a_lost_claim_a_given_back_claim_and_a_refused_publication_count_nothing() {
        let peers = Peers::default();
        let node = peers.of(Some("CN=edge-01"), NODE).expect("a node");
        // A claim another holder won, and one a send scan took and gave
        // back over a Journey not yet due: neither is asked as traffic.
        let claimed = Request::Claim(claim(), 0);
        node.count(Asked::of(&claimed), &Answer::Claim(None));
        node.count(Asked::of(&claimed), &Answer::Claim(Some(claim())));
        let released = Request::Release(claim());
        node.count(Asked::of(&released), &Answer::Yes(true));
        // A hand-on whose claim was no longer its holder's.
        node.count(Asked::HandOn, &Answer::Yes(false));
        // A Publication a standby refused, one refused, one that failed.
        node.count(Asked::Publish, &Answer::Standby("a standby".into()));
        node.count(
            Asked::Publish,
            &Answer::Refused("scope".into(), "refused".into()),
        );
        node.count(Asked::Publish, &Answer::Failed("failed".into()));
        assert_eq!(counted(&peers), (0, 0));
    }
}
