//! The standbys a primary hands its writes to (`super::ship`), each known by
//! the node its certificate names — never by where it connected from, which
//! changes with each connection and is kept as evidence — and what it handed
//! each: its writes one by one, the entries of each whole copy, and the bytes
//! of both, counted atomically as each frame is sent, so nothing on the path
//! waits for a count (ADR-0052 and ADR-0056, amendments 2026-11-13).
//!
//! What a standby was handed is kept across its connections for as long as
//! the primary runs, so what is drawn of it only grows; a standby is listed
//! only while a connection of its is handed writes. One whose certificate
//! names no node is known by where it connected from, for that connection
//! alone.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use xcore::lock::Locked;

use super::copies::DATABASES;

/// A standby its primary hands writes to now, as the primary knows it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Follower {
    /// The node its certificate names; none where it names none.
    pub node: Option<String>,
    /// Where its latest connection came from: evidence, never who it is.
    pub address: String,
    /// The writes handed it, each as the primary's engine took it.
    pub writes: u64,
    /// The entries handed it in whole copies of the databases.
    pub entries: u64,
    /// The bytes of the frames that carried those writes and entries.
    pub bytes: u64,
    /// How many writes the primary holds that it was not handed yet.
    pub behind: u64,
}

impl Follower {
    /// What the primary says of it in its standing: who, from where, and how
    /// far behind.
    #[must_use]
    pub fn said(&self) -> String {
        let who = match &self.node {
            Some(node) => format!("{node} from {}", self.address),
            None => self.address.clone(),
        };
        format!("{who} {} writes behind", self.behind)
    }
}

/// Who a standby is known as: the node its certificate names, or, naming
/// none, where it connected from.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Known {
    Node(String),
    Address(String),
}

/// What a primary handed one standby, counted as it is sent.
pub(super) struct Handed {
    known: Known,
    writes: AtomicU64,
    entries: AtomicU64,
    bytes: AtomicU64,
    now: Mutex<Now>,
}

/// How a standby is followed now.
struct Now {
    /// How many of its connections are handed writes.
    connections: usize,
    /// Where its latest connection came from.
    address: String,
    /// The next write of each database it is handed.
    next: [u64; DATABASES],
}

impl Handed {
    /// A write handed on, in a frame of `bytes`.
    pub(super) fn wrote(&self, bytes: usize) {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.add_bytes(bytes);
    }

    /// `entries` of a whole copy handed on, in a frame of `bytes`.
    pub(super) fn paged(&self, entries: usize, bytes: usize) {
        let entries = u64::try_from(entries).unwrap_or(u64::MAX);
        self.entries.fetch_add(entries, Ordering::Relaxed);
        self.add_bytes(bytes);
    }

    fn add_bytes(&self, bytes: usize) {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// The next write of each database it is handed is `next` now.
    pub(super) fn reached(&self, next: [u64; DATABASES]) {
        self.now.locked().next = next;
    }

    /// It as listed, behind a primary whose next writes are `holds`.
    fn follower(&self, holds: [u64; DATABASES]) -> Option<Follower> {
        let now = self.now.locked();
        if now.connections == 0 {
            return None;
        }
        let read = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        let behind = holds
            .iter()
            .sum::<u64>()
            .saturating_sub(now.next.iter().sum());
        Some(Follower {
            node: match &self.known {
                Known::Node(node) => Some(node.clone()),
                Known::Address(_) => None,
            },
            address: now.address.clone(),
            writes: read(&self.writes),
            entries: read(&self.entries),
            bytes: read(&self.bytes),
            behind,
        })
    }
}

/// Every standby a primary hands writes to, by who it is known as.
#[derive(Default)]
pub(super) struct Followers {
    by: Mutex<BTreeMap<Known, Arc<Handed>>>,
}

impl Followers {
    /// A connection of the standby whose certificate names `node`, from
    /// `address`, handed writes from `next` on: what it is handed is counted
    /// on what was counted of it before.
    pub(super) fn began(
        &self,
        (node, address): (Option<&str>, &str),
        next: [u64; DATABASES],
    ) -> Arc<Handed> {
        let known = node.map_or_else(
            || Known::Address(address.to_string()),
            |node| Known::Node(node.to_string()),
        );
        let mut by = self.by.locked();
        let handed = by.entry(known.clone()).or_insert_with(|| {
            Arc::new(Handed {
                known,
                writes: AtomicU64::new(0),
                entries: AtomicU64::new(0),
                bytes: AtomicU64::new(0),
                now: Mutex::new(Now {
                    connections: 0,
                    address: String::new(),
                    next,
                }),
            })
        });
        let mut now = handed.now.locked();
        now.connections += 1;
        now.address = address.to_string();
        now.next = next;
        drop(now);
        Arc::clone(handed)
    }

    /// A connection [`Followers::began`] gave `handed` for ended: a standby
    /// known by its address alone is forgotten with its last connection.
    pub(super) fn ended(&self, handed: &Handed) {
        let mut by = self.by.locked();
        let mut now = handed.now.locked();
        now.connections = now.connections.saturating_sub(1);
        if now.connections == 0 && matches!(handed.known, Known::Address(_)) {
            by.remove(&handed.known);
        }
    }

    /// Every standby handed writes now, behind a primary whose next writes
    /// are `holds`, in the order of who it is known as.
    pub(super) fn following(&self, holds: [u64; DATABASES]) -> Vec<Follower> {
        let by = self.by.locked();
        by.values()
            .filter_map(|handed| handed.follower(holds))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_standby_is_known_by_its_node_across_its_connections_and_counted_on() {
        let followers = Followers::default();
        let node = configure::fixture::test_cluster().node(1).name.clone();
        let first = followers.began((Some(&node), "127.0.0.1:5001"), [1, 1, 1]);
        first.paged(256, 9_000);
        first.reached([4, 1, 1]);
        first.wrote(100);
        followers.ended(&first);
        assert_eq!(followers.following([5, 1, 1]), [], "not following now");

        let again = followers.began((Some(&node), "127.0.0.1:5002"), [5, 1, 1]);
        again.wrote(50);
        again.reached([6, 1, 1]);
        let listed = followers.following([9, 2, 1]);
        assert_eq!(
            listed,
            [Follower {
                node: Some(node.clone()),
                address: "127.0.0.1:5002".into(),
                writes: 2,
                entries: 256,
                bytes: 9_150,
                behind: 4,
            }],
            "counted on across its connections, by the node its certificate names"
        );
        assert_eq!(
            listed[0].said(),
            format!("{node} from 127.0.0.1:5002 4 writes behind")
        );
        followers.ended(&again);
    }

    #[test]
    fn a_standby_naming_no_node_is_known_by_its_address_for_its_connection_alone() {
        let followers = Followers::default();
        let unnamed = followers.began((None, "127.0.0.1:5003"), [1, 1, 1]);
        unnamed.wrote(10);
        let listed = followers.following([2, 1, 1]);
        assert_eq!(listed.len(), 1);
        assert_eq!((listed[0].node.as_deref(), listed[0].writes), (None, 1));
        assert_eq!(listed[0].said(), "127.0.0.1:5003 1 writes behind");
        followers.ended(&unnamed);
        assert_eq!(followers.following([2, 1, 1]), []);
        let anew = followers.began((None, "127.0.0.1:5003"), [2, 1, 1]);
        assert_eq!(followers.following([2, 1, 1])[0].writes, 0, "forgotten");
        followers.ended(&anew);
        assert!(followers.by.locked().is_empty(), "nothing kept of it");
    }
}
