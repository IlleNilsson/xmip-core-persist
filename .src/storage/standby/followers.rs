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
//!
//! **The one list every node shows a standby by** (open problem 95; ADR-0056,
//! amendment 2026-11-22): each standby says its place among the Storage
//! nodes listed as it asks to follow, so a node matches the list to its own
//! `[storage] nodes` by place, never by a name; and each change of who
//! follows is told to every wait on the primary (`Watched::Standbys`), so a
//! node is told a standby stopped following as soon as its primary knows.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use xcore::lock::Locked;

use codec::cursor::Cursor;
use codec::field;
use codec::form::Form;
use codec::writer::ByteWriter;

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
    /// Its place among the Storage nodes listed, from zero, as it said
    /// asking to follow on its latest connection: none where it had not
    /// found itself among them.
    pub place: Option<u32>,
}

impl Form for Follower {
    fn write(&self, out: &mut Vec<u8>) {
        field::optional(out, self.node.as_deref(), field::text);
        field::text(out, &self.address);
        out.u64_be(self.writes)
            .u64_be(self.entries)
            .u64_be(self.bytes)
            .u64_be(self.behind);
        field::optional(out, self.place, |out, place| {
            out.u32_be(place);
        });
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        Ok(Self {
            node: field::read_optional(cursor, field::read_text)?,
            address: field::read_text(cursor)?,
            writes: cursor.u64_be()?,
            entries: cursor.u64_be()?,
            bytes: cursor.u64_be()?,
            behind: cursor.u64_be()?,
            place: field::read_optional(cursor, Cursor::u32_be)?,
        })
    }
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
    /// Its place among the Storage nodes listed, as its latest connection
    /// said it.
    place: Option<u32>,
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
            place: now.place,
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
    /// `address`, at `place` among the Storage nodes listed, handed writes
    /// from `next` on: what it is handed is counted on what was counted of
    /// it before.
    pub(super) fn began(
        &self,
        (node, address): (Option<&str>, &str),
        (next, place): ([u64; DATABASES], Option<u32>),
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
                    place,
                }),
            })
        });
        let mut now = handed.now.locked();
        now.connections += 1;
        now.address = address.to_string();
        now.next = next;
        now.place = place;
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
        let first = followers.began((Some(&node), "127.0.0.1:5001"), ([1, 1, 1], None));
        first.paged(256, 9_000);
        first.reached([4, 1, 1]);
        first.wrote(100);
        followers.ended(&first);
        assert_eq!(followers.following([5, 1, 1]), [], "not following now");

        let again = followers.began((Some(&node), "127.0.0.1:5002"), ([5, 1, 1], Some(1)));
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
                place: Some(1),
            }],
            "counted on across its connections, by the node its certificate names, \
             at the place its latest connection said"
        );
        assert_eq!(
            listed[0].said(),
            format!("{node} from 127.0.0.1:5002 4 writes behind")
        );
        assert_eq!(
            Vec::<Follower>::from_bytes(&listed.bytes()).expect("read"),
            listed,
            "as it crosses the wire"
        );
        followers.ended(&again);
    }

    #[test]
    fn a_standby_naming_no_node_is_known_by_its_address_for_its_connection_alone() {
        let followers = Followers::default();
        let unnamed = followers.began((None, "127.0.0.1:5003"), ([1, 1, 1], None));
        unnamed.wrote(10);
        let listed = followers.following([2, 1, 1]);
        assert_eq!(listed.len(), 1);
        assert_eq!((listed[0].node.as_deref(), listed[0].writes), (None, 1));
        assert_eq!(listed[0].said(), "127.0.0.1:5003 1 writes behind");
        followers.ended(&unnamed);
        assert_eq!(followers.following([2, 1, 1]), []);
        let anew = followers.began((None, "127.0.0.1:5003"), ([2, 1, 1], None));
        assert_eq!(followers.following([2, 1, 1])[0].writes, 0, "forgotten");
        followers.ended(&anew);
        assert!(followers.by.locked().is_empty(), "nothing kept of it");
    }
}
