//! The laid-out columns as Xmip Storage writes them: each record's times
//! set on Xmip Storage's clock, and, in the embedded engines, each of its
//! table's indexes kept as entries of its own, in the same write as the
//! record (proposed 2026-10-09).
//!
//! **One definition.** A table's columns and indexes are the schema's
//! (`super::schema::searchable`); a database server keeps them as columns
//! and indexes, and the embedded engines — key/value, with no columns —
//! keep each index as entries: a key of the index's number, its columns'
//! values in order and the record's identifier, and the identifier, sealed,
//! as the value ([`crate::EncryptedStore::apply_indexed`]). The smallest
//! faithful equivalent of an index: one range read finds what an index on
//! a server finds. The rest of a record's columns is a server's alone; the
//! embedded engines keep it in the sealed record.
//!
//! **In the clear, as on a server** (the owner, 2026-10-09: *Store it in
//! the clear*). An entry's key holds its values as the server's columns
//! do: an identifier as its sixteen bytes, words as their length and their
//! UTF-8, a time, a small number, a count or a flag big-endian, so it
//! sorts. An engine's files show what the indexes hold; the records stay
//! sealed.
//!
//! **Kept with the record.** A record written replaces the entries of the
//! one it replaces, and a record removed takes its entries with it, all in
//! its write: the record it replaces is read for them — a point read of one
//! key, which a server's `UPDATE` makes too.
use std::collections::HashMap;

use super::commit::{DEAD, HELD, JOURNEY, MESSAGE};
use super::dead::Dead;
use super::hold::Held;
use super::record::{AdministrationRecord, AuditEntry, JourneyRecord, MessageRecord};
use super::row::{Columned, Row, Value};
use super::schema::{Database, Index, TABLES, Table};
use crate::{EncryptedStore, Engine, IndexEntry, PersistError, RecordChange};

/// Where the administration database keeps an audit record the keeper
/// moved, by its identifier.
pub(crate) const KEPT_AUDIT: &str = "audit";

/// What every administration record's kind opens with
/// (`super::embedded`).
pub(crate) const ADMINISTRATION: &str = "administration/";

/// The searchable tables, each the record it keeps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Searched {
    Journey,
    Message,
    Held,
    Dead,
    Audit,
    Administration,
}

impl Searched {
    /// The table records of `kind` in `database` are, where it is one.
    fn of(database: Database, kind: &str) -> Option<Self> {
        Some(match (database, kind) {
            (Database::Runtime, JOURNEY) => Self::Journey,
            (Database::Runtime, MESSAGE) => Self::Message,
            (Database::Runtime, HELD) => Self::Held,
            (Database::Runtime, DEAD) => Self::Dead,
            (Database::Administration, KEPT_AUDIT) => Self::Audit,
            (Database::Administration, kind) if kind.starts_with(ADMINISTRATION) => {
                Self::Administration
            }
            _ => return None,
        })
    }

    const fn table(self) -> &'static str {
        match self {
            Self::Journey => "journey",
            Self::Message => "message",
            Self::Held => "held",
            Self::Dead => "dead_message",
            Self::Audit => "audit",
            Self::Administration => "administration",
        }
    }

    /// The row and the identifier of the record `bytes` hold, as it was
    /// kept.
    fn row(self, bytes: &[u8]) -> Result<(Row, u128), PersistError> {
        match self {
            Self::Journey => row::<JourneyRecord>(bytes),
            Self::Message => row::<MessageRecord>(bytes),
            Self::Held => row::<Held>(bytes),
            Self::Dead => row::<Dead>(bytes),
            Self::Audit => row::<AuditEntry>(bytes),
            Self::Administration => row::<AdministrationRecord>(bytes),
        }
    }

    /// `bytes` stamped at `now` over `before`: the bytes to keep, the row,
    /// and the identifier.
    fn stamped(
        self,
        bytes: &[u8],
        before: Option<&[u8]>,
        now: u64,
    ) -> Result<(Vec<u8>, Row, u128), PersistError> {
        match self {
            Self::Journey => stamp::<JourneyRecord>(bytes, before, now),
            Self::Message => stamp::<MessageRecord>(bytes, before, now),
            Self::Held => stamp::<Held>(bytes, before, now),
            Self::Dead => stamp::<Dead>(bytes, before, now),
            Self::Audit => stamp::<AuditEntry>(bytes, before, now),
            Self::Administration => stamp::<AdministrationRecord>(bytes, before, now),
        }
    }
}

fn row<T: Columned>(bytes: &[u8]) -> Result<(Row, u128), PersistError> {
    let record = T::from_bytes(bytes)?;
    Ok((record.row(), record.id()))
}

fn stamp<T: Columned>(
    bytes: &[u8],
    before: Option<&[u8]>,
    now: u64,
) -> Result<(Vec<u8>, Row, u128), PersistError> {
    let mut record = T::from_bytes(bytes)?;
    let before = before.map(T::from_bytes).transpose()?;
    record.stamp(now, before.as_ref());
    Ok((record.bytes(), record.row(), record.id()))
}

/// The table named `name` in `database`.
pub(crate) fn table(database: Database, name: &str) -> Option<&'static Table> {
    TABLES
        .iter()
        .find(|table| table.database == database && table.name == name)
}

/// One database's laid-out tables, as its records are written and its
/// indexes read.
pub(crate) struct Columns {
    database: Database,
}

impl Columns {
    /// The tables of `database`.
    pub(crate) const fn of(database: Database) -> Self {
        Self { database }
    }

    /// `changes` as they are written at `now` — each searchable record
    /// with Xmip Storage's times set — and the index entries that go with
    /// them: those of each record written, and the removal of those of
    /// each record it replaces or removes.
    ///
    /// # Errors
    ///
    /// Where a record it replaces cannot be read, or a record is not one.
    pub(crate) fn written<'a, E: Engine>(
        &self,
        store: &EncryptedStore<E>,
        changes: Vec<RecordChange<'a>>,
        now: u64,
    ) -> Result<(Vec<RecordChange<'a>>, Vec<IndexEntry>), PersistError> {
        let mut entries = Vec::new();
        let mut latest: HashMap<(&'a str, Vec<u8>), Option<Vec<u8>>> = HashMap::new();
        let mut written = Vec::with_capacity(changes.len());
        for (kind, key, value) in changes {
            let Some(searched) = Searched::of(self.database, kind) else {
                written.push((kind, key, value));
                continue;
            };
            let before = match latest.get(&(kind, key.clone())) {
                Some(before) => before.clone(),
                None => store.get(kind, &key)?,
            };
            if let Some(before) = &before {
                let (row, id) = searched.row(before)?;
                let gone = self.entries(searched.table(), &row, id)?;
                entries.extend(gone.into_iter().map(|key| (key, None)));
            }
            let value = match value {
                Some(bytes) => {
                    let (bytes, row, id) = searched.stamped(&bytes, before.as_deref(), now)?;
                    let kept = self.entries(searched.table(), &row, id)?;
                    entries.extend(kept.into_iter().map(|key| (key, Some(id))));
                    Some(bytes)
                }
                None => None,
            };
            latest.insert((kind, key.clone()), value.clone());
            written.push((kind, key, value));
        }
        Ok((written, entries))
    }

    /// The key of each entry of `table`'s indexes `row` belongs in: none
    /// for an index whose column it lacks, or whose flag it does not have.
    fn entries(&self, table: &str, row: &Row, id: u128) -> Result<Vec<Vec<u8>>, PersistError> {
        let table = self::table(self.database, table)
            .ok_or_else(|| super::record::malformed(format!("no table '{table}'")))?;
        let tag = id.to_be_bytes();
        let mut keys = Vec::new();
        'indexes: for index in table.indexes {
            if let Some(flag) = index.only
                && value(row, flag) != Some(&Value::Flag(true))
            {
                continue;
            }
            let mut key = vec![index.number];
            for column in index.columns {
                let Some(value) = value(row, column) else {
                    continue 'indexes;
                };
                key.extend(encoded(value));
            }
            key.extend(tag);
            keys.push(key);
        }
        Ok(keys)
    }

    /// The records `index` of `table` finds whose first columns are
    /// `equal` and whose next, where `range` says, is from one time to
    /// another, both included: up to `most`, in the index's order or the
    /// reverse.
    ///
    /// # Errors
    ///
    /// Where the index is not one, or an entry fails its tag.
    pub(crate) fn find<E: Engine>(
        &self,
        store: &EncryptedStore<E>,
        (table, index): (&str, &str),
        equal: &[Value],
        range: Option<(u64, u64)>,
        (most, reverse): (u32, bool),
    ) -> Result<Vec<u128>, PersistError> {
        let missing = || super::record::malformed(format!("no index {table}.{index}"));
        let table = self::table(self.database, table).ok_or_else(missing)?;
        let index: &Index = table
            .indexes
            .iter()
            .find(|candidate| candidate.name == index)
            .ok_or_else(missing)?;
        let mut prefix = vec![index.number];
        for value in equal.iter().take(index.columns.len()) {
            prefix.extend(encoded(value));
        }
        let (first, last) = match range {
            Some((from, to)) if from > to => return Ok(Vec::new()),
            Some((from, to)) => (
                [prefix.as_slice(), &from.to_be_bytes()].concat(),
                [prefix.as_slice(), &to.to_be_bytes(), &[0xFF; 17]].concat(),
            ),
            None => (prefix.clone(), [prefix.as_slice(), &[0xFF; 64]].concat()),
        };
        let most = usize::try_from(most).unwrap_or(usize::MAX);
        store.scan_index(&first, &last, most, reverse)
    }
}

/// `value` as an entry's key keeps it, in the clear: an identifier as
/// its sixteen bytes, words as their length and their UTF-8, and a time, a
/// count or a flag big-endian, so it sorts.
fn encoded(value: &Value) -> Vec<u8> {
    match value {
        Value::Time(time) | Value::Count(time) => time.to_be_bytes().to_vec(),
        Value::Flag(flag) => vec![u8::from(*flag)],
        Value::Text(text) => {
            let length = u32::try_from(text.len()).unwrap_or(u32::MAX);
            [length.to_be_bytes().as_slice(), text.as_bytes()].concat()
        }
        Value::Id(id) => id.to_be_bytes().to_vec(),
    }
}

/// The value `row` holds in `column`, where it holds one.
fn value<'r>(row: &'r Row, column: &str) -> Option<&'r Value> {
    row.iter()
        .find(|(name, _)| *name == column)
        .and_then(|(_, value)| value.as_ref())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::Memory;
    use crate::storage::{
        AdministrationKind, Ask, AuditFacts, DeadMessage, Embedded, Hold, JourneyFacts,
        MessageFacts, Publication, Query, Span, XmipStorage,
    };
    use secret::KekName;
    use std::sync::{Arc, Mutex};
    use xcore::{AuditId, Clock, JourneyId, MessageId};

    /// A record of each searchable table, as a row.
    fn rows() -> Vec<(&'static str, Row)> {
        let journey = JourneyRecord {
            journey: JourneyId::new(1),
            body: Vec::new(),
            facts: JourneyFacts::default(),
        };
        let message = MessageRecord {
            message: MessageId::new(2),
            body: Vec::new(),
            facts: MessageFacts::default(),
        };
        let held = Held {
            sequence: 0,
            hold: Hold {
                queue: 3,
                journey: JourneyId::new(1),
                body: Vec::new(),
            },
            held_unix_nanos: 0,
        };
        let dead = Dead {
            sequence: 0,
            message: DeadMessage::default(),
        };
        let audit = AuditEntry {
            id: AuditId::new(4),
            body: Vec::new(),
            audited: None,
            facts: AuditFacts::default(),
        };
        let administration = AdministrationRecord {
            kind: AdministrationKind::Operator,
            id: 5,
            body: Vec::new(),
            updated_unix_nanos: 0,
        };
        vec![
            ("journey", journey.row()),
            ("message", message.row()),
            ("held", held.row()),
            ("dead_message", dead.row()),
            ("audit", audit.row()),
            ("administration", administration.row()),
        ]
    }

    #[test]
    fn every_searchable_column_is_filled_from_its_record_and_every_index_from_its_row() {
        for (name, row) in rows() {
            let table = TABLES
                .iter()
                .find(|t| t.name == name && !t.indexes.is_empty())
                .expect("a searchable table");
            let names: Vec<&str> = row.iter().map(|(column, _)| *column).collect();
            for column in table.columns {
                let kept = table.key.contains(&column.name)
                    || table.unique.contains(&column.name)
                    || column.name == "body";
                assert!(
                    kept || names.contains(&column.name),
                    "{name}.{}",
                    column.name
                );
            }
            for named in &names {
                assert!(
                    table.columns.iter().any(|c| c.name == *named),
                    "{name}.{named}"
                );
            }
            for index in table.indexes {
                for column in index.columns {
                    assert!(names.contains(column), "{}: {column}", index.name);
                }
            }
        }
    }

    #[test]
    fn each_index_has_a_number_of_its_own_and_a_partial_one_filters_on_a_flag() {
        let mut numbers: Vec<u8> = TABLES
            .iter()
            .flat_map(|table| table.indexes.iter().map(|index| index.number))
            .collect();
        let count = numbers.len();
        numbers.sort_unstable();
        numbers.dedup();
        assert_eq!(numbers.len(), count);
        for table in TABLES {
            for index in table.indexes {
                if let Some(flag) = index.only {
                    let column = table.columns.iter().find(|c| c.name == flag);
                    let kind = column.expect("its flag").kind;
                    assert_eq!(kind, super::super::schema::Kind::Flag, "{}", index.name);
                }
            }
        }
    }

    /// A clock a test sets by hand.
    #[derive(Default)]
    struct Pinned(Mutex<i128>);

    impl Pinned {
        fn set(&self, nanos: i128) {
            *self.0.lock().expect("clock") = nanos;
        }
    }

    impl Clock for Pinned {
        fn unix_timestamp_nanos(&self) -> i128 {
            *self.0.lock().expect("clock")
        }
    }

    fn node() -> (Embedded<Memory, Memory>, Arc<Pinned>) {
        let keys = secret::Held::new(secret::fixture::Memory::default());
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

    fn find(node: &dyn XmipStorage, ask: Ask) -> Vec<u128> {
        let query = Query {
            ask,
            most: 10,
            newest_first: false,
        };
        node.query(&query).expect("asked")
    }

    fn span(from: u64, to: u64) -> Span {
        Span {
            from_unix_nanos: from,
            to_unix_nanos: to,
        }
    }

    /// The Journey `id + 100` of the Message `id`, bound for `port` in
    /// `state`.
    fn journey(id: u128, port: &str, state: &str) -> JourneyRecord {
        JourneyRecord {
            journey: JourneyId::new(id + 100),
            body: b"to send".to_vec(),
            facts: JourneyFacts {
                state: state.to_string(),
                send_port: Some(port.to_string()),
                previous_journey: Some(8),
                message: Some(id),
                ..JourneyFacts::default()
            },
        }
    }

    /// The Message `id` from `party`, its Journey ([`journey`]) held in the
    /// queue nine, and its audit record `id + 200`, a failure for the
    /// Message two.
    fn published(id: u128, party: &str, port: &str, state: &str) -> Publication {
        Publication {
            message: MessageRecord {
                message: MessageId::new(id),
                body: b"order".to_vec(),
                facts: MessageFacts {
                    party: Some(party.to_string()),
                    contract: Some("Order".to_string()),
                    previous_message: Some(7),
                    ..MessageFacts::default()
                },
            },
            journeys: vec![journey(id, port, state)],
            held: vec![Hold {
                queue: 9,
                journey: JourneyId::new(id + 100),
                body: Vec::new(),
            }],
            dead: None,
            audit: AuditEntry {
                id: AuditId::new(id + 200),
                body: b"published".to_vec(),
                audited: None,
                facts: AuditFacts {
                    occurred_unix_nanos: 50,
                    failed: id == 2,
                    artifact_name: Some("OrdersIn".to_string()),
                    journey: Some(id + 100),
                    message: Some(id),
                    ..AuditFacts::default()
                },
            },
            claims: Vec::new(),
            lease_nanos: 0,
        }
    }

    /// Three Publications: partner-x's to Billing at ten, partner-x's to
    /// Archive and Fabrikam's to Billing, in another state, at twenty.
    fn three() -> (Embedded<Memory, Memory>, Arc<Pinned>) {
        let (node, clock) = node();
        clock.set(10);
        node.publish(&published(1, "partner-x", "Billing", "Active"))
            .expect("published");
        clock.set(20);
        node.publish(&published(2, "partner-x", "Archive", "Active"))
            .expect("published");
        node.publish(&published(3, "Fabrikam", "Billing", "Completed"))
            .expect("published");
        (node, clock)
    }

    #[test]
    fn messages_are_found_by_party_contract_time_and_what_they_came_from() {
        let (node, _) = three();
        let party = |created| Ask::MessagesFromParty {
            party: "partner-x".to_string(),
            created,
        };
        assert_eq!(find(&node, party(Span::ALL)), [1, 2], "oldest first");
        assert_eq!(find(&node, party(span(15, 25))), [2]);
        let contract = Ask::MessagesOfContract {
            contract: "Order".to_string(),
            created: Span::ALL,
        };
        assert_eq!(find(&node, contract).len(), 3);
        let first = Ask::MessagesCreated {
            created: span(0, 10),
        };
        assert_eq!(find(&node, first), [1]);
        assert_eq!(find(&node, Ask::MessagesAfter { message: 7 }).len(), 3);
        let newest = Query {
            ask: party(Span::ALL),
            most: 1,
            newest_first: true,
        };
        assert_eq!(node.query(&newest).expect("asked"), [2], "newest, one");
    }

    #[test]
    fn journeys_are_found_by_send_port_and_state_and_a_rewrite_moves_them() {
        let (node, clock) = three();
        let billing = |state: &str| Ask::JourneysAtSendPort {
            send_port: "Billing".to_string(),
            state: state.to_string(),
        };
        assert_eq!(find(&node, billing("Active")), [101]);
        assert_eq!(find(&node, billing("Completed")), [103]);
        let waiting = Ask::JourneysInState {
            state: "Active".to_string(),
            updated: Span::ALL,
        };
        assert_eq!(find(&node, waiting.clone()), [101, 102], "by when written");
        assert_eq!(find(&node, Ask::JourneysAfter { journey: 8 }).len(), 3);
        assert_eq!(find(&node, Ask::JourneysHolding { message: 2 }), [102]);
        let held = Ask::Held {
            queue: 9,
            held: span(20, 20),
        };
        assert_eq!(find(&node, held).len(), 2);
        let crossed = Ask::JourneysAtSendPort {
            send_port: "partner-x".to_string(),
            state: "Active".to_string(),
        };
        assert_eq!(find(&node, crossed), [], "a Party is no Send Port");

        clock.set(30);
        node.write_journey(&journey(1, "Billing", "Completed"))
            .expect("written again");
        assert_eq!(find(&node, waiting), [102], "its old entries gone");
        let mut moved = find(&node, billing("Completed"));
        moved.sort_unstable();
        assert_eq!(moved, [101, 103]);
        let facts = node.read_journey(JourneyId::new(101)).expect("read");
        let facts = facts.expect("there").facts;
        let times = (facts.created_unix_nanos, facts.updated_unix_nanos);
        assert_eq!(times, (10, 30), "created kept, updated now");
    }

    #[test]
    fn audit_records_are_found_once_the_keeper_kept_them() {
        let (node, _) = three();
        let of_journey = Ask::AuditOfJourney { journey: 101 };
        assert_eq!(find(&node, of_journey.clone()), [], "not kept yet");
        assert_eq!(node.keep_audit(10).expect("kept"), 3);
        assert_eq!(find(&node, of_journey), [201]);
        assert_eq!(find(&node, Ask::AuditOfMessage { message: 3 }), [203]);
        let failed = Ask::AuditFailed {
            occurred: Span::ALL,
        };
        assert_eq!(find(&node, failed), [202], "only a failure");
        let artifact = Ask::AuditOfArtifact {
            artifact: "OrdersIn".to_string(),
            occurred: span(50, 50),
        };
        assert_eq!(find(&node, artifact).len(), 3);
        let before = Ask::AuditOccurred {
            occurred: span(0, 49),
        };
        assert_eq!(find(&node, before), []);
    }

    #[test]
    fn administration_and_dead_messages_are_found_by_time() {
        let (node, clock) = node();
        clock.set(5);
        let record = AdministrationRecord {
            kind: AdministrationKind::Operator,
            id: 6,
            body: b"paused".to_vec(),
            updated_unix_nanos: 0,
        };
        node.write_administration(&record).expect("written");
        let updated = |updated| Ask::AdministrationUpdated {
            kind: AdministrationKind::Operator,
            updated,
        };
        assert_eq!(find(&node, updated(span(5, 5))), [6]);
        clock.set(9);
        node.write_administration(&record).expect("written again");
        assert_eq!(find(&node, updated(span(5, 5))), [], "moved on");
        assert_eq!(find(&node, updated(span(9, 9))), [6]);
        node.remove_administration(AdministrationKind::Operator, 6)
            .expect("removed");
        assert_eq!(find(&node, updated(Span::ALL)), [], "gone with it");

        let mut unmatched = published(4, "partner-x", "Billing", "Active");
        unmatched.journeys.clear();
        unmatched.held.clear();
        unmatched.dead = Some(DeadMessage {
            queue: 11,
            message: MessageId::new(4),
            received_unix_nanos: 40,
            node: "a node".to_string(),
            location: "OrdersIn".to_string(),
            ..DeadMessage::default()
        });
        node.publish(&unmatched).expect("published");
        let dead = |queued| Ask::DeadMessages { queue: 11, queued };
        assert_eq!(find(&node, dead(span(40, 40))), [4]);
        assert_eq!(find(&node, dead(span(41, 99))), []);
    }

    #[test]
    fn a_publication_whose_write_fails_leaves_no_entry() {
        use super::super::commit::PLACES;
        use super::super::queue::places_key;
        let (node, _) = node();
        node.runtime()
            .put(PLACES, &places_key(9), b"torn")
            .expect("torn");
        assert!(
            node.publish(&published(1, "partner-x", "Billing", "Active"))
                .is_err()
        );
        let party = Ask::MessagesFromParty {
            party: "partner-x".to_string(),
            created: Span::ALL,
        };
        assert_eq!(find(&node, party), []);
        assert_eq!(find(&node, Ask::JourneysAfter { journey: 8 }), []);
    }
}
