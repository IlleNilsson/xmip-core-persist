//! Each searchable record's row: the values of its table's searchable
//! columns, by the names the schema gives them
//! (`super::schema::searchable`), and the times Xmip Storage sets as it
//! writes it ([`super::columns`]).

use super::dead::Dead;
use super::hold::Held;
use super::record::{AdministrationRecord, AuditEntry, Form, JourneyRecord, MessageRecord};

/// A value of a searchable column: in the clear — a time, a small number,
/// a count, a flag, a word — or hashed under its column's key — a name or
/// an identifier.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Value {
    Time(u64),
    Small(u16),
    Count(u64),
    Flag(bool),
    Text(String),
    Name(String),
    Id(u128),
}

/// A record's searchable columns, by name: `None` where it lacks the fact.
pub(crate) type Row = Vec<(&'static str, Option<Value>)>;

/// A record its table keeps searchable columns of.
pub(crate) trait Columned: Form {
    /// What its index entries find: its identifier.
    fn id(&self) -> u128;

    /// Xmip Storage's own times set to `now` — or, where a time is the
    /// first write's, kept from `before`, the record it replaces.
    fn stamp(&mut self, now: u64, before: Option<&Self>);

    fn row(&self) -> Row;
}

/// A time in nanoseconds since the Unix epoch as a column keeps it: none
/// before the epoch.
pub(crate) fn nanos(time: i128) -> u64 {
    u64::try_from(time.max(0)).unwrap_or(u64::MAX)
}

fn name(text: Option<&String>) -> Option<Value> {
    text.map(|text| Value::Name(text.clone()))
}

fn id(id: Option<u128>) -> Option<Value> {
    id.map(Value::Id)
}

impl Columned for JourneyRecord {
    fn id(&self) -> u128 {
        self.journey.value()
    }

    fn stamp(&mut self, now: u64, before: Option<&Self>) {
        self.facts.created_unix_nanos = before.map_or(now, |b| b.facts.created_unix_nanos);
        self.facts.updated_unix_nanos = now;
    }

    fn row(&self) -> Row {
        let facts = &self.facts;
        vec![
            ("created_at", Some(Value::Time(facts.created_unix_nanos))),
            ("updated_at", Some(Value::Time(facts.updated_unix_nanos))),
            ("state", Some(Value::Small(u16::from(facts.state)))),
            ("attempts", Some(Value::Count(u64::from(facts.attempts)))),
            ("depth", Some(Value::Count(u64::from(facts.depth)))),
            ("send_port_ref", name(facts.send_port.as_ref())),
            ("work_process_ref", name(facts.work_process.as_ref())),
            ("previous_journey_ref", id(facts.previous_journey)),
            ("message_ref", id(facts.message)),
        ]
    }
}

impl Columned for MessageRecord {
    fn id(&self) -> u128 {
        self.message.value()
    }

    fn stamp(&mut self, now: u64, before: Option<&Self>) {
        self.facts.created_unix_nanos = before.map_or(now, |b| b.facts.created_unix_nanos);
    }

    fn row(&self) -> Row {
        let facts = &self.facts;
        vec![
            ("created_at", Some(Value::Time(facts.created_unix_nanos))),
            (
                "generation",
                Some(Value::Count(u64::from(facts.generation))),
            ),
            (
                "created_by",
                Some(Value::Small(u16::from(facts.created_by))),
            ),
            ("size_bytes", Some(Value::Count(facts.size_bytes))),
            ("previous_message_ref", id(facts.previous_message)),
            ("party_ref", name(facts.party.as_ref())),
            ("contract_ref", name(facts.contract.as_ref())),
            ("stream_ref", id(facts.stream)),
        ]
    }
}

impl Columned for Held {
    fn id(&self) -> u128 {
        self.hold.journey.value()
    }

    fn stamp(&mut self, now: u64, before: Option<&Self>) {
        self.held_unix_nanos = before.map_or(now, |b| b.held_unix_nanos);
    }

    fn row(&self) -> Row {
        vec![
            ("queue", Some(Value::Id(self.hold.queue))),
            ("held_at", Some(Value::Time(self.held_unix_nanos))),
        ]
    }
}

impl Columned for Dead {
    fn id(&self) -> u128 {
        self.message.message.value()
    }

    /// Nothing: an entry's time is its Publication's, which it carries.
    fn stamp(&mut self, _: u64, _: Option<&Self>) {}

    fn row(&self) -> Row {
        let entry = &self.message;
        vec![
            ("queue", Some(Value::Id(entry.queue))),
            (
                "queued_at",
                Some(Value::Time(nanos(entry.received_unix_nanos))),
            ),
            ("node_ref", Some(Value::Name(entry.node.clone()))),
            (
                "receive_location_ref",
                Some(Value::Name(entry.location.clone())),
            ),
        ]
    }
}

impl Columned for AuditEntry {
    fn id(&self) -> u128 {
        self.id.value()
    }

    fn stamp(&mut self, now: u64, before: Option<&Self>) {
        self.facts.kept_unix_nanos = before.map_or(now, |b| b.facts.kept_unix_nanos);
    }

    fn row(&self) -> Row {
        let facts = &self.facts;
        let text = |text: &String| Some(Value::Text(text.clone()));
        vec![
            ("occurred_at", Some(Value::Time(facts.occurred_unix_nanos))),
            ("kept_at", Some(Value::Time(facts.kept_unix_nanos))),
            ("action", text(&facts.action)),
            ("phase", text(&facts.phase)),
            ("severity", text(&facts.severity)),
            ("failed", Some(Value::Flag(facts.failed))),
            ("program", text(&facts.program)),
            ("artifact_kind", facts.artifact_kind.as_ref().and_then(text)),
            ("cluster_ref", name(facts.cluster.as_ref())),
            ("node_ref", name(facts.node.as_ref())),
            ("artifact_ref", name(facts.artifact.as_ref())),
            ("journey_ref", id(facts.journey)),
            ("message_ref", id(facts.message)),
            ("execution_ref", id(facts.execution)),
        ]
    }
}

impl Columned for AdministrationRecord {
    fn id(&self) -> u128 {
        self.id
    }

    fn stamp(&mut self, now: u64, _: Option<&Self>) {
        self.updated_unix_nanos = now;
    }

    fn row(&self) -> Row {
        vec![
            ("kind", Some(Value::Text(self.kind.word().to_string()))),
            ("updated_at", Some(Value::Time(self.updated_unix_nanos))),
        ]
    }
}
