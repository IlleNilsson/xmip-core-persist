//! Each laid-out record's row: the values of its table's columns besides
//! its key and its body, by the names the schema gives them
//! (`super::schema::searchable`), in the clear, and the times Xmip Storage
//! sets as it writes it ([`super::columns`]).

use super::audited::KeptStream;
use super::dead::Dead;
use super::hold::Held;
use super::record::{AdministrationRecord, AuditEntry, Form, JourneyRecord, MessageRecord};

/// A value of a column: a time, a count, a flag, words, an identifier or
/// bytes, each as it is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Value {
    Time(u64),
    Count(u64),
    Flag(bool),
    Text(String),
    Id(u128),
    Bytes(Vec<u8>),
}

/// A record's columns, by name: `None` where it holds nothing there.
pub(crate) type Row = Vec<(&'static str, Option<Value>)>;

/// A record its table lays out in columns.
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

fn time(nanos: u64) -> Value {
    Value::Time(nanos)
}

fn count(number: impl Into<u64>) -> Value {
    Value::Count(number.into())
}

fn text(text: &str) -> Value {
    Value::Text(text.to_string())
}

fn text_maybe(text: Option<&String>) -> Option<Value> {
    text.map(|text| Value::Text(text.clone()))
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
            ("created_at", Some(time(facts.created_unix_nanos))),
            ("updated_at", Some(time(facts.updated_unix_nanos))),
            ("state", Some(text(&facts.state))),
            ("previous_journey", id(facts.previous_journey)),
            ("subscription", text_maybe(facts.subscription.as_ref())),
            (
                "cause_work_process",
                text_maybe(facts.cause_work_process.as_ref()),
            ),
            ("depth", Some(count(facts.depth))),
            ("work_process", text_maybe(facts.work_process.as_ref())),
            ("send_port", text_maybe(facts.send_port.as_ref())),
            ("send_location", Some(count(facts.send_location))),
            ("attempts", Some(count(facts.attempts))),
            ("message", id(facts.message)),
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
            ("created_at", Some(time(facts.created_unix_nanos))),
            ("previous_message", id(facts.previous_message)),
            ("generation", Some(count(facts.generation))),
            ("created_by", Some(text(&facts.created_by))),
            ("priority", Some(text(&facts.priority))),
            ("execution_profile", Some(text(&facts.execution_profile))),
            ("durability", Some(text(&facts.durability))),
            ("size_bytes", Some(count(facts.size_bytes))),
            ("party", text_maybe(facts.party.as_ref())),
            ("contract", text_maybe(facts.contract.as_ref())),
            ("stream", id(facts.stream)),
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
            ("held_at", Some(time(self.held_unix_nanos))),
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
            ("stream", Some(Value::Id(entry.stream.value()))),
            ("node", Some(text(&entry.node))),
            ("receive_location", Some(text(&entry.location))),
            ("queued_at", Some(time(nanos(entry.received_unix_nanos)))),
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
        vec![
            ("occurred_at", Some(time(facts.occurred_unix_nanos))),
            ("kept_at", Some(time(facts.kept_unix_nanos))),
            ("action", Some(text(&facts.action))),
            ("phase", Some(text(&facts.phase))),
            ("severity", Some(text(&facts.severity))),
            ("failed", Some(Value::Flag(facts.failed))),
            ("message_text", text_maybe(facts.text.as_ref())),
            ("program", Some(text(&facts.program))),
            ("host", Some(text(&facts.host))),
            ("process", Some(count(facts.process))),
            ("location", text_maybe(facts.location.as_ref())),
            ("hidden", Some(Value::Flag(facts.hidden))),
            ("execution", id(facts.execution)),
            ("journey", id(facts.journey)),
            ("message", id(facts.message)),
            ("artifact_kind", text_maybe(facts.artifact_kind.as_ref())),
            ("artifact_name", text_maybe(facts.artifact_name.as_ref())),
            (
                "artifact_version",
                text_maybe(facts.artifact_version.as_ref()),
            ),
            ("node", text_maybe(facts.node.as_ref())),
            ("cluster", text_maybe(facts.cluster.as_ref())),
        ]
    }
}

impl Columned for KeptStream {
    /// The audit record that carries it: what the index by its Stream finds.
    fn id(&self) -> u128 {
        self.audit.value()
    }

    /// Nothing: its times are the Stream's own record's.
    fn stamp(&mut self, _: u64, _: Option<&Self>) {}

    fn row(&self) -> Row {
        let stream = &self.stream;
        vec![
            ("stream", Some(Value::Id(stream.stream.value()))),
            ("length", Some(count(stream.length))),
            ("chunks", Some(count(stream.chunks))),
            ("digest", Some(Value::Bytes(stream.digest.to_vec()))),
            ("written_at", Some(time(stream.written_unix_nanos))),
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
            ("kind", Some(text(self.kind.word()))),
            ("updated_at", Some(time(self.updated_unix_nanos))),
        ]
    }
}
