//! The columns and indexes of the six tables a search reads (proposed
//! 2026-10-09; the owner, the same day: *Store it in the clear*, *All
//! columns shall be laid out*): every single value of each table's record in
//! a column of its own, in the clear, beside its sealed body, filled from
//! the record it writes (`super::super::facts`), and the indexes a query of
//! it uses (`super::super::query`).
//!
//! **What a column holds.** An identifier as the keys are kept, a name as
//! text, an enumeration — a state, a phase, how a Message was made — as its
//! word, as the enum names it, and a time, a count or a flag as it is. A
//! list — a Journey's entries, a Message's Sections and context, an audit
//! record's properties — is not split into tables of its own; it stays in
//! the body.
//! A value a record may lack is a column that may hold nothing, and an
//! index holds no row that lacks one of its columns' values.
//!
//! **Numbered once.** An index's number names it to the embedded engines,
//! which have no columns and keep each index as entries of its own
//! (`super::super::columns`); a number is never reused.

use super::{Column, Index, Kind};

const fn column(name: &'static str, kind: Kind) -> Column {
    Column {
        name,
        kind,
        null: false,
    }
}

/// A column a record may hold nothing in.
const fn maybe(name: &'static str, kind: Kind) -> Column {
    Column {
        name,
        kind,
        null: true,
    }
}

const fn index(number: u8, name: &'static str, columns: &'static [&'static str]) -> Index {
    Index {
        number,
        name,
        columns,
        only: None,
    }
}

pub(super) const STREAM: &[Column] = &[
    column("stream", Kind::Identifier),
    column("length", Kind::Number),
    column("chunks", Kind::Count),
    column("digest", Kind::Bytes),
    column("written_at", Kind::Time),
];

pub(super) const JOURNEY: &[Column] = &[
    column("journey", Kind::Identifier),
    column("body", Kind::Bytes),
    column("created_at", Kind::Time),
    column("updated_at", Kind::Time),
    column("state", Kind::Word),
    maybe("previous_journey", Kind::Identifier),
    maybe("subscription", Kind::Text),
    maybe("cause_work_process", Kind::Text),
    column("depth", Kind::Count),
    maybe("work_process", Kind::Text),
    maybe("send_port", Kind::Text),
    column("send_location", Kind::Count),
    column("attempts", Kind::Count),
    maybe("message", Kind::Identifier),
];

pub(super) const JOURNEY_INDEXES: &[Index] = &[
    index(1, "journey_state", &["state", "updated_at"]),
    index(2, "journey_send_port", &["send_port", "state"]),
    index(3, "journey_previous", &["previous_journey"]),
    index(4, "journey_message", &["message"]),
];

pub(super) const MESSAGE: &[Column] = &[
    column("message", Kind::Identifier),
    column("body", Kind::Bytes),
    column("created_at", Kind::Time),
    maybe("previous_message", Kind::Identifier),
    column("generation", Kind::Count),
    column("created_by", Kind::Word),
    column("priority", Kind::Word),
    column("execution_profile", Kind::Word),
    column("durability", Kind::Word),
    column("size_bytes", Kind::Number),
    maybe("party", Kind::Text),
    maybe("contract", Kind::Text),
    maybe("stream", Kind::Identifier),
];

pub(super) const MESSAGE_INDEXES: &[Index] = &[
    index(5, "message_created", &["created_at"]),
    index(6, "message_party", &["party", "created_at"]),
    index(7, "message_contract", &["contract", "created_at"]),
    index(8, "message_previous", &["previous_message"]),
    index(17, "message_stream", &["stream"]),
];

pub(super) const HELD: &[Column] = &[
    column("queue", Kind::Identifier),
    column("sequence", Kind::Number),
    column("journey", Kind::Identifier),
    column("body", Kind::Bytes),
    column("held_at", Kind::Time),
];

pub(super) const HELD_INDEXES: &[Index] = &[index(9, "held_time", &["queue", "held_at"])];

pub(super) const DEAD_MESSAGE: &[Column] = &[
    column("queue", Kind::Identifier),
    column("sequence", Kind::Number),
    column("message", Kind::Identifier),
    column("body", Kind::Bytes),
    column("stream", Kind::Identifier),
    column("node", Kind::Text),
    column("receive_location", Kind::Text),
    column("queued_at", Kind::Time),
];

pub(super) const DEAD_MESSAGE_INDEXES: &[Index] =
    &[index(10, "dead_message_time", &["queue", "queued_at"])];

pub(super) const AUDIT: &[Column] = &[
    column("id", Kind::Identifier),
    column("occurred_at", Kind::Time),
    column("kept_at", Kind::Time),
    column("action", Kind::Text),
    column("phase", Kind::Word),
    column("severity", Kind::Word),
    column("failed", Kind::Flag),
    maybe("message_text", Kind::LongText),
    column("program", Kind::Text),
    column("host", Kind::Text),
    column("process", Kind::Count),
    maybe("location", Kind::Text),
    column("hidden", Kind::Flag),
    maybe("execution", Kind::Identifier),
    maybe("journey", Kind::Identifier),
    maybe("message", Kind::Identifier),
    maybe("artifact_kind", Kind::Word),
    maybe("artifact_name", Kind::Text),
    maybe("artifact_version", Kind::Text),
    maybe("node", Kind::Text),
    maybe("cluster", Kind::Text),
    column("writer", Kind::Text),
    column("position", Kind::Number),
    column("previous_digest", Kind::Bytes),
    column("digest", Kind::Bytes),
    column("body_length", Kind::Number),
    column("body_chunks", Kind::Count),
    column("body_digest", Kind::Bytes),
];

/// By when; by the Journey, the Message, the artifact; the failures; and
/// each writer's audit chain in its order, for the walk that verifies it
/// (ADR-0070 clause 5).
pub(super) const AUDIT_INDEXES: &[Index] = &[
    index(11, "audit_occurred", &["occurred_at"]),
    index(12, "audit_journey", &["journey"]),
    index(13, "audit_message", &["message"]),
    index(14, "audit_artifact", &["artifact_name", "occurred_at"]),
    Index {
        only: Some("failed"),
        ..index(15, "audit_failed", &["failed", "occurred_at"])
    },
    index(19, "audit_chain", &["writer", "position"]),
];

pub(super) const AUDIT_STREAM: &[Column] = &[
    column("audit", Kind::Identifier),
    column("stream", Kind::Identifier),
    column("length", Kind::Number),
    column("chunks", Kind::Count),
    column("digest", Kind::Bytes),
    column("written_at", Kind::Time),
];

/// By the Stream: the audit records that carry it. By the record: its key,
/// the record's identifier first, on a server; the embedded engines read a
/// row by its key, the Streams a record carries being in the record.
pub(super) const AUDIT_STREAM_INDEXES: &[Index] = &[index(18, "audit_stream_stream", &["stream"])];

pub(super) const ADMINISTRATION: &[Column] = &[
    column("kind", Kind::Word),
    column("id", Kind::Identifier),
    column("body", Kind::Bytes),
    column("updated_at", Kind::Time),
];

pub(super) const ADMINISTRATION_INDEXES: &[Index] =
    &[index(16, "administration_updated", &["kind", "updated_at"])];
