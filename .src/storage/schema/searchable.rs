//! The columns and indexes of the six tables a search reads (proposed
//! 2026-10-09): each table's searchable facts in columns of its own beside
//! its sealed body, filled from the record it writes
//! (`super::super::facts`), and the indexes a query of it uses
//! (`super::super::query`).
//!
//! **What a column holds.** A time, a state, a count or a flag in the
//! clear; an identifier or a name as `_ref`, sixteen bytes of HMAC-SHA-256
//! under that column's own key, found by equality and never by a pattern.
//! A fact a record may lack is a column that may hold nothing, and an index
//! holds no row that lacks one of its columns' facts.
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

pub(super) const JOURNEY: &[Column] = &[
    column("journey", Kind::Identifier),
    column("body", Kind::Bytes),
    column("created_at", Kind::Time),
    column("updated_at", Kind::Time),
    column("state", Kind::Small),
    column("attempts", Kind::Count),
    column("depth", Kind::Count),
    maybe("send_port_ref", Kind::Digest),
    maybe("work_process_ref", Kind::Digest),
    maybe("previous_journey_ref", Kind::Digest),
    maybe("message_ref", Kind::Digest),
];

pub(super) const JOURNEY_INDEXES: &[Index] = &[
    index(1, "journey_state", &["state", "updated_at"]),
    index(2, "journey_send_port", &["send_port_ref", "state"]),
    index(3, "journey_previous", &["previous_journey_ref"]),
    index(4, "journey_message", &["message_ref"]),
];

pub(super) const MESSAGE: &[Column] = &[
    column("message", Kind::Identifier),
    column("body", Kind::Bytes),
    column("created_at", Kind::Time),
    column("generation", Kind::Count),
    column("created_by", Kind::Small),
    column("size_bytes", Kind::Number),
    maybe("previous_message_ref", Kind::Digest),
    maybe("party_ref", Kind::Digest),
    maybe("contract_ref", Kind::Digest),
    maybe("stream_ref", Kind::Digest),
];

pub(super) const MESSAGE_INDEXES: &[Index] = &[
    index(5, "message_created", &["created_at"]),
    index(6, "message_party", &["party_ref", "created_at"]),
    index(7, "message_contract", &["contract_ref", "created_at"]),
    index(8, "message_previous", &["previous_message_ref"]),
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
    column("queued_at", Kind::Time),
    column("node_ref", Kind::Digest),
    column("receive_location_ref", Kind::Digest),
];

pub(super) const DEAD_MESSAGE_INDEXES: &[Index] =
    &[index(10, "dead_message_time", &["queue", "queued_at"])];

pub(super) const AUDIT: &[Column] = &[
    column("id", Kind::Identifier),
    column("body", Kind::Bytes),
    column("occurred_at", Kind::Time),
    column("kept_at", Kind::Time),
    column("action", Kind::Text),
    column("phase", Kind::Text),
    column("severity", Kind::Text),
    column("failed", Kind::Flag),
    column("program", Kind::Text),
    maybe("artifact_kind", Kind::Text),
    maybe("cluster_ref", Kind::Digest),
    maybe("node_ref", Kind::Digest),
    maybe("artifact_ref", Kind::Digest),
    maybe("journey_ref", Kind::Digest),
    maybe("message_ref", Kind::Digest),
    maybe("execution_ref", Kind::Digest),
];

pub(super) const AUDIT_INDEXES: &[Index] = &[
    index(11, "audit_occurred", &["occurred_at"]),
    index(12, "audit_journey", &["journey_ref"]),
    index(13, "audit_message", &["message_ref"]),
    index(14, "audit_artifact", &["artifact_ref", "occurred_at"]),
    Index {
        only: Some("failed"),
        ..index(15, "audit_failed", &["failed", "occurred_at"])
    },
];

pub(super) const ADMINISTRATION: &[Column] = &[
    column("kind", Kind::Text),
    column("id", Kind::Identifier),
    column("body", Kind::Bytes),
    column("updated_at", Kind::Time),
];

pub(super) const ADMINISTRATION_INDEXES: &[Index] =
    &[index(16, "administration_updated", &["kind", "updated_at"])];
