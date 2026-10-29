//! Every table of Xmip Storage's three databases, in the order the scripts
//! make them ([`super::scripts`]).

use super::searchable::{
    ADMINISTRATION, ADMINISTRATION_INDEXES, AUDIT, AUDIT_INDEXES, AUDIT_STREAM,
    AUDIT_STREAM_INDEXES, DEAD_MESSAGE, DEAD_MESSAGE_INDEXES, FAILED_JOURNEY,
    FAILED_JOURNEY_INDEXES, HELD, HELD_INDEXES, JOURNEY, JOURNEY_INDEXES, MESSAGE, MESSAGE_INDEXES,
    SECRET, SECRET_INDEXES, STREAM,
};
use super::version::VERSIONS;
use super::{Column, Database, Kind, Table};

const fn column(name: &'static str, kind: Kind) -> Column {
    Column {
        name,
        kind,
        null: false,
        since: 1,
    }
}

/// A schema version, and when a database recorded it.
const VERSION_COLUMNS: &[Column] = &[
    column("version", Kind::Count),
    column("recorded_at", Kind::Time),
];

/// The table each database records the schema versions it held in, with
/// when ([`super::version`]).
const fn versions(database: Database) -> Table {
    Table {
        database,
        name: VERSIONS,
        since: 1,
        keeps: "each schema version the database has held, the one it was made at and each \
                an upgrade reached, with when; the version it holds is the highest",
        columns: VERSION_COLUMNS,
        key: &["version"],
        unique: &[],
        indexes: &[],
    }
}

/// Every table, in the order the scripts make them.
pub const TABLES: [Table; 25] = [
    versions(Database::Runtime),
    Table {
        database: Database::Runtime,
        name: "stream",
        since: 1,
        keeps: "every Stream, once, as written and never changed: its length, its chunks and \
                the SHA-256 digest of its bytes; a chunk and every Message referring to it \
                refer to it by its identifier",
        columns: STREAM,
        key: &["stream"],
        unique: &[],
        indexes: &[],
    },
    Table {
        database: Database::Runtime,
        name: "stream_chunk",
        since: 1,
        keeps: "every Stream's chunks, by its identifier and their number; a Stream ends \
                where it has no further chunk",
        columns: &[
            column("stream", Kind::Identifier),
            column("chunk", Kind::Count),
            column("bytes", Kind::Bytes),
        ],
        key: &["stream", "chunk"],
        unique: &[],
        indexes: &[],
    },
    Table {
        database: Database::Runtime,
        name: "message",
        since: 1,
        keeps: "every Message, as a step wrote it, every single value of it in a column of its own",
        columns: MESSAGE,
        key: &["message"],
        unique: &[],
        indexes: MESSAGE_INDEXES,
    },
    Table {
        database: Database::Runtime,
        name: "journey",
        since: 1,
        keeps: "every Journey, as a step wrote it, every single value of it in a column of its own",
        columns: JOURNEY,
        key: &["journey"],
        unique: &[],
        indexes: JOURNEY_INDEXES,
    },
    Table {
        database: Database::Runtime,
        name: "held",
        since: 1,
        keeps: "the Journeys a paused Subscription holds, at their place in its queue, \
                each once",
        columns: HELD,
        key: &["queue", "sequence"],
        unique: &["queue", "journey"],
        indexes: HELD_INDEXES,
    },
    Table {
        database: Database::Runtime,
        name: "held_places",
        since: 1,
        keeps: "each queue's first place, its next and how many it holds",
        columns: &[
            column("queue", Kind::Identifier),
            column("first_place", Kind::Number),
            column("next_place", Kind::Number),
            column("held", Kind::Number),
        ],
        key: &["queue"],
        unique: &[],
        indexes: &[],
    },
    Table {
        database: Database::Runtime,
        name: "held_due_places",
        since: 1,
        keeps: "each queue's places in the order its Journeys are due: the place no Journey \
                due since it was held is before, the due time and place no retry is before, \
                and how many keep a retry's due time",
        columns: &[
            column("queue", Kind::Identifier),
            column("first_due_place", Kind::Number),
            column("first_retry_at", Kind::Time),
            column("first_retry_place", Kind::Number),
            column("retrying", Kind::Number),
        ],
        key: &["queue"],
        unique: &[],
        indexes: &[],
    },
    Table {
        database: Database::Runtime,
        name: "failed_journey",
        since: 1,
        keeps: "the Journeys that failed waiting in each Send Port's queue for an operator, \
                in the order they failed, each once, with its place in the queue and why, \
                written with the failure and taken out with the operator's act",
        columns: FAILED_JOURNEY,
        key: &["queue", "sequence"],
        unique: &["queue", "journey"],
        indexes: FAILED_JOURNEY_INDEXES,
    },
    Table {
        database: Database::Runtime,
        name: "failed_journey_places",
        since: 1,
        keeps: "each queue's index of failed Journeys: its first place, its next and how \
                many failed Journeys wait in the queue",
        columns: &[
            column("queue", Kind::Identifier),
            column("first_place", Kind::Number),
            column("next_place", Kind::Number),
            column("failed", Kind::Number),
        ],
        key: &["queue"],
        unique: &[],
        indexes: &[],
    },
    Table {
        database: Database::Runtime,
        name: "dead_message",
        since: 1,
        keeps: "each node's Dead Message Queue: every Message nothing matched, at its place, \
                with its receive context, what its gates concluded, its promoted properties \
                and each Subscription's reason for declining, each once",
        columns: DEAD_MESSAGE,
        key: &["queue", "sequence"],
        unique: &["queue", "message"],
        indexes: DEAD_MESSAGE_INDEXES,
    },
    Table {
        database: Database::Runtime,
        name: "dead_message_places",
        since: 1,
        keeps: "each Dead Message Queue's first place, its next and how many it holds",
        columns: &[
            column("queue", Kind::Identifier),
            column("first_place", Kind::Number),
            column("next_place", Kind::Number),
            column("held", Kind::Number),
        ],
        key: &["queue"],
        unique: &[],
        indexes: &[],
    },
    Table {
        database: Database::Runtime,
        name: "dead_message_replayed",
        since: 1,
        keeps: "each Message replayed from a Dead Message Queue, so a Replay asked again \
                writes nothing twice",
        columns: &[
            column("queue", Kind::Identifier),
            column("message", Kind::Identifier),
        ],
        key: &["queue", "message"],
        unique: &[],
        indexes: &[],
    },
    Table {
        database: Database::Runtime,
        name: "publication",
        since: 1,
        keeps: "each Message published, with the digest of its Publication, so a Publication \
                asked again writes nothing and resets no Journey moved on since",
        columns: &[
            column("message", Kind::Identifier),
            column("digest", Kind::Bytes),
        ],
        key: &["message"],
        unique: &[],
        indexes: &[],
    },
    Table {
        database: Database::Runtime,
        name: "claim",
        since: 1,
        keeps: "the claim on each Journey: its holder, its token, when it lapses, and \
                whether it was given back or its step handed on",
        columns: &[
            column("journey", Kind::Identifier),
            column("holder", Kind::Text),
            column("token", Kind::Identifier),
            column("until_unix_nanos", Kind::Number),
            column("released", Kind::Flag),
            column("handed_on", Kind::Flag),
        ],
        key: &["journey"],
        unique: &[],
        indexes: &[],
    },
    Table {
        database: Database::Runtime,
        name: "audit",
        since: 1,
        keeps: "audit records as first written, until the audit keeper moves them",
        columns: &[
            column("sequence", Kind::Sequence),
            column("id", Kind::Identifier),
            column("body", Kind::Bytes),
        ],
        key: &["sequence"],
        unique: &[],
        indexes: &[],
    },
    versions(Database::Audit),
    Table {
        database: Database::Audit,
        name: "audit",
        since: 1,
        keeps: "audit records kept over time, each once, every single value of each in a \
                column of its own, each in its writer's audit chain: its number there, the \
                SHA-256 digest of the record before it and its own",
        columns: AUDIT,
        key: &["id"],
        unique: &[],
        indexes: AUDIT_INDEXES,
    },
    Table {
        database: Database::Audit,
        name: "audit_chain_head",
        since: 1,
        keeps: "where each writer's audit chain stands: the number of its last kept record \
                and that record's SHA-256 digest, written with it by the audit keeper",
        columns: &[
            column("writer", Kind::Text),
            column("position", Kind::Number),
            column("digest", Kind::Bytes),
        ],
        key: &["writer"],
        unique: &[],
        indexes: &[],
    },
    Table {
        database: Database::Audit,
        name: "audit_body_chunk",
        since: 1,
        keeps: "the body of each kept audit record, its record and the Message it carries, \
                in chunks by the record's identifier and their number, as a Stream is kept, \
                written with it by the audit keeper",
        columns: &[
            column("audit", Kind::Identifier),
            column("chunk", Kind::Count),
            column("bytes", Kind::Bytes),
        ],
        key: &["audit", "chunk"],
        unique: &[],
        indexes: &[],
    },
    Table {
        database: Database::Audit,
        name: "audit_stream",
        since: 1,
        keeps: "each Stream a kept audit record carries, once: its length, its chunks, the \
                SHA-256 digest of its bytes and when the Ledger wrote it, written with the \
                record by the audit keeper",
        columns: AUDIT_STREAM,
        key: &["audit", "stream"],
        unique: &[],
        indexes: AUDIT_STREAM_INDEXES,
    },
    Table {
        database: Database::Audit,
        name: "audit_stream_chunk",
        since: 1,
        keeps: "the bytes of each Stream a kept audit record carries, in chunks by the \
                record's identifier, the Stream's and their number, kept with it by the \
                audit keeper; a Stream two Sections share once",
        columns: &[
            column("audit", Kind::Identifier),
            column("stream", Kind::Identifier),
            column("chunk", Kind::Count),
            column("bytes", Kind::Bytes),
        ],
        key: &["audit", "stream", "chunk"],
        unique: &[],
        indexes: &[],
    },
    versions(Database::Administration),
    Table {
        database: Database::Administration,
        name: "administration",
        since: 1,
        keeps: "registration, membership, Modules, Handlers, deployment and operator state, \
                and when each was last written",
        columns: ADMINISTRATION,
        key: &["kind", "id"],
        unique: &[],
        indexes: ADMINISTRATION_INDEXES,
    },
    Table {
        database: Database::Administration,
        name: "secret",
        since: 1,
        keeps: "each secret a Location names, by its name: its kind, its value sealed by Xmip \
                under the administration database's data key, and when and by whom it was \
                last set; its value is never read back out",
        columns: SECRET,
        key: &["secret"],
        unique: &["name"],
        indexes: SECRET_INDEXES,
    },
];
