//! Every table of Xmip Storage's two databases, in the order the scripts
//! make them ([`super::scripts`]).

use super::searchable::{
    ADMINISTRATION, ADMINISTRATION_INDEXES, AUDIT, AUDIT_INDEXES, DEAD_MESSAGE,
    DEAD_MESSAGE_INDEXES, HELD, HELD_INDEXES, JOURNEY, JOURNEY_INDEXES, MESSAGE, MESSAGE_INDEXES,
    STREAM,
};
use super::{Column, Database, Kind, Table};

const fn column(name: &'static str, kind: Kind) -> Column {
    Column {
        name,
        kind,
        null: false,
    }
}

/// Every table, in the order the scripts make them.
pub const TABLES: [Table; 15] = [
    Table {
        database: Database::Runtime,
        name: "stream",
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
        keeps: "every Message, as a step wrote it, every single value of it in a column of its own",
        columns: MESSAGE,
        key: &["message"],
        unique: &[],
        indexes: MESSAGE_INDEXES,
    },
    Table {
        database: Database::Runtime,
        name: "journey",
        keeps: "every Journey, as a step wrote it, every single value of it in a column of its own",
        columns: JOURNEY,
        key: &["journey"],
        unique: &[],
        indexes: JOURNEY_INDEXES,
    },
    Table {
        database: Database::Runtime,
        name: "held",
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
        name: "dead_message",
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
    Table {
        database: Database::Administration,
        name: "audit",
        keeps: "audit records kept over time, each once, every single value of each in a \
                column of its own",
        columns: AUDIT,
        key: &["id"],
        unique: &[],
        indexes: AUDIT_INDEXES,
    },
    Table {
        database: Database::Administration,
        name: "audit_stream_chunk",
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
    Table {
        database: Database::Administration,
        name: "administration",
        keeps: "registration, membership, Modules, Handlers, deployment and operator state, \
                and when each was last written",
        columns: ADMINISTRATION,
        key: &["kind", "id"],
        unique: &[],
        indexes: ADMINISTRATION_INDEXES,
    },
];
