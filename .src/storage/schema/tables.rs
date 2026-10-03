//! Every table of Xmip Storage's two databases, in the order the scripts
//! make them ([`super::scripts`]).

use super::{Column, Database, Kind, Table};

const fn column(name: &'static str, kind: Kind) -> Column {
    Column { name, kind }
}

/// Every table, in the order the scripts make them.
pub const TABLES: [Table; 12] = [
    Table {
        database: Database::Runtime,
        name: "stream_chunk",
        keeps: "every Stream, in chunks",
        columns: &[
            column("stream", Kind::Identifier),
            column("chunk", Kind::Count),
            column("last", Kind::Flag),
            column("bytes", Kind::Bytes),
        ],
        key: &["stream", "chunk"],
        unique: &[],
    },
    Table {
        database: Database::Runtime,
        name: "message",
        keeps: "every Message, as a step wrote it",
        columns: &[
            column("message", Kind::Identifier),
            column("body", Kind::Bytes),
        ],
        key: &["message"],
        unique: &[],
    },
    Table {
        database: Database::Runtime,
        name: "journey",
        keeps: "every Journey, as a step wrote it",
        columns: &[
            column("journey", Kind::Identifier),
            column("body", Kind::Bytes),
        ],
        key: &["journey"],
        unique: &[],
    },
    Table {
        database: Database::Runtime,
        name: "held",
        keeps: "the Journeys a paused Subscription holds, at their place in its queue, \
                each once",
        columns: &[
            column("queue", Kind::Identifier),
            column("sequence", Kind::Number),
            column("journey", Kind::Identifier),
            column("body", Kind::Bytes),
        ],
        key: &["queue", "sequence"],
        unique: &["queue", "journey"],
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
    },
    Table {
        database: Database::Runtime,
        name: "dead_message",
        keeps: "each node's Dead Message Queue: every Message nothing matched, at its place, \
                with its receive context, what its gates concluded, its promoted properties \
                and each Subscription's reason for declining, each once",
        columns: &[
            column("queue", Kind::Identifier),
            column("sequence", Kind::Number),
            column("message", Kind::Identifier),
            column("body", Kind::Bytes),
        ],
        key: &["queue", "sequence"],
        unique: &["queue", "message"],
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
    },
    Table {
        database: Database::Administration,
        name: "audit",
        keeps: "audit records kept over time, each once",
        columns: &[column("id", Kind::Identifier), column("body", Kind::Bytes)],
        key: &["id"],
        unique: &[],
    },
    Table {
        database: Database::Administration,
        name: "administration",
        keeps: "registration, membership, Modules, Handlers, deployment and operator state",
        columns: &[
            column("kind", Kind::Text),
            column("id", Kind::Identifier),
            column("body", Kind::Bytes),
        ],
        key: &["kind", "id"],
        unique: &[],
    },
];
