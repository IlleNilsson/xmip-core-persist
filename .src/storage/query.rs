//! What Xmip Storage is asked to find (proposed 2026-10-09): one typed
//! question per searchable table, each over one of its indexes
//! (`super::schema::searchable`), answered with the identifiers of the
//! records found, which the caller reads as it reads any record.
//!
//! A name or an identifier is asked as the caller has it and found by
//! equality, as its column keeps it, in the clear; never by a pattern. A
//! time is a span, both ends included.

use codec::cursor::Cursor;

use super::record::{
    AdministrationKind, Form, malformed, read_byte, read_text, read_u32, read_u64, read_u128,
    write_byte, write_text, write_u32, write_u64, write_u128,
};
use super::row::Value;
use super::schema::Database;
use crate::PersistError;

/// From one time to another, both included, in nanoseconds since the Unix
/// epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Span {
    pub from_unix_nanos: u64,
    pub to_unix_nanos: u64,
}

impl Span {
    /// All of time.
    pub const ALL: Self = Self {
        from_unix_nanos: 0,
        to_unix_nanos: u64::MAX,
    };
}

/// What is asked, of which table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Ask {
    /// Journeys in a state, by when they were last written.
    JourneysInState { state: String, updated: Span },
    /// Journeys bound for a Send Port in a state.
    JourneysAtSendPort { send_port: String, state: String },
    /// Journeys a Journey caused.
    JourneysAfter { journey: u128 },
    /// Journeys holding a Message last.
    JourneysHolding { message: u128 },
    /// Messages by when they were written.
    MessagesCreated { created: Span },
    /// Messages from a Party, by when they were written.
    MessagesFromParty { party: String, created: Span },
    /// Messages read by a contract, by when they were written.
    MessagesOfContract { contract: String, created: Span },
    /// Messages made from a Message.
    MessagesAfter { message: u128 },
    /// What a queue holds, by when it was held.
    Held { queue: u128, held: Span },
    /// What a Dead Message Queue keeps, by when it was queued.
    DeadMessages { queue: u128, queued: Span },
    /// Audit records by when they happened.
    AuditOccurred { occurred: Span },
    /// Audit records about a Journey.
    AuditOfJourney { journey: u128 },
    /// Audit records about a Message.
    AuditOfMessage { message: u128 },
    /// Audit records about an artifact, by when they happened.
    AuditOfArtifact { artifact: String, occurred: Span },
    /// Failures, by when they happened.
    AuditFailed { occurred: Span },
    /// Audit records carrying a Stream (ADR-0070).
    AuditOfStream { stream: u128 },
    /// A writer's audit chain in its order, from the number `from` on
    /// (ADR-0070 clause 5).
    AuditChain { writer: String, from: u64 },
    /// Administration records of a kind, by when they were last written.
    AdministrationUpdated {
        kind: AdministrationKind,
        updated: Span,
    },
}

/// A question, and how many of its answers, in what order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Query {
    pub ask: Ask,
    pub most: u32,
    /// The index's order reversed: the newest first, where it ends in a
    /// time.
    pub newest_first: bool,
}

/// How a question is answered: which database, table and index, the
/// values its first columns equal, and the span of the next.
pub(crate) struct Plan {
    pub(crate) database: Database,
    pub(crate) table: &'static str,
    pub(crate) index: &'static str,
    pub(crate) equal: Vec<Value>,
    pub(crate) range: Option<(u64, u64)>,
}

impl Ask {
    /// How it is answered.
    pub(crate) fn plan(&self) -> Plan {
        let (database, table, index) = self.index();
        let (equal, span) = self.values();
        Plan {
            database,
            table,
            index,
            equal,
            range: span.map(|span| (span.from_unix_nanos, span.to_unix_nanos)),
        }
    }

    /// The index it reads: its database, its table and its name.
    const fn index(&self) -> (Database, &'static str, &'static str) {
        use Database::{Administration, Audit, Runtime};
        match self {
            Self::JourneysInState { .. } => (Runtime, "journey", "journey_state"),
            Self::JourneysAtSendPort { .. } => (Runtime, "journey", "journey_send_port"),
            Self::JourneysAfter { .. } => (Runtime, "journey", "journey_previous"),
            Self::JourneysHolding { .. } => (Runtime, "journey", "journey_message"),
            Self::MessagesCreated { .. } => (Runtime, "message", "message_created"),
            Self::MessagesFromParty { .. } => (Runtime, "message", "message_party"),
            Self::MessagesOfContract { .. } => (Runtime, "message", "message_contract"),
            Self::MessagesAfter { .. } => (Runtime, "message", "message_previous"),
            Self::Held { .. } => (Runtime, "held", "held_time"),
            Self::DeadMessages { .. } => (Runtime, "dead_message", "dead_message_time"),
            Self::AuditOccurred { .. } => (Audit, "audit", "audit_occurred"),
            Self::AuditOfJourney { .. } => (Audit, "audit", "audit_journey"),
            Self::AuditOfMessage { .. } => (Audit, "audit", "audit_message"),
            Self::AuditOfArtifact { .. } => (Audit, "audit", "audit_artifact"),
            Self::AuditFailed { .. } => (Audit, "audit", "audit_failed"),
            Self::AuditOfStream { .. } => (Audit, "audit_stream", "audit_stream_stream"),
            Self::AuditChain { .. } => (Audit, "audit", "audit_chain"),
            Self::AdministrationUpdated { .. } => {
                (Administration, "administration", "administration_updated")
            }
        }
    }

    /// The values its index's first columns equal, and the span of the
    /// next where it asks one.
    fn values(&self) -> (Vec<Value>, Option<Span>) {
        let name = |text: &String| Value::Text(text.clone());
        match self {
            Self::JourneysInState { state, updated } => (vec![name(state)], Some(*updated)),
            Self::JourneysAtSendPort { send_port, state } => {
                (vec![name(send_port), name(state)], None)
            }
            Self::JourneysAfter { journey: id }
            | Self::JourneysHolding { message: id }
            | Self::MessagesAfter { message: id }
            | Self::AuditOfJourney { journey: id }
            | Self::AuditOfMessage { message: id }
            | Self::AuditOfStream { stream: id } => (vec![Value::Id(*id)], None),
            Self::MessagesCreated { created: span } | Self::AuditOccurred { occurred: span } => {
                (Vec::new(), Some(*span))
            }
            Self::MessagesFromParty {
                party: text,
                created: span,
            }
            | Self::MessagesOfContract {
                contract: text,
                created: span,
            }
            | Self::AuditOfArtifact {
                artifact: text,
                occurred: span,
            } => (vec![name(text)], Some(*span)),
            Self::Held { queue, held: span }
            | Self::DeadMessages {
                queue,
                queued: span,
            } => (vec![Value::Id(*queue)], Some(*span)),
            Self::AuditFailed { occurred } => (vec![Value::Flag(true)], Some(*occurred)),
            // A span of numbers: the index's next column is the number.
            Self::AuditChain { writer, from } => {
                let numbers = Span {
                    from_unix_nanos: *from,
                    to_unix_nanos: u64::MAX,
                };
                (vec![name(writer)], Some(numbers))
            }
            Self::AdministrationUpdated { kind, updated } => {
                (vec![Value::Text(kind.word().to_string())], Some(*updated))
            }
        }
    }
}

impl Form for Span {
    fn write(&self, out: &mut Vec<u8>) {
        write_u64(out, self.from_unix_nanos);
        write_u64(out, self.to_unix_nanos);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            from_unix_nanos: read_u64(cursor)?,
            to_unix_nanos: read_u64(cursor)?,
        })
    }
}

impl Form for Ask {
    fn write(&self, out: &mut Vec<u8>) {
        match self {
            Self::JourneysInState { state, updated } => {
                write_byte(out, 1);
                write_text(out, state);
                updated.write(out);
            }
            Self::JourneysAtSendPort { send_port, state } => {
                write_byte(out, 2);
                write_text(out, send_port);
                write_text(out, state);
            }
            Self::JourneysAfter { journey: id }
            | Self::JourneysHolding { message: id }
            | Self::MessagesAfter { message: id }
            | Self::AuditOfJourney { journey: id }
            | Self::AuditOfMessage { message: id }
            | Self::AuditOfStream { stream: id } => {
                write_byte(out, self.number());
                write_u128(out, *id);
            }
            Self::MessagesCreated { created: span }
            | Self::AuditOccurred { occurred: span }
            | Self::AuditFailed { occurred: span } => {
                write_byte(out, self.number());
                span.write(out);
            }
            Self::MessagesFromParty {
                party: text,
                created: span,
            }
            | Self::MessagesOfContract {
                contract: text,
                created: span,
            }
            | Self::AuditOfArtifact {
                artifact: text,
                occurred: span,
            } => {
                write_byte(out, self.number());
                write_text(out, text);
                span.write(out);
            }
            Self::Held { queue, held: span }
            | Self::DeadMessages {
                queue,
                queued: span,
            } => {
                write_byte(out, self.number());
                write_u128(out, *queue);
                span.write(out);
            }
            Self::AdministrationUpdated { kind, updated } => {
                write_byte(out, 16);
                kind.write(out);
                updated.write(out);
            }
            Self::AuditChain { writer, from } => {
                write_byte(out, self.number());
                write_text(out, writer);
                write_u64(out, *from);
            }
        }
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(match read_byte(cursor)? {
            1 => Self::JourneysInState {
                state: read_text(cursor)?,
                updated: Span::read(cursor)?,
            },
            2 => Self::JourneysAtSendPort {
                send_port: read_text(cursor)?,
                state: read_text(cursor)?,
            },
            3 => Self::JourneysAfter {
                journey: read_u128(cursor)?,
            },
            4 => Self::JourneysHolding {
                message: read_u128(cursor)?,
            },
            5 => Self::MessagesCreated {
                created: Span::read(cursor)?,
            },
            6 => Self::MessagesFromParty {
                party: read_text(cursor)?,
                created: Span::read(cursor)?,
            },
            7 => Self::MessagesOfContract {
                contract: read_text(cursor)?,
                created: Span::read(cursor)?,
            },
            8 => Self::MessagesAfter {
                message: read_u128(cursor)?,
            },
            9 => Self::Held {
                queue: read_u128(cursor)?,
                held: Span::read(cursor)?,
            },
            10 => Self::DeadMessages {
                queue: read_u128(cursor)?,
                queued: Span::read(cursor)?,
            },
            11 => Self::AuditOccurred {
                occurred: Span::read(cursor)?,
            },
            12 => Self::AuditOfJourney {
                journey: read_u128(cursor)?,
            },
            13 => Self::AuditOfMessage {
                message: read_u128(cursor)?,
            },
            14 => Self::AuditOfArtifact {
                artifact: read_text(cursor)?,
                occurred: Span::read(cursor)?,
            },
            15 => Self::AuditFailed {
                occurred: Span::read(cursor)?,
            },
            16 => Self::AdministrationUpdated {
                kind: AdministrationKind::read(cursor)?,
                updated: Span::read(cursor)?,
            },
            17 => Self::AuditOfStream {
                stream: read_u128(cursor)?,
            },
            18 => Self::AuditChain {
                writer: read_text(cursor)?,
                from: read_u64(cursor)?,
            },
            other => return Err(malformed(format!("no question is numbered {other}"))),
        })
    }
}

impl Ask {
    /// Its number on the wire.
    const fn number(&self) -> u8 {
        match self {
            Self::JourneysInState { .. } => 1,
            Self::JourneysAtSendPort { .. } => 2,
            Self::JourneysAfter { .. } => 3,
            Self::JourneysHolding { .. } => 4,
            Self::MessagesCreated { .. } => 5,
            Self::MessagesFromParty { .. } => 6,
            Self::MessagesOfContract { .. } => 7,
            Self::MessagesAfter { .. } => 8,
            Self::Held { .. } => 9,
            Self::DeadMessages { .. } => 10,
            Self::AuditOccurred { .. } => 11,
            Self::AuditOfJourney { .. } => 12,
            Self::AuditOfMessage { .. } => 13,
            Self::AuditOfArtifact { .. } => 14,
            Self::AuditFailed { .. } => 15,
            Self::AdministrationUpdated { .. } => 16,
            Self::AuditOfStream { .. } => 17,
            Self::AuditChain { .. } => 18,
        }
    }
}

impl Form for Query {
    fn write(&self, out: &mut Vec<u8>) {
        self.ask.write(out);
        write_u32(out, self.most);
        write_byte(out, u8::from(self.newest_first));
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            ask: Ask::read(cursor)?,
            most: read_u32(cursor)?,
            newest_first: read_byte(cursor)? != 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::columns::table;

    #[test]
    fn every_question_names_an_index_of_its_table_and_comes_back_from_its_bytes() {
        let span = Span::ALL;
        let asks = [
            Ask::JourneysInState {
                state: "Failed".to_string(),
                updated: span,
            },
            Ask::JourneysAtSendPort {
                send_port: "Billing".to_string(),
                state: "Active".to_string(),
            },
            Ask::JourneysAfter { journey: 1 },
            Ask::JourneysHolding { message: 2 },
            Ask::MessagesCreated { created: span },
            Ask::MessagesFromParty {
                party: "partner-x".to_string(),
                created: span,
            },
            Ask::MessagesOfContract {
                contract: "Order".to_string(),
                created: span,
            },
            Ask::MessagesAfter { message: 3 },
            Ask::Held {
                queue: 4,
                held: span,
            },
            Ask::DeadMessages {
                queue: 5,
                queued: span,
            },
            Ask::AuditOccurred { occurred: span },
            Ask::AuditOfJourney { journey: 6 },
            Ask::AuditOfMessage { message: 7 },
            Ask::AuditOfArtifact {
                artifact: "OrdersIn".to_string(),
                occurred: span,
            },
            Ask::AuditFailed { occurred: span },
            Ask::AuditOfStream { stream: 8 },
            Ask::AuditChain {
                writer: "xmip-cli".to_string(),
                from: 1,
            },
            Ask::AdministrationUpdated {
                kind: AdministrationKind::Operator,
                updated: span,
            },
        ];
        for ask in asks {
            let plan = ask.plan();
            let table = table(plan.database, plan.table).expect("a table");
            let index = table.indexes.iter().find(|i| i.name == plan.index);
            let index = index.expect("an index of it");
            let ranged = usize::from(plan.range.is_some());
            assert!(plan.equal.len() + ranged <= index.columns.len(), "{ask:?}");
            let query = Query {
                ask,
                most: 9,
                newest_first: true,
            };
            assert_eq!(Query::from_bytes(&query.bytes()).expect("read"), query);
        }
    }
}
