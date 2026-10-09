//! What a record is searched by: the facts of a Journey, a Message and an
//! audit record that their tables keep in columns of their own beside the
//! sealed body (proposed 2026-10-09).
//!
//! **The writer says them, Xmip Storage keeps them.** A body is the step's
//! own serialization, which Xmip Storage keeps byte for byte and does not
//! read; the facts travel beside it, typed, filled by the writer from the
//! object the body is the serialization of — the runtime's Journey, Message
//! and audit record, each in one place. The times are Xmip Storage's own:
//! when it first wrote the record and when it last did, on its clock, the
//! one every claim is decided by, so what a caller sends there is replaced
//! ([`super::columns`]).
//!
//! **In the clear or hashed.** Times, states and counts are kept as they
//! are; every identifier and every name is kept as sixteen bytes of
//! HMAC-SHA-256 under its column's own key, so it is found by equality and
//! nothing else ([`super::columns`]).

use codec::cursor::Cursor;

use super::record::{
    Form, read_byte, read_text, read_u32, read_u64, read_u128, write_byte, write_text, write_u32,
    write_u64, write_u128,
};
use crate::PersistError;

/// A Journey's facts, as its record is written.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JourneyFacts {
    /// Its state, by the number the Journey's own form gives it.
    pub state: u8,
    /// Its tries at its Send Port.
    pub attempts: u32,
    /// How many links back to a Journey nothing caused.
    pub depth: u32,
    pub send_port: Option<String>,
    pub work_process: Option<String>,
    pub previous_journey: Option<u128>,
    /// The Message it holds last.
    pub message: Option<u128>,
    /// When Xmip Storage first wrote it, in nanoseconds since the Unix
    /// epoch: Xmip Storage's to set.
    pub created_unix_nanos: u64,
    /// When Xmip Storage last wrote it: Xmip Storage's to set.
    pub updated_unix_nanos: u64,
}

/// A Message's facts, as its record is written.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MessageFacts {
    /// How many times its content or metadata changed since it was received.
    pub generation: u32,
    /// How it was made, by the number the Message's own form gives it.
    pub created_by: u8,
    /// Its Sections' length together.
    pub size_bytes: u64,
    pub previous_message: Option<u128>,
    /// The Party it came from, as its context says.
    pub party: Option<String>,
    /// The contract its first Section is read by.
    pub contract: Option<String>,
    /// The Stream its first Section is over.
    pub stream: Option<u128>,
    /// When Xmip Storage first wrote it: Xmip Storage's to set.
    pub created_unix_nanos: u64,
}

/// An audit record's facts, as its writer said it (ADR-0062).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AuditFacts {
    /// When it happened, by its writer's clock.
    pub occurred_unix_nanos: u64,
    pub action: String,
    /// Its phase and its severity, in the words the record model writes.
    pub phase: String,
    pub severity: String,
    /// A failure: always audited, always kept.
    pub failed: bool,
    /// The program that wrote it.
    pub program: String,
    /// What the artifact it is about is: a Receive Location, a Send Port.
    pub artifact_kind: Option<String>,
    pub cluster: Option<String>,
    pub node: Option<String>,
    /// The artifact it is about, by name.
    pub artifact: Option<String>,
    pub journey: Option<u128>,
    pub message: Option<u128>,
    pub execution: Option<u128>,
    /// When the audit keeper kept it in the administration database:
    /// Xmip Storage's to set.
    pub kept_unix_nanos: u64,
}

fn write_text_maybe(out: &mut Vec<u8>, text: Option<&str>) {
    write_byte(out, u8::from(text.is_some()));
    if let Some(text) = text {
        write_text(out, text);
    }
}

fn read_text_maybe(cursor: &mut Cursor<'_>) -> Result<Option<String>, PersistError> {
    match read_byte(cursor)? {
        0 => Ok(None),
        _ => read_text(cursor).map(Some),
    }
}

fn write_id_maybe(out: &mut Vec<u8>, id: Option<u128>) {
    write_byte(out, u8::from(id.is_some()));
    if let Some(id) = id {
        write_u128(out, id);
    }
}

fn read_id_maybe(cursor: &mut Cursor<'_>) -> Result<Option<u128>, PersistError> {
    match read_byte(cursor)? {
        0 => Ok(None),
        _ => read_u128(cursor).map(Some),
    }
}

impl Form for JourneyFacts {
    fn write(&self, out: &mut Vec<u8>) {
        write_byte(out, self.state);
        write_u32(out, self.attempts);
        write_u32(out, self.depth);
        write_text_maybe(out, self.send_port.as_deref());
        write_text_maybe(out, self.work_process.as_deref());
        write_id_maybe(out, self.previous_journey);
        write_id_maybe(out, self.message);
        write_u64(out, self.created_unix_nanos);
        write_u64(out, self.updated_unix_nanos);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            state: read_byte(cursor)?,
            attempts: read_u32(cursor)?,
            depth: read_u32(cursor)?,
            send_port: read_text_maybe(cursor)?,
            work_process: read_text_maybe(cursor)?,
            previous_journey: read_id_maybe(cursor)?,
            message: read_id_maybe(cursor)?,
            created_unix_nanos: read_u64(cursor)?,
            updated_unix_nanos: read_u64(cursor)?,
        })
    }
}

impl Form for MessageFacts {
    fn write(&self, out: &mut Vec<u8>) {
        write_u32(out, self.generation);
        write_byte(out, self.created_by);
        write_u64(out, self.size_bytes);
        write_id_maybe(out, self.previous_message);
        write_text_maybe(out, self.party.as_deref());
        write_text_maybe(out, self.contract.as_deref());
        write_id_maybe(out, self.stream);
        write_u64(out, self.created_unix_nanos);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            generation: read_u32(cursor)?,
            created_by: read_byte(cursor)?,
            size_bytes: read_u64(cursor)?,
            previous_message: read_id_maybe(cursor)?,
            party: read_text_maybe(cursor)?,
            contract: read_text_maybe(cursor)?,
            stream: read_id_maybe(cursor)?,
            created_unix_nanos: read_u64(cursor)?,
        })
    }
}

impl Form for AuditFacts {
    fn write(&self, out: &mut Vec<u8>) {
        write_u64(out, self.occurred_unix_nanos);
        for text in [&self.action, &self.phase, &self.severity] {
            write_text(out, text);
        }
        write_byte(out, u8::from(self.failed));
        write_text(out, &self.program);
        for text in [
            &self.artifact_kind,
            &self.cluster,
            &self.node,
            &self.artifact,
        ] {
            write_text_maybe(out, text.as_deref());
        }
        for id in [self.journey, self.message, self.execution] {
            write_id_maybe(out, id);
        }
        write_u64(out, self.kept_unix_nanos);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            occurred_unix_nanos: read_u64(cursor)?,
            action: read_text(cursor)?,
            phase: read_text(cursor)?,
            severity: read_text(cursor)?,
            failed: read_byte(cursor)? != 0,
            program: read_text(cursor)?,
            artifact_kind: read_text_maybe(cursor)?,
            cluster: read_text_maybe(cursor)?,
            node: read_text_maybe(cursor)?,
            artifact: read_text_maybe(cursor)?,
            journey: read_id_maybe(cursor)?,
            message: read_id_maybe(cursor)?,
            execution: read_id_maybe(cursor)?,
            kept_unix_nanos: read_u64(cursor)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_fact_comes_back_from_its_bytes_as_it_was() {
        let journey = JourneyFacts {
            state: 5,
            attempts: 2,
            depth: 1,
            send_port: Some("Billing".to_string()),
            work_process: None,
            previous_journey: Some(7),
            message: Some(8),
            created_unix_nanos: 9,
            updated_unix_nanos: 10,
        };
        assert_eq!(
            JourneyFacts::from_bytes(&journey.bytes()).expect("read"),
            journey
        );
        let message = MessageFacts {
            generation: 1,
            created_by: 2,
            size_bytes: 3,
            previous_message: None,
            party: Some("Contoso".to_string()),
            contract: Some("Order".to_string()),
            stream: Some(4),
            created_unix_nanos: 5,
        };
        assert_eq!(
            MessageFacts::from_bytes(&message.bytes()).expect("read"),
            message
        );
        let audit = AuditFacts {
            occurred_unix_nanos: 1,
            action: "publish".to_string(),
            phase: "finished".to_string(),
            severity: "information".to_string(),
            failed: true,
            program: "xmip-service".to_string(),
            artifact_kind: Some("ReceiveLocation".to_string()),
            journey: Some(2),
            kept_unix_nanos: 3,
            ..AuditFacts::default()
        };
        assert_eq!(AuditFacts::from_bytes(&audit.bytes()).expect("read"), audit);
    }
}
