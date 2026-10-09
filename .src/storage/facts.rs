//! A record's fields laid out: every single value of a Journey, a Message
//! and an audit record, which their tables keep in columns of their own, in
//! the clear, beside the sealed body (proposed 2026-10-09; the owner, the
//! same day: *Store it in the clear*, *All columns shall be laid out*).
//!
//! **The writer says them, Xmip Storage keeps them.** A body is the step's
//! own serialization, which Xmip Storage keeps byte for byte and does not
//! read; the fields travel beside it, typed, filled by the writer from the
//! object the body is the serialization of — the runtime's Journey, Message
//! and audit record, each in one place. A list — a Journey's entries, a
//! Message's Sections and context, an audit record's properties — stays in
//! the body alone. The times are Xmip Storage's own: when it first wrote
//! the record and when it last did, on its clock, the one every claim is
//! decided by, so what a caller sends there is replaced
//! ([`super::columns`]).

use codec::cursor::Cursor;

use super::record::{
    Form, read_byte, read_text, read_u32, read_u64, read_u128, write_byte, write_text, write_u32,
    write_u64, write_u128,
};
use crate::PersistError;

/// A Journey's fields, as its record is written.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JourneyFacts {
    /// Its state, by its word (`JourneyState::word`).
    pub state: String,
    /// The Journey it came from.
    pub previous_journey: Option<u128>,
    /// What caused it: the Subscription that matched, and the Work Process
    /// it started where it started one.
    pub subscription: Option<String>,
    pub cause_work_process: Option<String>,
    /// How many links back to a Journey nothing caused.
    pub depth: u32,
    /// The Work Process it is in.
    pub work_process: Option<String>,
    pub send_port: Option<String>,
    /// Its active Send Location, by its place in the Send Port's order, and
    /// how often it has been tried.
    pub send_location: u32,
    pub attempts: u32,
    /// The Message it holds last: the last of its Message references.
    pub message: Option<u128>,
    /// When Xmip Storage first wrote it, in nanoseconds since the Unix
    /// epoch: Xmip Storage's to set.
    pub created_unix_nanos: u64,
    /// When Xmip Storage last wrote it: Xmip Storage's to set.
    pub updated_unix_nanos: u64,
}

/// A Message's fields, as its record is written.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MessageFacts {
    pub previous_message: Option<u128>,
    /// How many times its content or metadata changed since it was received.
    pub generation: u32,
    /// How it was made, and its treatment — priority, execution profile,
    /// durability — each by its word (`MessageCreationSource::word` and its
    /// siblings).
    pub created_by: String,
    pub priority: String,
    pub execution_profile: String,
    pub durability: String,
    /// Its Sections' length together.
    pub size_bytes: u64,
    /// The Party it came from, as its context says.
    pub party: Option<String>,
    /// The contract its first Section is read by, and the Stream it is over.
    pub contract: Option<String>,
    pub stream: Option<u128>,
    /// When Xmip Storage first wrote it: Xmip Storage's to set.
    pub created_unix_nanos: u64,
}

/// An audit record's fields, as its writer said it (ADR-0062).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AuditFacts {
    /// When it happened, by its writer's clock.
    pub occurred_unix_nanos: u64,
    pub action: String,
    /// Its phase and its severity, in the words the record model writes.
    pub phase: String,
    pub severity: String,
    /// A failure: the `failure` phase or an `error`, always kept.
    pub failed: bool,
    /// What the record says, in words, where it says anything.
    pub text: Option<String>,
    /// Its origin: the program, the machine, the process, the scope the
    /// process declared it serves, and whether its run is hidden.
    pub program: String,
    pub host: String,
    pub process: u32,
    pub location: Option<String>,
    pub hidden: bool,
    /// The Message execution it belongs to, where it belongs to one.
    pub execution: Option<u128>,
    pub journey: Option<u128>,
    pub message: Option<u128>,
    /// The artifact the execution is at: its identifier, what it is, its
    /// name and its version.
    pub artifact: Option<u128>,
    pub artifact_kind: Option<String>,
    pub artifact_name: Option<String>,
    pub artifact_version: Option<String>,
    pub node: Option<u128>,
    pub cluster: Option<u128>,
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
        write_text(out, &self.state);
        write_id_maybe(out, self.previous_journey);
        write_text_maybe(out, self.subscription.as_deref());
        write_text_maybe(out, self.cause_work_process.as_deref());
        write_u32(out, self.depth);
        write_text_maybe(out, self.work_process.as_deref());
        write_text_maybe(out, self.send_port.as_deref());
        write_u32(out, self.send_location);
        write_u32(out, self.attempts);
        write_id_maybe(out, self.message);
        write_u64(out, self.created_unix_nanos);
        write_u64(out, self.updated_unix_nanos);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            state: read_text(cursor)?,
            previous_journey: read_id_maybe(cursor)?,
            subscription: read_text_maybe(cursor)?,
            cause_work_process: read_text_maybe(cursor)?,
            depth: read_u32(cursor)?,
            work_process: read_text_maybe(cursor)?,
            send_port: read_text_maybe(cursor)?,
            send_location: read_u32(cursor)?,
            attempts: read_u32(cursor)?,
            message: read_id_maybe(cursor)?,
            created_unix_nanos: read_u64(cursor)?,
            updated_unix_nanos: read_u64(cursor)?,
        })
    }
}

impl Form for MessageFacts {
    fn write(&self, out: &mut Vec<u8>) {
        write_id_maybe(out, self.previous_message);
        write_u32(out, self.generation);
        for word in [
            &self.created_by,
            &self.priority,
            &self.execution_profile,
            &self.durability,
        ] {
            write_text(out, word);
        }
        write_u64(out, self.size_bytes);
        write_text_maybe(out, self.party.as_deref());
        write_text_maybe(out, self.contract.as_deref());
        write_id_maybe(out, self.stream);
        write_u64(out, self.created_unix_nanos);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            previous_message: read_id_maybe(cursor)?,
            generation: read_u32(cursor)?,
            created_by: read_text(cursor)?,
            priority: read_text(cursor)?,
            execution_profile: read_text(cursor)?,
            durability: read_text(cursor)?,
            size_bytes: read_u64(cursor)?,
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
        write_text_maybe(out, self.text.as_deref());
        write_text(out, &self.program);
        write_text(out, &self.host);
        write_u32(out, self.process);
        write_text_maybe(out, self.location.as_deref());
        write_byte(out, u8::from(self.hidden));
        for id in [self.execution, self.journey, self.message, self.artifact] {
            write_id_maybe(out, id);
        }
        for text in [
            &self.artifact_kind,
            &self.artifact_name,
            &self.artifact_version,
        ] {
            write_text_maybe(out, text.as_deref());
        }
        write_id_maybe(out, self.node);
        write_id_maybe(out, self.cluster);
        write_u64(out, self.kept_unix_nanos);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            occurred_unix_nanos: read_u64(cursor)?,
            action: read_text(cursor)?,
            phase: read_text(cursor)?,
            severity: read_text(cursor)?,
            failed: read_byte(cursor)? != 0,
            text: read_text_maybe(cursor)?,
            program: read_text(cursor)?,
            host: read_text(cursor)?,
            process: read_u32(cursor)?,
            location: read_text_maybe(cursor)?,
            hidden: read_byte(cursor)? != 0,
            execution: read_id_maybe(cursor)?,
            journey: read_id_maybe(cursor)?,
            message: read_id_maybe(cursor)?,
            artifact: read_id_maybe(cursor)?,
            artifact_kind: read_text_maybe(cursor)?,
            artifact_name: read_text_maybe(cursor)?,
            artifact_version: read_text_maybe(cursor)?,
            node: read_id_maybe(cursor)?,
            cluster: read_id_maybe(cursor)?,
            kept_unix_nanos: read_u64(cursor)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_field_comes_back_from_its_bytes_as_it_was() {
        let journey = JourneyFacts {
            state: "Failed".to_string(),
            previous_journey: Some(7),
            subscription: Some("billing".to_string()),
            cause_work_process: None,
            depth: 1,
            work_process: Some("Approval".to_string()),
            send_port: Some("Billing".to_string()),
            send_location: 1,
            attempts: 2,
            message: Some(8),
            created_unix_nanos: 9,
            updated_unix_nanos: 10,
        };
        assert_eq!(
            JourneyFacts::from_bytes(&journey.bytes()).expect("read"),
            journey
        );
        let message = MessageFacts {
            previous_message: None,
            generation: 1,
            created_by: "Receive".to_string(),
            priority: "Normal".to_string(),
            execution_profile: "Business".to_string(),
            durability: "Recoverable".to_string(),
            size_bytes: 3,
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
            text: Some("refused".to_string()),
            program: "xmip-service".to_string(),
            host: "a host".to_string(),
            process: 4711,
            hidden: true,
            artifact_kind: Some("ReceiveLocation".to_string()),
            journey: Some(2),
            node: Some(6),
            kept_unix_nanos: 3,
            ..AuditFacts::default()
        };
        assert_eq!(AuditFacts::from_bytes(&audit.bytes()).expect("read"), audit);
    }
}
