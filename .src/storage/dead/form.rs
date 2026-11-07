//! The one binary form of each Dead Message Queue record ([`super`]): on the
//! wire between a node and a Storage node, and sealed in the runtime
//! database.

use codec::CodecError;
use codec::cursor::Cursor;
use codec::field;
use codec::form::Form;
use codec::writer::ByteWriter;
use xcore::{MessageId, StreamId};

use super::{Dead, DeadEntry, DeadMessage, DeadQueue, Named, Replay, Replayed};
use crate::storage::audit_entry::AuditEntry;

impl Form for Named {
    fn write(&self, out: &mut Vec<u8>) {
        field::text(out, &self.name);
        field::text(out, &self.value);
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        Ok(Self {
            name: field::read_text(cursor)?,
            value: field::read_text(cursor)?,
        })
    }
}

/// The form's number, the first byte of every entry written.
const FORM: u8 = 1;

impl Form for DeadMessage {
    fn write(&self, out: &mut Vec<u8>) {
        out.byte(FORM);
        out.u128_be(self.queue);
        out.u128_be(self.message.value());
        out.u128_be(self.stream.value());
        field::text(out, &self.node);
        field::text(out, &self.location);
        out.i128_be(self.received_unix_nanos);
        self.validation.write(out);
        self.promoted.write(out);
        self.declines.write(out);
        field::counted(out, &self.body);
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        let form = cursor.byte()?;
        if form != FORM {
            return Err(CodecError::new(format!(
                "a Dead Message Queue entry of form {form}, and this reads form {FORM}"
            )));
        }
        Ok(Self {
            queue: cursor.u128_be()?,
            message: MessageId::new(cursor.u128_be()?),
            stream: StreamId::new(cursor.u128_be()?),
            node: field::read_text(cursor)?,
            location: field::read_text(cursor)?,
            received_unix_nanos: cursor.i128_be()?,
            validation: Vec::read(cursor)?,
            promoted: Vec::read(cursor)?,
            declines: Vec::read(cursor)?,
            body: field::read_counted(cursor)?.to_vec(),
        })
    }
}

impl Form for Dead {
    fn write(&self, out: &mut Vec<u8>) {
        out.u64_be(self.sequence);
        self.message.write(out);
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        Ok(Self {
            sequence: cursor.u64_be()?,
            message: DeadMessage::read(cursor)?,
        })
    }
}

impl Form for DeadQueue {
    fn write(&self, out: &mut Vec<u8>) {
        out.u64_be(self.first);
        out.u64_be(self.next);
        out.u64_be(self.count);
        self.dead.write(out);
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        Ok(Self {
            first: cursor.u64_be()?,
            next: cursor.u64_be()?,
            count: cursor.u64_be()?,
            dead: Vec::read(cursor)?,
        })
    }
}

impl Form for Replay {
    fn write(&self, out: &mut Vec<u8>) {
        out.u128_be(self.queue);
        out.u128_be(self.message.value());
        self.journeys.write(out);
        self.held.write(out);
        self.audit.write(out);
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        Ok(Self {
            queue: cursor.u128_be()?,
            message: MessageId::new(cursor.u128_be()?),
            journeys: Vec::read(cursor)?,
            held: Vec::read(cursor)?,
            audit: AuditEntry::read(cursor)?,
        })
    }
}

impl Form for DeadEntry {
    fn write(&self, out: &mut Vec<u8>) {
        match self {
            Self::Kept(dead) => {
                out.byte(0);
                dead.write(out);
            }
            Self::Replayed => {
                out.byte(1);
            }
            Self::Never => {
                out.byte(2);
            }
        }
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        match cursor.byte()? {
            0 => Ok(Self::Kept(Box::new(Dead::read(cursor)?))),
            1 => Ok(Self::Replayed),
            2 => Ok(Self::Never),
            other => Err(CodecError::new(format!(
                "no Dead Message Queue entry standing is numbered {other}"
            ))),
        }
    }
}

impl Form for Replayed {
    fn write(&self, out: &mut Vec<u8>) {
        out.byte(match self {
            Self::Now => 0,
            Self::Before => 1,
            Self::Absent => 2,
        });
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        match cursor.byte()? {
            0 => Ok(Self::Now),
            1 => Ok(Self::Before),
            2 => Ok(Self::Absent),
            other => Err(CodecError::new(format!(
                "no Replay answer is numbered {other}"
            ))),
        }
    }
}
