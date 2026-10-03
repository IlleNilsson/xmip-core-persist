//! The one binary form of each Dead Message Queue record ([`super`]): on the
//! wire between a node and a Storage node, and sealed in the runtime
//! database.

use codec::cursor::Cursor;
use xcore::{MessageId, StreamId};

use super::{Dead, DeadEntry, DeadMessage, DeadQueue, Named, Replay, Replayed};
use crate::PersistError;
use crate::storage::record::{
    AuditEntry, Form, malformed, read_byte, read_bytes, read_i128, read_text, read_u64,
    read_u128, write_byte, write_bytes, write_i128, write_text, write_u64, write_u128,
};

impl Form for Named {
    fn write(&self, out: &mut Vec<u8>) {
        write_text(out, &self.name);
        write_text(out, &self.value);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            name: read_text(cursor)?,
            value: read_text(cursor)?,
        })
    }
}

/// The form's number, the first byte of every entry written.
const FORM: u8 = 1;

impl Form for DeadMessage {
    fn write(&self, out: &mut Vec<u8>) {
        write_byte(out, FORM);
        write_u128(out, self.queue);
        write_u128(out, self.message.value());
        write_u128(out, self.stream.value());
        write_text(out, &self.node);
        write_text(out, &self.location);
        write_i128(out, self.received_unix_nanos);
        self.validation.write(out);
        self.promoted.write(out);
        self.declines.write(out);
        write_bytes(out, &self.body);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        let form = read_byte(cursor)?;
        if form != FORM {
            return Err(malformed(format!(
                "a Dead Message Queue entry of form {form}, and this reads form {FORM}"
            )));
        }
        Ok(Self {
            queue: read_u128(cursor)?,
            message: MessageId::new(read_u128(cursor)?),
            stream: StreamId::new(read_u128(cursor)?),
            node: read_text(cursor)?,
            location: read_text(cursor)?,
            received_unix_nanos: read_i128(cursor)?,
            validation: Vec::read(cursor)?,
            promoted: Vec::read(cursor)?,
            declines: Vec::read(cursor)?,
            body: read_bytes(cursor)?,
        })
    }
}

impl Form for Dead {
    fn write(&self, out: &mut Vec<u8>) {
        write_u64(out, self.sequence);
        self.message.write(out);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            sequence: read_u64(cursor)?,
            message: DeadMessage::read(cursor)?,
        })
    }
}

impl Form for DeadQueue {
    fn write(&self, out: &mut Vec<u8>) {
        write_u64(out, self.first);
        write_u64(out, self.next);
        write_u64(out, self.count);
        self.dead.write(out);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            first: read_u64(cursor)?,
            next: read_u64(cursor)?,
            count: read_u64(cursor)?,
            dead: Vec::read(cursor)?,
        })
    }
}

impl Form for Replay {
    fn write(&self, out: &mut Vec<u8>) {
        write_u128(out, self.queue);
        write_u128(out, self.message.value());
        self.journeys.write(out);
        self.held.write(out);
        self.audit.write(out);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            queue: read_u128(cursor)?,
            message: MessageId::new(read_u128(cursor)?),
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
                write_byte(out, 0);
                dead.write(out);
            }
            Self::Replayed => write_byte(out, 1),
            Self::Never => write_byte(out, 2),
        }
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        match read_byte(cursor)? {
            0 => Ok(Self::Kept(Box::new(Dead::read(cursor)?))),
            1 => Ok(Self::Replayed),
            2 => Ok(Self::Never),
            other => Err(malformed(format!(
                "no Dead Message Queue entry standing is numbered {other}"
            ))),
        }
    }
}

impl Form for Replayed {
    fn write(&self, out: &mut Vec<u8>) {
        write_byte(
            out,
            match self {
                Self::Now => 0,
                Self::Before => 1,
                Self::Absent => 2,
            },
        );
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        match read_byte(cursor)? {
            0 => Ok(Self::Now),
            1 => Ok(Self::Before),
            2 => Ok(Self::Absent),
            other => Err(malformed(format!("no Replay answer is numbered {other}"))),
        }
    }
}
