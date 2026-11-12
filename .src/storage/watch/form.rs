//! The bytes of what a wait watches and answers (`super`), as the wire
//! carries them (`super::super::wire`).

use codec::CodecError;
use codec::cursor::Cursor;
use codec::form::Form;
use codec::writer::ByteWriter;

use super::super::due::DueQueue;
use super::{Moved, Position, Watched};

impl Form for Watched {
    fn write(&self, out: &mut Vec<u8>) {
        let (kind, id) = match self {
            Self::Queue(id) => (0, id),
            Self::Head(id) => (1, id),
            Self::Instance(id) => (2, id),
            Self::Key(id) => (3, id),
        };
        out.byte(kind);
        out.u128_be(*id);
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        let kind = cursor.byte()?;
        let id = cursor.u128_be()?;
        Ok(match kind {
            0 => Self::Queue(id),
            1 => Self::Head(id),
            2 => Self::Instance(id),
            3 => Self::Key(id),
            other => return Err(CodecError::new(format!("nothing watched is kind {other}"))),
        })
    }
}

impl Form for Position {
    fn write(&self, out: &mut Vec<u8>) {
        self.watched.write(out);
        out.u64_be(self.at);
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        Ok(Self {
            watched: Watched::read(cursor)?,
            at: cursor.u64_be()?,
        })
    }
}

impl Form for Moved {
    fn write(&self, out: &mut Vec<u8>) {
        self.position.write(out);
        out.byte(u8::from(self.due.is_some()));
        if let Some(due) = &self.due {
            due.write(out);
        }
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        let position = Position::read(cursor)?;
        let due = match cursor.byte()? {
            0 => None,
            _ => Some(DueQueue::read(cursor)?),
        };
        Ok(Self { position, due })
    }
}
