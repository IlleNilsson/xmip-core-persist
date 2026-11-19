//! The forms a held Message is kept in: a [`Hold`], a [`Held`] at its place
//! and a [`HeldQueue`] as read, each written and read once here; and what
//! a batch reads of a held one without copying its body (2026-11-17).

use codec::cursor::Cursor;
use codec::field;
use codec::form::Form;
use codec::writer::ByteWriter;
use xcore::JourneyId;

use super::{Held, HeldQueue, Hold};

impl Form for Hold {
    fn capacity(&self) -> usize {
        self.body.len() + 48
    }

    fn write(&self, out: &mut Vec<u8>) {
        out.u128_be(self.queue);
        out.u128_be(self.journey.value());
        field::counted(out, &self.body);
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        let (queue, journey, body) = Self::fields(cursor, <[u8]>::to_vec)?;
        Ok(Self {
            queue,
            journey,
            body,
        })
    }
}

impl Hold {
    /// Its queue, its Journey and its body as its form keeps them, `body`
    /// taking the body's bytes: the one reading of the form.
    fn fields<'a, B>(
        cursor: &mut Cursor<'a>,
        body: impl FnOnce(&'a [u8]) -> B,
    ) -> codec::Result<(u128, JourneyId, B)> {
        let queue = cursor.u128_be()?;
        let journey = JourneyId::new(cursor.u128_be()?);
        Ok((queue, journey, body(field::read_counted(cursor)?)))
    }
}

impl Form for Held {
    fn capacity(&self) -> usize {
        self.hold.capacity() + 48
    }

    fn write(&self, out: &mut Vec<u8>) {
        out.u64_be(self.sequence);
        self.hold.write(out);
        out.u64_be(self.held_unix_nanos);
        out.byte(u8::from(self.failed));
        out.byte(u8::from(self.due_unix_nanos.is_some()));
        out.i128_be(self.due_unix_nanos.unwrap_or_default());
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        let sequence = cursor.u64_be()?;
        let hold = Hold::read(cursor)?;
        let (held_unix_nanos, failed, due_unix_nanos) = Self::standing_in(cursor)?;
        Ok(Self {
            sequence,
            hold,
            held_unix_nanos,
            failed,
            due_unix_nanos,
        })
    }
}

impl Held {
    /// The Journey of the Message held in `bytes`, read without copying its
    /// body: whose claim holds it (`super::watch`; 2026-11-17).
    pub(crate) fn journey_of(bytes: &[u8]) -> codec::Result<JourneyId> {
        let mut cursor = Cursor::new(bytes);
        cursor.u64_be()?;
        let (_, journey, ()) = Hold::fields(&mut cursor, drop)?;
        Ok(journey)
    }

    /// Whether the Message held in `bytes` failed, and when it is due,
    /// read past its body without copying it: what a batch settles its
    /// queue's places by when each is due by (`super::due`; 2026-11-17).
    pub(crate) fn standing(bytes: &[u8]) -> codec::Result<(bool, Option<i128>)> {
        let mut cursor = Cursor::new(bytes);
        cursor.u64_be()?;
        Hold::fields(&mut cursor, drop)?;
        let (_, failed, due) = Self::standing_in(&mut cursor)?;
        if !cursor.is_empty() {
            return Err(codec::CodecError::new(format!(
                "{} bytes left after a held Message",
                cursor.remaining().len()
            )));
        }
        Ok((failed, due))
    }

    /// When it was held, whether it failed and when it is due, as its form
    /// keeps them after its hold: the one reading of them.
    fn standing_in(cursor: &mut Cursor<'_>) -> codec::Result<(u64, bool, Option<i128>)> {
        let held_unix_nanos = cursor.u64_be()?;
        let failed = cursor.byte()? != 0;
        let waits = cursor.byte()? != 0;
        let due = cursor.i128_be()?;
        Ok((held_unix_nanos, failed, waits.then_some(due)))
    }
}

impl Form for HeldQueue {
    fn write(&self, out: &mut Vec<u8>) {
        out.u64_be(self.first);
        out.u64_be(self.next);
        out.u64_be(self.count);
        self.held.write(out);
        out.i128_be(self.read_unix_nanos);
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        Ok(Self {
            first: cursor.u64_be()?,
            next: cursor.u64_be()?,
            count: cursor.u64_be()?,
            held: Vec::read(cursor)?,
            read_unix_nanos: cursor.i128_be()?,
        })
    }
}
