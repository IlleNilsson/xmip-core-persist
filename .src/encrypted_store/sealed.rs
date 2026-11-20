//! A batch sealed as its engine keeps it: every engine key one after the
//! other in one buffer, every sealed value in another, and where each lies
//! — what the engine is handed, borrowed (`BorrowedChange`). Until
//! 2026-11-20 each key and each sealed value was an allocation of its own,
//! some fifty of a Message's two batches of the writer, each freed again
//! once the engine had copied them.

use std::ops::Range;

use secret::DataKey;

use super::{INDEX, IndexEntry, LAYOUT, RecordChange};
use crate::{BorrowedChange, PersistError};

/// The length of a record's engine key: its keyed hash.
const HASHED: usize = 32;

/// A record's identifier as an index entry's sealed value holds it.
const IDENTIFIER: usize = 16;

/// A sealer: what is sealed where its change's key is the first argument,
/// appended to the second.
pub(super) type Seal<'s> = &'s dyn Fn(&[u8], &mut Vec<u8>) -> Result<(), PersistError>;

/// A batch sealed into two buffers.
pub(super) struct Sealed {
    keys: Vec<u8>,
    values: Vec<u8>,
    /// Where each change's key lies, and its value where it writes one.
    at: Vec<(Range<usize>, Option<Range<usize>>)>,
}

impl Sealed {
    /// Nothing sealed yet, with room for `changes` and `entries` once
    /// sealed: neither buffer ever grows.
    pub(super) fn with_room(changes: &[RecordChange<'_>], entries: &[IndexEntry]) -> Self {
        let sealed = |length: usize| DataKey::sealed_length(LAYOUT_LENGTH + length);
        let keys = changes.len() * HASHED
            + entries
                .iter()
                .map(|(key, _)| INDEX.len() + key.len())
                .sum::<usize>();
        let values = changes
            .iter()
            .filter_map(|(_, _, value)| value.as_ref().map(|value| sealed(value.len())))
            .sum::<usize>()
            + entries
                .iter()
                .filter(|(_, record)| record.is_some())
                .count()
                * sealed(IDENTIFIER);
        Self {
            keys: Vec::with_capacity(keys),
            values: Vec::with_capacity(values),
            at: Vec::with_capacity(changes.len() + entries.len()),
        }
    }

    /// A change of the key `parts` make, one after the other, and — where
    /// `seal` is given — its value, sealed by it after the values before.
    ///
    /// # Errors
    ///
    /// The sealer's; the batch is not written then.
    pub(super) fn push(
        &mut self,
        parts: &[&[u8]],
        seal: Option<Seal<'_>>,
    ) -> Result<(), PersistError> {
        let start = self.keys.len();
        for part in parts {
            self.keys.extend_from_slice(part);
        }
        let key = start..self.keys.len();
        let value = match seal {
            Some(seal) => {
                let from = self.values.len();
                seal(&self.keys[key.clone()], &mut self.values)?;
                Some(from..self.values.len())
            }
            None => None,
        };
        self.at.push((key, value));
        Ok(())
    }

    /// Whether each buffer holds as much as the room it was made with:
    /// sized right, never grown.
    #[cfg(test)]
    pub(super) fn filled(&self) -> bool {
        self.keys.len() == self.keys.capacity() && self.values.len() == self.values.capacity()
    }

    /// Each change, as the engine is handed it.
    pub(super) fn changes(&self) -> Vec<BorrowedChange<'_>> {
        self.at
            .iter()
            .map(|(key, value)| {
                let value = value.clone().map(|value| &self.values[value]);
                (&self.keys[key.clone()], value)
            })
            .collect()
    }
}

/// How long the mark of the layout a value was sealed in is (`LAYOUT`).
const LAYOUT_LENGTH: usize = [LAYOUT].len();
