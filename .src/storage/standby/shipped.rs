//! What a primary sends a standby following it, frame after frame on the
//! connection the standby asked on, and what a Storage node says of its
//! standing (`super`).

use codec::cursor::Cursor;

use super::copies::DATABASES;
use crate::storage::record::{
    Form, malformed, read_byte, read_bytes, read_text, read_u32, read_u64, read_u128, write_byte,
    write_bytes, write_text, write_u32, write_u64, write_u128,
};
use crate::{Change, Entry, PersistError};

/// What a Storage node is now, as it answers a node or another Storage
/// node asking (`Request::Standing`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Standing {
    /// This Storage node's process, new each time it starts: how a Storage
    /// node finds itself among those its configuration lists.
    pub process: u128,
    /// Whether it is the primary, serving every operation; else a standby,
    /// or one still finding its primary.
    pub primary: bool,
    /// The epoch it is in: raised by each promotion, so a primary of an
    /// earlier epoch is known for one (ADR-0056, amendment 2026-10-22).
    pub epoch: u64,
    /// The history its writes are numbered in.
    pub history: u128,
    /// The number of the next write each database takes.
    pub next: [u64; DATABASES],
}

impl Standing {
    /// How many writes it holds of its history, all databases together:
    /// of two standbys of one history, the one holding more is promoted.
    #[must_use]
    pub fn held(&self) -> u64 {
        self.next.iter().sum()
    }
}

impl Form for Standing {
    fn write(&self, out: &mut Vec<u8>) {
        write_u128(out, self.process);
        write_byte(out, u8::from(self.primary));
        write_u64(out, self.epoch);
        write_u128(out, self.history);
        for next in self.next {
            write_u64(out, next);
        }
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            process: read_u128(cursor)?,
            primary: read_byte(cursor)? != 0,
            epoch: read_u64(cursor)?,
            history: read_u128(cursor)?,
            next: [read_u64(cursor)?, read_u64(cursor)?, read_u64(cursor)?],
        })
    }
}

/// Where a standby asks to be followed from: the epoch it knows, the
/// history it holds and the next write of each database.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Follow {
    pub epoch: u64,
    pub history: u128,
    pub next: [u64; DATABASES],
}

impl Form for Follow {
    fn write(&self, out: &mut Vec<u8>) {
        write_u64(out, self.epoch);
        write_u128(out, self.history);
        for next in self.next {
            write_u64(out, next);
        }
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            epoch: read_u64(cursor)?,
            history: read_u128(cursor)?,
            next: [read_u64(cursor)?, read_u64(cursor)?, read_u64(cursor)?],
        })
    }
}

/// One frame from a primary to a standby following it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Shipped {
    /// Nothing written meanwhile; the primary is there, in its epoch.
    Beat(u64),
    /// A write of a database, numbered, its changes as the engine took them.
    Copy(u8, u64, Vec<Change>),
    /// The standby is copied whole: the history it then holds, the epoch,
    /// and each database's data key, handed inside the mutual TLS
    /// (`secret::DataKey::handed`).
    Whole(u128, u64, Vec<Vec<u8>>),
    /// Entries of a database copied whole, in the order of their keys.
    Entries(u8, Vec<Entry>),
    /// A database copied whole up to the write numbered here, its copies
    /// following from it.
    Copied(u8, u64),
    /// The Storage node asked is not the primary, in words.
    Declined(String),
}

fn write_change(out: &mut Vec<u8>, (key, value): &Change) {
    write_bytes(out, key);
    write_byte(out, u8::from(value.is_some()));
    if let Some(value) = value {
        write_bytes(out, value);
    }
}

fn read_change(cursor: &mut Cursor<'_>) -> Result<Change, PersistError> {
    let key = read_bytes(cursor)?;
    let value = match read_byte(cursor)? {
        0 => None,
        _ => Some(read_bytes(cursor)?),
    };
    Ok((key, value))
}

fn write_count(out: &mut Vec<u8>, count: usize) {
    write_u32(out, u32::try_from(count).unwrap_or(u32::MAX));
}

impl Form for Shipped {
    fn write(&self, out: &mut Vec<u8>) {
        match self {
            Self::Beat(epoch) => {
                write_byte(out, 0);
                write_u64(out, *epoch);
            }
            Self::Copy(database, number, changes) => {
                write_byte(out, 1);
                write_byte(out, *database);
                write_u64(out, *number);
                write_count(out, changes.len());
                for change in changes {
                    write_change(out, change);
                }
            }
            Self::Whole(history, epoch, keys) => {
                write_byte(out, 2);
                write_u128(out, *history);
                write_u64(out, *epoch);
                write_count(out, keys.len());
                for key in keys {
                    write_bytes(out, key);
                }
            }
            Self::Entries(database, entries) => {
                write_byte(out, 3);
                write_byte(out, *database);
                write_count(out, entries.len());
                for (key, value) in entries {
                    write_bytes(out, key);
                    write_bytes(out, value);
                }
            }
            Self::Copied(database, next) => {
                write_byte(out, 4);
                write_byte(out, *database);
                write_u64(out, *next);
            }
            Self::Declined(reason) => {
                write_byte(out, 5);
                write_text(out, reason);
            }
        }
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        let database = |cursor: &mut Cursor<'_>| {
            let database = read_byte(cursor)?;
            if usize::from(database) < DATABASES {
                Ok(database)
            } else {
                Err(malformed(format!("no database is numbered {database}")))
            }
        };
        Ok(match read_byte(cursor)? {
            0 => Self::Beat(read_u64(cursor)?),
            1 => {
                let (database, number) = (database(cursor)?, read_u64(cursor)?);
                let changes = (0..read_u32(cursor)?)
                    .map(|_| read_change(cursor))
                    .collect::<Result<_, _>>()?;
                Self::Copy(database, number, changes)
            }
            2 => {
                let (history, epoch) = (read_u128(cursor)?, read_u64(cursor)?);
                let keys = (0..read_u32(cursor)?)
                    .map(|_| read_bytes(cursor))
                    .collect::<Result<_, _>>()?;
                Self::Whole(history, epoch, keys)
            }
            3 => {
                let database = database(cursor)?;
                let entries = (0..read_u32(cursor)?)
                    .map(|_| Ok((read_bytes(cursor)?, read_bytes(cursor)?)))
                    .collect::<Result<_, PersistError>>()?;
                Self::Entries(database, entries)
            }
            4 => Self::Copied(database(cursor)?, read_u64(cursor)?),
            5 => Self::Declined(read_text(cursor)?),
            other => return Err(malformed(format!("no frame is numbered {other}"))),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_frame_and_a_standing_cross_the_wire_as_they_were() {
        let frames = [
            Shipped::Beat(3),
            Shipped::Copy(
                1,
                9,
                vec![(b"k".to_vec(), Some(b"v".to_vec())), (b"g".to_vec(), None)],
            ),
            Shipped::Whole(7, 2, vec![vec![1; 32], vec![2; 32], vec![3; 32]]),
            Shipped::Entries(2, vec![(b"k".to_vec(), b"v".to_vec())]),
            Shipped::Copied(0, 12),
            Shipped::Declined("a standby".to_string()),
        ];
        for frame in frames {
            assert_eq!(Shipped::from_bytes(&frame.bytes()).expect("read"), frame);
        }
        let standing = Standing {
            process: 5,
            primary: true,
            epoch: 2,
            history: 8,
            next: [1, 2, 3],
        };
        assert_eq!(
            Standing::from_bytes(&standing.bytes()).expect("read"),
            standing
        );
        assert_eq!(standing.held(), 6);
        assert!(Shipped::from_bytes(&[1, 3, 0, 0, 0, 0, 0, 0, 0, 0]).is_err());
    }
}
