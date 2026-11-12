//! What a primary sends a standby following it, frame after frame on the
//! connection the standby asked on, and what a Storage node says of its
//! standing (`super`).

use codec::CodecError;
use codec::cursor::Cursor;
use codec::field;
use codec::form::Form;
use codec::writer::ByteWriter;

use super::bootstrap::Bootstrap;
use super::copies::DATABASES;
use crate::{Change, Entry};

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
    /// How far its bootstrap by a whole copy is: it is not promoted until
    /// ready (ADR-0056, amendment 2026-11-12).
    pub bootstrap: Bootstrap,
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
        out.u128_be(self.process);
        out.byte(u8::from(self.primary));
        out.u64_be(self.epoch);
        out.u128_be(self.history);
        for next in self.next {
            out.u64_be(next);
        }
        self.bootstrap.write(out);
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        Ok(Self {
            process: cursor.u128_be()?,
            primary: cursor.byte()? != 0,
            epoch: cursor.u64_be()?,
            history: cursor.u128_be()?,
            next: [cursor.u64_be()?, cursor.u64_be()?, cursor.u64_be()?],
            bootstrap: Bootstrap::read(cursor)?,
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
        out.u64_be(self.epoch);
        out.u128_be(self.history);
        for next in self.next {
            out.u64_be(next);
        }
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        Ok(Self {
            epoch: cursor.u64_be()?,
            history: cursor.u128_be()?,
            next: [cursor.u64_be()?, cursor.u64_be()?, cursor.u64_be()?],
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
    /// A database's pages all handed on: its copies follow from the write
    /// numbered first, and what the pages hold is one state of the
    /// primary's once the standby has replayed up to the write numbered
    /// second (`super::bootstrap`).
    Copied(u8, u64, u64),
    /// The Storage node asked is not the primary, in words.
    Declined(String),
}

fn write_change(out: &mut Vec<u8>, (key, value): &Change) {
    field::counted(out, key);
    out.byte(u8::from(value.is_some()));
    if let Some(value) = value {
        field::counted(out, value);
    }
}

fn read_change(cursor: &mut Cursor<'_>) -> codec::Result<Change> {
    let key = field::read_counted(cursor)?.to_vec();
    let value = match cursor.byte()? {
        0 => None,
        _ => Some(field::read_counted(cursor)?.to_vec()),
    };
    Ok((key, value))
}

fn write_count(out: &mut Vec<u8>, count: usize) {
    out.u32_be(u32::try_from(count).unwrap_or(u32::MAX));
}

impl Form for Shipped {
    fn write(&self, out: &mut Vec<u8>) {
        match self {
            Self::Beat(epoch) => {
                out.byte(0);
                out.u64_be(*epoch);
            }
            Self::Copy(database, number, changes) => {
                out.byte(1);
                out.byte(*database);
                out.u64_be(*number);
                write_count(out, changes.len());
                for change in changes {
                    write_change(out, change);
                }
            }
            Self::Whole(history, epoch, keys) => {
                out.byte(2);
                out.u128_be(*history);
                out.u64_be(*epoch);
                write_count(out, keys.len());
                for key in keys {
                    field::counted(out, key);
                }
            }
            Self::Entries(database, entries) => {
                out.byte(3);
                out.byte(*database);
                write_count(out, entries.len());
                for (key, value) in entries {
                    field::counted(out, key);
                    field::counted(out, value);
                }
            }
            Self::Copied(database, from, through) => {
                out.byte(4);
                out.byte(*database);
                out.u64_be(*from);
                out.u64_be(*through);
            }
            Self::Declined(reason) => {
                out.byte(5);
                field::text(out, reason);
            }
        }
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        let database = |cursor: &mut Cursor<'_>| {
            let database = cursor.byte()?;
            if usize::from(database) < DATABASES {
                Ok(database)
            } else {
                Err(CodecError::new(format!(
                    "no database is numbered {database}"
                )))
            }
        };
        Ok(match cursor.byte()? {
            0 => Self::Beat(cursor.u64_be()?),
            1 => {
                let (database, number) = (database(cursor)?, cursor.u64_be()?);
                let changes = (0..cursor.u32_be()?)
                    .map(|_| read_change(cursor))
                    .collect::<Result<_, _>>()?;
                Self::Copy(database, number, changes)
            }
            2 => {
                let (history, epoch) = (cursor.u128_be()?, cursor.u64_be()?);
                let keys = (0..cursor.u32_be()?)
                    .map(|_| field::read_counted(cursor).map(<[u8]>::to_vec))
                    .collect::<Result<_, _>>()?;
                Self::Whole(history, epoch, keys)
            }
            3 => {
                let database = database(cursor)?;
                let entries = (0..cursor.u32_be()?)
                    .map(|_| {
                        Ok((
                            field::read_counted(cursor)?.to_vec(),
                            field::read_counted(cursor)?.to_vec(),
                        ))
                    })
                    .collect::<codec::Result<_>>()?;
                Self::Entries(database, entries)
            }
            4 => Self::Copied(database(cursor)?, cursor.u64_be()?, cursor.u64_be()?),
            5 => Self::Declined(field::read_text(cursor)?),
            other => return Err(CodecError::new(format!("no frame is numbered {other}"))),
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
            Shipped::Copied(0, 12, 15),
            Shipped::Declined("a standby".to_string()),
        ];
        for frame in frames {
            assert_eq!(Shipped::from_bytes(&frame.bytes()).expect("read"), frame);
        }
        let catching_up = Bootstrap::CatchingUp {
            epoch: 2,
            through: [4, 2, 3],
        };
        for bootstrap in [Bootstrap::Ready, Bootstrap::Copying, catching_up] {
            let standing = Standing {
                process: 5,
                primary: false,
                epoch: 2,
                history: 8,
                next: [1, 2, 3],
                bootstrap,
            };
            assert_eq!(
                Standing::from_bytes(&standing.bytes()).expect("read"),
                standing
            );
            assert_eq!(standing.held(), 6);
        }
        assert!(Shipped::from_bytes(&[1, 3, 0, 0, 0, 0, 0, 0, 0, 0]).is_err());
    }
}
