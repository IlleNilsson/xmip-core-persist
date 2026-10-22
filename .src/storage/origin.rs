//! A Stream's place in the sequences it holds end to end, kept until the
//! last Journey born from it ends (ADR-0024 and ADR-0066, amendments
//! 2026-10-22).
//!
//! The owner, 2026-10-22: *A default correlation set is its receive
//! location name. … Default one stream, message, journey at a time. Put it
//! in the ledger if it is not processed now.* A sequence is a queue in the
//! Ledger, its places held in the order written; a Stream's place in one
//! is held by its origin — the Message it was received as — from its
//! Publication on, and the Journeys born from it count whether it still
//! needs it: each says, in its own record, its origin and whether it is
//! open (`JourneyFacts::origin` and `open`). Xmip Storage counts them in
//! the write that writes them, here, so no step can count them wrong: the
//! last that closes lets the origin's places go, in that same write, and
//! the next Stream of each sequence heads it.

use codec::cursor::Cursor;
use xcore::JourneyId;

use super::commit::{Batch, JOURNEY};
use super::hold::{self, Hold};
use super::record::{Form, JourneyRecord, read_u32, read_u128, write_u32, write_u128};
use crate::{EncryptedStore, Engine, PersistError};

/// Where an origin's count of open Journeys and its places are kept.
pub(crate) const ORIGIN: &str = "sequence-origin";

/// An origin's open Journeys, and the queues it holds a place in.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Origin {
    open: u32,
    queues: Vec<u128>,
}

impl Form for Origin {
    fn write(&self, out: &mut Vec<u8>) {
        write_u32(out, self.open);
        write_u32(out, u32::try_from(self.queues.len()).unwrap_or(u32::MAX));
        for queue in &self.queues {
            write_u128(out, *queue);
        }
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            open: read_u32(cursor)?,
            queues: (0..read_u32(cursor)?)
                .map(|_| read_u128(cursor))
                .collect::<Result<_, _>>()?,
        })
    }
}

/// `record`, about to be written in `batch`, counted for its origin: an
/// origin's first open Journey keeps its count and the queues `places`
/// holds it a place in; each Journey that opens or closes since moves the
/// count; the last to close lets every place go and the count with it.
/// Nothing for a Journey with no origin, or one whose openness did not
/// change.
pub(crate) fn counted<R: Engine>(
    store: &EncryptedStore<R>,
    batch: &mut Batch<'_>,
    record: &JourneyRecord,
    places: &[Hold],
) -> Result<(), PersistError> {
    let Some(origin) = record.facts.origin else {
        return Ok(());
    };
    let before = batch
        .read(store, JOURNEY, &record.journey.value().to_be_bytes())?
        .map(|bytes| JourneyRecord::from_bytes(&bytes))
        .transpose()?;
    let was = before.is_some_and(|b| b.facts.open && b.facts.origin == Some(origin));
    if was == record.facts.open {
        return Ok(());
    }
    let key = origin.to_be_bytes();
    let mut counted = match batch.read(store, ORIGIN, &key)? {
        Some(bytes) => Origin::from_bytes(&bytes)?,
        None => Origin {
            open: 0,
            queues: places
                .iter()
                .filter(|place| place.journey.value() == origin)
                .map(|place| place.queue)
                .collect(),
        },
    };
    if record.facts.open {
        counted.open += 1;
    } else {
        counted.open = counted.open.saturating_sub(1);
    }
    if counted.open == 0 {
        for queue in &counted.queues {
            hold::let_go_of(store, batch, *queue, JourneyId::new(origin))?;
        }
        batch.put(ORIGIN, key.to_vec(), None);
    } else {
        batch.put(ORIGIN, key.to_vec(), Some(counted.bytes()));
    }
    Ok(())
}
