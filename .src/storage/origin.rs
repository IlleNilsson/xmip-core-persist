//! A place held for many Messages at once, kept until the last of their
//! Journeys closes (ADR-0024 and ADR-0066, amendments 2026-10-22 and 2026-10-27):
//! a Stream's place in the sequences it holds end to end, and a Send Port
//! Group delivery's place in the Group's sequence.
//!
//! The owner, 2026-10-22: *A default correlation set is its receive
//! location name. … Default one stream, message, journey at a time. Put it
//! in the ledger if it is not processed now.* A sequence is a queue in the
//! Ledger, its places held in the order written; a Stream's place in one
//! is held by its origin — the Message it was received as — from its
//! Publication on, and the Journeys born from it count whether it still
//! needs it: each says, in its own record, its origin and whether it is
//! open (`JourneyFacts::origin` and `open`).
//!
//! And, 2026-10-27, of a Send Port Group: *The result is a set of ports
//! that succeeded or failed … one has to accumulate the results.* A
//! delivery by a Group naming a Correlation holds its key until every
//! member has succeeded or failed: each member says its delivery and
//! whether it is open (`JourneyFacts::group_delivery` and `group_open`).
//!
//! Xmip Storage counts them in the write that writes them, here, so no
//! step can count them wrong: the last that closes lets the holder's places
//! go, in that same write, and the next of each sequence heads it.

use codec::cursor::Cursor;
use xcore::JourneyId;

use super::commit::{Batch, JOURNEY};
use super::hold::{self, Hold};
use super::record::{Form, JourneyRecord, read_u32, read_u128, write_u32, write_u128};
use crate::{EncryptedStore, Engine, PersistError};

/// Where a holder's count of open Journeys and its places are kept.
pub(crate) const ORIGIN: &str = "sequence-origin";

/// A holder's open Journeys, and the queues it holds a place in.
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

/// The holders `record` counts toward, each with whether it keeps it
/// open: its origin, and its Send Port Group delivery.
fn holders(record: &JourneyRecord) -> [(Option<u128>, bool); 2] {
    let facts = &record.facts;
    [
        (facts.origin, facts.open),
        (facts.group_delivery, facts.group_open),
    ]
}

/// `record`, about to be written in `batch`, counted for each holder it
/// names: each Journey that opens or closes moves the holder's count, the
/// queues `places` holds it a place in are kept beside it, and the last to
/// close lets every place go and the count with it. Nothing for a holder
/// whose openness did not change and that takes no place.
pub(crate) fn counted<R: Engine>(
    store: &EncryptedStore<R>,
    batch: &mut Batch<'_>,
    record: &JourneyRecord,
    places: &[Hold],
) -> Result<(), PersistError> {
    let before = batch
        .read(store, JOURNEY, &record.journey.value().to_be_bytes())?
        .map(|bytes| JourneyRecord::from_bytes(&bytes))
        .transpose()?;
    let kept = before.as_ref().map(holders);
    for (at, (holder, open)) in holders(record).into_iter().enumerate() {
        let Some(holder) = holder else {
            continue;
        };
        let was = kept.is_some_and(|kept| kept[at] == (Some(holder), true));
        let takes = places.iter().any(|place| place.journey.value() == holder);
        if was != open || takes {
            count(
                store,
                batch,
                (holder, (was != open).then_some(open)),
                places,
            )?;
        }
    }
    Ok(())
}

/// One Journey of `holder` opened or closed, `moved` saying which where
/// it did, and the places `places` takes for `holder` kept with it: a
/// place taken late, once its Stream heads, is let go with the rest.
fn count<R: Engine>(
    store: &EncryptedStore<R>,
    batch: &mut Batch<'_>,
    (holder, moved): (u128, Option<bool>),
    places: &[Hold],
) -> Result<(), PersistError> {
    let key = holder.to_be_bytes();
    let mut counted = match batch.read(store, ORIGIN, &key)? {
        Some(bytes) => Origin::from_bytes(&bytes)?,
        None => Origin {
            open: 0,
            queues: Vec::new(),
        },
    };
    for place in places
        .iter()
        .filter(|place| place.journey.value() == holder)
    {
        if !counted.queues.contains(&place.queue) {
            counted.queues.push(place.queue);
        }
    }
    match moved {
        Some(true) => counted.open += 1,
        Some(false) => counted.open = counted.open.saturating_sub(1),
        None => {}
    }
    if counted.open == 0 {
        for queue in &counted.queues {
            hold::let_go_of(store, batch, *queue, JourneyId::new(holder))?;
        }
        batch.put(ORIGIN, key.to_vec(), None);
    } else {
        batch.put(ORIGIN, key.to_vec(), Some(counted.bytes()));
    }
    Ok(())
}
