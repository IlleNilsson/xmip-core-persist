//! What the one writer keeps of the records it wrote (2026-11-15): each
//! record a condition is decided on — a claim, a Journey, a queue's entry,
//! its index and its places, its places by when each is due, a holder's
//! count, the audit sequence, a failed Message's and a Dead Message Queue's
//! records, a Work Process Instance and its keys — as the last batch that
//! wrote it left it, so a batch after it reads it from here (`Batch::before`),
//! neither from the engine nor decrypted again. A Message's hand-on reads
//! nothing from the store that its Publication wrote. What no batch decides
//! on again is not kept: a Message, a Stream and its chunks, an audit record
//! and a Publication's own record.
//!
//! **Kept once in the engine.** What a batch wrote is kept only once the
//! batch is in the engine, so a batch refused keeps what the store keeps.
//! The writer is the one writer of each, so what it keeps is what the store
//! holds for as long as it writes: a Storage node opened anew — restarted,
//! or a standby promoted, which copied its primary's writes beneath no
//! writer of its own — keeps none and reads each again, and a writer that
//! panicked forgets them all.
//!
//! **Its own copy, trusted** (ADR-0018, amendment 2026-11-15). A record read
//! from here is the writer's own copy of what it sealed, not authenticated
//! against the store again; every other read is, as before.
//!
//! **Bounded in bytes**, its Storage node's `[tuning] storage_writer_kept`
//! (`Embedded::keep_written`): past it the first kept is let go of first,
//! read again from the store by the next batch that asks. It replaces each
//! queue's places by when each is due, which the writer kept alone until
//! 2026-11-15, by the queue (`super::super::due`).

use std::collections::{HashMap, VecDeque};

use super::super::{failed, instance, origin};
use super::{
    CLAIM, DEAD, DEAD_MESSAGE, DEAD_PLACES, DUE_PLACES, HELD, HELD_JOURNEY, JOURNEY, PLACES,
    SEQUENCE,
};
use crate::RecordChange;
use crate::encrypted_store::place::place;

/// What one place kept costs beside its bytes and its value: its slot in
/// the map and in the order, and their lengths, about.
const OVERHEAD: usize = 96;

/// The records the writer keeps, by place — its bytes, as the store seals
/// it for — and the order it first kept them in.
#[derive(Debug, Default)]
pub(crate) struct Kept {
    places: HashMap<Vec<u8>, Option<Vec<u8>>>,
    /// Each place kept, the first kept first: what is let go of first.
    order: VecDeque<Vec<u8>>,
    /// What it keeps, in bytes, as [`OVERHEAD`] counts.
    bytes: usize,
}

impl Kept {
    /// What the store holds at the place `at`, where the writer keeps it:
    /// `Some(None)` where it keeps that nothing is there.
    pub(crate) fn get(&self, at: &[u8]) -> Option<&Option<Vec<u8>>> {
        if self.places.is_empty() {
            return None;
        }
        self.places.get(at)
    }

    /// `changes`, a batch's in the engine now, kept as they leave each
    /// place, at most `most` bytes in all: the first kept let go of first
    /// past it, and nothing kept where it is none.
    pub(crate) fn written(&mut self, changes: Vec<RecordChange<'static>>, most: usize) {
        if most == 0 {
            return self.forget();
        }
        for (kind, key, value) in changes {
            if keeps(kind) {
                self.keep(place(kind, &key).to_vec(), value);
            }
        }
        while self.bytes > most {
            let Some(first) = self.order.pop_front() else {
                break;
            };
            if let Some(value) = self.places.remove(&first) {
                self.bytes -= cost(&first, value.as_deref());
            }
        }
    }

    /// Nothing kept: each read again from the store.
    pub(crate) fn forget(&mut self) {
        self.places.clear();
        self.order.clear();
        self.bytes = 0;
    }

    /// How many places it keeps.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.places.len()
    }

    /// Each place it keeps, and what it keeps there.
    #[cfg(test)]
    pub(crate) fn each(&self) -> impl Iterator<Item = (&Vec<u8>, &Option<Vec<u8>>)> {
        self.places.iter()
    }

    fn keep(&mut self, place: Vec<u8>, value: Option<Vec<u8>>) {
        let added = cost(&place, value.as_deref());
        if let Some(kept) = self.places.get_mut(&place) {
            self.bytes = self.bytes - cost(&place, kept.as_deref()) + added;
            *kept = value;
            return;
        }
        self.bytes += added;
        self.order.push_back(place.clone());
        self.places.insert(place, value);
    }
}

/// Whether the writer keeps records of `kind`: those a later batch decides
/// a condition on.
fn keeps(kind: &str) -> bool {
    let queue = |kinds: super::super::queue::Kinds| [kinds.entry, kinds.by, kinds.places];
    [
        CLAIM,
        JOURNEY,
        HELD,
        HELD_JOURNEY,
        PLACES,
        DUE_PLACES,
        origin::ORIGIN,
        SEQUENCE,
        DEAD,
        DEAD_MESSAGE,
        DEAD_PLACES,
        instance::INSTANCE,
        instance::INSTANCE_KEY,
    ]
    .into_iter()
    .chain(queue(failed::KINDS))
    .any(|kept| kept == kind)
}

/// What keeping `value` at `place` costs, in bytes: the place twice, in
/// the map and in the order, its value and [`OVERHEAD`].
fn cost(place: &[u8], value: Option<&[u8]>) -> usize {
    2 * place.len() + value.map_or(0, <[u8]>::len) + OVERHEAD
}

#[cfg(test)]
mod tests;
