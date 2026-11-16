//! The heads of the sequences a batch not yet durable moved, each with when
//! it is (`XmipStorage::read_head`): a place let go of is readable before
//! its sync, and the next of its sequence lets its far end see nothing
//! before the write that let it head is durable, so the order survives any
//! crash (the owner, 2026-11-15: *Wait for the disk*; ADR-0018, amendment
//! 2026-11-15). Entered before the batch is in the engine, so a read that
//! finds a head moved finds the batch that moved it here; let go of once
//! its sync told it, so what is kept is no more than the batches under way,
//! and a read of a head no batch under way moved finds nothing to wait for.

use std::collections::HashMap;
use std::sync::Mutex;
use xcore::lock::Locked;

use super::super::durable::Durable;
use super::super::watch::{Change, Watched};

/// By queue, the batches under way that moved its head, oldest first.
#[derive(Debug, Default)]
pub(crate) struct Moving(Mutex<HashMap<u128, Vec<Durable>>>);

impl Moving {
    /// Each head `changes` moves entered with `durable`, its batch's, before
    /// the batch is readable; what a sync told since let go of.
    pub(super) fn entered(&self, changes: &[Change], durable: &Durable) {
        let mut moving = self.0.locked();
        moving.retain(|_, batches| {
            batches.retain(|batch| batch.told() != Some(true));
            !batches.is_empty()
        });
        for change in changes {
            if let Watched::Head(queue) = change.watched {
                moving.entry(queue).or_default().push(durable.clone());
            }
        }
    }

    /// The batch `durable` is of refused whole, nothing of it readable: let
    /// go of, and told so, so a read that took it meanwhile acts on nothing
    /// it read then and reads again.
    pub(super) fn refused(&self, durable: &Durable) {
        durable.tell(Err("the write was refused".to_string()));
        self.0.locked().retain(|_, batches| {
            batches.retain(|batch| !batch.same(durable));
            !batches.is_empty()
        });
    }

    /// When the write that last moved `queue`'s head is durable, where it is
    /// not yet: none where no batch under way moved it. The latest is waited
    /// on alone, since a sync makes durable everything written before it.
    pub(super) fn head(&self, queue: u128) -> Option<Durable> {
        let moving = self.0.locked();
        let latest = moving.get(&queue)?.last()?;
        (latest.told() != Some(true)).then(|| latest.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const QUEUE: u128 = 7;

    fn head(queue: u128) -> Change {
        Change {
            watched: Watched::Head(queue),
            at: Some(1),
            own: None,
        }
    }

    #[test]
    fn a_head_moved_by_a_batch_under_way_waits_for_it_and_for_nothing_once_told() {
        let moving = Moving::default();
        assert!(moving.head(QUEUE).is_none(), "moved by none");
        let (first, second) = (Durable::pending(), Durable::pending());
        moving.entered(&[head(QUEUE)], &first);
        let tail = Change {
            watched: Watched::Queue(QUEUE + 1),
            ..head(QUEUE + 1)
        };
        moving.entered(&[head(QUEUE), tail], &second);
        assert!(
            moving.head(QUEUE).is_some_and(|d| d.same(&second)),
            "the latest"
        );
        assert!(moving.head(QUEUE + 1).is_none(), "a tail moved, no head");
        first.tell(Ok(()));
        second.tell(Ok(()));
        assert!(moving.head(QUEUE).is_none(), "durable: nothing to wait for");
        moving.entered(&[], &Durable::pending());
        assert!(moving.0.locked().is_empty(), "what was told let go of");
    }

    #[test]
    fn a_batch_refused_is_told_so_and_the_one_before_it_waited_on_again() {
        let moving = Moving::default();
        let (written, refused) = (Durable::pending(), Durable::pending());
        moving.entered(&[head(QUEUE)], &written);
        moving.entered(&[head(QUEUE)], &refused);
        let taken = moving.head(QUEUE).expect("the refused batch's");
        moving.refused(&refused);
        assert!(taken.wait().is_err(), "a read that took it acts on nothing");
        assert!(
            moving.head(QUEUE).is_some_and(|d| d.same(&written)),
            "the batch before it, still syncing"
        );
    }
}
