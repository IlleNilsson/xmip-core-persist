//! A queue's places, and a sequence's head, asked of a Storage node
//! (`XmipStorage::read_held`, `XmipStorage::read_head`): the head answered
//! by the Storage node once the write that last moved it is durable there,
//! so a node sends the next of a sequence on it only then (the owner,
//! 2026-11-15: *Wait for the disk*; ADR-0018, amendment 2026-11-15).

use super::super::hold::{Head, HeldQueue};
use super::super::wire::{Answer, Request};
use super::StorageClient;
use super::expected::expected;
use crate::PersistError;

impl StorageClient {
    /// What `queue` holds, up to `most` of its places from `from` on.
    pub(super) fn places(
        &self,
        queue: u128,
        (from, most): (u64, u32),
    ) -> Result<HeldQueue, PersistError> {
        let asked = Request::ReadHeld(queue, from, most);
        expected(self.ask(&asked)?, held_queue)
    }

    /// The sequence `queue`'s first `most` places, answered once durable
    /// on the Storage node: nothing to wait for here.
    pub(super) fn head(&self, queue: u128, most: u32) -> Result<Head, PersistError> {
        let held = expected(self.ask(&Request::ReadHead(queue, most))?, held_queue)?;
        Ok(Head { held, moving: None })
    }
}

/// A queue as the Storage node read it, or the answer that is not one.
fn held_queue(answer: Answer) -> Result<HeldQueue, Answer> {
    match answer {
        Answer::Held(queue) => Ok(queue),
        other => Err(other),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use xcore::JourneyId;

    use super::super::ahead::tests::served;
    use crate::fixture::crowd::{LEASE, QUEUE, hand_on, publication};
    use crate::storage::XmipStorage;

    #[test]
    fn a_head_read_over_the_wire_is_answered_once_the_hand_on_that_moved_it_is_durable() {
        let (server, client, hooks) = served(Duration::from_secs(10));
        let client = Arc::new(client);
        client.publish(publication(1..3)).expect("published");
        let holder = configure::fixture::test_cluster().node_scope(0);
        let claim = client.claim(JourneyId::new(1), &holder, 1, LEASE);
        let mut first = hand_on(claim.expect("asked").expect("claimed"));
        first.leaves = vec![QUEUE];
        hooks.hold_syncs();
        let handing = {
            let client = Arc::clone(&client);
            std::thread::spawn(move || client.hand_on(first))
        };
        assert!(hooks.wait_held(Duration::from_secs(10)), "its sync held");
        let reading = {
            let client = Arc::clone(&client);
            std::thread::spawn(move || client.read_head(QUEUE, 1))
        };
        // A while for an answer that would not wait for the disk.
        std::thread::sleep(Duration::from_millis(50));
        assert!(!reading.is_finished(), "unanswered while the hand-on syncs");
        hooks.release_syncs();
        let head = reading.join().expect("joined").expect("read");
        assert_eq!(head.held.held[0].hold.journey, JourneyId::new(2), "moved");
        assert!(head.moving.is_none(), "durable once answered");
        assert!(handing.join().expect("joined").expect("handed on").0);
        server.stop();
    }
}
