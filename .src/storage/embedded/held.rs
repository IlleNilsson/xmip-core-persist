//! A queue's places read from the runtime database, and a sequence's head
//! read with when the write that last moved it is durable
//! (`super::super::XmipStorage::read_head`; ADR-0018, amendment
//! 2026-11-15).

use super::super::hold::{self, Head, HeldQueue};
use super::Embedded;
use crate::{Engine, PersistError};

impl<R: Engine + 'static, A: Engine> Embedded<R, A> {
    /// What `queue` holds, up to `most` of its places from `from` on, as
    /// the runtime database holds it now, readable and maybe not yet
    /// durable.
    pub(super) fn places(&self, queue: u128, from: (u64, u32)) -> Result<HeldQueue, PersistError> {
        let now = self.clock.unix_timestamp_nanos();
        hold::read(&self.runtime, queue, from, now)
    }

    /// The sequence `queue`'s first `most` places, and the batch under way
    /// that last moved its head, where one did: looked up once read, since
    /// a batch's heads are known before it is readable
    /// (`super::super::commit`).
    pub(super) fn head(&self, queue: u128, most: u32) -> Result<Head, PersistError> {
        let held = self.places(queue, (0, most))?;
        let moving = self.committer.moving(queue);
        Ok(Head { held, moving })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use secret::{Held, KekName};
    use xcore::JourneyId;

    use crate::fixture::crowd::{LEASE, QUEUE, hand_on, publication};
    use crate::fixture::{Hooked, Hooks, Memory};
    use crate::storage::{Embedded, XmipStorage};

    #[test]
    fn a_head_moved_by_a_hand_on_not_yet_durable_is_read_with_when_it_is() {
        let hooks = Arc::new(Hooks::default());
        let keys = Held::new(secret::fixture::Memory::default());
        let kek = KekName::new("storage").expect("a name");
        let runtime = Hooked::new(Memory::default(), &hooks);
        let node = Embedded::open(runtime, Memory::default(), Memory::default(), &keys, &kek);
        let node = Arc::new(node.expect("opened"));
        let (_, durable) = node.publish(publication(1..3)).expect("published");
        durable.wait().expect("durable");
        let head = node.read_head(QUEUE, 1).expect("read");
        assert!(head.moving.is_none(), "no write under way moved it");
        let holder = configure::fixture::test_cluster().node_scope(0);
        let claim = node.claim(JourneyId::new(1), &holder, 1, LEASE);
        let mut first = hand_on(claim.expect("asked").expect("claimed"));
        first.leaves = vec![QUEUE];
        hooks.hold_syncs();
        let handing = {
            let node = Arc::clone(&node);
            std::thread::spawn(move || node.hand_on(first))
        };
        assert!(hooks.wait_held(Duration::from_secs(10)), "its sync held");
        let head = node.read_head(QUEUE, 1).expect("read");
        assert_eq!(
            head.held.held[0].hold.journey,
            JourneyId::new(2),
            "the next heads it: the hand-on is readable"
        );
        let moving = head.moving.expect("the hand-on, not yet durable");
        assert_eq!(moving.told(), None);
        hooks.release_syncs();
        assert!(moving.wait().is_ok(), "durable once synced");
        let (handed, _) = handing.join().expect("joined").expect("handed on");
        assert!(handed);
        let head = node.read_head(QUEUE, 1).expect("read");
        assert!(head.moving.is_none(), "durable: nothing to wait for");
    }
}
