//! What the embedded Storage node's one writer answered, as each operation
//! that submits to it reads the answer (`super::super::commit`): written,
//! a claim — once durable, or ahead of its sync with when it is durable —
//! the claims a Publication's node holds — answered before its sync, with
//! when it is durable — or a verdict.

use super::super::commit::{Done, Op};
use super::super::durable::Durable;
use super::super::record::{Claim, malformed};
use super::Embedded;
use crate::{Engine, PersistError};

impl<R: Engine + 'static, A: Engine> Embedded<R, A> {
    pub(super) fn written(&self, op: Op) -> Result<(), PersistError> {
        self.committer.submit(op).map(drop)
    }

    pub(super) fn claimed(&self, op: Op) -> Result<Option<Claim>, PersistError> {
        let (claim, durable) = self.claimed_ahead(op)?;
        durable.wait()?;
        Ok(claim)
    }

    /// A claim, answered before its batch is synced, and when it is
    /// durable: what a send under it lets its far end see it by (ADR-0018,
    /// amendment 2026-11-12).
    pub(super) fn claimed_ahead(&self, op: Op) -> Result<(Option<Claim>, Durable), PersistError> {
        match self.committer.ahead(op)? {
            (Done::Claim(claim), durable) => Ok((claim, durable)),
            _ => Err(malformed("the writer answered no claim")),
        }
    }

    pub(super) fn held(&self, op: Op) -> Result<Vec<Claim>, PersistError> {
        match self.committer.submit(op)? {
            Done::Claims(held) => Ok(held),
            _ => Err(malformed("the writer answered no claims")),
        }
    }

    /// The claims a Publication's node holds, answered before its batch
    /// is synced, and when it is durable.
    pub(super) fn held_ahead(&self, op: Op) -> Result<(Vec<Claim>, Durable), PersistError> {
        match self.committer.ahead(op)? {
            (Done::Claims(held), durable) => Ok((held, durable)),
            _ => Err(malformed("the writer answered no claims")),
        }
    }

    pub(super) fn yes(&self, op: Op) -> Result<bool, PersistError> {
        match self.committer.submit(op)? {
            Done::Yes(yes) => Ok(yes),
            _ => Err(malformed("the writer answered no verdict")),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use secret::{Held, KekName};
    use xcore::JourneyId;

    use crate::fixture::{Hooked, Hooks, Memory};
    use crate::storage::{Embedded, XmipStorage};

    type Node = Embedded<Hooked<Memory>, Memory>;

    /// An embedded Storage node whose runtime database's syncs `hooks` steer.
    fn hooked() -> (Node, Arc<Hooks>) {
        let hooks = Arc::new(Hooks::default());
        let keys = Held::new(secret::fixture::Memory::default());
        let kek = KekName::new("storage").expect("a name");
        let runtime = Hooked::new(Memory::default(), &hooks);
        let node = Embedded::open(runtime, Memory::default(), Memory::default(), &keys, &kek);
        (node.expect("opened"), hooks)
    }

    const JOURNEY: JourneyId = JourneyId::new(0x0199_0000_0000_7000_8000_0000_0000_00a1);
    const LEASE: Duration = Duration::from_secs(30);

    fn holder() -> String {
        configure::fixture::test_cluster().node_scope(0)
    }

    #[test]
    fn a_claim_ahead_is_answered_while_its_sync_is_held_and_durable_once_it_is_not() {
        let (node, hooks) = hooked();
        hooks.hold_syncs();
        let (taken, durable) = node
            .claim_ahead(JOURNEY, &holder(), 7, LEASE)
            .expect("answered");
        let taken = taken.expect("taken");
        assert!(hooks.wait_held(Duration::from_secs(10)), "its sync is held");
        assert_eq!(durable.told(), None, "readable, not durable");
        // Asked again under its token: that claim, durable with it.
        let (again, repeated) = node
            .claim_ahead(JOURNEY, &holder(), 7, LEASE)
            .expect("answered");
        assert_eq!(again, Some(taken.clone()), "the claim it holds");
        assert_eq!(repeated.told(), None, "not durable before it");
        // Another holder finds it held, readable or not.
        let other = configure::fixture::test_cluster().node_scope(1);
        let (refused, _) = node.claim_ahead(JOURNEY, &other, 8, LEASE).expect("asked");
        assert_eq!(refused, None);
        hooks.release_syncs();
        durable.wait().expect("durable");
        repeated.wait().expect("durable with it");
        assert_eq!(durable.told(), Some(true));
    }

    #[test]
    fn a_claim_ahead_whose_sync_fails_is_never_durable() {
        let (node, hooks) = hooked();
        hooks.hold_syncs();
        let (taken, durable) = node
            .claim_ahead(JOURNEY, &holder(), 7, LEASE)
            .expect("answered");
        assert!(taken.is_some(), "taken, readable");
        assert!(hooks.wait_held(Duration::from_secs(10)), "its sync is held");
        hooks.fail_syncs(true);
        hooks.release_syncs();
        assert!(durable.wait().is_err(), "lost: never durable");
        assert_eq!(durable.told(), Some(false));
        let stopped = node.claim(JOURNEY, &holder(), 7, LEASE);
        assert!(stopped.is_err(), "the Storage node stopped (fail-stop)");
    }
}
