//! Where what a wait watches stands, read from the database that keeps it
//! (`super::super::watch`): a queue's tail and head, an Instance and a key
//! from the runtime database, and the `membership` table and the operator's
//! acts asked of a node and each act's decision from the administration
//! database. Each kind has its arm here, and nowhere else. And what a test
//! reads of the watch: how many wait on one, and how many counts it keeps.
use super::super::watch::{self, Watched};
use super::Embedded;
use crate::{Engine, PersistError};

impl<R: Engine + 'static, A: Engine> Embedded<R, A> {
    /// Where `watched` stands now, read from the database that keeps it.
    pub(super) fn stands(&self, watched: Watched) -> Result<u64, PersistError> {
        match watched {
            Watched::Membership => self.members_written(),
            Watched::Acts(node) => self.acts_stand(node),
            Watched::Act(act) => self.act_stands(act),
            runtime => watch::stands(&self.runtime, runtime),
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
impl<R: Engine + 'static, A: Engine> Embedded<R, A> {
    /// How many wait on `watched` now: what a test waits for before it
    /// writes, so a wait is listed rather than begun after the write.
    #[must_use]
    pub fn waiting_on(&self, watched: Watched) -> usize {
        self.watch.waiting_on(watched)
    }

    /// How many counts its watch keeps, and of them how many none waits on
    /// now: what a test of its bound reads.
    #[must_use]
    pub fn watch_kept(&self) -> (usize, usize) {
        self.watch.kept()
    }
}
