//! Where what a wait watches stands, read from the database that keeps it
//! (`super::super::watch`): a queue's tail and head, an Instance and a key
//! from the runtime database, and the `membership` table and the operator's
//! acts asked of a node and each act's decision from the administration
//! database; and who follows it, as a primary of several counts it, from
//! no database. Each kind has its arm here, and nowhere else. And what a
//! test reads of the watch: how many wait on one, and how many counts it
//! keeps.
use std::sync::atomic::Ordering;

use super::super::watch::{self, Change, Watched};
use super::Embedded;
use crate::{Engine, PersistError};

impl<R: Engine + 'static, A: Engine> Embedded<R, A> {
    /// Where `watched` stands now, read from the database that keeps it.
    pub(super) fn stands(&self, watched: Watched) -> Result<u64, PersistError> {
        match watched {
            Watched::Membership => self.members_written(),
            Watched::Acts(node) => self.acts_stand(node),
            Watched::Act(act) => self.act_stands(act),
            Watched::Standbys => Ok(self.standbys_stand()),
            runtime => watch::stands(&self.runtime, runtime),
        }
    }

    /// Who follows it changed — a standby began or ended following
    /// (`super::super::standby::followers`) — told to every wait on it.
    pub(crate) fn standbys_moved(&self) {
        self.standbys.fetch_add(1, Ordering::AcqRel);
        self.watch.moved(&[Change {
            watched: Watched::Standbys,
            at: Some(self.standbys_stand()),
            own: None,
        }]);
    }

    /// Who follows it, as a count: its epoch above how many times that
    /// changed in it, so the count of another primary, in another epoch, is
    /// never taken for its own.
    fn standbys_stand(&self) -> u64 {
        let changed = self.standbys.load(Ordering::Acquire) & u64::from(u32::MAX);
        (self.epoch.load(Ordering::Acquire) << 32) | changed
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
