//! The Storage epoch the embedded Storage node is the primary in: the high
//! half of every claim's fence (`super::super::claim::fence`; the owner,
//! 2026-10-27).

use std::sync::atomic::Ordering;

use super::Embedded;
use crate::Engine;

impl<R: Engine + 'static, A: Engine> Embedded<R, A> {
    /// It is the primary in the Storage `epoch`: every claim it takes from
    /// now is fenced by it, above every fence of an earlier epoch.
    pub fn in_epoch(&self, epoch: u64) {
        self.epoch.fetch_max(epoch, Ordering::AcqRel);
    }
}
