//! What a Storage node is now, in which epoch (`super`): its role taken up
//! only once the epoch is kept, durable; and everyone waiting on it told as
//! it changes — the threads that wait for it to settle, and its node, which
//! publishes at once (open problem 140; ADR-0056, amendment 2026-11-24).

use std::sync::{Arc, PoisonError};
use std::time::{Duration, Instant};
use xcore::lock::{Locked, ReadWriteLocked};

use super::{EPOCH, Primary, StorageNode};
use crate::PersistError;

/// What a Storage node is now.
pub(super) enum Role {
    /// Asking the others, before it follows one or is promoted.
    Finding,
    /// The primary, and its databases.
    Primary(Arc<Primary>),
    /// A standby of the primary at this address.
    Standby(String),
}

/// Its role, and the latest epoch it met.
pub(super) struct Held {
    pub(super) epoch: u64,
    pub(super) role: Role,
}

/// What a Storage node tells as its standing changes.
pub(super) type Told = Arc<dyn Fn() + Send + Sync>;

impl StorageNode {
    /// `told` called each time what it is changes — promoted, standing by,
    /// handed its first frame, copied or caught up — on the thread that
    /// changed it: what its node publishes by at once, so a deployment
    /// waiting for it to be ready is told, never asking again and again
    /// (open problem 140).
    pub fn telling(&self, told: impl Fn() + Send + Sync + 'static) {
        *self.telling.locked() = Some(Arc::new(told));
    }

    /// What it is changed: told where it was asked to tell.
    pub(super) fn tell(&self) {
        let told = self.telling.locked().clone();
        if let Some(told) = told {
            told();
        }
    }

    /// Every wait on `settled` woken: what it waits on changed.
    pub(super) fn notify_settled(&self) {
        let _settling = self.settling.locked();
        self.settled.notify_all();
    }

    /// Wait while `holds`, `most` at the longest: whatever it waits on tells
    /// `settled` as it changes.
    pub(super) fn settle_while(&self, most: Duration, holds: impl Fn() -> bool) {
        let deadline = Instant::now() + most;
        let mut settling = self.settling.locked();
        while holds() {
            let Some(left) = deadline.checked_duration_since(Instant::now()) else {
                break;
            };
            settling = self
                .settled
                .wait_timeout(settling, left)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }

    /// `epoch` written, durable on return, where it is later than the one
    /// held, and held from then: before any role is taken in it, so a node
    /// started again never holds an epoch before one it took a role in.
    ///
    /// # Errors
    ///
    /// Where it could not be written; the epoch held is as it was then.
    pub(super) fn keep_epoch(&self, epoch: u64) -> Result<(), PersistError> {
        if epoch <= self.held().epoch {
            return Ok(());
        }
        self.engines[0].write(EPOCH, &epoch.to_be_bytes())?;
        let mut held = self.held.write_locked();
        held.epoch = held.epoch.max(epoch);
        Ok(())
    }

    /// `role` held from now, in `epoch`, the epoch kept first where it is
    /// later than the one kept ([`StorageNode::keep_epoch`]).
    ///
    /// # Errors
    ///
    /// Where the epoch could not be kept; no role is taken then.
    pub(super) fn take_up(&self, role: Role, epoch: u64) -> Result<(), PersistError> {
        self.keep_epoch(epoch)?;
        self.stand(role);
        Ok(())
    }

    /// `role` held from now, in the epoch held, told.
    pub(super) fn stand(&self, role: Role) {
        let was = std::mem::replace(&mut self.held.write_locked().role, role);
        // A primary no more: every wait on its queues refused, so the node
        // waiting moves to the new primary at once (`super::watch`).
        if let Role::Primary(primary) = was {
            primary.refuse_waits("this Storage node is the primary no more");
        }
        self.notify_settled();
        self.tell();
    }
}
