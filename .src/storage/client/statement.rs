//! One statement, one Storage node (the owner, 2026-10-03; `super`): a
//! client over the Storage node a statement began on, every operation of
//! the statement asked of it alone.

use std::sync::Arc;

use super::super::XmipStorage;
use super::StorageClient;

impl StorageClient {
    /// This client over one Storage node, for one statement: the one it is
    /// over already, or the next round robin ([`XmipStorage::pinned`]).
    pub(super) fn pinned_client(&self) -> Option<Self> {
        let index = self.pinned.or_else(|| self.turn().next())?;
        Some(self.pinned_to(index))
    }

    /// This client over the Storage node listed at `index` alone.
    pub(super) fn pinned_to(&self, index: usize) -> Self {
        Self {
            nodes: Arc::clone(&self.nodes),
            next: Arc::clone(&self.next),
            config: Arc::clone(&self.config),
            timeout: self.timeout,
            pass_over: self.pass_over,
            epoch: Arc::clone(&self.epoch),
            pinned: Some(index),
        }
    }
}

/// `storage` for one statement: a receive cycle's chunks, its Publication
/// and its Journeys asked of one Storage node, so the Publication's one
/// sync covers the chunks before it ([`XmipStorage::write_chunk`]).
#[must_use]
pub fn statement(storage: &Arc<dyn XmipStorage>) -> Arc<dyn XmipStorage> {
    storage.pinned().unwrap_or_else(|| Arc::clone(storage))
}
