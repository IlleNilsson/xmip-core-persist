//! The runtime database's syncs, on a thread of their own, so the one
//! writer never waits for the disk: a batch is decided, written and
//! answered, its [`Durable`] handed here, and the writer takes the next
//! batch while this syncs. Every batch handed here while a sync runs is
//! made durable by the next one, together — group commit of the syncs, as
//! the writer's batches are group commit of the writes (`deployment-model.md`
//! section 7). Until 2026-10-21 the writer synced each batch itself before
//! it read the next, so a Publication waited out the previous Message's
//! hand-on sync before its own was decided (open problem 33).
//!
//! A sync makes durable everything written before it began
//! (`Engine::sync`; `RocksDB` writes its log at a sync alone,
//! `manual_wal_flush`), and a batch's [`Durable`] reaches this thread only
//! once the batch is written, so the sync that tells it holds it.
//!
//! A sync that fails tells every batch it was for, and stops the store
//! (`EncryptedStore::sync`): every later batch is refused before it is
//! written, and every sync after it fails at once, so nothing is told
//! durable over writes the failed sync may have lost.

use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::JoinHandle;
use std::time::Duration;
use xcore::spent::Spent;

use super::super::durable::Durable;
use crate::{EncryptedStore, Engine, PersistError};

/// The sync thread, and the way to it.
pub(super) struct Syncer {
    written: Option<Sender<Durable>>,
    thread: Option<JoinHandle<()>>,
}

impl Syncer {
    /// The syncs of `store`.
    ///
    /// # Errors
    ///
    /// When the thread is not started.
    pub(super) fn start<R: Engine + 'static>(
        store: Arc<EncryptedStore<R>>,
    ) -> Result<Self, PersistError> {
        let (written, inbox) = mpsc::channel();
        let thread = std::thread::Builder::new()
            .name("xmip-storage-sync".to_string())
            .spawn(move || sync(&store, &inbox))
            .map_err(|error| PersistError::engine("xmip-storage", error))?;
        Ok(Self {
            written: Some(written),
            thread: Some(thread),
        })
    }

    /// `durable`, a batch written, told once a sync begun after it
    /// returns.
    pub(super) fn written(&self, durable: Durable) {
        let Some(written) = &self.written else {
            return durable.tell(Err("the Storage node's sync has stopped".to_string()));
        };
        if let Err(mpsc::SendError(durable)) = written.send(durable) {
            durable.tell(Err("the Storage node's sync has stopped".to_string()));
        }
    }
}

impl Drop for Syncer {
    /// Every batch handed over is synced before the thread ends.
    fn drop(&mut self) {
        self.written = None;
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// One sync for every batch written while the last ran, until the writer
/// is gone. What it waited for the disk is counted on an account of its
/// own (`xcore::spent::on_sync`) and shared equally by every write it made
/// durable, as the writer's work on a batch is ([`Durable::synced`]).
fn sync<R: Engine>(store: &EncryptedStore<R>, inbox: &Receiver<Durable>) {
    while let Ok(first) = inbox.recv() {
        let mut written = vec![first];
        written.extend(inbox.try_iter());
        let account = Spent::begin();
        let entered = account.entered();
        let synced = store.sync().map_err(|failed| failed.to_string());
        drop(entered);
        let writes = written.iter().fold(0_u32, |writes, durable| {
            writes.saturating_add(durable.writes())
        });
        let each = account.disk_sync(Duration::ZERO) / writes.max(1);
        for durable in written {
            durable.synced_each(each);
            durable.tell(synced.clone());
        }
    }
}
