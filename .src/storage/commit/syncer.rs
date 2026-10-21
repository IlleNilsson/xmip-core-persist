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

use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::JoinHandle;

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
/// is gone.
fn sync<R: Engine>(store: &EncryptedStore<R>, inbox: &Receiver<Durable>) {
    while let Ok(first) = inbox.recv() {
        let mut written = vec![first];
        written.extend(inbox.try_iter());
        let synced = store.sync().map_err(|failed| failed.to_string());
        for durable in written {
            durable.tell(synced.clone());
        }
    }
}
