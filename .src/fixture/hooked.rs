//! An engine a test steers: every call handed to the engine beneath, but a
//! sync held until the test lets it go, a sync or a durable write failed,
//! or a panic once an unsynced write is in, on demand. How a test stalls
//! the Ledger between a write's answer and its sync, kills a process there,
//! or makes the disk refuse.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::{Duration, Instant};

use crate::{Change, Engine, Entry, PersistError, Reads};

/// What a test tells a [`Hooked`] engine to do.
#[derive(Default)]
pub struct Hooks {
    /// Whether a sync waits until let go.
    holding: Mutex<bool>,
    woken: Condvar,
    /// How many syncs wait now.
    held: AtomicUsize,
    failing_syncs: AtomicBool,
    failing_writes: AtomicBool,
    panicking: AtomicBool,
}

impl Hooks {
    /// Every sync from now on waits until [`Hooks::release_syncs`].
    pub fn hold_syncs(&self) {
        *self.holding.lock().unwrap_or_else(PoisonError::into_inner) = true;
    }

    /// Every sync waiting goes on, and none waits from now on.
    pub fn release_syncs(&self) {
        *self.holding.lock().unwrap_or_else(PoisonError::into_inner) = false;
        self.woken.notify_all();
    }

    /// Every sync from now on fails, or none does: one held fails once let
    /// go.
    pub fn fail_syncs(&self, failing: bool) {
        self.failing_syncs.store(failing, Ordering::SeqCst);
    }

    /// Every write durable on return fails from now on, or none does.
    pub fn fail_writes(&self, failing: bool) {
        self.failing_writes.store(failing, Ordering::SeqCst);
    }

    /// Every unsynced write from now on panics once it is in the engine,
    /// or none does.
    pub fn panic_after_deferred(&self, panicking: bool) {
        self.panicking.store(panicking, Ordering::SeqCst);
    }

    /// Whether a sync waits, by `within`.
    #[must_use]
    pub fn wait_held(&self, within: Duration) -> bool {
        let deadline = Instant::now() + within;
        while self.held.load(Ordering::SeqCst) == 0 {
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::yield_now();
        }
        true
    }

    fn sync(&self) -> Result<(), PersistError> {
        let holding = self.holding.lock().unwrap_or_else(PoisonError::into_inner);
        self.held.fetch_add(1, Ordering::SeqCst);
        let released = self
            .woken
            .wait_while(holding, |holding| *holding)
            .unwrap_or_else(PoisonError::into_inner);
        self.held.fetch_sub(1, Ordering::SeqCst);
        drop(released);
        if self.failing_syncs.load(Ordering::SeqCst) {
            return Err(PersistError::engine("hooked", "the sync failed, as told"));
        }
        Ok(())
    }

    fn write(&self) -> Result<(), PersistError> {
        if self.failing_writes.load(Ordering::SeqCst) {
            return Err(PersistError::engine("hooked", "the write failed, as told"));
        }
        Ok(())
    }
}

/// `engine`, steered by its [`Hooks`].
pub struct Hooked<E> {
    engine: E,
    hooks: Arc<Hooks>,
}

impl<E: Engine> Hooked<E> {
    /// `engine`, steered by `hooks`.
    pub fn new(engine: E, hooks: &Arc<Hooks>) -> Self {
        Self {
            engine,
            hooks: Arc::clone(hooks),
        }
    }

    /// The engine beneath.
    pub fn beneath(&self) -> &E {
        &self.engine
    }
}

impl<E: Engine> Engine for Hooked<E> {
    fn engine(&self) -> &'static str {
        self.engine.engine()
    }

    fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, PersistError> {
        self.engine.read(key)
    }

    fn write(&self, key: &[u8], value: &[u8]) -> Result<(), PersistError> {
        self.hooks.write()?;
        self.engine.write(key, value)
    }

    fn write_new(&self, key: &[u8], value: &[u8]) -> Result<bool, PersistError> {
        self.hooks.write()?;
        self.engine.write_new(key, value)
    }

    fn remove(&self, key: &[u8]) -> Result<(), PersistError> {
        self.hooks.write()?;
        self.engine.remove(key)
    }

    fn apply(&self, batch: &[Change]) -> Result<(), PersistError> {
        self.hooks.write()?;
        self.engine.apply(batch)
    }

    fn apply_deferred(&self, batch: &[Change]) -> Result<(), PersistError> {
        self.engine.apply_deferred(batch)?;
        assert!(
            !self.hooks.panicking.load(Ordering::SeqCst),
            "a panic once the write is in the engine, as told"
        );
        Ok(())
    }

    fn sync(&self) -> Result<(), PersistError> {
        self.hooks.sync()?;
        self.engine.sync()
    }

    fn scan(
        &self,
        first: &[u8],
        last: &[u8],
        most: usize,
        reverse: bool,
    ) -> Result<Vec<Entry>, PersistError> {
        self.engine.scan(first, last, most, reverse)
    }

    fn view(&self) -> Result<Box<dyn Reads + '_>, PersistError> {
        self.engine.view()
    }
}
