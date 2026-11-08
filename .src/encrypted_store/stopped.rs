//! A store stopped for good once a sync of its engine failed, or once the
//! one writer above it panicked with a batch in the engine: fail-stop.
//!
//! A sync that fails leaves the writes before it in the engine, readable
//! and answered, and not on disk — and an engine may still write them
//! later, or may not. Nothing can tell which, so nothing more is read or
//! written through the store: every write refused, so none is answered
//! over rows that may vanish, and every read refused, so nothing acts on a
//! row that may not survive. What the store holds is what the engine's log
//! holds once it is opened again — the store is opened anew over it.

use std::sync::OnceLock;

use crate::PersistError;

/// Why the store stopped, once it has.
#[derive(Default)]
pub(super) struct Stopped(OnceLock<String>);

impl Stopped {
    /// Nothing, while the store has not stopped.
    ///
    /// # Errors
    ///
    /// [`PersistError::Engine`] of `engine` once it has: why, and that it
    /// is opened again from its log.
    pub(super) fn check(&self, engine: &'static str) -> Result<(), PersistError> {
        match self.0.get() {
            None => Ok(()),
            Some(why) => Err(PersistError::engine(
                engine,
                format!(
                    "the store stopped: {why}; nothing is read or written until it is \
                     opened again from its log"
                ),
            )),
        }
    }

    /// The store stopped for `why`; the first reason stands.
    pub(super) fn stop(&self, why: String) {
        let _ = self.0.set(why);
    }
}
