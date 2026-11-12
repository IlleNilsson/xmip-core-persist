//! The embedded Storage node opened: its three stores, each brought to this
//! build's schema, and its one writer started (`super`).

use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use secret::{KekName, KeyStore};
use xcore::{Clock, SystemClock};

use super::super::columns::Columns;
use super::super::commit::Committer;
use super::super::monotonic_clock::MonotonicClock;
use super::super::row;
use super::super::schema::Database;
use super::super::watch::Watch;
use super::{Embedded, versioned};
use crate::{EncryptedStore, Engine, PersistError};

impl<R: Engine + 'static, A: Engine> Embedded<R, A> {
    /// The node's databases over `runtime`, `administration` and `audit`,
    /// each sealed under its own data key wrapped by `keys` under `kek`.
    ///
    /// # Errors
    ///
    /// Where a store does not open — its key does not unwrap, its engine
    /// refuses — or the writer cannot start.
    pub fn open(
        runtime: R,
        administration: A,
        audit: A,
        keys: &dyn KeyStore,
        kek: &KekName,
    ) -> Result<Self, PersistError> {
        Self::over(
            EncryptedStore::open(runtime, keys, kek)?,
            EncryptedStore::open(administration, keys, kek)?,
            EncryptedStore::open(audit, keys, kek)?,
            Arc::new(SystemClock),
        )
    }

    /// The node over three stores already open, its time `clock` counted on
    /// monotonically (`MonotonicClock`), each brought to this build's schema.
    ///
    /// # Errors
    ///
    /// Where a database holds a schema version newer than this build's
    /// ([`PersistError::Schema`]), a store cannot record its version, or
    /// the writer cannot start.
    pub fn over(
        runtime: EncryptedStore<R>,
        administration: EncryptedStore<A>,
        audit: EncryptedStore<A>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, PersistError> {
        let now = row::nanos(clock.unix_timestamp_nanos());
        let versions = versioned::each((&runtime, &administration, &audit), now)?;
        let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::new(clock));
        let runtime = Arc::new(runtime);
        let runtime_columns = Arc::new(Columns::of(Database::Runtime));
        let epoch = Arc::new(AtomicU64::new(0));
        let watch = Arc::new(Watch::default());
        let committer = Committer::start(
            Arc::clone(&runtime),
            (
                Arc::clone(&clock),
                Arc::clone(&runtime_columns),
                Arc::clone(&epoch),
                Arc::clone(&watch),
            ),
        )?;
        Ok(Self {
            epoch,
            runtime_columns,
            administration_columns: Columns::of(Database::Administration),
            audit_columns: Columns::of(Database::Audit),
            runtime,
            administration,
            audit,
            committer,
            clock,
            administering: Mutex::new(()),
            keeping: Mutex::new(()),
            versions,
            watch,
        })
    }
}
