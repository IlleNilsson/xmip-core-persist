//! Each of the embedded Storage node's databases brought to this build's
//! schema version as it opens (`super::super::schema::version`; open
//! problem 41): the version it holds read from its `schema_version` table,
//! each version after it recorded, and one newer than the build refused in
//! words.
//!
//! **A step adds nothing else here.** The embedded engines keep sealed
//! records, not columns: a table added is a kind of record no record of
//! the database had, and a column added a value a record written before
//! lacks, read as nothing. An index added to a table already made would
//! need its entries written for the records already kept, and the
//! definition refuses one until a step does (`super::super::schema`'s
//! upgrade).
//!
//! Each row is the version as its key, big-endian, and when it was
//! recorded, nanoseconds since the Unix epoch on Xmip Storage's clock.

use super::super::schema::Database;
use super::super::schema::version::{self, DatabaseVersion, VERSION, VERSIONS};
use super::Embedded;
use crate::{EncryptedStore, Engine, PersistError, RecordChange};

impl<R: Engine + 'static, A: Engine> Embedded<R, A> {
    /// The schema version each database holds — runtime, administration,
    /// audit — brought to this build's as it opened: what the node
    /// keeping them publishes.
    #[must_use]
    pub const fn versions(&self) -> &[DatabaseVersion; 3] {
        &self.versions
    }
}

/// The runtime, the administration and the audit databases, each at this
/// build's [`VERSION`], `now` being Xmip Storage's time: the version each
/// holds now, in that order.
///
/// # Errors
///
/// As [`reached`], for the first that is refused.
pub(super) fn each<R: Engine, A: Engine>(
    (runtime, administration, audit): (&EncryptedStore<R>, &EncryptedStore<A>, &EncryptedStore<A>),
    now: u64,
) -> Result<[DatabaseVersion; 3], PersistError> {
    Ok([
        reached(runtime, Database::Runtime, VERSION, now)?,
        reached(administration, Database::Administration, VERSION, now)?,
        reached(audit, Database::Audit, VERSION, now)?,
    ])
}

/// `store`, the database `database`, at schema version `built`, `now`
/// being Xmip Storage's time: the version it holds now. The rows each
/// version after the one it held are written in one write, durable on
/// return, before the node writes anything else to it.
///
/// # Errors
///
/// [`PersistError::Schema`] where it holds a version newer than `built`,
/// and what the store could not read or write.
pub(super) fn reached<E: Engine>(
    store: &EncryptedStore<E>,
    database: Database,
    built: u32,
    now: u64,
) -> Result<DatabaseVersion, PersistError> {
    let recorded = |held: u32| -> Result<bool, PersistError> {
        Ok(store.get(VERSIONS, &held.to_be_bytes())?.is_some())
    };
    let held = version::found(recorded, built)?;
    let steps =
        version::steps(database, held, built).map_err(|reason| PersistError::Schema { reason })?;
    let rows: Vec<RecordChange<'_>> = steps
        .map(|step| {
            let row = now.to_be_bytes().to_vec();
            (VERSIONS, step.to_be_bytes().to_vec(), Some(row))
        })
        .collect();
    if !rows.is_empty() {
        store.apply(&rows)?;
    }
    Ok(DatabaseVersion {
        database,
        version: built,
        engine: store.engine().engine(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::Memory;
    use secret::{Held, KekName};

    fn opened(engine: &Memory) -> EncryptedStore<&Memory> {
        let keys = Held::new(secret::fixture::Memory::default());
        let kek = KekName::new("storage").expect("name");
        EncryptedStore::open(engine, &keys, &kek).expect("opened")
    }

    fn rows(store: &EncryptedStore<&Memory>) -> Vec<u32> {
        (1..=8)
            .filter(|at: &u32| {
                let row = store.get(VERSIONS, &at.to_be_bytes()).expect("read");
                row.is_some()
            })
            .collect()
    }

    #[test]
    fn a_database_is_recorded_at_the_build_and_steps_up_to_a_later_one_never_down() {
        let engine = Memory::default();
        let store = opened(&engine);
        let made = reached(&store, Database::Runtime, 1, 7).expect("made");
        assert_eq!((made.version, made.engine), (1, "memory"));
        assert_eq!(made.evidence(), "schema version 1 on memory");
        assert_eq!(rows(&store), [1]);
        let before = engine.everything();
        reached(&store, Database::Runtime, 1, 8).expect("again");
        assert_eq!(
            engine.everything(),
            before,
            "at it already: nothing written"
        );

        let later = reached(&store, Database::Runtime, 3, 9).expect("stepped");
        assert_eq!(later.version, 3);
        assert_eq!(rows(&store), [1, 2, 3]);
        let kept = store.get(VERSIONS, &3_u32.to_be_bytes()).expect("read");
        assert_eq!(
            kept,
            Some(9_u64.to_be_bytes().to_vec()),
            "when it was recorded"
        );

        let refused = reached(&store, Database::Runtime, 2, 10).expect_err("newer");
        let said = refused.to_string();
        assert!(
            said.starts_with(
                "the runtime database holds schema version 3, newer than this \
                 build's 2"
            ),
            "{said}"
        );
        assert_eq!(rows(&store), [1, 2, 3], "nothing written");
    }

    #[test]
    fn a_storage_node_opens_each_database_at_the_build_and_refuses_one_newer() {
        let runtime: &'static Memory = Box::leak(Box::default());
        let (administration, audit) = (Memory::default(), Memory::default());
        let keys = Held::new(secret::fixture::Memory::default());
        let kek = KekName::new("storage").expect("name");
        let open = || super::super::Embedded::open(runtime, &administration, &audit, &keys, &kek);
        let node = open().expect("opened");
        let said: Vec<String> = node
            .versions()
            .iter()
            .map(|held| format!("{}: {}", held.database.word(), held.evidence()))
            .collect();
        let at = version::VERSION;
        assert_eq!(
            said,
            [
                format!("runtime: schema version {at} on memory"),
                format!("administration: schema version {at} on memory"),
                format!("audit: schema version {at} on memory"),
            ]
        );
        drop(node);
        let newer = EncryptedStore::open(&audit, &keys, &kek).expect("the audit database");
        let row = 0_u64.to_be_bytes();
        newer
            .put(VERSIONS, &(at + 1).to_be_bytes(), &row)
            .expect("recorded");
        let refused = open().err().expect("newer than the build");
        let said = refused.to_string();
        assert!(
            said.starts_with("the audit database holds schema version"),
            "{said}"
        );
    }
}
