//! The two tables a Work Process's Instances are kept in, in the runtime
//! database (`runtime-model.md` section 22; ADR-0066, amendments 2026-10-22
//! and 2026-11-25): Process Instance, searched by its Work Process, newest
//! written first, and Instance Key beside it. `super::tables` lists them
//! among every table, in the order the scripts make them.

use super::searchable::{PROCESS_INSTANCE, PROCESS_INSTANCE_INDEXES, PROCESS_INSTANCE_KEY};
use super::{Database, Table};

/// Process Instance.
pub(super) const INSTANCE_TABLE: Table = Table {
    database: Database::Runtime,
    name: "process_instance",
    since: 1,
    keeps: "every Work Process Instance, by its identifier: its state in the form the \
            Work Process engine writes, sealed, written whole at every turn only over \
            the turn it was read at, with its Work Process, its turns, whether it has \
            ended and when it was last written in columns of their own",
    columns: PROCESS_INSTANCE,
    key: &["instance"],
    unique: &[],
    indexes: PROCESS_INSTANCE_INDEXES,
};

/// Instance Key.
pub(super) const INSTANCE_KEY_TABLE: Table = Table {
    database: Database::Runtime,
    name: "instance_key",
    since: 1,
    keeps: "each key a Work Process Instance is correlated by, named from its Work \
            Process, Correlation and values, and the Instance it belongs to; written \
            only where it belongs to no Instance or to the one writing it",
    columns: PROCESS_INSTANCE_KEY,
    key: &["instance_key"],
    unique: &[],
    indexes: &[],
};
