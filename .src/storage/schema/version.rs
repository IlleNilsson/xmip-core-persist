//! The schema version each database holds, and how one that holds an
//! older version reaches this build's (open problem 41; the owner,
//! 2026-10-19, agreeing to the proposal as written: *each database records
//! its schema version, and the upgrade scripts are generated as the
//! difference between releases, adding only*).
//!
//! **Every database records it**, in a table of its own, `schema_version`:
//! one row for each version it has held, the one it was made at and each
//! an upgrade reached, with when. The version it holds is the highest. On a
//! database server the scripts write the rows (`super::scripts`,
//! `super::upgrade`); on an embedded Storage node the node itself
//! (`super::super::embedded`).
//!
//! **A version is everything the schema holds at it.** Every table, column
//! and index says the version that added it (`since`); the schema at a
//! version is what was added at it or before, and the difference between
//! two releases is what was added after the first, up to the second. There
//! is no way to say a drop or a rename: an upgrade only adds.
//!
//! **A database newer than the build is refused**, in words naming both
//! versions: a database is never stepped down.

use super::Database;

/// The schema version this build makes and reads. Until the first release
/// the schema is version 1 and changes in place: nothing is released, so
/// nothing is upgraded (CONTRIBUTING: deleted, never deprecated).
pub const VERSION: u32 = 1;

/// The table every database records its schema versions in.
pub const VERSIONS: &str = "schema_version";

/// The schema version one database holds, and what it is kept on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DatabaseVersion {
    pub database: Database,
    /// The version it holds: the highest it recorded.
    pub version: u32,
    /// What it is kept on, in the engine's or the server's own word.
    pub engine: &'static str,
}

impl DatabaseVersion {
    /// What a node publishes of it: `schema version <n> on <engine>`.
    #[must_use]
    pub fn evidence(&self) -> String {
        format!("schema version {} on {}", self.version, self.engine)
    }
}

/// The version a database holds, asked of it one version at a time by
/// `recorded`, whether it recorded that version: from `built`, the
/// build's, upward while it did — a database newer than the build — or
/// downward until it did. `None` for a database that recorded none: one
/// just made. A start reads a handful of rows.
///
/// # Errors
///
/// What `recorded` could not read.
pub fn found<E>(
    mut recorded: impl FnMut(u32) -> Result<bool, E>,
    built: u32,
) -> Result<Option<u32>, E> {
    if recorded(built)? {
        let mut held = built;
        while held < u32::MAX && recorded(held + 1)? {
            held += 1;
        }
        return Ok(Some(held));
    }
    for version in (1..built).rev() {
        if recorded(version)? {
            return Ok(Some(version));
        }
    }
    Ok(None)
}

/// The versions `database`, holding `held`, records to reach `built`, in
/// order: `built` alone for one just made, each after `held` for one that
/// holds an older version, and none for one at `built`.
///
/// # Errors
///
/// The refusal, in words naming both versions, where it holds a version
/// newer than `built`.
pub fn steps(
    database: Database,
    held: Option<u32>,
    built: u32,
) -> Result<std::ops::RangeInclusive<u32>, String> {
    match held {
        None => Ok(built..=built),
        Some(held) if held > built => Err(format!(
            "the {} database holds schema version {held}, newer than this build's {built}: \
             start it with a build of version {held} or later; a database is never stepped down",
            database.word()
        )),
        Some(held) => Ok(held + 1..=built),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn held(versions: &[u32], built: u32) -> Option<u32> {
        let asked = |version: u32| Ok::<_, ()>(versions.contains(&version));
        found(asked, built).expect("read")
    }

    #[test]
    fn the_version_held_is_the_highest_recorded_whichever_side_of_the_build_it_is() {
        assert_eq!(held(&[], 3), None);
        assert_eq!(held(&[1], 3), Some(1));
        assert_eq!(held(&[1, 2, 3], 3), Some(3));
        assert_eq!(held(&[3], 3), Some(3), "made at it");
        assert_eq!(held(&[1, 2, 3, 4, 5], 3), Some(5), "newer");
    }

    #[test]
    fn a_database_steps_up_to_the_build_and_never_down() {
        let runtime = Database::Runtime;
        assert_eq!(steps(runtime, None, 1), Ok(1..=1), "made at it");
        assert_eq!(steps(runtime, Some(1), 1).map(Iterator::count), Ok(0));
        assert_eq!(steps(runtime, Some(1), 3), Ok(2..=3));
        let refused = steps(Database::Audit, Some(4), 3).expect_err("newer");
        assert!(
            refused.starts_with(
                "the audit database holds schema version 4, newer than this \
                 build's 3"
            ),
            "{refused}"
        );
    }
}
