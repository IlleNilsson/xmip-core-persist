//! The upgrade scripts: the difference between two releases' schemas, in
//! each server's dialect, adding only (open problem 41; the owner,
//! 2026-10-19, agreeing to *the upgrade scripts are generated as the
//! difference between releases, adding only*).
//!
//! **The difference is what was added.** Every table, column and index
//! says the version that added it (`since`), so what an upgrade from one
//! release to a later one makes is everything added after the first, up to
//! the second: a table made whole, a column added to a table already there,
//! an index made; then each version reached recorded in `schema_version`.
//! Nothing is dropped or renamed — the definition has no way to say it.
//!
//! **What the definition may say** ([`problems`]): a version from 1 to the
//! build's; nothing of a table older than the table; a column added to a
//! table already made holds nothing in the rows already there, so it is
//! one that may hold nothing; a key is the table's from the first. And an
//! index added to a table already made is refused for now: an embedded
//! Storage node keeps an index as entries written with each record, and no
//! step yet writes them for the records already kept (drafted for the
//! owner, 2026-10-20).

use std::fmt::Write as _;

use sha2::{Digest, Sha256};

use super::super::database::Server;
use super::version::VERSION;
use super::{Database, TABLES, Table, dialect, indexed, made, run_in};

/// The released schema versions and the fingerprint of each
/// ([`fingerprint`]): a released schema never changes again, so a test
/// fails where the definition no longer holds one as it was released.
/// Empty until the first release, which records version 1.
pub const RELEASED: &[(u32, &str)] = &[];

/// An upgrade script's file name: `04-<domain>-upgrade-<from>-<to>.sql`.
pub(super) fn name(database: Database, from: u32, to: u32) -> String {
    format!("04-{}-upgrade-{from}-{to}.sql", database.word())
}

/// Every script an operator runs on `server` to bring each database from
/// schema version `from` to `to`, at most this build's [`VERSION`]: its
/// file name and its text, one to each database.
///
/// # Errors
///
/// In words, where `from` is not a release before `to` or `to` is past
/// this build's version.
pub fn upgrade(server: Server, from: u32, to: u32) -> Result<Vec<(String, String)>, String> {
    upgrade_of(&TABLES, VERSION, server, (from, to))
}

fn upgrade_of(
    tables: &[Table],
    built: u32,
    server: Server,
    (from, to): (u32, u32),
) -> Result<Vec<(String, String)>, String> {
    if from == 0 || from >= to || to > built {
        return Err(format!(
            "an upgrade is from one schema version to a later one, from 1 to this build's \
             {built}; {from} to {to} is none"
        ));
    }
    let problems = problems(tables, built);
    if !problems.is_empty() {
        return Err(problems.join("; "));
    }
    Ok(Database::ALL
        .into_iter()
        .map(|database| {
            let text = script(tables, server, database, from, to);
            (name(database, from, to), text)
        })
        .collect())
}

/// What `tables`, a schema at most at version `built`, says that a
/// schema may not, in words naming the table.
pub(super) fn problems(tables: &[Table], built: u32) -> Vec<String> {
    let mut problems = Vec::new();
    for table in tables {
        let mut said = |problem: String| problems.push(format!("{}: {problem}", table.name));
        let versions = std::iter::once(table.since)
            .chain(table.columns.iter().map(|column| column.since))
            .chain(table.indexes.iter().map(|index| index.since));
        if versions
            .into_iter()
            .any(|since| since == 0 || since > built)
        {
            said(format!("a version is from 1 to {built}"));
        }
        for column in table.columns {
            if column.since < table.since {
                said(format!("{} is older than its table", column.name));
            }
            if column.since > table.since && !column.null {
                said(format!(
                    "{} is added to a table already made, so it may hold nothing",
                    column.name
                ));
            }
            let keyed = table.key.iter().chain(table.unique);
            if column.since > table.since && keyed.into_iter().any(|key| *key == column.name) {
                said(format!(
                    "{} is a key, and a key is the table's from the first",
                    column.name
                ));
            }
        }
        for index in table.indexes {
            if index.since > table.since {
                said(format!(
                    "{} is an index added to a table already made, which an embedded Storage \
                     node cannot fill yet for the records it keeps",
                    index.name
                ));
            }
        }
    }
    problems
}

/// The upgrade of `database` on `server` from `from` to `to`.
pub(super) fn script(
    tables: &[Table],
    server: Server,
    database: Database,
    from: u32,
    to: u32,
) -> String {
    let what = format!(
        "the {} database's schema from version {from} to {to}",
        database.word()
    );
    let mut text = dialect::head(server, &what, &run_in(server, database));
    text.push_str(&dialect::opened(server, database));
    let added = |since: u32| from < since && since <= to;
    for table in tables.iter().filter(|table| table.database == database) {
        if added(table.since) {
            text.push_str(&made(server, table, to));
            continue;
        }
        if table.since > to {
            continue;
        }
        for column in table.columns.iter().filter(|column| added(column.since)) {
            let _ = write!(
                text,
                "-- Added to {} at version {}.\n{}",
                table.name,
                column.since,
                dialect::added(server, table.name, &super::column(server, column))
            );
        }
        for index in table.indexes.iter().filter(|index| added(index.since)) {
            text.push_str(&indexed(server, table, index));
        }
    }
    text.push_str(&dialect::recorded(server, from + 1..=to));
    text.push_str(&dialect::regranted(server));
    text
}

/// The SHA-256 fingerprint of the schema `tables` holds at `version`, in
/// lowercase hexadecimal: every table, column, key and index of it, by
/// name, kind and order, and nothing of what the scripts say of them.
#[must_use]
pub fn fingerprint(tables: &[Table], version: u32) -> String {
    let mut text = String::new();
    for table in tables.iter().filter(|table| table.since <= version) {
        let _ = write!(text, "{}.{}(", table.database.word(), table.name);
        for column in table
            .columns
            .iter()
            .filter(|column| column.since <= version)
        {
            let _ = write!(text, "{} {:?} {},", column.name, column.kind, column.null);
        }
        let _ = write!(text, ") key {:?} unique {:?}", table.key, table.unique);
        for index in table.indexes.iter().filter(|index| index.since <= version) {
            let (number, name, columns) = (index.number, index.name, index.columns);
            let _ = write!(text, " index {number} {name} {columns:?} {:?}", index.only);
        }
        text.push('\n');
    }
    Sha256::digest(text.as_bytes())
        .iter()
        .fold(String::new(), |mut hex, byte| {
            let _ = write!(hex, "{byte:02x}");
            hex
        })
}

#[cfg(test)]
mod tests {
    use super::super::{Column, Index, Kind};
    use super::*;

    const fn column(name: &'static str, kind: Kind, since: u32) -> Column {
        Column {
            name,
            kind,
            null: false,
            since,
        }
    }

    const PARTY: &[Column] = &[
        column("party", Kind::Identifier, 1),
        column("name", Kind::Text, 1),
        Column {
            null: true,
            ..column("country", Kind::Word, 2)
        },
    ];

    const RELEASES: &[Table] = &[
        Table {
            database: Database::Administration,
            name: "party",
            since: 1,
            keeps: "each Party",
            columns: PARTY,
            key: &["party"],
            unique: &[],
            indexes: &[Index {
                number: 90,
                name: "party_name",
                columns: &["name"],
                only: None,
                since: 1,
            }],
        },
        Table {
            database: Database::Administration,
            name: "party_contact",
            since: 3,
            keeps: "each Party's contacts",
            columns: &[
                column("party", Kind::Identifier, 3),
                column("contact", Kind::Text, 3),
            ],
            key: &["party", "contact"],
            unique: &[],
            indexes: &[],
        },
    ];

    #[test]
    fn an_upgrade_adds_what_each_release_after_the_first_added_and_records_each() {
        let scripts = upgrade_of(RELEASES, 3, Server::PostgreSql, (1, 3)).expect("written");
        let names: Vec<&str> = scripts.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(
            names,
            [
                "04-runtime-upgrade-1-3.sql",
                "04-administration-upgrade-1-3.sql",
                "04-audit-upgrade-1-3.sql"
            ]
        );
        let administration = &scripts[1].1;
        assert!(administration.contains("ALTER TABLE xmip.party ADD COLUMN country text NULL;\n"));
        assert!(administration.contains("CREATE TABLE xmip.party_contact (\n"));
        assert!(
            !administration.contains("CREATE TABLE xmip.party (\n"),
            "made before"
        );
        assert!(!administration.contains("party_name"), "indexed before");
        for version in [2, 3] {
            let row = format!("VALUES ({version}, now());");
            assert!(administration.contains(&row), "{administration}");
            assert!(scripts[0].1.contains(&row), "every database records it");
        }
        assert!(!administration.contains("VALUES (1, "), "held already");
        for word in ["DROP", "RENAME", "DELETE FROM"] {
            assert!(!administration.contains(word), "{word}: {administration}");
        }
        assert!(administration.ends_with("RESET ROLE;\n"));

        let later = upgrade_of(RELEASES, 3, Server::SqlServer, (2, 3)).expect("written");
        assert!(!later[1].1.contains("ALTER TABLE"), "added at 2");
        assert!(later[1].1.contains("CREATE TABLE xmip.party_contact (\n"));
        let maria = upgrade_of(RELEASES, 3, Server::MariaDb, (1, 2)).expect("written");
        assert!(
            maria[1]
                .1
                .contains("ALTER TABLE `party` ADD COLUMN `country` varchar(32) NULL;\n")
        );
        assert!(!maria[1].1.contains("party_contact"), "added at 3");
    }

    #[test]
    fn an_upgrade_that_is_none_or_a_definition_that_would_not_only_add_is_refused_in_words() {
        for (from, to) in [(0, 1), (2, 2), (3, 2), (1, 4)] {
            let refused = upgrade_of(RELEASES, 3, Server::PostgreSql, (from, to));
            let refused = refused.expect_err("no upgrade");
            assert!(
                refused.contains(&format!("{from} to {to} is none")),
                "{refused}"
            );
        }
        let mut required = PARTY.to_vec();
        required[2].null = false;
        let required: &'static [Column] = Box::leak(required.into_boxed_slice());
        let indexed = [Index {
            since: 2,
            ..RELEASES[0].indexes[0]
        }];
        let indexed: &'static [Index] = Box::leak(Box::new(indexed));
        let wrong = [Table {
            columns: required,
            indexes: indexed,
            ..RELEASES[0]
        }];
        let problems = problems(&wrong, 3);
        assert_eq!(problems.len(), 2, "{problems:?}");
        assert!(problems[0].contains("country is added to a table already made"));
        assert!(problems[1].contains("party_name is an index added"));
        assert!(upgrade_of(&wrong, 3, Server::MariaDb, (1, 2)).is_err());
    }

    #[test]
    fn the_schema_says_only_what_a_schema_may_and_every_release_is_as_it_was() {
        assert_eq!(problems(&TABLES, VERSION), [] as [String; 0]);
        for (version, fingerprinted) in RELEASED {
            assert_eq!(
                fingerprint(&TABLES, *version),
                *fingerprinted,
                "FAILED: version {version} is released and its schema changed; a release \
                 only adds, at a version of its own"
            );
        }
        assert_ne!(fingerprint(RELEASES, 1), fingerprint(RELEASES, 2));
        assert_eq!(fingerprint(RELEASES, 3), fingerprint(RELEASES, 4));
        let refused = upgrade(Server::PostgreSql, 1, VERSION + 1).expect_err("past it");
        assert!(refused.contains("this build's"), "{refused}");
    }
}
