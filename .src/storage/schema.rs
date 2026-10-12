//! Xmip Storage's three databases on a server IT runs, one to each data
//! domain, said once: every table, its columns and its key, the roles that
//! own and use them, and the scripts an operator runs to make them, in each
//! server's dialect.
//!
//! The scripts under `deploy/database/<server>/` are generated from this
//! and nothing else, and the estate root's `cargo test --test database`
//! fails when one differs from what [`scripts`] writes, so what IT runs and
//! what the backend reads and writes are one definition (the owner,
//! 2026-10-01: *We have to give PostgreSQL and MSSQL IT-operators help in
//! regards to settings and database schemas*; and, 2026-10-12, of MariaDB:
//! *make it a provider for Storage nodes and create scripts for it*).
//!
//! **Least privilege.** The login a Storage node connects as reads and
//! writes the tables and does nothing else; the schema is owned by a role
//! no one logs in as, which the operator who changes it takes on. An
//! identifier is a `UUIDv7` (`deployment-model.md` section 7): `uuid` on
//! PostgreSQL, and `binary(16)` on SQL Server and MariaDB — on SQL Server
//! not `uniqueidentifier`, whose sort order would scatter `UUIDv7` keys
//! (`doc/record-identifier.md`, *Boundary two*).

use std::fmt::Write as _;

use super::database::Server;

mod dialect;
mod searchable;
mod tables;

pub use tables::TABLES;

/// Which of the three databases, one to each data domain, a table is in.
/// Each is made by scripts of its own, so each may be on a server of its
/// own (the owner, 2026-10-10: *The audit part might be better of in its
/// own database so it can be hosted on a different set of nodes, different
/// storage*).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Database {
    /// The runtime database: the Ledger.
    Runtime,
    /// The administration database.
    Administration,
    /// The audit database: audit records kept over time.
    Audit,
}

impl Database {
    /// All three, in the order their scripts are listed.
    pub const ALL: [Self; 3] = [Self::Runtime, Self::Administration, Self::Audit];

    /// The database's name on the server, as the scripts make it.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Runtime => "xmip_runtime",
            Self::Administration => "xmip_administration",
            Self::Audit => "xmip_audit",
        }
    }

    /// The data domain's word: what its scripts are named by.
    #[must_use]
    pub const fn word(self) -> &'static str {
        match self {
            Self::Runtime => "runtime",
            Self::Administration => "administration",
            Self::Audit => "audit",
        }
    }
}

/// What a column holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// A `UUIDv7`.
    Identifier,
    /// Bytes as they were given.
    Bytes,
    /// Words.
    Text,
    /// A whole number, 64 bits.
    Number,
    /// A whole number, 32 bits.
    Count,
    /// Yes or no.
    Flag,
    /// The server's own ascending number, given as a row is written.
    Sequence,
    /// Words of any length: a record's own text, never a key.
    LongText,
    /// A moment, as near the nanosecond as the server keeps it:
    /// `timestamptz`, `datetime2(7)` on SQL Server and `datetime(6)` on
    /// MariaDB, in UTC.
    Time,
    /// A word an enumeration names one of its values by: a state, a phase,
    /// how a Message was made — `text`, `nvarchar(32)` on SQL Server and
    /// `varchar(32)` on MariaDB, the longest word with room to spare.
    Word,
}

/// One column.
#[derive(Clone, Copy, Debug)]
pub struct Column {
    pub name: &'static str,
    pub kind: Kind,
    /// Whether a row may hold nothing there: a fact its record may lack.
    pub null: bool,
}

/// One table.
#[derive(Clone, Copy, Debug)]
pub struct Table {
    pub database: Database,
    pub name: &'static str,
    /// What it keeps, said in each script above it.
    pub keeps: &'static str,
    pub columns: &'static [Column],
    /// The columns its primary key is, in order.
    pub key: &'static [&'static str],
    /// The columns no two rows share besides the key, where any.
    pub unique: &'static [&'static str],
    /// What it is searched by, beside its key.
    pub indexes: &'static [Index],
}

/// One index of a table: its columns in order, equality on the first and
/// a range on the last; and, where it has one, the flag a row must have
/// for the index to hold it — a partial index, filtered on SQL Server;
/// MariaDB keeps none, and its index holds every row.
#[derive(Clone, Copy, Debug)]
pub struct Index {
    /// Its number, one to each index of every database and never reused:
    /// what the embedded engines file its entries under
    /// (`super::columns`).
    pub number: u8,
    pub name: &'static str,
    pub columns: &'static [&'static str],
    pub only: Option<&'static str>,
}

/// The schema every database keeps its tables in.
pub const SCHEMA: &str = "xmip";
/// The role that owns the schema; no one logs in as it.
pub const OWNER: &str = "xmip_owner";
/// The login a Storage node connects as.
pub const LOGIN: &str = "xmip_storage";

/// Every script an operator runs for `server`, in the order they are run:
/// its file name and its text.
///
/// Per data domain: the number is the step, the word the domain. Every
/// server runs `01-roles.sql`; then each domain it holds is made by
/// `02-<domain>-database.sql` and filled by `03-<domain>-schema.sql`. One
/// server holding all three runs them as they sort; a server holding one
/// domain runs the roles and that domain's two, and the domains need
/// nothing of one another.
#[must_use]
pub fn scripts(server: Server) -> Vec<(String, String)> {
    let mut scripts = vec![("01-roles.sql".to_string(), dialect::roles(server))];
    for database in Database::ALL {
        let name = format!("02-{}-database.sql", database.word());
        scripts.push((name, dialect::created(server, database)));
    }
    for database in Database::ALL {
        let name = format!("03-{}-schema.sql", database.word());
        scripts.push((name, tables(server, database)));
    }
    scripts
}

fn tables(server: Server, database: Database) -> String {
    let run = match server {
        Server::PostgreSql => {
            format!(
                "Run as a superuser, connected to the {} database.",
                database.name()
            )
        }
        Server::SqlServer | Server::MariaDb => dialect::run(server).to_string(),
    };
    let what = format!("the {} database's schema", database.word());
    let mut text = dialect::head(server, &what, &run);
    text.push_str(&dialect::schema(server, database));
    for table in TABLES.iter().filter(|table| table.database == database) {
        let named = |names: &[&str]| -> String {
            let quoted: Vec<String> = names
                .iter()
                .map(|name| dialect::quoted(server, name))
                .collect();
            quoted.join(", ")
        };
        let lines: Vec<String> = table
            .columns
            .iter()
            .map(|column| {
                let null = if column.null { "NULL" } else { "NOT NULL" };
                format!(
                    "    {} {} {null}",
                    dialect::quoted(server, column.name),
                    dialect::kind(server, column.kind)
                )
            })
            .chain(std::iter::once(format!(
                "    CONSTRAINT {}_key PRIMARY KEY ({})",
                table.name,
                named(table.key)
            )))
            .chain((!table.unique.is_empty()).then(|| {
                format!(
                    "    CONSTRAINT {}_unique UNIQUE ({})",
                    table.name,
                    named(table.unique)
                )
            }))
            .collect();
        let _ = write!(
            text,
            "-- {}.\nCREATE TABLE {} (\n{}\n){}\n",
            table.keeps,
            dialect::table(server, table.name),
            lines.join(",\n"),
            dialect::ended(server, true)
        );
        for index in table.indexes {
            let only = match (server, index.only) {
                (Server::MariaDb, _) | (_, None) => String::new(),
                (Server::PostgreSql, Some(flag)) => format!(" WHERE {flag}"),
                (Server::SqlServer, Some(flag)) => format!(" WHERE {flag} = 1"),
            };
            let _ = writeln!(
                text,
                "CREATE INDEX {} ON {} ({}){only}{}",
                dialect::quoted(server, index.name),
                dialect::table(server, table.name),
                named(index.columns),
                dialect::ended(server, false)
            );
        }
    }
    text.push_str(&dialect::granted(server, database));
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_key_is_a_column_of_its_table_and_every_table_is_in_its_domains_script() {
        for table in TABLES {
            for key in table.key.iter().chain(table.unique) {
                assert!(
                    table.columns.iter().any(|column| column.name == *key),
                    "{}: {key}",
                    table.name
                );
            }
        }
        for server in Server::ALL {
            let scripts = scripts(server);
            let names: Vec<&str> = scripts.iter().map(|(name, _)| name.as_str()).collect();
            assert_eq!(
                names,
                [
                    "01-roles.sql",
                    "02-runtime-database.sql",
                    "02-administration-database.sql",
                    "02-audit-database.sql",
                    "03-runtime-schema.sql",
                    "03-administration-schema.sql",
                    "03-audit-schema.sql",
                ]
            );
            for table in TABLES {
                let creates = format!("CREATE TABLE {} (", dialect::table(server, table.name));
                // `audit` is a table of two databases: the runtime
                // database's as first written, the audit database's kept.
                let made_in: Vec<String> = TABLES
                    .iter()
                    .filter(|named| named.name == table.name)
                    .map(|named| format!("03-{}-schema.sql", named.database.word()))
                    .collect();
                for (name, script) in &scripts {
                    assert_eq!(
                        script.contains(&creates),
                        made_in.contains(name),
                        "{server:?} {name} {}",
                        table.name
                    );
                }
            }
            for database in Database::ALL {
                let made = format!(
                    "CREATE DATABASE {}",
                    dialect::quoted(server, database.name())
                );
                let making: Vec<&str> = scripts
                    .iter()
                    .filter(|(_, script)| script.contains(&made))
                    .map(|(name, _)| name.as_str())
                    .collect();
                assert_eq!(making, [format!("02-{}-database.sql", database.word())]);
            }
        }
    }

    #[test]
    fn the_kept_audit_is_in_the_audit_database_and_nowhere_else() {
        let audit: Vec<&str> = TABLES
            .iter()
            .filter(|table| table.database == Database::Audit)
            .map(|table| table.name)
            .collect();
        assert_eq!(
            audit,
            [
                "audit",
                "audit_chain_head",
                "audit_body_chunk",
                "audit_stream",
                "audit_stream_chunk"
            ]
        );
        let administration = TABLES
            .iter()
            .filter(|table| table.database == Database::Administration)
            .map(|table| table.name);
        assert!(administration.eq(["administration"]));
    }

    #[test]
    fn a_storage_node_may_read_and_write_and_nothing_more() {
        for server in Server::ALL {
            for (_, script) in scripts(server).iter().skip(4) {
                let granted: Vec<&str> = script
                    .lines()
                    .filter(|line| line.starts_with("GRANT") && line.contains(LOGIN))
                    .collect();
                assert!(!granted.is_empty(), "{server:?}");
                for grant in granted {
                    let rights = grant.split(" ON ").next().unwrap_or_default();
                    for right in ["CREATE", "ALTER", "DROP", "CONTROL", "ALL", "REFERENCES"] {
                        assert!(!rights.contains(right), "{server:?}: {grant}");
                    }
                }
            }
        }
        let sql_server = scripts(Server::SqlServer);
        for (name, made) in &sql_server[1..4] {
            assert!(made.contains("DELAYED_DURABILITY = DISABLED"), "{name}");
        }
        assert!(sql_server[4].1.contains("binary(16)"));
        assert!(!sql_server[4].1.contains("uniqueidentifier"));
    }

    #[test]
    fn on_mariadb_the_login_needs_tls_and_every_table_is_innodb_in_utf8mb4() {
        let mariadb = scripts(Server::MariaDb);
        assert!(
            mariadb[0]
                .1
                .contains("CREATE USER 'xmip_storage'@'%' REQUIRE SSL ACCOUNT LOCK;")
        );
        for (name, made) in &mariadb[1..4] {
            assert!(made.contains("CHARACTER SET utf8mb4"), "{name}");
        }
        for (name, schema) in &mariadb[4..] {
            let tables = schema.matches("CREATE TABLE ").count();
            assert_eq!(schema.matches(") ENGINE=InnoDB;").count(), tables, "{name}");
            assert!(!schema.contains(" WHERE "), "{name}: no partial index");
        }
    }
}
