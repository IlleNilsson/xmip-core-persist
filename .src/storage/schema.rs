//! Xmip Storage's two databases on a server IT runs, said once: every
//! table, its columns and its key, the roles that own and use them, and
//! the scripts an operator runs to make them, in each server's dialect.
//!
//! The scripts under `deploy/database/<server>/` are generated from this
//! and nothing else, and the estate root's `cargo test --test database`
//! fails when one differs from what [`scripts`] writes, so what IT runs and
//! what the backend reads and writes are one definition (the owner,
//! 2026-10-01: *We have to give PostgreSQL and MSSQL IT-operators help in
//! regards to settings and database schemas*).
//!
//! **Least privilege.** The login a Storage node connects as reads and
//! writes the tables and does nothing else; the schema is owned by a role
//! no one logs in as, which the operator who changes it takes on. An
//! identifier is a `UUIDv7` (`deployment-model.md` section 7): `uuid` on
//! PostgreSQL, and on SQL Server `binary(16)`, not `uniqueidentifier`,
//! whose sort order would scatter `UUIDv7` keys
//! (`doc/record-identifier.md`, *Boundary two*).

use std::fmt::Write as _;

use super::database::Server;

mod searchable;
mod tables;

pub use tables::TABLES;

/// Which of the two databases a table is in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Database {
    /// The runtime database: the Ledger.
    Runtime,
    /// The administration database.
    Administration,
}

impl Database {
    /// Both, in the order they are made.
    pub const ALL: [Self; 2] = [Self::Runtime, Self::Administration];

    /// The database's name on the server, as the scripts make it.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Runtime => "xmip_runtime",
            Self::Administration => "xmip_administration",
        }
    }

    const fn word(self) -> &'static str {
        match self {
            Self::Runtime => "runtime",
            Self::Administration => "administration",
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
    /// `timestamptz`, and `datetime2(7)` on SQL Server, in UTC.
    Time,
    /// A word an enumeration names one of its values by: a state, a phase,
    /// how a Message was made — `text`, and `nvarchar(32)` on SQL Server,
    /// the longest word with room to spare.
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
/// for the index to hold it — a partial index, filtered on SQL Server.
#[derive(Clone, Copy, Debug)]
pub struct Index {
    /// Its number, one to each index of both databases and never reused:
    /// what the embedded engines file its entries under
    /// (`super::columns`).
    pub number: u8,
    pub name: &'static str,
    pub columns: &'static [&'static str],
    pub only: Option<&'static str>,
}

/// The schema both databases keep their tables in.
pub const SCHEMA: &str = "xmip";
/// The role that owns the schema; no one logs in as it.
pub const OWNER: &str = "xmip_owner";
/// The login a Storage node connects as.
pub const LOGIN: &str = "xmip_storage";

/// Every script an operator runs for `server`, in the order they are run:
/// its file name and its text.
#[must_use]
pub fn scripts(server: Server) -> Vec<(String, String)> {
    let mut scripts = vec![
        ("01-roles.sql".to_string(), roles(server)),
        ("02-databases.sql".to_string(), databases(server)),
    ];
    for (number, database) in (3..).zip(Database::ALL) {
        let name = format!("{number:02}-{}.sql", database.word());
        scripts.push((name, tables(server, database)));
    }
    scripts
}

fn head(server: Server, what: &str, run: &str) -> String {
    format!(
        "-- Xmip Storage on {}: {what}.\n\
         --\n\
         -- Generated by xmip-core-persist (storage::schema), the definition the\n\
         -- backend reads and writes by; do not edit. `cargo test --test database`\n\
         -- at the estate root fails when this file differs from what it writes.\n\
         -- What to run, in what order and why: README.md beside this file.\n\
         --\n\
         -- {run}\n\n",
        name(server)
    )
}

const fn name(server: Server) -> &'static str {
    match server {
        Server::PostgreSql => "PostgreSQL",
        Server::SqlServer => "SQL Server",
    }
}

fn roles(server: Server) -> String {
    match server {
        Server::PostgreSql => {
            let mut text = head(
                server,
                "the roles",
                "Run as a superuser, connected to the postgres database.",
            );
            let _ = write!(
                text,
                "-- Owns the schema; no one logs in as it. An operator who changes the\n\
                 -- schema is granted it.\n\
                 CREATE ROLE {OWNER} NOLOGIN;\n\n\
                 -- The login a Storage node connects as: reads and writes the tables,\n\
                 -- nothing more. Its password is set by the operator, never in a file:\n\
                 -- \\password {LOGIN}\n\
                 CREATE ROLE {LOGIN} LOGIN;\n"
            );
            text
        }
        Server::SqlServer => {
            let mut text = head(
                server,
                "the login",
                "Run as a sysadmin with sqlcmd, its password given as the scripting\n\
                 -- variable StoragePassword (sqlcmd -v StoragePassword=...), never in a file.",
            );
            let _ = write!(
                text,
                "-- The login a Storage node connects as: reads and writes the tables,\n\
                 -- nothing more.\n\
                 CREATE LOGIN {LOGIN} WITH PASSWORD = N'$(StoragePassword)', CHECK_POLICY = ON;\n\
                 GO\n"
            );
            text
        }
    }
}

fn databases(server: Server) -> String {
    let run = match server {
        Server::PostgreSql => "Run as a superuser, connected to the postgres database.",
        Server::SqlServer => "Run as a sysadmin with sqlcmd.",
    };
    let mut text = head(server, "the two databases", run);
    for database in Database::ALL {
        let name = database.name();
        match server {
            Server::PostgreSql => {
                let _ = write!(
                    text,
                    "CREATE DATABASE {name} OWNER {OWNER} ENCODING 'UTF8' TEMPLATE template0;\n\
                     REVOKE ALL ON DATABASE {name} FROM PUBLIC;\n\
                     GRANT CONNECT ON DATABASE {name} TO {LOGIN};\n\n"
                );
            }
            Server::SqlServer => {
                let _ = write!(
                    text,
                    "CREATE DATABASE {name};\nGO\n\
                     -- Every commit durable before it returns: Xmip counts a write once\n\
                     -- the database has it (runtime-model.md section 3).\n\
                     ALTER DATABASE {name} SET DELAYED_DURABILITY = DISABLED;\nGO\n\n"
                );
            }
        }
    }
    text
}

fn tables(server: Server, database: Database) -> String {
    let name = database.name();
    let run = match server {
        Server::PostgreSql => {
            format!("Run as a superuser, connected to the {name} database.")
        }
        Server::SqlServer => "Run as a sysadmin with sqlcmd.".to_string(),
    };
    let mut text = head(server, &format!("the {} database", database.word()), &run);
    match server {
        Server::PostgreSql => {
            let _ = write!(
                text,
                "SET ROLE {OWNER};\nCREATE SCHEMA {SCHEMA} AUTHORIZATION {OWNER};\n\n"
            );
        }
        Server::SqlServer => {
            let _ = write!(
                text,
                "USE {name};\nGO\nCREATE ROLE {OWNER};\nGO\n\
                 CREATE SCHEMA {SCHEMA} AUTHORIZATION {OWNER};\nGO\n\n"
            );
        }
    }
    for table in TABLES.iter().filter(|table| table.database == database) {
        let lines: Vec<String> = table
            .columns
            .iter()
            .map(|column| {
                let null = if column.null { "NULL" } else { "NOT NULL" };
                format!("    {} {} {null}", column.name, kind(server, column.kind))
            })
            .chain(std::iter::once(format!(
                "    CONSTRAINT {}_key PRIMARY KEY ({})",
                table.name,
                table.key.join(", ")
            )))
            .chain((!table.unique.is_empty()).then(|| {
                format!(
                    "    CONSTRAINT {}_unique UNIQUE ({})",
                    table.name,
                    table.unique.join(", ")
                )
            }))
            .collect();
        let _ = write!(
            text,
            "-- {}.\nCREATE TABLE {SCHEMA}.{} (\n{}\n);\n{}\n",
            table.keeps,
            table.name,
            lines.join(",\n"),
            if server == Server::SqlServer {
                "GO\n"
            } else {
                ""
            }
        );
        text.push_str(&indexes(server, table));
    }
    match server {
        Server::PostgreSql => {
            let _ = write!(
                text,
                "GRANT USAGE ON SCHEMA {SCHEMA} TO {LOGIN};\n\
                 GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {SCHEMA} TO {LOGIN};\n\
                 RESET ROLE;\n"
            );
        }
        Server::SqlServer => {
            let _ = write!(
                text,
                "CREATE USER {LOGIN} FOR LOGIN {LOGIN};\nGO\n\
                 GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::{SCHEMA} TO {LOGIN};\nGO\n"
            );
        }
    }
    text
}

/// The `CREATE INDEX` of each index of `table`, each followed by a blank
/// line.
fn indexes(server: Server, table: &Table) -> String {
    let mut text = String::new();
    for index in table.indexes {
        let only = match (server, index.only) {
            (_, None) => String::new(),
            (Server::PostgreSql, Some(flag)) => format!(" WHERE {flag}"),
            (Server::SqlServer, Some(flag)) => format!(" WHERE {flag} = 1"),
        };
        let _ = write!(
            text,
            "CREATE INDEX {} ON {SCHEMA}.{} ({}){only};
{}
",
            index.name,
            table.name,
            index.columns.join(", "),
            if server == Server::SqlServer {
                "GO
"
            } else {
                ""
            }
        );
    }
    text
}

const fn kind(server: Server, kind: Kind) -> &'static str {
    match (server, kind) {
        (Server::PostgreSql, Kind::Identifier) => "uuid",
        (Server::PostgreSql, Kind::Bytes) => "bytea",
        (Server::PostgreSql, Kind::Text | Kind::LongText | Kind::Word) => "text",
        (Server::PostgreSql | Server::SqlServer, Kind::Number) => "bigint",
        (Server::PostgreSql, Kind::Count) => "integer",
        (Server::PostgreSql, Kind::Flag) => "boolean",
        (Server::PostgreSql, Kind::Sequence) => "bigint GENERATED ALWAYS AS IDENTITY",
        (Server::SqlServer, Kind::Identifier) => "binary(16)",
        (Server::SqlServer, Kind::Bytes) => "varbinary(max)",
        (Server::SqlServer, Kind::Text) => "nvarchar(400)",
        (Server::SqlServer, Kind::LongText) => "nvarchar(max)",
        (Server::SqlServer, Kind::Count) => "int",
        (Server::SqlServer, Kind::Flag) => "bit",
        (Server::SqlServer, Kind::Sequence) => "bigint IDENTITY(1, 1)",
        (Server::PostgreSql, Kind::Time) => "timestamptz",
        (Server::SqlServer, Kind::Time) => "datetime2(7)",
        (Server::SqlServer, Kind::Word) => "nvarchar(32)",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_key_is_a_column_of_its_table_and_every_table_is_in_one_script() {
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
                    "02-databases.sql",
                    "03-runtime.sql",
                    "04-administration.sql"
                ]
            );
            for table in TABLES {
                let creates = format!("CREATE TABLE {SCHEMA}.{} (", table.name);
                let script = &scripts[if table.database == Database::Runtime {
                    2
                } else {
                    3
                }]
                .1;
                assert!(script.contains(&creates), "{server:?} {}", table.name);
            }
        }
    }

    #[test]
    fn a_storage_node_may_read_and_write_and_nothing_more() {
        for server in Server::ALL {
            for (_, script) in scripts(server).iter().skip(2) {
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
        assert!(sql_server[1].1.contains("DELAYED_DURABILITY = DISABLED"));
        assert!(sql_server[2].1.contains("binary(16)"));
        assert!(!sql_server[2].1.contains("uniqueidentifier"));
    }
}
