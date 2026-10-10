//! What each data domain's database is kept on, and the one reading of a
//! connection to a database server (option A, `deployment-model.md`
//! section 7).
//!
//! Xmip Storage keeps three databases, one to each data domain — runtime,
//! administration, audit — and each names its own storage and connection
//! in a table of its own (the owner, 2026-10-10: *i would do it like
//! runtime, storage, connection string. Same for audit and
//! administration*, and *Yes, better*):
//!
//! ```toml
//! [runtime]
//! storage = "postgresql"
//! connection = "host=db-1.example port=5432 dbname=xmip_runtime user=xmip_storage"
//!
//! [audit]
//! storage = "sqlite"
//! connection = "D:/Xmip/data/storage/audit.sqlite"
//! ```
//!
//! `storage` is a [`Technology`]; each domain may be on another. On an
//! embedded engine the connection is the store's path; on a database
//! server it is the server's own connection string, read here
//! ([`Connection::read`]). The password is never in it: the node's
//! configuration names the secret it is kept under, in
//! `[storage.database] password` (`xmip-core-configure`).

use std::fmt;

use super::schema::Database;

/// The kinds of database server Xmip Storage can be in front of: PostgreSQL
/// first (the owner, 2026-10-01: *Don't leave out the elephant, Postgres*),
/// SQL Server after it (`deployment-model.md` section 7).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Server {
    PostgreSql,
    SqlServer,
}

impl Server {
    /// Every kind.
    pub const ALL: [Self; 2] = [Self::PostgreSql, Self::SqlServer];

    /// The word `storage` names it by, and the folder under
    /// `deploy/database` its operators' guide and scripts are in.
    #[must_use]
    pub const fn word(self) -> &'static str {
        match self {
            Self::PostgreSql => "postgresql",
            Self::SqlServer => "sqlserver",
        }
    }

    /// The port the server listens on unless IT says otherwise.
    #[must_use]
    pub const fn port(self) -> u16 {
        match self {
            Self::PostgreSql => 5432,
            Self::SqlServer => 1433,
        }
    }
}

/// What a data domain's database is kept on: an embedded engine, mounted
/// beside this source, or a database server.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Technology {
    /// `RocksDB`, `xmip-core-persist-rocksdb`.
    RocksDb,
    /// `SQLite`, `xmip-core-persist-sqlite`.
    Sqlite,
    /// A database server IT runs.
    Server(Server),
}

impl Technology {
    /// Every one, in the order a refusal names them.
    pub const ALL: [Self; 4] = [
        Self::RocksDb,
        Self::Sqlite,
        Self::Server(Server::PostgreSql),
        Self::Server(Server::SqlServer),
    ];

    /// The word `storage` names it by.
    #[must_use]
    pub const fn word(self) -> &'static str {
        match self {
            Self::RocksDb => "rocksdb",
            Self::Sqlite => "sqlite",
            Self::Server(server) => server.word(),
        }
    }

    /// The technology `word` names, where it names one.
    #[must_use]
    pub fn named(word: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|technology| technology.word() == word)
    }

    /// The embedded engine `database` is kept on, the one its domain may
    /// name: `RocksDB` for the runtime database, always, and `SQLite` for
    /// the administration and the audit databases (ADR-0015, amendment
    /// 2026-10-01; ADR-0070, amendment 2026-10-10).
    #[must_use]
    pub const fn embedded(database: Database) -> Self {
        match database {
            Database::Runtime => Self::RocksDb,
            Database::Administration | Database::Audit => Self::Sqlite,
        }
    }
}

/// One connection to a database on a server, as its connection string says
/// it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Connection {
    pub server: Server,
    /// The login, where the string names one.
    pub login: Option<String>,
    pub host: String,
    pub port: u16,
    pub database: String,
}

impl Connection {
    /// The connection `text` says, in `server`'s own form: PostgreSQL's
    /// keyword and value pairs, `host=<host> [port=<port>] dbname=<database>
    /// [user=<login>]`, and SQL Server's, `Server=[tcp:]<host>[,<port>];
    /// Database=<database>[;User Id=<login>]`. Nothing else is read: no
    /// password, no option.
    ///
    /// # Errors
    ///
    /// What is wrong with it, in words naming the form it takes.
    pub fn read(server: Server, text: &str) -> Result<Self, String> {
        let form = match server {
            Server::PostgreSql => "host=<host> [port=<port>] dbname=<database> [user=<login>]",
            Server::SqlServer => "Server=<host>[,<port>];Database=<database>[;User Id=<login>]",
        };
        let refused = |why: String| format!("'{text}' {why}; write {form}");
        let pairs: Vec<&str> = match server {
            Server::PostgreSql => text.split_whitespace().collect(),
            Server::SqlServer => text.split(';').map(str::trim).collect(),
        };
        let (mut host, mut port, mut database, mut login) = (None, None, None, None);
        for pair in pairs.into_iter().filter(|pair| !pair.is_empty()) {
            let (key, value) = pair
                .split_once('=')
                .map(|(key, value)| (key.trim().to_ascii_lowercase(), value.trim()))
                .ok_or_else(|| refused(format!("holds '{pair}', which is no key and value")))?;
            if value.is_empty() || value.contains(char::is_whitespace) {
                return Err(refused(format!("leaves '{key}' empty or spaced")));
            }
            match (server, key.as_str()) {
                (Server::PostgreSql, "host") => host = Some(value.to_string()),
                (Server::PostgreSql, "port") => port = Some(value),
                (Server::PostgreSql, "dbname") | (Server::SqlServer, "database") => {
                    database = Some(value.to_string());
                }
                (Server::PostgreSql, "user") | (Server::SqlServer, "user id") => {
                    login = Some(value.to_string());
                }
                (Server::SqlServer, "server") => {
                    let value = value.strip_prefix("tcp:").unwrap_or(value);
                    let (named, numbered) = value
                        .split_once(',')
                        .map_or((value, None), |(host, port)| (host, Some(port)));
                    host = Some(named.to_string());
                    port = numbered;
                }
                _ => return Err(refused(format!("carries '{key}', which is not read"))),
            }
        }
        let port = match port {
            None => server.port(),
            Some(port) => port
                .parse::<u16>()
                .ok()
                .filter(|port| *port > 0)
                .ok_or_else(|| refused("names no port from 1 to 65535".to_string()))?,
        };
        let (Some(host), Some(database)) = (host, database) else {
            return Err(refused("names no host or no database".to_string()));
        };
        Ok(Self {
            server,
            login,
            host,
            port,
            database,
        })
    }
}

impl fmt::Display for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (host, port, database) = (&self.host, self.port, &self.database);
        match self.server {
            Server::PostgreSql => {
                write!(f, "host={host} port={port} dbname={database}")?;
                self.login
                    .as_ref()
                    .map_or(Ok(()), |login| write!(f, " user={login}"))
            }
            Server::SqlServer => {
                write!(f, "Server=tcp:{host},{port};Database={database}")?;
                self.login
                    .as_ref()
                    .map_or(Ok(()), |login| write!(f, ";User Id={login}"))
            }
        }
    }
}

/// One data domain's table, as the configuration says it: its storage and
/// its connection.
#[derive(Clone, Copy, Debug)]
pub struct Domain<'a> {
    pub database: Database,
    pub storage: &'a str,
    pub connection: &'a str,
}

/// What is wrong with the data domains' tables a configuration holds, and,
/// where one names a database server, with the secret the password is kept
/// under; in words, each opening with the table it is about. Two domains
/// naming one database on one server are refused: the three are separate.
#[must_use]
pub fn problems(named: &[Domain<'_>], password: Option<&str>) -> Vec<String> {
    let mut problems = Vec::new();
    let mut servers: Vec<(Database, Connection)> = Vec::new();
    for domain in named {
        let said = |problem: String| format!("[{}] {problem}", domain.database.word());
        let Some(technology) = Technology::named(domain.storage) else {
            let words: Vec<&str> = Technology::ALL.iter().map(|t| t.word()).collect();
            problems.push(said(format!(
                "storage: '{}' is none Xmip Storage keeps a database on; write {}",
                domain.storage,
                words.join(", ")
            )));
            continue;
        };
        match technology {
            Technology::Server(server) => match Connection::read(server, domain.connection) {
                Ok(connection) => servers.push((domain.database, connection)),
                Err(problem) => problems.push(said(format!("connection: {problem}"))),
            },
            embedded if embedded != Technology::embedded(domain.database) => {
                problems.push(said(format!(
                    "storage: the {} database is kept on {} where it is embedded, not {}",
                    domain.database.word(),
                    Technology::embedded(domain.database).word(),
                    embedded.word()
                )));
            }
            _ if domain.connection.trim().is_empty() => {
                problems.push(said("connection names no path".to_string()));
            }
            _ => {}
        }
    }
    for (at, (database, connection)) in servers.iter().enumerate() {
        let same = |(_, earlier): &&(Database, Connection)| {
            (
                &earlier.server,
                &earlier.host,
                earlier.port,
                &earlier.database,
            ) == (
                &connection.server,
                &connection.host,
                connection.port,
                &connection.database,
            )
        };
        if let Some((earlier, _)) = servers[..at].iter().find(same) {
            problems.push(format!(
                "[{}] connection names the {} database's; the runtime, the administration \
                 and the audit databases are separate",
                database.word(),
                earlier.word()
            ));
        }
    }
    if !servers.is_empty() && password.is_none_or(|password| password.trim().is_empty()) {
        problems.push(
            "[storage.database] password names no secret, and a database server is named"
                .to_string(),
        );
    }
    problems
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_connection_is_read_in_its_servers_form_with_its_port_where_none_is_named() {
        let read = Connection::read(
            Server::PostgreSql,
            "host=db-1 port=5432 dbname=xmip_runtime",
        );
        let runtime = read.expect("reads");
        assert_eq!((runtime.port, runtime.login.as_deref()), (5432, None));
        assert_eq!(
            runtime.to_string(),
            "host=db-1 port=5432 dbname=xmip_runtime"
        );
        let named = Connection::read(
            Server::SqlServer,
            "Server=tcp:sql-1.example,14330;Database=xmip_audit;User Id=xmip_storage",
        );
        let named = named.expect("reads");
        assert_eq!((named.host.as_str(), named.port), ("sql-1.example", 14330));
        assert_eq!(named.login.as_deref(), Some("xmip_storage"));
        let plain = Connection::read(Server::SqlServer, "Server=sql-1;Database=xmip_audit");
        assert_eq!(plain.expect("reads").port, 1433);
    }

    #[test]
    fn a_connection_that_is_not_one_is_refused_in_words() {
        for (server, connection, why) in [
            (Server::PostgreSql, "dbname=d", "no host or no database"),
            (Server::PostgreSql, "host=h", "no host or no database"),
            (
                Server::PostgreSql,
                "host=h port=0 dbname=d",
                "from 1 to 65535",
            ),
            (
                Server::PostgreSql,
                "host=h dbname=d password=x",
                "'password'",
            ),
            (Server::PostgreSql, "postgresql://u@h/d", "no key and value"),
            (
                Server::SqlServer,
                "Server=h;Database=d;Encrypt=false",
                "'encrypt'",
            ),
        ] {
            let refused = Connection::read(server, connection).expect_err(connection);
            assert!(refused.contains(why), "{connection}: {refused}");
        }
    }

    fn named<'a>(database: Database, storage: &'a str, connection: &'a str) -> Domain<'a> {
        Domain {
            database,
            storage,
            connection,
        }
    }

    #[test]
    fn each_domain_may_be_on_another_technology_and_two_on_one_database_are_refused() {
        let apart = [
            named(
                Database::Runtime,
                "postgresql",
                "host=db-1 dbname=xmip_runtime",
            ),
            named(
                Database::Administration,
                "sqlserver",
                "Server=sql-1;Database=a",
            ),
            named(
                Database::Audit,
                "sqlite",
                "D:/Xmip/data/storage/audit.sqlite",
            ),
        ];
        assert!(problems(&apart, Some("p")).is_empty());
        let same = [
            named(Database::Administration, "postgresql", "host=h dbname=xmip"),
            named(
                Database::Audit,
                "postgresql",
                "host=h port=5432 dbname=xmip",
            ),
        ];
        let refused = problems(&same, Some("p"));
        assert_eq!(refused.len(), 1, "{refused:?}");
        assert!(refused[0].starts_with("[audit] connection names the administration"));
        let unsealed = problems(&same[..1], None);
        assert!(
            unsealed[0].contains("password names no secret"),
            "{unsealed:?}"
        );
        let embedded = [named(Database::Audit, "sqlite", "a.sqlite")];
        assert!(problems(&embedded, None).is_empty(), "no secret needed");
    }

    #[test]
    fn an_unknown_storage_or_another_embedded_engine_is_refused_in_words() {
        let wrong = [
            named(Database::Runtime, "sqlite", "runtime.sqlite"),
            named(Database::Administration, "mysql", "x"),
            named(Database::Audit, "sqlite", " "),
        ];
        let refused = problems(&wrong, None);
        assert!(refused[0].contains("kept on rocksdb"), "{refused:?}");
        assert!(refused[1].contains("rocksdb, sqlite, postgresql, sqlserver"));
        assert!(refused[2].starts_with("[audit] connection names no path"));
    }
}
