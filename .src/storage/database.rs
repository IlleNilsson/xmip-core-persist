//! The database server a Storage node is in front of, where IT runs one
//! (option A, `deployment-model.md` section 7), and the one reading of a
//! connection to it.
//!
//! Xmip Storage keeps two databases on every backend, and behind a server
//! they are two separate databases, which IT may place on different
//! servers (the owner, 2026-10-01: *We still need the distinction between
//! runtime and administration databases, regardless of backend database
//! technology*). A node's configuration names both in `[storage.database]`
//! (`xmip-core-configure`), each as a connection read here:
//! `<server>://<login>@<host>[:<port>]/<database>`. The password is never
//! in it: the configuration names the secret it is kept under.

use std::fmt;

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

    /// The word a connection opens with, and the folder under
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

/// One connection to a database on a server.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Connection {
    pub server: Server,
    pub login: String,
    pub host: String,
    pub port: u16,
    pub database: String,
}

impl Connection {
    /// The connection `text` says.
    ///
    /// # Errors
    ///
    /// What is wrong with it, in words naming the form it takes.
    pub fn read(text: &str) -> Result<Self, String> {
        let refused = |why: &str| {
            format!(
                "'{text}' {why}; write <postgresql|sqlserver>://<login>@<host>[:<port>]/<database>"
            )
        };
        let (scheme, rest) = text
            .split_once("://")
            .ok_or_else(|| refused("names no server"))?;
        let server = Server::ALL
            .into_iter()
            .find(|server| server.word() == scheme)
            .ok_or_else(|| refused("names a server Xmip Storage is not in front of"))?;
        let (login, rest) = rest
            .split_once('@')
            .ok_or_else(|| refused("names no login"))?;
        let (authority, database) = rest
            .split_once('/')
            .ok_or_else(|| refused("names no database"))?;
        let (host, port) = match authority.rsplit_once(':') {
            Some((host, port)) if !port.contains(']') => {
                let port = port
                    .parse::<u16>()
                    .ok()
                    .filter(|port| *port > 0)
                    .ok_or_else(|| refused("names no port from 1 to 65535"))?;
                (host, port)
            }
            _ => (authority, server.port()),
        };
        let words = [login, host, database];
        if words
            .iter()
            .any(|word| word.is_empty() || word.contains(char::is_whitespace))
        {
            return Err(refused("leaves its login, host or database empty"));
        }
        if database.contains(['/', '?', ';']) || login.contains(':') {
            return Err(refused("carries more than a login and a database name"));
        }
        Ok(Self {
            server,
            login: login.to_string(),
            host: host.to_string(),
            port,
            database: database.to_string(),
        })
    }
}

impl fmt::Display for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (server, login, host) = (self.server.word(), &self.login, &self.host);
        write!(
            f,
            "{server}://{login}@{host}:{}/{}",
            self.port, self.database
        )
    }
}

/// What is wrong with a Storage node's two connections and the secret its
/// password is kept under, in words, each opening with `[storage.database]`.
#[must_use]
pub fn problems(runtime: &str, administration: &str, password: &str) -> Vec<String> {
    let said = |problem: String| format!("[storage.database] {problem}");
    let mut problems = Vec::new();
    let read = [("runtime", runtime), ("administration", administration)].map(|(key, text)| {
        Connection::read(text)
            .map_err(|problem| problems.push(said(format!("{key}: {problem}"))))
            .ok()
    });
    if let [Some(runtime), Some(administration)] = &read {
        if runtime.server != administration.server {
            problems.push(said(format!(
                "names a {} runtime database and a {} administration database; both are on \
                 one kind of server",
                runtime.server.word(),
                administration.server.word()
            )));
        }
        if runtime == administration {
            problems.push(said(
                "names one database for both; the runtime and the administration databases \
                 are separate"
                    .to_string(),
            ));
        }
    }
    if password.trim().is_empty() {
        problems.push(said("password names no secret".to_string()));
    }
    problems
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_connection_is_read_with_the_servers_port_where_none_is_named() {
        let runtime = Connection::read("postgresql://xmip_storage@db-1.example/xmip_runtime");
        let runtime = runtime.expect("reads");
        assert_eq!(runtime.server, Server::PostgreSql);
        assert_eq!(runtime.port, 5432);
        assert_eq!(
            runtime.to_string(),
            "postgresql://xmip_storage@db-1.example:5432/xmip_runtime"
        );
        let named = Connection::read("sqlserver://xmip_storage@[fd00::7]:14330/xmip_runtime");
        let named = named.expect("reads");
        assert_eq!((named.host.as_str(), named.port), ("[fd00::7]", 14330));
        let ipv6 = Connection::read("sqlserver://xmip_storage@[fd00::7]/xmip_runtime");
        assert_eq!(ipv6.expect("reads").port, 1433);
    }

    #[test]
    fn a_connection_that_is_not_one_is_refused_in_words() {
        for (connection, why) in [
            ("mysql://u@h/d", "not in front of"),
            ("postgresql://h/d", "names no login"),
            ("postgresql://u@h", "names no database"),
            ("postgresql://u@h:0/d", "from 1 to 65535"),
            ("postgresql://u:secret@h/d", "more than a login"),
            ("sqlserver://u@h/d?encrypt=false", "more than a login"),
        ] {
            let refused = Connection::read(connection).expect_err(connection);
            assert!(refused.contains(why), "{connection}: {refused}");
        }
    }

    #[test]
    fn one_database_for_both_two_kinds_of_server_or_no_secret_is_a_problem() {
        assert!(problems("postgresql://x@h/r", "postgresql://x@h/a", "p").is_empty());
        let same = problems("sqlserver://x@h/xmip", "sqlserver://x@h/xmip", "p");
        assert!(same[0].contains("separate"), "{same:?}");
        let mixed = problems("sqlserver://x@h/r", "postgresql://x@h/a", " ");
        assert_eq!(mixed.len(), 2, "{mixed:?}");
        let wrong = problems("nothing", "postgresql://x@h/a", "p");
        assert!(
            wrong[0].starts_with("[storage.database] runtime: 'nothing'"),
            "{wrong:?}"
        );
    }
}
