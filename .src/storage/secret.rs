//! The secrets Locations name, kept in the administration database's
//! `secret` table (ADR-0019, amendment 2026-10-29): a table of their own,
//! every value but the secret's own in a column of its own, found by name.
//!
//! **Sealed by Xmip, whatever keeps it.** A Storage node seals a secret's
//! value before any engine or database server sees it — AES-256-GCM under
//! the administration database's data key, the key the key home's
//! key-encryption key wraps (ADR-0063 clauses 2 and 4), its name and kind
//! the associated data — and the `sealed` column holds ciphertext alone
//! ([`sealed`]); whoever reads the table or its backups, a database
//! server's administrator included, reads no secret (ADR-0063, amendment
//! 2026-10-29). On an embedded Storage node the record is sealed whole
//! again, as every record there, and its index entry holds its name alone.
//! Between a node and a Storage node it travels inside Xmip's mutual TLS.
//! It is kept usable, not as a verifier: the node presents it to the party
//! it connects to.
//!
//! **Never read back out.** Xmip Storage answers a secret's value to the
//! node that presents it ([`StoredSecrets`]); what a surface lists is
//! [`SecretEntry`]: the name, the kind, and when and by whom it was set.

use std::sync::Arc;

use codec::cursor::Cursor;
use xcore::{Failure, Secret, SecretKind, SecretName, Secrets};

use super::XmipStorage;
use super::record::{
    Form, malformed, read_bytes, read_text, read_u64, read_u128, write_bytes, write_text,
    write_u64, write_u128,
};
use crate::PersistError;

mod sealed;

pub(crate) use sealed::{KeptSecret, SEALING};

/// What a secret's records are kept under in the administration database.
pub(crate) const SECRET: &str = "secret";

/// A secret as Xmip Storage keeps it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecretRecord {
    /// Its `UUIDv7`, minted once by whoever set it first; a secret set
    /// again keeps the one it had.
    pub id: u128,
    pub secret: Secret,
    /// When Xmip Storage last wrote it, in nanoseconds since the Unix
    /// epoch: Xmip Storage's to set.
    pub set_unix_nanos: u64,
    /// Who set it, as the surface's connection proved them.
    pub set_by: String,
}

/// What a surface is told of a secret: never its value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecretEntry {
    pub name: SecretName,
    pub kind: SecretKind,
    pub set_unix_nanos: u64,
    pub set_by: String,
}

impl SecretRecord {
    /// What a surface is told of it.
    #[must_use]
    pub fn entry(&self) -> SecretEntry {
        SecretEntry {
            name: self.secret.name().clone(),
            kind: self.secret.kind(),
            set_unix_nanos: self.set_unix_nanos,
            set_by: self.set_by.clone(),
        }
    }
}

fn write_kind(out: &mut Vec<u8>, kind: SecretKind) {
    write_text(out, kind.word());
}

fn read_kind(cursor: &mut Cursor<'_>) -> Result<SecretKind, PersistError> {
    let word = read_text(cursor)?;
    SecretKind::named(&word).ok_or_else(|| malformed(format!("no secret kind is '{word}'")))
}

fn read_name(cursor: &mut Cursor<'_>) -> Result<SecretName, PersistError> {
    SecretName::new(read_text(cursor)?).map_err(malformed)
}

impl Form for SecretRecord {
    fn write(&self, out: &mut Vec<u8>) {
        write_u128(out, self.id);
        write_text(out, self.secret.name().as_str());
        write_kind(out, self.secret.kind());
        write_bytes(out, self.secret.value());
        write_u64(out, self.set_unix_nanos);
        write_text(out, &self.set_by);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        let id = read_u128(cursor)?;
        let name = read_name(cursor)?;
        let kind = read_kind(cursor)?;
        let secret = Secret::new(name, kind, read_bytes(cursor)?).map_err(malformed)?;
        Ok(Self {
            id,
            secret,
            set_unix_nanos: read_u64(cursor)?,
            set_by: read_text(cursor)?,
        })
    }
}

impl Form for SecretEntry {
    fn write(&self, out: &mut Vec<u8>) {
        write_text(out, self.name.as_str());
        write_kind(out, self.kind);
        write_u64(out, self.set_unix_nanos);
        write_text(out, &self.set_by);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            name: read_name(cursor)?,
            kind: read_kind(cursor)?,
            set_unix_nanos: read_u64(cursor)?,
            set_by: read_text(cursor)?,
        })
    }
}

/// The secrets a node's Locations name, read from Xmip Storage as each
/// connects (`transport::Secured`): the one [`Secrets`] a node is handed.
pub struct StoredSecrets(pub Arc<dyn XmipStorage>);

impl Secrets for StoredSecrets {
    fn secret(&self, name: &SecretName) -> Result<Option<Secret>, Failure> {
        self.0
            .read_secret(name)
            .map(|kept| kept.map(|record| record.secret))
            .map_err(|failed| match failed {
                // Not reached, or reached where it could not answer yet:
                // the next connection may find it.
                PersistError::Unreachable { .. }
                | PersistError::Standby { .. }
                | PersistError::Engine { .. } => Failure::retryable(failed.to_string()),
                _ => Failure::permanent(failed.to_string()),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::super::wire::{Answer, Request};
    use super::*;

    #[test]
    fn a_secret_comes_back_from_its_bytes_and_its_entry_holds_no_value() {
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH);
        let drawn = format!("drawn-{:x}", now.map(|t| t.as_nanos()).unwrap_or_default());
        let name = SecretName::new("partner-x").expect("a name");
        let secret = Secret::new(name, SecretKind::Password, drawn.clone().into()).expect("one");
        let record = SecretRecord {
            id: 7,
            secret,
            set_unix_nanos: 9,
            set_by: "CN=operator".to_string(),
        };
        let back = SecretRecord::from_bytes(&record.bytes()).expect("read");
        assert_eq!(back.secret.text(), drawn);
        assert_eq!(back.entry(), record.entry());
        let entry = record.entry().bytes();
        assert!(!entry.windows(drawn.len()).any(|w| w == drawn.as_bytes()));

        for asked in [
            Request::WriteSecret(Box::new(record.clone())),
            Request::ReadSecret(record.secret.name().clone()),
            Request::ListSecrets,
        ] {
            assert_eq!(Request::from_bytes(&asked.bytes()).expect("crossed"), asked);
        }
        for answered in [
            Answer::Secret(Some(Box::new(record.clone()))),
            Answer::Secrets(vec![record.entry()]),
        ] {
            assert_eq!(
                Answer::from_bytes(&answered.bytes()).expect("crossed"),
                answered
            );
        }
    }
}
