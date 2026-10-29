//! A secret as the `secret` table keeps it: its value sealed by Xmip,
//! whatever store keeps the row (ADR-0063, amendment 2026-10-29).
//!
//! **Sealed once, here.** AES-256-GCM under the administration database's
//! data key, derived for this one purpose ([`SEALING`]) — the key the key
//! home's key-encryption key wraps (ADR-0063 clauses 2 and 4) — with the
//! secret's name and kind as associated data, so a value moved to another
//! name or kind is refused. A Storage node seals a value before any
//! engine or database server sees it and opens it only for the node that
//! presents it; the column holds ciphertext alone, so whoever reads the
//! table or its backups — an administrator of the database server
//! included — reads no secret. The embedded engines and a server backend
//! both keep this one form.

use codec::cursor::Cursor;
use secret::DataKey;
use xcore::{Secret, SecretKind, SecretName};

use super::{SecretEntry, SecretRecord, read_kind, read_name, write_kind};
use crate::PersistError;
use crate::storage::record::{
    Form, read_bytes, read_text, read_u64, read_u128, write_bytes, write_text, write_u64,
    write_u128,
};
use crate::storage::row::{Columned, Row, Value};

/// What a secret's value is sealed under: the administration database's
/// data key, derived for this purpose alone.
pub(crate) const SEALING: &[u8] = b"xmip-core-persist/secret-value";

/// A secret as its table's row keeps it: everything in the clear but its
/// value, which is sealed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct KeptSecret {
    pub(crate) id: u128,
    pub(crate) name: SecretName,
    pub(crate) kind: SecretKind,
    /// The value, sealed ([`SecretRecord::sealed`]).
    pub(crate) sealed: Vec<u8>,
    pub(crate) set_unix_nanos: u64,
    pub(crate) set_by: String,
}

/// What a value is bound to: its name and kind.
fn bound(name: &SecretName, kind: SecretKind) -> Vec<u8> {
    [
        b"xmip.secret\0".as_slice(),
        name.as_str().as_bytes(),
        b"\0",
        kind.word().as_bytes(),
    ]
    .concat()
}

impl SecretRecord {
    /// It as its row keeps it, its value sealed under `key`.
    ///
    /// # Errors
    ///
    /// [`PersistError::Key`] where the sealing failed.
    pub(crate) fn sealed(&self, key: &DataKey) -> Result<KeptSecret, PersistError> {
        let (name, kind) = (self.secret.name(), self.secret.kind());
        Ok(KeptSecret {
            id: self.id,
            name: name.clone(),
            kind,
            sealed: key.seal(&bound(name, kind), self.secret.value())?,
            set_unix_nanos: self.set_unix_nanos,
            set_by: self.set_by.clone(),
        })
    }
}

impl KeptSecret {
    /// The secret, its value opened under `key`: for the node that
    /// presents it.
    ///
    /// # Errors
    ///
    /// [`PersistError::Refused`] where the value fails its tag — altered,
    /// sealed under another key, or moved from another name or kind.
    pub(crate) fn opened(&self, key: &DataKey) -> Result<SecretRecord, PersistError> {
        let refused = |reason: String| PersistError::Refused {
            scope: format!("the secret '{}'", self.name),
            reason,
        };
        let value = key
            .open(&bound(&self.name, self.kind), &self.sealed)
            .map_err(|failed| refused(failed.to_string()))?;
        Ok(SecretRecord {
            id: self.id,
            secret: Secret::new(self.name.clone(), self.kind, value).map_err(refused)?,
            set_unix_nanos: self.set_unix_nanos,
            set_by: self.set_by.clone(),
        })
    }

    /// What a surface is told of it.
    pub(crate) fn entry(&self) -> SecretEntry {
        SecretEntry {
            name: self.name.clone(),
            kind: self.kind,
            set_unix_nanos: self.set_unix_nanos,
            set_by: self.set_by.clone(),
        }
    }
}

impl Form for KeptSecret {
    fn write(&self, out: &mut Vec<u8>) {
        write_u128(out, self.id);
        write_text(out, self.name.as_str());
        write_kind(out, self.kind);
        write_bytes(out, &self.sealed);
        write_u64(out, self.set_unix_nanos);
        write_text(out, &self.set_by);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            id: read_u128(cursor)?,
            name: read_name(cursor)?,
            kind: read_kind(cursor)?,
            sealed: read_bytes(cursor)?,
            set_unix_nanos: read_u64(cursor)?,
            set_by: read_text(cursor)?,
        })
    }
}

impl Columned for KeptSecret {
    fn id(&self) -> u128 {
        self.id
    }

    fn stamp(&mut self, now: u64, _before: Option<&Self>) {
        self.set_unix_nanos = now;
    }

    /// Its columns besides its key and its sealed value, which no index
    /// holds.
    fn row(&self) -> Row {
        vec![
            ("name", Some(Value::Text(self.name.to_string()))),
            ("kind", Some(Value::Text(self.kind.word().to_string()))),
            ("set_at", Some(Value::Time(self.set_unix_nanos))),
            ("set_by", Some(Value::Text(self.set_by.clone()))),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_value_is_kept_sealed_and_opens_only_under_its_name_and_kind() {
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
        let key = DataKey::generate();
        let kept = record.sealed(&key).expect("sealed");
        let row = kept.bytes();
        assert!(
            !row.windows(drawn.len()).any(|w| w == drawn.as_bytes()),
            "in the clear"
        );
        assert_eq!(
            KeptSecret::from_bytes(&row)
                .expect("read")
                .opened(&key)
                .expect("opened"),
            record
        );

        let moved = KeptSecret {
            name: SecretName::new("partner-y").expect("a name"),
            ..kept.clone()
        };
        assert!(matches!(
            moved.opened(&key),
            Err(PersistError::Refused { .. })
        ));
        assert!(
            kept.opened(&DataKey::generate()).is_err(),
            "another key opens nothing"
        );
        assert!(kept.row().iter().all(|(column, _)| *column != "sealed"));
    }
}
