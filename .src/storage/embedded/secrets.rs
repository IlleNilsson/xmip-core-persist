//! The embedded Storage node's secrets: the administration database's
//! `secret` table, each value sealed by Xmip before the record is written
//! (`super::super::secret::sealed`), the record sealed whole again as every
//! record here, and found by its name's index entry.

use std::sync::PoisonError;

use xcore::SecretName;

use super::super::record::Form;
use super::super::row::Value;
use super::super::secret::{KeptSecret, SEALING, SECRET, SecretEntry, SecretRecord};
use super::Embedded;
use crate::{Engine, PersistError};

/// The table and the index a secret is found by.
const BY_NAME: (&str, &str) = ("secret", "secret_name");

impl<R: Engine + 'static, A: Engine> Embedded<R, A> {
    /// `secret` written, keeping the identifier of the one of its name it
    /// replaces: `write_secret`.
    pub(super) fn secret_set(&self, secret: &SecretRecord) -> Result<(), PersistError> {
        let _one = self
            .administering
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut kept = secret.sealed(&self.administration.derived(SEALING)?)?;
        if let Some(id) = self.secret_id(secret.secret.name())? {
            kept.id = id;
        }
        let tables = (&self.administration, &self.administration_columns);
        let change = (SECRET, kept.id.to_be_bytes().to_vec(), Some(kept.bytes()));
        self.indexed(tables, vec![change])
    }

    /// The secret `name`, or `None`: `read_secret`.
    pub(super) fn secret_read(
        &self,
        name: &SecretName,
    ) -> Result<Option<SecretRecord>, PersistError> {
        let Some(id) = self.secret_id(name)? else {
            return Ok(None);
        };
        let key = self.administration.derived(SEALING)?;
        self.secret_by_id(id)?
            .map(|kept| kept.opened(&key))
            .transpose()
    }

    /// Every secret, without its value: `list_secrets`.
    pub(super) fn secret_list(&self) -> Result<Vec<SecretEntry>, PersistError> {
        let ids = self.administration_columns.find(
            &self.administration,
            BY_NAME,
            &[],
            None,
            (u32::MAX, false),
        )?;
        let mut entries = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(kept) = self.secret_by_id(id)? {
                entries.push(kept.entry());
            }
        }
        Ok(entries)
    }

    fn secret_id(&self, name: &SecretName) -> Result<Option<u128>, PersistError> {
        let equal = [Value::Text(name.to_string())];
        let found = self.administration_columns.find(
            &self.administration,
            BY_NAME,
            &equal,
            None,
            (1, false),
        )?;
        Ok(found.first().copied())
    }

    fn secret_by_id(&self, id: u128) -> Result<Option<KeptSecret>, PersistError> {
        self.administration
            .get(SECRET, &id.to_be_bytes())?
            .map(|bytes| KeptSecret::from_bytes(&bytes))
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use secret::{Held, KekName};
    use xcore::{Secret, SecretKind, SystemClock};

    use super::*;
    use crate::EncryptedStore;
    use crate::fixture::Memory;
    use crate::storage::XmipStorage;

    fn node() -> Embedded<Memory, Memory> {
        let keys = Held::new(secret::fixture::Memory::default());
        let kek = KekName::new("storage").expect("name");
        Embedded::over(
            EncryptedStore::open(Memory::default(), &keys, &kek).expect("runtime"),
            EncryptedStore::open(Memory::default(), &keys, &kek).expect("administration"),
            EncryptedStore::open(Memory::default(), &keys, &kek).expect("audit"),
            Arc::new(SystemClock),
        )
        .expect("node")
    }

    fn drawn() -> String {
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH);
        format!("drawn-{:x}", now.map(|t| t.as_nanos()).unwrap_or_default())
    }

    fn record(id: u128, name: &str, value: &str) -> SecretRecord {
        let name = SecretName::new(name).expect("a name");
        SecretRecord {
            id,
            secret: Secret::new(name, SecretKind::Password, value.into()).expect("one"),
            set_unix_nanos: 0,
            set_by: "CN=operator".to_string(),
        }
    }

    #[test]
    fn a_secret_set_again_keeps_its_identifier_and_is_kept_sealed() {
        let node = node();
        let name = SecretName::new("partner-x").expect("a name");
        assert_eq!(node.read_secret(&name).expect("read"), None);
        let (first, second) = (drawn(), format!("{}-again", drawn()));
        node.write_secret(&record(7, "partner-x", &first))
            .expect("set");
        node.write_secret(&record(9, "partner-y", &first))
            .expect("set");
        node.write_secret(&record(8, "partner-x", &second))
            .expect("set again");

        let read = node.read_secret(&name).expect("read").expect("set");
        assert_eq!((read.id, read.secret.text()), (7, second.as_str()));
        assert!(read.set_unix_nanos > 0, "Xmip Storage's time");
        let listed = node.list_secrets().expect("listed");
        let names: Vec<&str> = listed.iter().map(|entry| entry.name.as_str()).collect();
        assert_eq!(names, ["partner-x", "partner-y"]);
        assert_eq!(listed[0].kind, SecretKind::Password);

        // Its row, opened from the record seal every record here has,
        // holds the value sealed again: never as it was set.
        let row = node
            .administration()
            .get(SECRET, &7u128.to_be_bytes())
            .expect("read")
            .expect("kept");
        for value in [&first, &second] {
            assert!(
                !row.windows(value.len()).any(|w| w == value.as_bytes()),
                "the row"
            );
        }
        let everything = node.administration().engine().everything();
        for value in [&first, &second] {
            let found = everything
                .windows(value.len())
                .any(|w| w == value.as_bytes());
            assert!(
                !found,
                "a value lies in the clear in the administration database"
            );
        }
    }
}
