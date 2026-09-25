//! The one layer that encrypts what Xmip stores of its own (ADR-0063
//! clause 2), whatever engine holds the bytes.

use crate::{Engine, PersistError};
use secret::{DataKey, KekName, KeyStore, SecretError};

/// Where the store's own data key is kept, wrapped, in the engine: the one
/// engine key that is not a keyed hash. A keyed hash is thirty-two bytes,
/// so this, at twenty-three, can never be one.
const DATA_KEY: &[u8] = b"\0xmip-core-persist/key\0";

/// The HKDF purposes the store's two keys are derived for. One wrapped key,
/// two uses, and no key used for both.
const RECORD_PURPOSE: &[u8] = b"xmip-core-persist/record";
const LOOKUP_PURPOSE: &[u8] = b"xmip-core-persist/lookup";

/// The first byte of every sealed record: the layout it was written in, so
/// a later one can be told from this.
const LAYOUT: u8 = 1;

/// Records sealed before the engine sees them, and authenticated when they
/// come back.
///
/// Each record is AES-256-GCM under a fresh nonce, with its own place — its
/// kind and key — as associated data, so a record copied under another key
/// is refused. The key it is found by is HMAC-SHA-256 of that place under a
/// second key, so the engine's files hold neither what is stored nor the
/// names it is stored under. Both keys derive from one data key, created
/// with the store and kept in it wrapped by the key home (`xmip-core-secret`).
pub struct EncryptedStore<E> {
    engine: E,
    record: DataKey,
    lookup: DataKey,
}

impl<E: Engine> EncryptedStore<E> {
    /// The store over `engine`, its data key unwrapped by `keys` under
    /// `kek` — or, for an engine that has none yet, created and wrapped.
    ///
    /// # Errors
    ///
    /// [`PersistError::Key`] when the key home does not hold `kek` or it is
    /// not the key that wrapped this store's data key, and
    /// [`PersistError::Engine`] when the engine cannot be read or written.
    pub fn open(engine: E, keys: &dyn KeyStore, kek: &KekName) -> Result<Self, PersistError> {
        let data = if let Some(wrapped) = engine.read(DATA_KEY)? {
            keys.unwrap(kek, &wrapped)?
        } else {
            let fresh = DataKey::generate()?;
            let wrapped = keys.wrap(kek, &fresh)?;
            if engine.write_new(DATA_KEY, &wrapped)? {
                fresh
            } else {
                // Another opener got there first; its key is the key.
                let kept = engine.read(DATA_KEY)?.ok_or_else(|| {
                    PersistError::engine(engine.engine(), "the data key vanished")
                })?;
                keys.unwrap(kek, &kept)?
            }
        };
        Ok(Self {
            record: data.derive(RECORD_PURPOSE)?,
            lookup: data.derive(LOOKUP_PURPOSE)?,
            engine,
        })
    }

    /// `value` as the record `key` of `kind`, replacing what was there.
    ///
    /// # Errors
    ///
    /// [`PersistError::Engine`] when the engine cannot write, and
    /// [`PersistError::Key`] when sealing fails.
    pub fn put(&self, kind: &str, key: &[u8], value: &[u8]) -> Result<(), PersistError> {
        let place = place(kind, key);
        let sealed = self.seal(&place, value)?;
        self.engine.write(&self.lookup.keyed_hash(&place), &sealed)
    }

    /// `value` as the record `key` of `kind` only if there is none: `true`
    /// when written, `false` when one was there.
    ///
    /// # Errors
    ///
    /// As [`EncryptedStore::put`].
    pub fn put_new(&self, kind: &str, key: &[u8], value: &[u8]) -> Result<bool, PersistError> {
        let place = place(kind, key);
        let sealed = self.seal(&place, value)?;
        self.engine
            .write_new(&self.lookup.keyed_hash(&place), &sealed)
    }

    /// The record `key` of `kind`, authenticated, or `None`.
    ///
    /// # Errors
    ///
    /// [`PersistError::Refused`] when the record fails its authentication
    /// tag — the caller audits it as a failure with that scope and reason
    /// (ADR-0062) — and [`PersistError::Engine`] when the engine cannot read.
    pub fn get(&self, kind: &str, key: &[u8]) -> Result<Option<Vec<u8>>, PersistError> {
        let place = place(kind, key);
        let Some(stored) = self.engine.read(&self.lookup.keyed_hash(&place))? else {
            return Ok(None);
        };
        let refused = |reason: String| PersistError::Refused {
            scope: format!("{} record of kind '{kind}'", self.engine.engine()),
            reason,
        };
        let Some((&LAYOUT, sealed)) = stored.split_first() else {
            return Err(refused("not a record this store wrote".to_string()));
        };
        match self.record.open(&place, sealed) {
            Ok(value) => Ok(Some(value)),
            Err(SecretError::Refused { reason }) => Err(refused(reason)),
            Err(other) => Err(other.into()),
        }
    }

    /// The record `key` of `kind` gone.
    ///
    /// # Errors
    ///
    /// [`PersistError::Engine`] when the engine cannot write.
    pub fn remove(&self, kind: &str, key: &[u8]) -> Result<(), PersistError> {
        self.engine
            .remove(&self.lookup.keyed_hash(&place(kind, key)))
    }

    /// The engine beneath, as it is: what it holds is ciphertext.
    pub fn engine(&self) -> &E {
        &self.engine
    }

    /// The key the engine holds the record `key` of `kind` under, for a
    /// test that reaches past the layer to the bytes.
    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub fn lookup_key(&self, kind: &str, key: &[u8]) -> [u8; 32] {
        self.lookup.keyed_hash(&place(kind, key))
    }

    fn seal(&self, place: &[u8], value: &[u8]) -> Result<Vec<u8>, PersistError> {
        let sealed = self.record.seal(place, value)?;
        Ok([&[LAYOUT], sealed.as_slice()].concat())
    }
}

/// A record's place: its kind, a zero byte, its key. What it is sealed for
/// and what its lookup key is hashed from.
fn place(kind: &str, key: &[u8]) -> Vec<u8> {
    [kind.as_bytes(), &[0], key].concat()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::{Memory, conformance};
    use secret::Held;

    fn kek() -> KekName {
        KekName::new("runtime").expect("name")
    }

    #[test]
    fn a_record_comes_back_as_it_was_put() {
        let keys = Held::new(secret::fixture::Memory::default());
        let store = EncryptedStore::open(Memory::default(), &keys, &kek()).expect("open");
        store.put("journey", b"7", b"payload").expect("put");
        assert_eq!(
            store.get("journey", b"7").expect("get"),
            Some(b"payload".to_vec())
        );
        assert_eq!(store.get("journey", b"8").expect("get"), None);
        assert_eq!(store.get("lease", b"7").expect("get"), None);
        store.remove("journey", b"7").expect("removed");
        assert_eq!(store.get("journey", b"7").expect("get"), None);
    }

    #[test]
    fn the_layer_over_memory_passes_what_every_engine_must() {
        let memory = Memory::default();
        conformance(|| &memory, || memory.everything());
    }

    #[test]
    fn a_record_moved_under_another_key_is_refused() {
        let keys = Held::new(secret::fixture::Memory::default());
        let store = EncryptedStore::open(Memory::default(), &keys, &kek()).expect("open");
        store.put("journey", b"7", b"seven").expect("put");
        store.put("journey", b"8", b"eight").expect("put");
        let from = store.lookup_key("journey", b"7");
        let to = store.lookup_key("journey", b"8");
        let moved = store.engine.read(&from).expect("read").expect("there");
        store.engine.write(&to, &moved).expect("moved");
        assert!(matches!(
            store.get("journey", b"8"),
            Err(PersistError::Refused { .. })
        ));
    }

    #[test]
    fn a_store_reopened_with_another_key_home_is_refused() {
        let keys = Held::new(secret::fixture::Memory::default());
        let store = EncryptedStore::open(Memory::default(), &keys, &kek()).expect("open");
        store.put("journey", b"7", b"payload").expect("put");
        let engine = store.engine;
        let other = Held::new(secret::fixture::Memory::default());
        let refused = EncryptedStore::open(engine, &other, &kek());
        assert!(matches!(
            refused,
            Err(PersistError::Key(SecretError::MissingKek { .. }))
        ));
    }
}
