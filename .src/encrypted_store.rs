//! The one layer that encrypts what Xmip stores of its own (ADR-0063
//! clause 2), whatever engine holds the bytes.

use crate::{Change, Engine, PersistError};
use secret::{DataKey, KekName, KeyStore, SecretError};

/// Where the store's own data key is kept, wrapped, in the engine: the one
/// engine key that is not a keyed hash. A keyed hash is thirty-two bytes,
/// so this, at twenty-three, can never be one.
const DATA_KEY: &[u8] = b"\0xmip-core-persist/key\0";

/// The HKDF purposes the store's three keys are derived for. One wrapped
/// key, three uses, and no key used for two.
const RECORD_PURPOSE: &[u8] = b"xmip-core-persist/record";
const LOOKUP_PURPOSE: &[u8] = b"xmip-core-persist/lookup";
/// The root of the column keys: each searchable column's own key is
/// derived from it in turn, the column's name the info
/// ([`EncryptedStore::column_key`]).
const COLUMN_PURPOSE: &[u8] = b"xmip-core-persist/column";

/// What every index entry's engine key opens with, so the entries sort
/// together, apart from the records: a keyed hash begins with these seven
/// bytes once in 2^56, and is refused by its tag if a range ever meets one.
const INDEX: &[u8] = b"\0index\0";
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
/// names it is stored under. An index entry ([`EncryptedStore::apply_indexed`])
/// is the one key not hashed again, so it sorts: built by the caller from
/// keyed hashes under each column's own key and from times and numbers in
/// the clear, its value the record's identifier, sealed. Every key
/// derives from one data key, created with the store and kept in it
/// wrapped by the key home (`xmip-core-secret`).
pub struct EncryptedStore<E> {
    engine: E,
    record: DataKey,
    lookup: DataKey,
    /// The root each column's key is derived from.
    column: DataKey,
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
            let fresh = DataKey::generate();
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
            column: data.derive(COLUMN_PURPOSE)?,
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
        self.opened(&place, &stored, &format!("record of kind '{kind}'"))
            .map(Some)
    }

    /// What `stored` seals for `place`, authenticated; `what` names it in
    /// the scope of a refusal.
    fn opened(&self, place: &[u8], stored: &[u8], what: &str) -> Result<Vec<u8>, PersistError> {
        let refused = |reason: String| PersistError::Refused {
            scope: format!("{} {what}", self.engine.engine()),
            reason,
        };
        let Some((&LAYOUT, sealed)) = stored.split_first() else {
            return Err(refused("not a record this store wrote".to_string()));
        };
        match self.record.open(place, sealed) {
            Ok(value) => Ok(value),
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

    /// Every change of `changes` as one write through the engine's
    /// [`Engine::apply`]: all or none, durable on return. A change is a
    /// record's kind, its key and its new value, `None` removing it.
    ///
    /// # Errors
    ///
    /// As [`EncryptedStore::put`]; nothing is written then.
    pub fn apply(&self, changes: &[RecordChange<'_>]) -> Result<(), PersistError> {
        self.engine.apply(&self.sealed(changes)?)
    }

    /// Every change of `changes` and every index entry of `entries` as one
    /// write through the engine's [`Engine::apply`], all or none, durable
    /// on return, so an index never disagrees with its records. An entry is
    /// its key after the index mark and the identifier of the record it
    /// finds, sealed with its whole key as associated data — or `None`,
    /// removing it.
    ///
    /// # Errors
    ///
    /// As [`EncryptedStore::apply`]; nothing is written then.
    pub fn apply_indexed(
        &self,
        changes: &[RecordChange<'_>],
        entries: &[IndexEntry],
    ) -> Result<(), PersistError> {
        let mut batch = self.sealed(changes)?;
        for (key, record) in entries {
            let key = [INDEX, key.as_slice()].concat();
            let sealed = record
                .map(|record| self.seal(&key, &record.to_be_bytes()))
                .transpose()?;
            batch.push((key, sealed));
        }
        self.engine.apply(&batch)
    }

    /// The records the index entries from `first` to `last` find, both
    /// included, each key after the index mark, in their order — or the
    /// reverse — up to `most`: one range of the engine, each authenticated.
    ///
    /// # Errors
    ///
    /// [`PersistError::Refused`] when an entry fails its tag, and
    /// [`PersistError::Engine`] when the engine cannot read.
    pub fn scan_index(
        &self,
        first: &[u8],
        last: &[u8],
        most: usize,
        reverse: bool,
    ) -> Result<Vec<u128>, PersistError> {
        let (first, last) = ([INDEX, first].concat(), [INDEX, last].concat());
        let entries = self.engine.scan(&first, &last, most, reverse)?;
        entries
            .iter()
            .map(|(key, stored)| {
                let record = self.opened(key, stored, "index entry")?;
                let record: [u8; 16] = record.try_into().map_err(|_| PersistError::Refused {
                    scope: format!("{} index entry", self.engine.engine()),
                    reason: "not a record's identifier".to_string(),
                })?;
                Ok(u128::from_be_bytes(record))
            })
            .collect()
    }

    /// The key of the searchable column `column`, derived by HKDF from the
    /// store's column root with its name as the info: a value hashed under
    /// it (`DataKey::keyed_hash`) is found by equality in that column and
    /// matches nothing in another.
    ///
    /// # Errors
    ///
    /// [`PersistError::Key`] where the derivation fails.
    pub fn column_key(&self, column: &str) -> Result<DataKey, PersistError> {
        Ok(self.column.derive(column.as_bytes())?)
    }

    /// Every change of `changes` as one write through the engine's
    /// [`Engine::apply_deferred`]: all or none, durable with the next write
    /// that is durable on return, not on its own.
    ///
    /// # Errors
    ///
    /// As [`EncryptedStore::apply`].
    pub fn apply_deferred(&self, changes: &[RecordChange<'_>]) -> Result<(), PersistError> {
        self.engine.apply_deferred(&self.sealed(changes)?)
    }

    /// `changes` as the engine keeps them: each place a keyed hash, each
    /// value sealed.
    fn sealed(&self, changes: &[RecordChange<'_>]) -> Result<Vec<Change>, PersistError> {
        changes
            .iter()
            .map(|(kind, key, value)| {
                let place = place(kind, key);
                let sealed = value
                    .as_ref()
                    .map(|value| self.seal(&place, value))
                    .transpose()?;
                Ok((self.lookup.keyed_hash(&place).to_vec(), sealed))
            })
            .collect()
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

/// One change of a batch at the layer: a record's kind, its key and its new
/// value, `None` removing it.
pub type RecordChange<'a> = (&'a str, Vec<u8>, Option<Vec<u8>>);

/// One index entry: its key after the index mark, and the record it finds,
/// `None` removing it.
pub type IndexEntry = (Vec<u8>, Option<u128>);

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
    fn a_batch_is_written_whole() {
        let keys = Held::new(secret::fixture::Memory::default());
        let store = EncryptedStore::open(Memory::default(), &keys, &kek()).expect("open");
        store.put("claim", b"7", b"held").expect("put");
        store
            .apply(&[
                ("journey", b"7".to_vec(), Some(b"done".to_vec())),
                ("journey", b"8".to_vec(), Some(b"next".to_vec())),
                ("claim", b"7".to_vec(), None),
            ])
            .expect("applied");
        assert_eq!(
            store.get("journey", b"7").expect("get"),
            Some(b"done".to_vec())
        );
        assert_eq!(
            store.get("journey", b"8").expect("get"),
            Some(b"next".to_vec())
        );
        assert_eq!(store.get("claim", b"7").expect("get"), None);
    }

    #[test]
    fn the_layer_over_memory_passes_what_every_engine_must() {
        let memory = Memory::default();
        conformance(|| &memory, || memory.everything());
    }

    #[test]
    fn the_layer_over_a_boxed_engine_passes_what_every_engine_must() {
        let memory = Memory::default();
        conformance(
            || -> Box<dyn Engine + '_> { Box::new(&memory) },
            || memory.everything(),
        );
    }

    #[test]
    fn a_value_hashes_apart_under_every_column_and_every_store() {
        let keys = Held::new(secret::fixture::Memory::default());
        let store = EncryptedStore::open(Memory::default(), &keys, &kek()).expect("open");
        let other = EncryptedStore::open(Memory::default(), &keys, &kek()).expect("open");
        let hash = |store: &EncryptedStore<Memory>, column: &str| {
            store
                .column_key(column)
                .expect("key")
                .keyed_hash(b"Billing")
        };
        assert_ne!(
            hash(&store, "journey.send_port_ref"),
            hash(&store, "message.party_ref")
        );
        assert_eq!(
            hash(&store, "journey.send_port_ref"),
            hash(&store, "journey.send_port_ref")
        );
        assert_ne!(
            hash(&other, "journey.send_port_ref"),
            hash(&store, "journey.send_port_ref")
        );
    }

    #[test]
    fn an_index_entry_moved_under_another_key_is_refused() {
        let keys = Held::new(secret::fixture::Memory::default());
        let store = EncryptedStore::open(Memory::default(), &keys, &kek()).expect("open");
        store
            .apply_indexed(&[], &[(b"a1".to_vec(), Some(7)), (b"a3".to_vec(), Some(8))])
            .expect("indexed");
        assert_eq!(
            store.scan_index(b"a", b"a\xFF", 10, false).expect("read"),
            [7, 8]
        );
        assert_eq!(
            store.scan_index(b"a", b"a\xFF", 1, true).expect("read"),
            [8]
        );
        let entry = store
            .engine
            .read(b"\0index\0a1")
            .expect("read")
            .expect("there");
        store.engine.write(b"\0index\0a2", &entry).expect("moved");
        assert!(matches!(
            store.scan_index(b"a", b"a\xFF", 10, false),
            Err(PersistError::Refused { .. })
        ));
        store
            .apply_indexed(&[], &[(b"a2".to_vec(), None)])
            .expect("removed");
        assert_eq!(
            store.scan_index(b"a", b"a\xFF", 10, false).expect("read"),
            [7, 8]
        );
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
