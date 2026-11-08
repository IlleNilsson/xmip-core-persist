//! The one layer that encrypts what Xmip stores of its own (ADR-0063
//! clause 2), whatever engine holds the bytes.

mod index;
mod place;
mod stopped;
mod viewed;

use place::place;
use stopped::Stopped;

use crate::{Change, Engine, PersistError, Reads};
use secret::{DataKey, KekName, KeyStore, SecretError};
use viewed::Now;
pub use viewed::{IndexScan, Viewed};
// Every write the engine is asked for, timed as the I/O of the Message the
// asking thread works for, if it works for one (`xcore::spent`): the
// Ledger's appends and its syncs, never the sealing before them.
use xcore::spent::on_io;

/// Where the store's own data key is kept, wrapped, in the engine: the one
/// engine key that is not a keyed hash. A keyed hash is thirty-two bytes,
/// so this, at twenty-three, can never be one.
pub(crate) const DATA_KEY: &[u8] = b"\0xmip-core-persist/key\0";

/// What every engine key of a store's own begins with — its data key, and
/// where a standby keeps how far it copied (`storage::standby`) — none of
/// them a record: never copied from a primary as written.
pub(crate) const OWN: &[u8] = b"\0xmip-core-persist/";

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
/// is refused. The key it is found by is keyed BLAKE3 of that place under a
/// second key, so the engine's files hold neither what is stored nor the
/// names it is stored under. An index entry ([`EncryptedStore::apply_indexed`])
/// is the one key not hashed: the caller builds it in the clear, so it
/// sorts, and its value, the identifier of the record it finds, is sealed.
/// Both keys derive from one data key, created with the store and kept in
/// it wrapped by the key home (`xmip-core-secret`).
///
/// **A store whose sync failed stops** ([`EncryptedStore::sync`]): every
/// read and write through it is refused from then on, until it is opened
/// again over its engine's log (`stopped`).
pub struct EncryptedStore<E> {
    engine: E,
    record: DataKey,
    lookup: DataKey,
    stopped: Stopped,
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
            engine,
            stopped: Stopped::default(),
        })
    }

    /// Nothing, while the store has not stopped.
    ///
    /// # Errors
    ///
    /// [`PersistError::Engine`] once it has, saying why.
    fn alive(&self) -> Result<(), PersistError> {
        self.stopped.check(self.engine.engine())
    }

    /// The store stopped for `why`: nothing read or written through it any
    /// more (`stopped`).
    pub(crate) fn stop(&self, why: String) {
        self.stopped.stop(why);
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

    /// A key derived from the store's own for `purpose` alone: what Xmip
    /// Storage seals a value under before the store keeps it, so a value is
    /// ciphertext whatever keeps the record (`storage::secret::sealed`).
    ///
    /// # Errors
    ///
    /// [`PersistError::Key`] where the derivation failed.
    pub(crate) fn derived(&self, purpose: &[u8]) -> Result<DataKey, PersistError> {
        Ok(self.record.derive(purpose)?)
    }

    /// The record `key` of `kind`, authenticated, or `None`.
    ///
    /// # Errors
    ///
    /// [`PersistError::Refused`] when the record fails its authentication
    /// tag — the caller audits it as a failure with that scope and reason
    /// (ADR-0062) — and [`PersistError::Engine`] when the engine cannot read.
    pub fn get(&self, kind: &str, key: &[u8]) -> Result<Option<Vec<u8>>, PersistError> {
        self.get_in(&Now(&self.engine), kind, key)
    }

    /// One consistent view of the store ([`Engine::view`]): the records as
    /// they stood when it was taken, an index entry and the record it finds
    /// always agreeing.
    ///
    /// # Errors
    ///
    /// [`PersistError::Engine`] when the engine cannot give one.
    pub fn viewed(&self) -> Result<Viewed<'_, E>, PersistError> {
        Ok(Viewed {
            store: self,
            reads: self.engine.view()?,
        })
    }

    /// [`EncryptedStore::get`] through `reads`.
    fn get_in(
        &self,
        reads: &dyn Reads,
        kind: &str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, PersistError> {
        self.alive()?;
        let place = place(kind, key);
        let Some(stored) = reads.read(&self.lookup.keyed_hash(&place))? else {
            return Ok(None);
        };
        self.opened(&place, &stored, &|| format!("record of kind '{kind}'"))
            .map(Some)
    }

    /// What `stored` seals for `place`, authenticated; `what` names it in
    /// the scope of a refusal, said only for one.
    fn opened(
        &self,
        place: &[u8],
        stored: &[u8],
        what: &dyn Fn() -> String,
    ) -> Result<Vec<u8>, PersistError> {
        let refused = |reason: String| PersistError::Refused {
            scope: format!("{} {}", self.engine.engine(), what()),
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
        self.alive()?;
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
        let sealed = self.sealed(changes)?;
        on_io(|| self.engine.apply(&sealed))
    }

    /// Every write before now durable, as the engine syncs
    /// ([`Engine::sync`]). A sync that fails stops the store: the writes
    /// before it may or may not reach the disk, so none is read or written
    /// through it again until it is opened anew over its engine's log.
    ///
    /// # Errors
    ///
    /// [`PersistError::Engine`] when the engine cannot sync, or the store
    /// has stopped.
    pub fn sync(&self) -> Result<(), PersistError> {
        self.alive()?;
        let synced = on_io(|| self.engine.sync());
        if let Err(failed) = &synced {
            self.stop(format!("a sync failed: {failed}"));
        }
        synced
    }

    /// Every change of `changes` as one write through the engine's
    /// [`Engine::apply_deferred`]: all or none, durable with the next write
    /// that is durable on return, not on its own.
    ///
    /// # Errors
    ///
    /// As [`EncryptedStore::apply`].
    pub fn apply_deferred(&self, changes: &[RecordChange<'_>]) -> Result<(), PersistError> {
        let sealed = self.sealed(changes)?;
        on_io(|| self.engine.apply_deferred(&sealed))
    }

    /// `changes` as the engine keeps them: each place a keyed hash, each
    /// value sealed.
    fn sealed(&self, changes: &[RecordChange<'_>]) -> Result<Vec<Change>, PersistError> {
        self.alive()?;
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
        self.alive()?;
        Ok(self.record.seal_after(&[LAYOUT], place, value)?)
    }
}

/// One change of a batch at the layer: a record's kind, its key and its new
/// value, `None` removing it.
pub type RecordChange<'a> = (&'a str, Vec<u8>, Option<Vec<u8>>);

/// One index entry: its key after the index mark, and the record it finds,
/// `None` removing it.
pub type IndexEntry = (Vec<u8>, Option<u128>);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::{Memory, conformance};
    use secret::Held;
    use std::assert_matches;

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
        assert_eq!(
            store
                .scan_index_keyed(b"a", b"a\xFF", 1, false)
                .expect("read"),
            [(b"a1".to_vec(), 7)],
            "the key it was kept by"
        );
        let entry = store
            .engine
            .read(b"\0index\0a1")
            .expect("read")
            .expect("there");
        store.engine.write(b"\0index\0a2", &entry).expect("moved");
        assert_matches!(
            store.scan_index(b"a", b"a\xFF", 10, false),
            Err(PersistError::Refused { .. })
        );
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
        assert_matches!(
            store.get("journey", b"8"),
            Err(PersistError::Refused { .. })
        );
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
