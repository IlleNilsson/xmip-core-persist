//! What every engine is proved with, for tests only: an engine in memory,
//! and the one set of checks every engine technology runs against itself.
//!
//! The checks are here rather than in each engine's tests, so `rocksdb` and
//! `sqlite` prove the same things by the same code (ADR-0044). Behind the
//! `test-support` feature, which an engine enables from its dev-dependencies
//! alone.

use crate::{EncryptedStore, Engine, PersistError};
use secret::{DataKey, Held, KekName, KeyStore, SecretError};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Mutex, MutexGuard};

/// An engine in memory: what the layer hands an engine, and what an
/// engine's files would hold.
#[derive(Default)]
pub struct Memory {
    records: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl Memory {
    fn records(&self) -> MutexGuard<'_, BTreeMap<Vec<u8>, Vec<u8>>> {
        self.records.lock().expect("not poisoned")
    }

    /// Every key and value, end to end: what a scan of the files would see.
    #[must_use]
    pub fn everything(&self) -> Vec<u8> {
        self.records()
            .iter()
            .flat_map(|(key, value)| [key.as_slice(), value.as_slice()].concat())
            .collect()
    }
}

impl Engine for Memory {
    fn engine(&self) -> &'static str {
        "memory"
    }

    fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, PersistError> {
        Ok(self.records().get(key).cloned())
    }

    fn write(&self, key: &[u8], value: &[u8]) -> Result<(), PersistError> {
        self.records().insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn write_new(&self, key: &[u8], value: &[u8]) -> Result<bool, PersistError> {
        let mut records = self.records();
        if records.contains_key(key) {
            return Ok(false);
        }
        records.insert(key.to_vec(), value.to_vec());
        Ok(true)
    }

    fn remove(&self, key: &[u8]) -> Result<(), PersistError> {
        self.records().remove(key);
        Ok(())
    }
}

/// The record every check writes: a name and a value a scan can look for.
const KIND: &str = "journey";
const KEY: &[u8] = b"orders-4711";
const VALUE: &[u8] = b"PAYLOAD-4711-plain";

fn kek() -> KekName {
    KekName::new("runtime").expect("name")
}

/// Every file under `directory`, end to end: what an engine keeping its
/// records there hands [`conformance`] to scan.
///
/// # Panics
///
/// When the directory or a file in it cannot be read.
#[must_use]
pub fn everything_in(directory: &Path) -> Vec<u8> {
    let mut bytes = Vec::new();
    for entry in std::fs::read_dir(directory).expect("directory").flatten() {
        let path = entry.path();
        if path.is_dir() {
            bytes.extend(everything_in(&path));
        } else {
            bytes.extend(std::fs::read(&path).expect("file"));
        }
    }
    bytes
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// The checks every engine passes. `open` opens the engine on the same
/// records each time, and each is dropped before the next is opened, as an
/// engine that locks its files requires; `everything` is every byte the
/// engine keeps, its files end to end ([`everything_in`]).
///
/// Proves: a record comes back through the encryption after the engine is
/// closed and reopened; neither the record's key, its kind nor its value is
/// anywhere in the files; a tampered record is refused with its scope; the
/// store does not open under another key of the same name.
///
/// # Panics
///
/// When any of those does not hold.
pub fn conformance<E: Engine>(open: impl Fn() -> E, everything: impl Fn() -> Vec<u8>) {
    let keys = Held::new(secret::fixture::Memory::default());
    {
        let store = EncryptedStore::open(open(), &keys, &kek()).expect("open");
        store.put(KIND, KEY, VALUE).expect("put");
        assert!(store.put_new("lease", KEY, b"first").expect("put_new"));
        assert!(!store.put_new("lease", KEY, b"second").expect("put_new"));
    }
    {
        let store = EncryptedStore::open(open(), &keys, &kek()).expect("reopen");
        assert_eq!(store.get(KIND, KEY).expect("get"), Some(VALUE.to_vec()));
        assert_eq!(
            store.get("lease", KEY).expect("get"),
            Some(b"first".to_vec())
        );
    }

    let files = everything();
    assert!(!files.is_empty(), "the engine wrote no files");
    for needle in [KEY, VALUE, KIND.as_bytes()] {
        assert!(
            !contains(&files, needle),
            "'{}' is in the engine's files",
            String::from_utf8_lossy(needle)
        );
    }

    refuses_a_tampered_record(&open, &keys);
    refuses_another_key(&open);
}

fn refuses_a_tampered_record<E: Engine>(open: &impl Fn() -> E, keys: &dyn KeyStore) {
    let store = EncryptedStore::open(open(), keys, &kek()).expect("reopen");
    let located = store.lookup_key(KIND, KEY);
    let mut sealed = store.engine().read(&located).expect("read").expect("there");
    let last = sealed.len() - 1;
    sealed[last] ^= 1;
    store.engine().write(&located, &sealed).expect("tampered");
    let refused = store.get(KIND, KEY);
    let Err(PersistError::Refused { scope, .. }) = refused else {
        panic!("a tampered record must be refused, got {refused:?}");
    };
    assert!(scope.contains(store.engine().engine()), "{scope}");
}

fn refuses_another_key<E: Engine>(open: &impl Fn() -> E) {
    let other = Held::new(secret::fixture::Memory::default());
    other
        .wrap(&kek(), &DataKey::generate().expect("key"))
        .expect("another key of the same name");
    let refused = EncryptedStore::open(open(), &other, &kek());
    assert!(
        matches!(refused, Err(PersistError::Key(SecretError::Refused { .. }))),
        "another key must be refused"
    );
}
