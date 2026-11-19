//! A record's place: its kind, a zero byte, its key — what it is sealed
//! for and what its lookup key is hashed from. Every read and write of a
//! record makes one, so one that fits is made on the stack, never taken
//! from the heap: a kind and an identifier, or two, always do. The one
//! writer finds what a batch wrote, read or keeps by it too
//! (`storage::commit`; 2026-11-15), so a record's key is made once a lookup,
//! and **a map keyed by places keeps each as it was made**, in the map's
//! own room, never copied to the heap, and hashes it a word at a time
//! ([`PlaceMap`]; 2026-11-17): a batch's writes and reads, what it laid
//! out, and what its writer keeps.

use core::borrow::Borrow;
use core::hash::{BuildHasherDefault, Hash, Hasher};
use core::ops::Deref;
use std::collections::HashMap;

/// A map keyed by places, as the one writer keeps them ([`PlaceHasher`]).
pub(crate) type PlaceMap<V> = HashMap<Place, V, BuildHasherDefault<PlaceHasher>>;

/// A place's hash, a word at a time, as a compiler hashes its own tables:
/// a rotate, an exclusive or and a multiply each. A place is a kind and
/// identifiers Xmip drew or named, never bytes a far end chose, so no
/// seed is kept against chosen collisions; the map's one use is the
/// writer's own (2026-11-17).
#[derive(Default)]
pub(crate) struct PlaceHasher(u64);

/// The multiplier: odd, its bits spread.
const SPREAD: u64 = 0x51_7c_c1_b7_27_22_0a_95;

impl PlaceHasher {
    fn add(&mut self, word: u64) {
        self.0 = (self.0.rotate_left(5) ^ word).wrapping_mul(SPREAD);
    }
}

impl Hasher for PlaceHasher {
    fn write(&mut self, bytes: &[u8]) {
        let (words, rest) = bytes.as_chunks::<8>();
        for word in words {
            self.add(u64::from_le_bytes(*word));
        }
        if !rest.is_empty() {
            let mut last = [0; 8];
            last[..rest.len()].copy_from_slice(rest);
            self.add(u64::from_le_bytes(last));
        }
    }

    fn write_usize(&mut self, length: usize) {
        self.add(u64::try_from(length).unwrap_or(u64::MAX));
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

/// The most a place keeps on the stack.
const SHORT: usize = 64;

/// A record's place, as bytes: equal, hashed and looked up as its bytes
/// are.
#[derive(Clone)]
pub(crate) enum Place {
    Short { bytes: [u8; SHORT], length: usize },
    Long(Vec<u8>),
}

impl Deref for Place {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match self {
            Self::Short { bytes, length } => &bytes[..*length],
            Self::Long(bytes) => bytes,
        }
    }
}

impl Borrow<[u8]> for Place {
    fn borrow(&self) -> &[u8] {
        self
    }
}

impl PartialEq for Place {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl Eq for Place {}

impl Hash for Place {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (**self).hash(state);
    }
}

impl core::fmt::Debug for Place {
    fn fmt(&self, out: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        (**self).fmt(out)
    }
}

/// `kind`'s record `key`'s place.
pub(crate) fn place(kind: &str, key: &[u8]) -> Place {
    let length = kind.len() + 1 + key.len();
    if length > SHORT {
        return Place::Long([kind.as_bytes(), &[0], key].concat());
    }
    let mut bytes = [0; SHORT];
    bytes[..kind.len()].copy_from_slice(kind.as_bytes());
    bytes[kind.len() + 1..length].copy_from_slice(key);
    Place::Short { bytes, length }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_place_is_its_kind_a_zero_and_its_key_however_long() {
        for key in [&b"7"[..], &[9; 100]] {
            let expected = [b"journey".as_slice(), &[0], key].concat();
            assert_eq!(&*place("journey", key), expected.as_slice());
        }
    }
}
