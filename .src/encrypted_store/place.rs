//! A record's place: its kind, a zero byte, its key — what it is sealed
//! for and what its lookup key is hashed from. Every read and write of a
//! record makes one, so one that fits is made on the stack, never taken
//! from the heap: a kind and an identifier, or two, always do.

use core::ops::Deref;

/// The most a place keeps on the stack.
const SHORT: usize = 64;

/// A record's place, as bytes.
pub(super) enum Place {
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

/// `kind`'s record `key`'s place.
pub(super) fn place(kind: &str, key: &[u8]) -> Place {
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
