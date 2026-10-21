//! A range of an index read in its order, each entry with the values its
//! key holds after the columns asked to be equal, so where each is in that
//! order is known without reading its record: a queue's page by place
//! (`super::super::queue`) and a queue's parts by when each is due
//! (`super::super::due`).

use super::super::record::malformed;
use super::super::row::Value;
use super::Columns;
use crate::{EncryptedStore, Engine, PersistError};

impl Columns {
    /// The records `index` of `table` finds whose first columns are
    /// `equal` and whose last — a place, a count or a time — is `from` or
    /// later: up to `most`, in the index's order, each with that last
    /// value as its entry's key holds it, so where each is in that order is
    /// known without reading it. One range of the engine.
    ///
    /// # Errors
    ///
    /// Where the index is not one whose last column follows `equal`, or an
    /// entry fails its tag.
    pub(crate) fn find_from<E: Engine>(
        &self,
        store: &EncryptedStore<E>,
        at: (&str, &str),
        equal: &[Value],
        from: u64,
        most: usize,
    ) -> Result<Vec<(u64, u128)>, PersistError> {
        let found = self.find_ordered(store, at, equal, (&[from], u64::MAX), most)?;
        Ok(found
            .into_iter()
            .map(|(values, id)| (values.first().copied().unwrap_or_default(), id))
            .collect())
    }

    /// The records `index` of `table` finds whose first columns are
    /// `equal`, and whose columns after them — each a place, a count or a
    /// time — are from the values `from` on, the first of them `to` at the
    /// most: up to `most`, in the index's order, each with those values as
    /// its entry's key holds them. One range of the engine.
    ///
    /// # Errors
    ///
    /// Where the index is not one whose columns after `equal` are as many
    /// as `from` says or more, or an entry fails its tag.
    pub(crate) fn find_ordered<E: Engine>(
        &self,
        store: &EncryptedStore<E>,
        at: (&str, &str),
        equal: &[Value],
        (from, to): (&[u64], u64),
        most: usize,
    ) -> Result<Vec<(Vec<u64>, u128)>, PersistError> {
        let (index, prefix) = self.prefix(at, equal)?;
        let trailing = index.columns.len().saturating_sub(equal.len());
        let ordered = || malformed(format!("{}: not in that order", index.name));
        let Some(&start) = from.first() else {
            return Err(ordered());
        };
        if from.len() > trailing || start > to {
            return Ok(Vec::new());
        }
        let mut first = prefix.clone();
        for value in from {
            first.extend(value.to_be_bytes());
        }
        // Past every key whose first value after `equal` is `to`: its other
        // values, eight bytes each, and the record's identifier, sixteen.
        let last = [
            prefix.as_slice(),
            &to.to_be_bytes(),
            &vec![0xFF; 8 * (trailing - 1) + 17],
        ]
        .concat();
        let entries = store.scan_index_keyed(&first, &last, most, false)?;
        entries
            .into_iter()
            .map(|(key, id)| {
                let values = key
                    .get(prefix.len()..key.len().saturating_sub(16))
                    .filter(|values| values.len() == 8 * trailing)
                    .ok_or_else(ordered)?;
                let (values, _) = values.as_chunks::<8>();
                let values = values.iter().map(|value| u64::from_be_bytes(*value));
                let values = values.collect();
                Ok((values, id))
            })
            .collect()
    }
}
