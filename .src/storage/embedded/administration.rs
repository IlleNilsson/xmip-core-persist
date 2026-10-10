//! The embedded Storage node's administration database: what must be
//! shared and kept over time, each record under its kind and identifier,
//! written by one writer at a time in this process (`deployment-model.md`
//! section 7).

use std::sync::PoisonError;

use super::super::columns::ADMINISTRATION;
use super::super::record::{AdministrationKind, AdministrationRecord, Form};
use super::Embedded;
use crate::{Engine, PersistError};

impl<R: Engine + 'static, A: Engine> Embedded<R, A> {
    /// The record of `kind` and `id` written as `bytes`, or removed where
    /// they are none, with its index entries, in one write:
    /// `write_administration` and `remove_administration`.
    pub(super) fn administered(
        &self,
        kind: AdministrationKind,
        id: u128,
        bytes: Option<Vec<u8>>,
    ) -> Result<(), PersistError> {
        let _one = self
            .administering
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let kind = kind_of(kind);
        let tables = (&self.administration, &self.administration_columns);
        self.indexed(tables, vec![(&kind, id.to_be_bytes().to_vec(), bytes)])
    }

    /// The record of `kind` and `id`, or `None`: `read_administration`.
    pub(super) fn administration_record(
        &self,
        kind: AdministrationKind,
        id: u128,
    ) -> Result<Option<AdministrationRecord>, PersistError> {
        self.administration
            .get(&kind_of(kind), &id.to_be_bytes())?
            .map(|bytes| AdministrationRecord::from_bytes(&bytes))
            .transpose()
    }
}

/// What a record of `kind` is kept under.
fn kind_of(kind: AdministrationKind) -> String {
    format!("{ADMINISTRATION}{}", kind.word())
}
