//! The embedded Storage node's `membership` table
//! (`super::super::membership`): a node's row written with its node's row
//! before it, no longer the latest, in one write, each numbered by the
//! table's writes, and every waiter on the table told; the members now read
//! by one range of the index of latest rows.
use codec::form::Form;
use xcore::lock::Locked;

use super::super::membership::{MEMBERSHIP, MemberRecord};
use super::super::row::Value;
use super::super::watch::{Change, Watched};
use super::Embedded;
use crate::{Engine, PersistError};

/// The index of each node's latest row.
const LATEST: (&str, &str) = (MEMBERSHIP, "membership_latest");

/// The index of the rows by the table's write that last wrote each.
const WRITTEN: (&str, &str) = (MEMBERSHIP, "membership_written");

impl<R: Engine + 'static, A: Engine> Embedded<R, A> {
    /// `member` written as its node's latest row, the row of its node it
    /// replaces as the latest written no longer the latest, in one write:
    /// `write_member`. Every waiter on the table is told once it is written.
    pub(super) fn member_written(&self, member: &MemberRecord) -> Result<(), PersistError> {
        let _one = self.administering.locked();
        let mut written = self.members_written()?;
        let mut changes = Vec::new();
        for process in self.latest_of(&member.node)? {
            if process == member.process {
                continue;
            }
            if let Some(earlier) = self.member(process)? {
                written += 1;
                let earlier = MemberRecord {
                    latest: false,
                    written,
                    ..earlier
                };
                changes.push((MEMBERSHIP, key(process), Some(earlier.bytes())));
            }
        }
        written += 1;
        let row = MemberRecord {
            latest: true,
            written,
            ..member.clone()
        };
        changes.push((MEMBERSHIP, key(member.process), Some(row.bytes())));
        let tables = (&self.administration, &self.administration_columns);
        self.indexed(tables, changes)?;
        self.watch.moved(&[Change {
            watched: Watched::Membership,
            at: Some(written),
            own: None,
        }]);
        Ok(())
    }

    /// Each node's latest row, by its node: `read_members`.
    pub(super) fn members_now(&self) -> Result<Vec<MemberRecord>, PersistError> {
        let latest = [Value::Flag(true)];
        let found = self.administration_columns.find(
            &self.administration,
            LATEST,
            &latest,
            None,
            (u32::MAX, false),
        )?;
        let mut members = Vec::with_capacity(found.len());
        for process in found {
            members.extend(self.member(process)?);
        }
        Ok(members)
    }

    /// How many writes the table has taken: the number of its latest, what
    /// a wait on it counts; zero where it holds no row.
    pub(super) fn members_written(&self) -> Result<u64, PersistError> {
        let last = self.administration_columns.find(
            &self.administration,
            WRITTEN,
            &[],
            None,
            (1, true),
        )?;
        match last.first() {
            Some(process) => Ok(self.member(*process)?.map_or(0, |row| row.written)),
            None => Ok(0),
        }
    }

    /// The latest row of `node`: one, or none before it first wrote.
    fn latest_of(&self, node: &str) -> Result<Vec<u128>, PersistError> {
        let equal = [Value::Flag(true), Value::Text(node.to_string())];
        self.administration_columns.find(
            &self.administration,
            LATEST,
            &equal,
            None,
            (u32::MAX, false),
        )
    }

    fn member(&self, process: u128) -> Result<Option<MemberRecord>, PersistError> {
        self.administration
            .get(MEMBERSHIP, &process.to_be_bytes())?
            .map(|bytes| MemberRecord::from_bytes(&bytes))
            .transpose()
            .map_err(PersistError::from)
    }
}

fn key(process: u128) -> Vec<u8> {
    process.to_be_bytes().to_vec()
}

#[cfg(test)]
mod tests;
