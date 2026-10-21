//! A search of the embedded Storage node's databases: one index of one
//! table, in the database the query's plan names (`super::super::query`).

use super::super::query::Query;
use super::super::schema::Database;
use super::Embedded;
use crate::{Engine, PersistError};

impl<R: Engine + 'static, A: Engine> Embedded<R, A> {
    /// The records `query` finds: `query`.
    pub(super) fn searched(&self, query: &Query) -> Result<Vec<u128>, PersistError> {
        let plan = query.ask.plan();
        let asked = (query.most, query.newest_first);
        let at = (plan.table, plan.index);
        match plan.database {
            Database::Runtime => {
                self.runtime_columns
                    .find(&self.runtime, at, &plan.equal, plan.range, asked)
            }
            Database::Administration => self.administration_columns.find(
                &self.administration,
                at,
                &plan.equal,
                plan.range,
                asked,
            ),
            Database::Audit => {
                self.audit_columns
                    .find(&self.audit, at, &plan.equal, plan.range, asked)
            }
        }
    }
}
