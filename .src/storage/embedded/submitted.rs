//! What the embedded Storage node's one writer answered, as each operation
//! that submits to it reads the answer (`super::super::commit`): written,
//! a claim, the claims a Publication's node holds, or a verdict.

use super::super::commit::{Done, Op};
use super::super::record::{Claim, malformed};
use super::Embedded;
use crate::{Engine, PersistError};

impl<R: Engine + 'static, A: Engine> Embedded<R, A> {
    pub(super) fn written(&self, op: Op) -> Result<(), PersistError> {
        self.committer.submit(op).map(drop)
    }

    pub(super) fn claimed(&self, op: Op) -> Result<Option<Claim>, PersistError> {
        match self.committer.submit(op)? {
            Done::Claim(claim) => Ok(claim),
            _ => Err(malformed("the writer answered no claim")),
        }
    }

    pub(super) fn held(&self, op: Op) -> Result<Vec<Claim>, PersistError> {
        match self.committer.submit(op)? {
            Done::Claims(held) => Ok(held),
            _ => Err(malformed("the writer answered no claims")),
        }
    }

    pub(super) fn yes(&self, op: Op) -> Result<bool, PersistError> {
        match self.committer.submit(op)? {
            Done::Yes(yes) => Ok(yes),
            _ => Err(malformed("the writer answered no verdict")),
        }
    }
}
