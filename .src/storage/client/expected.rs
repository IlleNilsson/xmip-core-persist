//! The answer each operation expects of a Storage node, or why there was
//! none ([`super::StorageClient`]).

use super::super::record::Claim;
use super::super::wire::Answer;
use crate::PersistError;

/// The answer an operation expected, or why there was none.
pub(super) fn expected<T>(
    answer: Answer,
    take: impl FnOnce(Answer) -> Result<T, Answer>,
) -> Result<T, PersistError> {
    match answer {
        Answer::Refused(scope, reason) => Err(PersistError::Refused { scope, reason }),
        Answer::Failed(reason) => Err(PersistError::Failed { reason }),
        other => take(other).map_err(|other| PersistError::Record {
            reason: format!("a Storage node answered {other:?}"),
        }),
    }
}

pub(super) fn done(answer: Answer) -> Result<(), Answer> {
    match answer {
        Answer::Done => Ok(()),
        other => Err(other),
    }
}

pub(super) fn claimed(answer: Answer) -> Result<Option<Claim>, Answer> {
    match answer {
        Answer::Claim(claim) => Ok(claim),
        other => Err(other),
    }
}

pub(super) fn held(answer: Answer) -> Result<Vec<Claim>, Answer> {
    match answer {
        Answer::Claims(held) => Ok(held),
        other => Err(other),
    }
}

pub(super) fn yes(answer: Answer) -> Result<bool, Answer> {
    match answer {
        Answer::Yes(yes) => Ok(yes),
        other => Err(other),
    }
}
