//! What a Message is acted on under its claim, asked of a Storage node
//! (`XmipStorage::read_claimed`; `super::super::claimed`): one request,
//! one round trip, where the four reads one by one were four (open problem
//! 46, 2026-11-12).

use xcore::JourneyId;

use super::super::claimed::Claimed;
use super::super::wire::{Answer, Request};
use super::StorageClient;
use super::operations::expected;
use crate::PersistError;

impl StorageClient {
    /// The place `at` in `queue`, `journey`, its last Message and that
    /// Message's Stream's record, read at a Storage node as one.
    pub(super) fn claimed(
        &self,
        queue: u128,
        at: u64,
        journey: JourneyId,
    ) -> Result<Claimed, PersistError> {
        expected(
            self.ask(&Request::ReadClaimed(queue, at, journey))?,
            |answer| match answer {
                Answer::Claimed(claimed) => Ok(*claimed),
                other => Err(other),
            },
        )
    }
}
