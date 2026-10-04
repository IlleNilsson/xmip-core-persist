//! Each of Xmip Storage's operations as a node asks it of a Storage node:
//! its request, and the answer it expects ([`super::StorageClient`]).

use std::sync::Arc;
use std::time::Duration;

use xcore::{AuditId, JourneyId, MessageId, StreamId};

use super::super::XmipStorage;
use super::super::dead::{DeadEntry, DeadQueue, Replay, Replayed};
use super::super::hand_on::HandOn;
use super::super::hold::HeldQueue;
use super::super::publication::Publication;
use super::super::record::{
    AdministrationKind, AdministrationRecord, AuditEntry, Claim, JourneyRecord, MessageRecord,
    StreamChunk,
};
use super::super::wire::{Answer, Request};
use super::StorageClient;
use crate::PersistError;

/// The answer an operation expected, or why there was none.
fn expected<T>(
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

fn done(answer: Answer) -> Result<(), Answer> {
    match answer {
        Answer::Done => Ok(()),
        other => Err(other),
    }
}

fn claimed(answer: Answer) -> Result<Option<Claim>, Answer> {
    match answer {
        Answer::Claim(claim) => Ok(claim),
        other => Err(other),
    }
}

fn yes(answer: Answer) -> Result<bool, Answer> {
    match answer {
        Answer::Yes(yes) => Ok(yes),
        other => Err(other),
    }
}

impl XmipStorage for StorageClient {
    fn pinned(&self) -> Option<Arc<dyn XmipStorage>> {
        self.pinned_client()
            .map(|pinned| Arc::new(pinned) as Arc<dyn XmipStorage>)
    }

    fn write_chunk(&self, chunk: &StreamChunk) -> Result<(), PersistError> {
        expected(self.ask(&Request::WriteChunk(chunk.clone()))?, done)
    }

    fn read_chunk(
        &self,
        stream: StreamId,
        index: u32,
    ) -> Result<Option<StreamChunk>, PersistError> {
        expected(
            self.ask(&Request::ReadChunk(stream, index))?,
            |answer| match answer {
                Answer::Chunk(chunk) => Ok(chunk),
                other => Err(other),
            },
        )
    }

    fn write_message(&self, message: &MessageRecord) -> Result<(), PersistError> {
        expected(self.ask(&Request::WriteMessage(message.clone()))?, done)
    }

    fn read_message(&self, message: MessageId) -> Result<Option<MessageRecord>, PersistError> {
        expected(
            self.ask(&Request::ReadMessage(message))?,
            |answer| match answer {
                Answer::Message(record) => Ok(record),
                other => Err(other),
            },
        )
    }

    fn write_journey(&self, journey: &JourneyRecord) -> Result<(), PersistError> {
        expected(self.ask(&Request::WriteJourney(journey.clone()))?, done)
    }

    fn publish(&self, publication: &Publication) -> Result<(), PersistError> {
        expected(self.ask(&Request::Publish(publication.clone()))?, done)
    }

    fn read_held(&self, queue: u128, from: u64, most: u32) -> Result<HeldQueue, PersistError> {
        expected(
            self.ask(&Request::ReadHeld(queue, from, most))?,
            |answer| match answer {
                Answer::Held(queue) => Ok(queue),
                other => Err(other),
            },
        )
    }

    fn read_dead(&self, queue: u128, from: u64, most: u32) -> Result<DeadQueue, PersistError> {
        expected(
            self.ask(&Request::ReadDead(queue, from, most))?,
            |answer| match answer {
                Answer::DeadQueue(queue) => Ok(queue),
                other => Err(other),
            },
        )
    }

    fn read_dead_message(
        &self,
        queue: u128,
        message: MessageId,
    ) -> Result<DeadEntry, PersistError> {
        expected(
            self.ask(&Request::ReadDeadMessage(queue, message))?,
            |answer| match answer {
                Answer::Dead(dead) => Ok(dead),
                other => Err(other),
            },
        )
    }

    fn replay(&self, replay: &Replay) -> Result<Replayed, PersistError> {
        expected(
            self.ask(&Request::Replay(replay.clone()))?,
            |answer| match answer {
                Answer::Replayed(replayed) => Ok(replayed),
                other => Err(other),
            },
        )
    }

    fn read_journey(&self, journey: JourneyId) -> Result<Option<JourneyRecord>, PersistError> {
        expected(
            self.ask(&Request::ReadJourney(journey))?,
            |answer| match answer {
                Answer::Journey(record) => Ok(record),
                other => Err(other),
            },
        )
    }

    fn claim(
        &self,
        journey: JourneyId,
        holder: &str,
        token: u128,
        lease: Duration,
    ) -> Result<Option<Claim>, PersistError> {
        let claim = Claim {
            journey,
            holder: holder.to_string(),
            token,
            until_unix_nanos: 0,
        };
        expected(self.ask(&Request::claim(claim, lease))?, claimed)
    }

    fn renew(&self, claim: &Claim, lease: Duration) -> Result<Option<Claim>, PersistError> {
        expected(self.ask(&Request::renew(claim.clone(), lease))?, claimed)
    }

    fn release(&self, claim: &Claim) -> Result<bool, PersistError> {
        expected(self.ask(&Request::Release(claim.clone()))?, yes)
    }

    fn hand_on(&self, hand_on: &HandOn) -> Result<bool, PersistError> {
        expected(self.ask(&Request::HandOn(hand_on.clone()))?, yes)
    }

    fn write_audit(&self, entry: &AuditEntry) -> Result<(), PersistError> {
        expected(self.ask(&Request::WriteAudit(entry.clone()))?, done)
    }

    fn keep_audit(&self, most: u32) -> Result<u32, PersistError> {
        expected(
            self.ask(&Request::KeepAudit(most))?,
            |answer| match answer {
                Answer::Count(count) => Ok(count),
                other => Err(other),
            },
        )
    }

    fn read_kept_audit(&self, id: AuditId) -> Result<Option<AuditEntry>, PersistError> {
        expected(
            self.ask(&Request::ReadKeptAudit(id))?,
            |answer| match answer {
                Answer::Audit(entry) => Ok(entry),
                other => Err(other),
            },
        )
    }

    fn write_administration(&self, record: &AdministrationRecord) -> Result<(), PersistError> {
        expected(
            self.ask(&Request::WriteAdministration(record.clone()))?,
            done,
        )
    }

    fn read_administration(
        &self,
        kind: AdministrationKind,
        id: u128,
    ) -> Result<Option<AdministrationRecord>, PersistError> {
        expected(
            self.ask(&Request::ReadAdministration(kind, id))?,
            |answer| match answer {
                Answer::Administration(record) => Ok(record),
                other => Err(other),
            },
        )
    }

    fn remove_administration(
        &self,
        kind: AdministrationKind,
        id: u128,
    ) -> Result<(), PersistError> {
        expected(self.ask(&Request::RemoveAdministration(kind, id))?, done)
    }
}
