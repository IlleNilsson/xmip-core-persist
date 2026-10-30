//! A step handed on, as Xmip Storage takes it: the one record of a hand-on
//! (`runtime-model.md` section 3, *Work moves by claim*), which its writer
//! decides as one write ([`super::claim::hand_on`]).

use codec::cursor::Cursor;

use super::failed::Failure;
use super::hold::Hold;
use super::record::{
    Claim, Form, JourneyRecord, MessageRecord, read_byte, read_u32, read_u64, read_u128,
    write_byte, write_u32, write_u64, write_u128,
};
use crate::PersistError;

/// One step handed on: its result, what it made and the Journeys of the
/// Messages that go on from it, written with the claim released, as one
/// write (`runtime-model.md` section 3: *Every hand-on is one atomic write
/// — the step's result, the Message's next place and the claim released
/// together*).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HandOn {
    pub claim: Claim,
    /// The claimed Message's Journey as the step left it.
    pub result: JourneyRecord,
    /// The Messages the step made.
    pub messages: Vec<MessageRecord>,
    /// The Journeys of the Messages that go on from it.
    pub next: Vec<JourneyRecord>,
    /// The queues the claimed Message leaves: its place in each let go of
    /// — a send that is done, or a held Message moved on.
    pub leaves: Vec<u128>,
    /// The places Messages take, each at the end of its queue: where the
    /// claimed Message, or one that goes on from it, waits next.
    pub queued: Vec<Hold>,
    /// The queues the claimed Message moves to the end of, keeping what its
    /// place kept beside it: an operator's Retry of a Message that failed,
    /// taken up again after everything that waits there now. A queue it
    /// holds no place in is passed over.
    pub requeued: Vec<u128>,
    /// Where the step leaves the claimed Message Failed, waiting in its
    /// queue for an operator: that queue and why, the Message entered in
    /// the queue's index of failed Messages and counted there, in this one
    /// write ([`super::failed`]).
    pub failed: Option<Failure>,
    /// The queues whose index of failed Messages the claimed Message leaves:
    /// an operator's Retry or Dismiss of it.
    pub cleared: Vec<u128>,
    /// Where the step leaves the claimed Message waiting for a retry: the
    /// queue it waits in and how long, its due time — Xmip Storage's now and
    /// that long — kept at its place there in this one write, its claim
    /// released with the rest, so nothing holds it until a scan finds it due
    /// (`runtime-model.md` section 10; open problem 43).
    pub waits: Option<Wait>,
    /// Where the holder goes on with the Message at once — a held Message
    /// moved to the queue of a Send Port its node sends — the claim kept for
    /// this many nanoseconds from now instead of released. A holder that
    /// dies meanwhile lets it lapse at that time, and another node takes it
    /// up.
    pub kept_for_nanos: Option<u64>,
}

/// Where a Message waits for a retry: its queue, and how long from Xmip
/// Storage's now it is due again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Wait {
    pub queue: u128,
    pub after_nanos: u64,
}

impl Form for HandOn {
    fn write(&self, out: &mut Vec<u8>) {
        self.claim.write(out);
        self.result.write(out);
        self.messages.write(out);
        self.next.write(out);
        write_queues(out, &self.leaves);
        self.queued.write(out);
        write_queues(out, &self.requeued);
        write_byte(out, u8::from(self.failed.is_some()));
        if let Some(failure) = &self.failed {
            failure.write(out);
        }
        write_queues(out, &self.cleared);
        write_byte(out, u8::from(self.waits.is_some()));
        if let Some(wait) = &self.waits {
            write_u128(out, wait.queue);
            write_u64(out, wait.after_nanos);
        }
        write_byte(out, u8::from(self.kept_for_nanos.is_some()));
        if let Some(nanos) = self.kept_for_nanos {
            write_u64(out, nanos);
        }
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            claim: Claim::read(cursor)?,
            result: JourneyRecord::read(cursor)?,
            messages: Vec::read(cursor)?,
            next: Vec::read(cursor)?,
            leaves: read_queues(cursor)?,
            queued: Vec::read(cursor)?,
            requeued: read_queues(cursor)?,
            failed: match read_byte(cursor)? {
                0 => None,
                _ => Some(Failure::read(cursor)?),
            },
            cleared: read_queues(cursor)?,
            waits: match read_byte(cursor)? {
                0 => None,
                _ => Some(Wait {
                    queue: read_u128(cursor)?,
                    after_nanos: read_u64(cursor)?,
                }),
            },
            kept_for_nanos: match read_byte(cursor)? {
                0 => None,
                _ => Some(read_u64(cursor)?),
            },
        })
    }
}

fn write_queues(out: &mut Vec<u8>, queues: &[u128]) {
    write_u32(out, u32::try_from(queues.len()).unwrap_or(u32::MAX));
    for queue in queues {
        write_u128(out, *queue);
    }
}

fn read_queues(cursor: &mut Cursor<'_>) -> Result<Vec<u128>, PersistError> {
    (0..read_u32(cursor)?).map(|_| read_u128(cursor)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{JourneyFacts, MessageFacts};
    use xcore::{JourneyId, MessageId};

    #[test]
    fn a_hand_on_comes_back_from_its_bytes_as_it_was() {
        let claim = Claim {
            journey: JourneyId::new(0x0199_0000_0000_7000_8000_0000_0000_0007),
            holder: configure::fixture::test_cluster().node_scope(0),
            token: 42,
            until_unix_nanos: -5,
            fence: 0,
        };
        let hand_on = HandOn {
            claim: claim.clone(),
            result: JourneyRecord {
                journey: claim.journey,
                body: b"done".to_vec(),
                facts: JourneyFacts::default(),
            },
            messages: vec![MessageRecord {
                message: MessageId::new(8),
                body: vec![0, 1, 2],
                facts: MessageFacts::default(),
            }],
            next: Vec::new(),
            leaves: vec![7, 8],
            queued: vec![Hold {
                queue: 9,
                journey: claim.journey,
                body: b"facts".to_vec(),
            }],
            requeued: vec![10],
            failed: Some(Failure {
                queue: 9,
                reason: "refused".to_string(),
            }),
            cleared: vec![11],
            waits: Some(Wait {
                queue: 9,
                after_nanos: 20_000_000,
            }),
            kept_for_nanos: Some(5_000_000_000),
        };
        assert_eq!(
            HandOn::from_bytes(&hand_on.bytes()).expect("hand-on"),
            hand_on
        );
        let waits_not = HandOn {
            kept_for_nanos: None,
            failed: None,
            waits: None,
            ..hand_on
        };
        assert_eq!(
            HandOn::from_bytes(&waits_not.bytes()).expect("hand-on"),
            waits_not
        );
    }
}
