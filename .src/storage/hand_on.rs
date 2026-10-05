//! A step handed on, as Xmip Storage takes it: the one record of a hand-on
//! (`runtime-model.md` section 3, *Work moves by claim*), which its writer
//! decides as one write ([`super::claim::hand_on`]).

use codec::cursor::Cursor;

use super::hold::Hold;
use super::record::{
    Claim, Form, JourneyRecord, MessageRecord, read_byte, read_u32, read_u64, read_u128,
    write_byte, write_u32, write_u64, write_u128,
};
use crate::PersistError;

/// One step handed on: its result, what it made and the Journeys that go
/// on from it, written with the claim released, as one write
/// (`runtime-model.md` section 3: *Every hand-on is one atomic write — the
/// step's result, the next Journey and the claim released together*).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HandOn {
    pub claim: Claim,
    /// The claimed Journey as the step left it.
    pub result: JourneyRecord,
    /// The Messages the step made.
    pub messages: Vec<MessageRecord>,
    /// The Journeys that go on from it.
    pub next: Vec<JourneyRecord>,
    /// The queues the claimed Journey leaves: its place in each let go of
    /// — a send that is done, or a held Journey moved on.
    pub leaves: Vec<u128>,
    /// The places Journeys take, each at the end of its queue: where the
    /// claimed Journey, or one that goes on from it, waits next.
    pub queued: Vec<Hold>,
    /// The queues the claimed Journey moves to the end of, keeping what its
    /// place kept beside it: an operator's Retry of a Journey that failed,
    /// taken up again after everything that waits there now. A queue it
    /// holds no place in is passed over.
    pub requeued: Vec<u128>,
    /// Where the step waits rather than ends — a retry's backoff — the
    /// claim kept for this many nanoseconds from now instead of released:
    /// the due time is in the Ledger, and no thread holds it
    /// (`runtime-model.md` section 10). A holder that dies meanwhile lets
    /// it lapse at that time, and another node takes it up.
    pub kept_for_nanos: Option<u64>,
}

impl Form for HandOn {
    fn write(&self, out: &mut Vec<u8>) {
        self.claim.write(out);
        self.result.write(out);
        self.messages.write(out);
        self.next.write(out);
        write_u32(out, u32::try_from(self.leaves.len()).unwrap_or(u32::MAX));
        for queue in &self.leaves {
            write_u128(out, *queue);
        }
        self.queued.write(out);
        write_u32(out, u32::try_from(self.requeued.len()).unwrap_or(u32::MAX));
        for queue in &self.requeued {
            write_u128(out, *queue);
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
            leaves: (0..read_u32(cursor)?)
                .map(|_| read_u128(cursor))
                .collect::<Result<_, _>>()?,
            queued: Vec::read(cursor)?,
            requeued: (0..read_u32(cursor)?)
                .map(|_| read_u128(cursor))
                .collect::<Result<_, _>>()?,
            kept_for_nanos: match read_byte(cursor)? {
                0 => None,
                _ => Some(read_u64(cursor)?),
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xcore::{JourneyId, MessageId};

    #[test]
    fn a_hand_on_comes_back_from_its_bytes_as_it_was() {
        let claim = Claim {
            journey: JourneyId::new(0x0199_0000_0000_7000_8000_0000_0000_0007),
            holder: configure::fixture::test_cluster().node_scope(0),
            token: 42,
            until_unix_nanos: -5,
        };
        let hand_on = HandOn {
            claim: claim.clone(),
            result: JourneyRecord {
                journey: claim.journey,
                body: b"done".to_vec(),
            },
            messages: vec![MessageRecord {
                message: MessageId::new(8),
                body: vec![0, 1, 2],
            }],
            next: Vec::new(),
            leaves: vec![7, 8],
            queued: vec![Hold {
                queue: 9,
                journey: claim.journey,
                body: b"facts".to_vec(),
            }],
            requeued: vec![10],
            kept_for_nanos: Some(5_000_000_000),
        };
        assert_eq!(
            HandOn::from_bytes(&hand_on.bytes()).expect("hand-on"),
            hand_on
        );
        let waits_not = HandOn {
            kept_for_nanos: None,
            ..hand_on
        };
        assert_eq!(
            HandOn::from_bytes(&waits_not.bytes()).expect("hand-on"),
            waits_not
        );
    }
}
