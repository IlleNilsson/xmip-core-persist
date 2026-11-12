//! What a node reads of a Message under the claim it took on its Journey,
//! asked as one (`XmipStorage::read_claimed`; open problem 46, 2026-11-12):
//! its place in its queue, its Journey, the Message its Journey holds last,
//! and the record of the Stream that Message's first Section is over — the
//! Message and the Stream found by what Xmip Storage keeps of each in
//! columns (`super::facts`), never by reading a body. Until 2026-11-12 a
//! node woken by another's Publication asked them one by one, four round
//! trips after its claim, 0.9 ms in the Essentials.

use codec::cursor::Cursor;
use codec::form::Form;
use codec::writer::ByteWriter;
use xcore::{JourneyId, MessageId, StreamId};

use super::XmipStorage;
use super::hold::HeldQueue;
use super::record::{JourneyRecord, MessageRecord};
use super::stream::StreamRecord;
use crate::PersistError;

/// What [`read`] found, each `None` where none is kept.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Claimed {
    /// The queue as read from the place asked, one entry at most.
    pub place: HeldQueue,
    pub journey: Option<JourneyRecord>,
    /// The Message the Journey holds last, as its columns say.
    pub message: Option<MessageRecord>,
    /// The Stream the Message's first Section is over, as its columns say.
    pub stream: Option<StreamRecord>,
}

/// The place `at` in `queue` read from `storage`, one entry at most, and
/// `journey`, its last Message and that Message's Stream: what a claim on
/// `journey` is acted on under.
///
/// # Errors
///
/// When one cannot be read, or fails its tag.
pub fn read<S: XmipStorage + ?Sized>(
    storage: &S,
    (queue, at): (u128, u64),
    journey: JourneyId,
) -> Result<Claimed, PersistError> {
    let place = storage.read_held(queue, at, 1)?;
    let journey = storage.read_journey(journey)?;
    let message = match journey.as_ref().and_then(|kept| kept.facts.message) {
        Some(message) => storage.read_message(MessageId::new(message))?,
        None => None,
    };
    let stream = match message.as_ref().and_then(|kept| kept.facts.stream) {
        Some(stream) => storage.read_stream(StreamId::new(stream))?,
        None => None,
    };
    Ok(Claimed {
        place,
        journey,
        message,
        stream,
    })
}

fn write_optional<T: Form>(out: &mut Vec<u8>, value: Option<&T>) {
    out.byte(u8::from(value.is_some()));
    if let Some(value) = value {
        value.write(out);
    }
}

fn read_optional<T: Form>(cursor: &mut Cursor<'_>) -> codec::Result<Option<T>> {
    match cursor.byte()? {
        0 => Ok(None),
        _ => T::read(cursor).map(Some),
    }
}

impl Form for Claimed {
    fn write(&self, out: &mut Vec<u8>) {
        self.place.write(out);
        write_optional(out, self.journey.as_ref());
        write_optional(out, self.message.as_ref());
        write_optional(out, self.stream.as_ref());
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        Ok(Self {
            place: HeldQueue::read(cursor)?,
            journey: read_optional(cursor)?,
            message: read_optional(cursor)?,
            stream: read_optional(cursor)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::crowd::{QUEUE, node};
    use crate::storage::{
        AuditEntry, AuditFacts, Hold, JourneyFacts, MessageFacts, Publication, StreamChunk,
    };
    use xcore::AuditId;

    #[test]
    fn a_journey_its_last_message_and_that_message_s_stream_are_read_with_its_place() {
        let (node, _) = node();
        let (journey, message, stream) = (JourneyId::new(5), MessageId::new(6), StreamId::new(7));
        let last = StreamChunk {
            stream,
            index: 0,
            bytes: b"order".to_vec(),
        };
        let record = StreamRecord {
            stream,
            length: 5,
            chunks: 1,
            digest: [1; super::super::DIGEST],
            written_unix_nanos: 0,
        };
        node.write_stream(&last, &record).expect("written");
        let publication = Publication {
            message: MessageRecord {
                message,
                body: b"its Sections".to_vec(),
                facts: MessageFacts {
                    stream: Some(stream.value()),
                    ..MessageFacts::default()
                },
            },
            journeys: vec![JourneyRecord {
                journey,
                body: b"to send".to_vec(),
                facts: JourneyFacts {
                    message: Some(message.value()),
                    ..JourneyFacts::default()
                },
            }],
            held: vec![Hold {
                queue: QUEUE,
                journey,
                body: Vec::new(),
            }],
            dead: None,
            audit: AuditEntry {
                id: AuditId::new(5),
                body: Vec::new(),
                audited: None,
                facts: AuditFacts::default(),
            },
            claims: Vec::new(),
            lease_nanos: 0,
        };
        node.publish(publication).expect("published");
        let claimed = node.read_claimed(QUEUE, 0, journey).expect("read");
        assert_eq!(claimed.place.held.len(), 1, "its place");
        assert_eq!(claimed.journey.map(|kept| kept.journey), Some(journey));
        assert_eq!(claimed.message.map(|kept| kept.message), Some(message));
        assert_eq!(claimed.stream.map(|kept| kept.length), Some(5));
        let nothing = node
            .read_claimed(QUEUE, 1, JourneyId::new(9))
            .expect("read");
        assert!(nothing.place.held.is_empty(), "nothing at that place");
        assert_eq!(
            (nothing.journey.is_none(), nothing.message.is_none()),
            (true, true)
        );
        assert_eq!(nothing.stream, None);
        let claimed = node.read_claimed(QUEUE, 0, journey).expect("read");
        let crossed = Claimed::from_bytes(&claimed.bytes()).expect("read back");
        assert_eq!(crossed, claimed, "across a frame as it was");
    }
}
