//! What a paused Subscription holds, in the Ledger (`runtime-model.md`
//! section 9: *A paused Subscription's Journey is written and held until
//! the Subscription is resumed, and the held ones are picked up oldest
//! first*).
//!
//! **A held Message is a held Journey.** Its Stream and its Message are in
//! the Ledger from the receive, and its Journey from the Publication; what
//! holding adds is the Journey's place in its Subscription's queue — a
//! [`Hold`], written in the Publication's one write, so a receive whose
//! hold was not written is not acknowledged. Xmip Storage numbers each as
//! it is written, from zero, as it numbers every queue ([`super::queue`]).
//!
//! **Held once, by its Journey.** A held Journey is kept under its Journey's
//! identifier as well as its place, so a Publication asked again after a
//! lost answer finds it held already and takes no second place: one
//! Journey, one delivery (the dedup key is the Journey's identifier).
//!
//! **Released only once its Journey is.** [`super::XmipStorage::release_held`]
//! writes the Journey as the step left it and lets go of its place in one
//! write: a Journey that was delivered, or stopped for good. One whose send
//! failed is written Failed and keeps its place — the Message stays with
//! its Journey (`runtime-model.md` section 12).
//!
//! A queue is found by its identifier: [`named`], the name-based `UUID` of
//! the URI naming the Subscription, so the operator state that pauses it in
//! the administration database is found under the same.

use codec::cursor::Cursor;
use xcore::JourneyId;

use super::commit::{Batch, HELD, HELD_JOURNEY, PLACES};
use super::queue::{self, Kinds};
use super::record::{Form, read_bytes, read_u64, read_u128, write_bytes, write_u64, write_u128};
use crate::{EncryptedStore, Engine, PersistError};

/// A Journey to hold: which queue, which Journey, and what the holder
/// needs to pick it up, in its own form.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hold {
    /// The queue: the Subscription's identifier ([`named`]).
    pub queue: u128,
    pub journey: JourneyId,
    /// The holder's own words, kept as given.
    pub body: Vec<u8>,
}

/// A Journey held, at its place in its queue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Held {
    /// Its place, given by Xmip Storage as it was held: oldest lowest.
    pub sequence: u64,
    pub hold: Hold,
}

/// A queue as read: where it starts and ends, how many it holds, and the
/// ones asked for, oldest first.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HeldQueue {
    /// The place of the oldest it still holds; `next` where it holds none.
    pub first: u64,
    /// The place the next it holds takes.
    pub next: u64,
    /// How many it holds.
    pub count: u64,
    /// The ones read, oldest first.
    pub held: Vec<Held>,
}

/// The URL namespace of RFC 9562 section 6.6, which an `xmip://` URI is
/// named in.
const URL_NAMESPACE: u128 = 0x6ba7_b811_9dad_11d1_80b4_00c0_4fd4_30c8;

/// The identifier of what `uri` names: its name-based `UUID`, version 5 of
/// RFC 9562 (SHA-1 in the URL namespace), the same for the same URI on
/// every node — how a record about something configuration names, a
/// Subscription's queue and its operator state, is found by it.
#[must_use]
pub fn named(uri: &str) -> u128 {
    version_five(URL_NAMESPACE, uri.as_bytes())
}

/// The version 5 `UUID` of `name` in `namespace` (RFC 9562 section 5.5).
fn version_five(namespace: u128, name: &[u8]) -> u128 {
    let named = [namespace.to_be_bytes().as_slice(), name].concat();
    let digest = codec::sha1::digest(&named);
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    u128::from_be_bytes(bytes)
}

/// Where a Subscription's queue keeps what it holds.
pub(crate) const KINDS: Kinds = Kinds {
    entry: HELD,
    by: HELD_JOURNEY,
    places: PLACES,
};

/// `kept` kept at the end of its queue, in `batch`: the place it takes is
/// the queue's next. A Journey its queue holds already keeps the place it
/// has, so a Publication asked again holds it once.
pub(crate) fn keep<R: Engine>(
    store: &EncryptedStore<R>,
    batch: &mut Batch<'_>,
    kept: &Hold,
) -> Result<(), PersistError> {
    let about = (kept.queue, kept.journey.value());
    queue::place(store, batch, KINDS, about, |sequence| {
        Held {
            sequence,
            hold: kept.clone(),
        }
        .bytes()
    })
    .map(drop)
}

/// The Journey held at `sequence` in `queue` let go of, in `batch`: its
/// place gone, and the queue's first moved past every place let go of
/// before it. Letting go of what is not held changes nothing, so a release
/// asked again after a lost answer does nothing twice.
pub(crate) fn let_go<R: Engine>(
    store: &EncryptedStore<R>,
    batch: &mut Batch<'_>,
    queue: u128,
    sequence: u64,
) -> Result<(), PersistError> {
    let Some(bytes) = queue::take_out(store, batch, KINDS, queue, sequence)? else {
        return Ok(());
    };
    let held = Held::from_bytes(&bytes)?;
    queue::forget(batch, KINDS, (queue, held.hold.journey.value()));
    Ok(())
}

/// `queue` as `store` holds it: its places, and up to `most` it holds from
/// the place `from` on, oldest first. A place let go of out of turn is
/// passed over.
pub(crate) fn read<R: Engine>(
    store: &EncryptedStore<R>,
    queue: u128,
    from: u64,
    most: u32,
) -> Result<HeldQueue, PersistError> {
    let (places, entries) = queue::page(store, KINDS, queue, from, most)?;
    Ok(HeldQueue {
        first: places.first,
        next: places.next,
        count: places.count,
        held: entries
            .iter()
            .map(|bytes| Held::from_bytes(bytes))
            .collect::<Result<_, _>>()?,
    })
}

impl Form for Hold {
    fn write(&self, out: &mut Vec<u8>) {
        write_u128(out, self.queue);
        write_u128(out, self.journey.value());
        write_bytes(out, &self.body);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            queue: read_u128(cursor)?,
            journey: JourneyId::new(read_u128(cursor)?),
            body: read_bytes(cursor)?,
        })
    }
}

impl Form for Held {
    fn write(&self, out: &mut Vec<u8>) {
        write_u64(out, self.sequence);
        self.hold.write(out);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            sequence: read_u64(cursor)?,
            hold: Hold::read(cursor)?,
        })
    }
}

impl Form for HeldQueue {
    fn write(&self, out: &mut Vec<u8>) {
        write_u64(out, self.first);
        write_u64(out, self.next);
        write_u64(out, self.count);
        self.held.write(out);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            first: read_u64(cursor)?,
            next: read_u64(cursor)?,
            count: read_u64(cursor)?,
            held: Vec::read(cursor)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_name_is_its_version_five_uuid_the_same_every_time() {
        // The DNS namespace's 'python.org', as Python's uuid5 has it.
        let dns = 0x6ba7_b810_9dad_11d1_80b4_00c0_4fd4_30c8;
        assert_eq!(
            version_five(dns, b"python.org"),
            0x8863_13e1_3b8a_5372_9b90_0c9a_ee19_9e5d
        );
        let node = configure::fixture::test_cluster().node_scope(0);
        let one = named(&format!("{node}/subscription/orders"));
        assert_eq!(one, named(&format!("{node}/subscription/orders")));
        assert_ne!(one, named(&format!("{node}/subscription/billing")));
        assert_eq!((one >> 76) & 0xf, 5, "version 5");
        assert_eq!((one >> 62) & 0x3, 0b10, "the RFC's variant");
    }

    #[test]
    fn a_held_queue_comes_back_from_its_bytes_as_it_was() {
        let queue = HeldQueue {
            first: 2,
            next: 5,
            count: 2,
            held: vec![Held {
                sequence: 3,
                hold: Hold {
                    queue: 9,
                    journey: JourneyId::new(4),
                    body: b"facts".to_vec(),
                },
            }],
        };
        assert_eq!(HeldQueue::from_bytes(&queue.bytes()).expect("read"), queue);
    }
}
