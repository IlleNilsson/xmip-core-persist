//! What the one writer keeps of the records it wrote: within its bound, its
//! kinds alone, forgotten by a writer that panicked; and a Message's hand-on
//! read from it, none of its records from the store.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize};

use secret::{Held, KekName};
use xcore::{Clock, JourneyId, SystemClock};

use super::super::waiting::Decides;
use super::super::{Op, Writer};
use super::*;
use crate::EncryptedStore;
use crate::fixture::crowd::{LEASE, QUEUE, hand_on, node, published};
use crate::fixture::{Hooked, Hooks, Memory};
use crate::storage::XmipStorage;
use crate::storage::columns::Columns;
use crate::storage::record::Claim;
use crate::storage::schema::Database;

/// `key` of `kind` left holding four bytes of `value`, or nothing.
fn written(kind: &'static str, key: u8, value: Option<u8>) -> RecordChange<'static> {
    (kind, vec![key], value.map(|value| vec![value; 4]))
}

#[test]
fn past_the_most_its_node_says_the_first_kept_is_let_go_of_and_one_kept_is_replaced() {
    let one = cost(&place(CLAIM, &[0]), Some(&vec![0; 4]));
    let mut kept = Kept::default();
    let four = (0..4).map(|key| written(CLAIM, key, Some(key)));
    kept.written(four.collect(), 3 * one);
    assert_eq!(kept.len(), 3);
    assert_eq!(kept.get(&place(CLAIM, &[0])), None, "the first let go of");
    kept.written(vec![written(CLAIM, 1, Some(9))], 3 * one);
    assert_eq!(
        kept.get(&place(CLAIM, &[1])),
        Some(&Some(vec![9; 4])),
        "replaced"
    );
    assert_eq!(kept.order.len(), 3, "replaced, not kept twice");
    kept.written(vec![written(CLAIM, 2, None)], 3 * one);
    assert_eq!(kept.get(&place(CLAIM, &[2])), Some(&None), "kept taken out");
    // Its node says fewer: the first kept let go of at the next write.
    kept.written(vec![written(CLAIM, 7, Some(7))], one);
    assert_eq!(kept.len(), 1);
    assert_eq!(kept.get(&place(CLAIM, &[7])), Some(&Some(vec![7; 4])));
    kept.written(vec![written(CLAIM, 8, Some(8))], 0);
    assert!(kept.places.is_empty() && kept.order.is_empty(), "none kept");
    assert_eq!(kept.bytes, 0);
}

#[test]
fn what_no_batch_decides_on_again_is_not_kept() {
    let mut kept = Kept::default();
    let never = [
        super::super::MESSAGE,
        super::super::AUDIT,
        super::super::PUBLICATION,
        super::super::CHUNK,
        super::super::STREAM,
    ];
    let changes = never.into_iter().map(|kind| written(kind, 1, Some(1)));
    kept.written(changes.collect(), usize::MAX);
    assert_eq!(kept.len(), 0);
    let failed = written(failed::KINDS.places, 1, Some(2));
    kept.written(vec![failed, written(origin::ORIGIN, 1, None)], usize::MAX);
    assert_eq!(kept.len(), 2);
    kept.forget();
    assert_eq!((kept.len(), kept.bytes), (0, 0));
}

/// A writer over a store in memory its `hooks` steer.
fn writer(hooks: &Arc<Hooks>) -> Writer<Hooked<Memory>> {
    let keys = Held::new(secret::fixture::Memory::default());
    let kek = KekName::new("storage").expect("a name");
    let engine = Hooked::new(Memory::default(), hooks);
    let store = Arc::new(EncryptedStore::open(engine, &keys, &kek).expect("opened"));
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let columns = Arc::new(Columns::of(Database::Runtime));
    let told = (clock, columns, Arc::new(AtomicU64::new(1)), Arc::default());
    let most = Arc::new(AtomicUsize::new(crate::storage::WRITER_KEPT));
    Writer::start(store, told, (most, Arc::default())).expect("the writer")
}

/// A claim on the Message of the Journey `number`.
fn claim(number: u128) -> Op {
    Op::Claim {
        claim: Claim {
            journey: JourneyId::new(0x0199_0000_0000_7000_8000_0000_0000_0000 + number),
            holder: configure::fixture::test_cluster().node_scope(0),
            token: number,
            until_unix_nanos: 0,
            fence: 0,
        },
        lease_nanos: 30_000_000_000,
    }
}

#[test]
fn a_writer_keeps_what_its_batch_wrote_and_nothing_once_one_panicked() {
    let hooks = Arc::new(Hooks::default());
    let mut writer = writer(&hooks);
    let (reply, _answer) = std::sync::mpsc::sync_channel(1);
    writer.committed(vec![(claim(1), reply)]);
    assert_eq!(writer.kept.len(), 1, "its claim");
    hooks.panic_after_deferred(true);
    let (reply, _answer) = std::sync::mpsc::sync_channel(1);
    let panicked = catch_unwind(AssertUnwindSafe(|| {
        writer.committed(vec![(claim(2), reply)]);
    }));
    assert!(panicked.is_err(), "the batch panicked once written");
    assert_eq!(writer.kept.len(), 0, "the batch it panicked in held it");
    writer.recover();
    assert_eq!(writer.kept.len(), 0, "nor does it keep any after");
}

#[test]
fn a_hand_on_of_a_message_just_published_reads_none_of_its_records_from_the_store() {
    let (node, _) = node();
    published(&node, 1..2);
    let holder = configure::fixture::test_cluster().node_scope(0);
    let claim = node.claim(JourneyId::new(1), &holder, 1, LEASE);
    let mut sent = hand_on(claim.expect("asked").expect("claimed"));
    sent.leaves = vec![QUEUE];
    let counted = node.runtime().engine();
    counted.touched();
    assert!(node.hand_on(sent).expect("handed on").0);
    assert_eq!(
        counted.touched(),
        (0, 0, 0),
        "its claim, its Journey, its place, its index, its queue's places and by when \
         each is due: each kept by the writer as the Publication and the claim left it"
    );
}
