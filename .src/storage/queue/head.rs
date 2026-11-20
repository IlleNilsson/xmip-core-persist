//! A head read where it is ([`super::page`]): the same answer one ordered
//! range of the index reads, whatever the queue holds — Messages held,
//! waiting for a retry, failed, claimed by another node, settled in order
//! and out of it — and, where the first is held at the place the queue
//! keeps as its first, no range read.

use codec::form::Form;
use xcore::JourneyId;

use super::super::hold::KINDS;
use super::{Places, page, places_key, scanned};
use crate::EncryptedStore;
use crate::fixture::Counted;
use crate::fixture::crowd::{HOUR, LEASE, Node, QUEUE, fails, hand_on, node, published, waits};
use crate::storage::{HandOn, XmipStorage};

/// The page of one from `from`, as one ordered range of the index reads
/// it in one view.
fn ranged(store: &EncryptedStore<Box<Counted>>, from: u64) -> (Places, Vec<Vec<u8>>) {
    let view = store.viewed().expect("a view");
    let places = view
        .get(KINDS.places, &places_key(QUEUE))
        .expect("its places read")
        .map_or_default(|bytes| Places::from_bytes(&bytes).expect("places"));
    if places.count == 0 {
        return (places, Vec::new());
    }
    let at = from.max(places.first);
    scanned(&view, (KINDS, QUEUE), places, (at, 1)).expect("a range read")
}

/// From every place before `past`, the page of one read where it is and
/// as the range reads it, alike; and whether the head was read with no
/// range.
fn alike(node: &Node, past: u64) -> bool {
    let store = node.runtime();
    for from in 0..past {
        let paged = page(store, KINDS, QUEUE, from, 1).expect("a page");
        assert_eq!(paged, ranged(store, from), "from {from}");
    }
    store.engine().read();
    page(store, KINDS, QUEUE, 0, 1).expect("the head");
    store.engine().read().1 == 0
}

/// The Journey `id` claimed by `holder` under `token` and handed on as
/// `how` says.
fn handed(node: &Node, (holder, token): (&str, u128), id: u128, how: impl FnOnce(&mut HandOn)) {
    let claim = node.claim(JourneyId::new(id), holder, token, LEASE);
    let mut handing = hand_on(claim.expect("asked").expect("claimed"));
    how(&mut handing);
    assert!(
        node.hand_on(handing).expect("handed on").0,
        "{id} handed on"
    );
}

/// The Journey `id` sent: its place let go of.
fn settled(node: &Node, id: u128) {
    handed(node, ("a node", 100 + id), id, |handing| {
        handing.leaves = vec![QUEUE];
    });
}

#[test]
fn a_head_read_where_it_is_answers_as_the_range_does_whatever_its_queue_holds() {
    let (node, clock) = node();
    published(&node, 1..7);
    assert!(alike(&node, 8), "six held, the first read where it is");
    handed(&node, ("a node", 1), 1, waits(HOUR));
    assert!(alike(&node, 8), "the first waiting for a retry");
    handed(&node, ("a node", 2), 2, fails);
    assert!(alike(&node, 8), "the second failed");
    let claimed = node.claim(JourneyId::new(3), "another node", 3, LEASE);
    assert!(claimed.expect("asked").is_some(), "claimed by another node");
    assert!(alike(&node, 8), "the third claimed by another node");
    settled(&node, 1);
    assert!(
        !alike(&node, 8),
        "the first settled: none held where the first is kept"
    );
    settled(&node, 4);
    assert!(!alike(&node, 8), "the fourth settled out of turn");
    settled(&node, 2);
    clock.pass(LEASE * 2);
    settled(&node, 3);
    assert!(!alike(&node, 8), "two left, the first kept where it was");
    for id in [5, 6] {
        settled(&node, id);
    }
    assert!(alike(&node, 10), "emptied: nothing read past its places");
    published(&node, 7..9);
    assert!(
        alike(&node, 10),
        "held again after it was emptied: the first read where it is"
    );
}
