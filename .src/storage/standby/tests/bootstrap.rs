//! A standby's bootstrap by a whole copy, frame by frame as its primary
//! sends them (ADR-0056, amendment 2026-11-12): copying, then catching up,
//! then ready, each state kept across a restart, a catch-up going on only
//! under its copy's epoch, and a whole copy starting it again.

use super::*;
use crate::storage::standby::bootstrap::BOOTSTRAP;
use crate::storage::standby::follow::Taking;
use crate::storage::standby::{Follow, Shipped};
use crate::{Change, Entry};
use secret::DataKey;

/// The history the primary's writes are numbered in.
const HISTORY: u128 = 0x5eed;

/// The epoch of the primary a copy comes from.
const EPOCH: u64 = 3;

/// A Storage node over `own`, listed alone and started on nothing: as it
/// stands once started again over what it keeps.
fn over(own: &Own, authority: &Authority) -> Arc<StorageNode> {
    let keys = (
        Arc::clone(&own.keys),
        KekName::new("storage").expect("a name"),
    );
    let listed = ["127.0.0.1:1".to_string()];
    StorageNode::new(
        own.engines.clone(),
        keys,
        &listed,
        &authority.issue(authority),
        timings(),
    )
    .expect("a Storage node")
}

/// A whole copy's start from the primary of `epoch`, with its data keys.
fn whole(epoch: u64) -> Shipped {
    let keys = (0..3)
        .map(|_| DataKey::generate().handed().to_vec())
        .collect();
    Shipped::Whole(HISTORY, epoch, keys)
}

fn entry(key: &[u8], value: &[u8]) -> Entry {
    (key.to_vec(), value.to_vec())
}

fn change(key: &[u8], value: &[u8]) -> Change {
    (key.to_vec(), Some(value.to_vec()))
}

/// Each frame of `frames` taken by `node`, in order.
fn taken(node: &StorageNode, taking: &mut Taking, frames: Vec<Shipped>) {
    for frame in frames {
        node.take(frame, taking).expect("taken");
    }
}

fn read(own: &Own, key: &[u8]) -> Option<Vec<u8>> {
    own.engines[0].read(key).expect("read")
}

#[test]
fn a_whole_copy_is_copying_then_catching_up_then_ready_each_kept_across_a_restart() {
    let authority = Authority::new();
    let own = Own::new();
    let node = over(&own, &authority);
    assert_eq!(node.bootstrap(), Bootstrap::Ready, "never copied");
    let mut taking = Taking::new();
    node.take(whole(EPOCH), &mut taking).expect("taken");
    assert_eq!(node.bootstrap(), Bootstrap::Copying);
    assert_eq!(
        over(&own, &authority).bootstrap(),
        Bootstrap::Copying,
        "started again copying"
    );

    // The write numbered 5 changed both keys between the pages: the first
    // page was read before it, the second after.
    let pages = vec![
        Shipped::Entries(0, vec![entry(b"a", b"before")]),
        Shipped::Entries(0, vec![entry(b"b", b"after")]),
        Shipped::Copied(0, 5, 6),
        Shipped::Copied(1, 1, 1),
        Shipped::Copied(2, 1, 1),
    ];
    taken(&node, &mut taking, pages);
    let catching_up = Bootstrap::CatchingUp {
        epoch: EPOCH,
        through: [6, 1, 1],
    };
    assert_eq!(node.bootstrap(), catching_up);
    assert_eq!(
        read(&own, b"a"),
        Some(b"before".to_vec()),
        "the write in part"
    );
    assert_eq!(read(&own, b"b"), Some(b"after".to_vec()));
    drop(node);

    // Started again catching up: it goes on from where its copies follow,
    // in its copy's history, and is not promoted meanwhile.
    let again = over(&own, &authority);
    assert_eq!(again.bootstrap(), catching_up);
    let asked = Follow {
        epoch: EPOCH,
        history: HISTORY,
        next: [5, 1, 1],
    };
    assert_eq!(again.asked(), asked);
    let said = again.said();
    assert!(said.contains("catching up, 1 writes to go"), "{said}");
    assert!(again.promote(EPOCH + 1).is_err(), "{}", again.said());
    let mut taking = Taking::new();
    let replayed = vec![change(b"a", b"after"), change(b"b", b"after")];
    taken(&again, &mut taking, vec![Shipped::Copy(0, 5, replayed)]);
    taken(&again, &mut taking, vec![Shipped::Beat(EPOCH)]);
    assert_eq!(again.bootstrap(), Bootstrap::Ready, "{}", again.said());
    assert_eq!(read(&own, b"a"), Some(b"after".to_vec()), "one state");
    assert_eq!(read(&own, BOOTSTRAP), None, "nothing kept once ready");
    drop(again);
    assert_eq!(
        over(&own, &authority).bootstrap(),
        Bootstrap::Ready,
        "started again ready"
    );
}

#[test]
fn a_catch_up_goes_on_only_under_its_copy_s_epoch_and_a_whole_copy_starts_it_again() {
    let authority = Authority::new();
    let own = Own::new();
    let node = over(&own, &authority);
    let mut taking = Taking::new();
    let copied = vec![
        whole(EPOCH),
        Shipped::Copied(0, 1, 4),
        Shipped::Copied(1, 1, 1),
        Shipped::Copied(2, 1, 1),
    ];
    taken(&node, &mut taking, copied);
    assert!(matches!(node.bootstrap(), Bootstrap::CatchingUp { .. }));
    assert_eq!(node.asked().history, HISTORY, "it goes on");

    // The primary of a later epoch: its writes from here need not be those
    // the pages reflect, so it is copied whole again.
    node.keep_epoch(EPOCH + 1).expect("kept");
    assert_eq!(node.asked().history, 0, "copied whole again");
    taken(&node, &mut taking, vec![whole(EPOCH + 1)]);
    assert_eq!(node.bootstrap(), Bootstrap::Copying, "from catching up");
    assert_eq!(node.asked().history, 0, "copying, it is copied whole");

    // No write while the primary scanned: ready at once.
    let copied = vec![
        Shipped::Copied(0, 1, 1),
        Shipped::Copied(1, 1, 1),
        Shipped::Copied(2, 1, 1),
    ];
    taken(&node, &mut taking, copied);
    assert_eq!(node.bootstrap(), Bootstrap::Ready);
    assert_eq!(node.asked().history, HISTORY, "ready, it goes on");
    assert_eq!(
        node.standing().epoch,
        EPOCH + 1,
        "the epoch a whole copy's wipe leaves"
    );
    drop(node);
    assert_eq!(over(&own, &authority).standing().epoch, EPOCH + 1);
}
