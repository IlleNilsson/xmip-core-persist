//! Each queue's places by when each is due as the one writer settles them
//! (`super::super::settle`), read through what it keeps of what it wrote
//! (`commit::kept`): what the store holds after every write that moves a
//! due, a write the engine refused, and a restart — and what the writer
//! keeps of every record, what the store holds of it.

use std::sync::Arc;

use xcore::{Clock, JourneyId};

use super::super::{DUE_PLACES, DuePlaces, NO_RETRY, places_key, said};
use crate::EncryptedStore;
use crate::fixture::crowd::{HOUR, LEASE, Pinned, QUEUE, hand_on, publication};
use crate::fixture::{Hooked, Hooks, Memory};
use crate::storage::commit::HELD;
use crate::storage::queue::entry_key;
use crate::storage::{Embedded, Failure, HandOn, Wait, XmipStorage};

/// A node whose runtime database outlives it, steered by its hooks.
type Node = Embedded<Arc<Hooked<Memory>>, Memory>;

/// What a node opens over, and opens over again as it restarts.
struct Opened {
    runtime: Arc<Hooked<Memory>>,
    hooks: Arc<Hooks>,
    keys: secret::Held<secret::fixture::Memory>,
    clock: Arc<Pinned>,
}

impl Opened {
    fn new() -> Self {
        let hooks = Arc::new(Hooks::default());
        let runtime = Arc::new(Hooked::new(Memory::default(), &hooks));
        let keys = secret::Held::new(secret::fixture::Memory::default());
        let clock = Arc::new(Pinned::default());
        clock.pass(1_000);
        Self {
            runtime,
            hooks,
            keys,
            clock,
        }
    }

    /// A node over the runtime database, its writer new.
    fn opened(&self) -> Node {
        let kek = secret::KekName::new("storage").expect("a name");
        let open = |engine| EncryptedStore::open(engine, &self.keys, &kek);
        Embedded::over(
            open(Arc::clone(&self.runtime)).expect("runtime"),
            EncryptedStore::open(Memory::default(), &self.keys, &kek).expect("administration"),
            EncryptedStore::open(Memory::default(), &self.keys, &kek).expect("audit"),
            Arc::clone(&self.clock) as Arc<dyn Clock>,
        )
        .expect("a node")
    }
}

/// The Message of the Journey `id` handed on as `how` leaves it: whether
/// Xmip Storage took it.
fn handed(node: &Node, id: u128, how: impl FnOnce(&mut HandOn)) -> bool {
    let holder = configure::fixture::test_cluster().node_scope(0);
    let claim = node.claim(JourneyId::new(id), &holder, id, LEASE);
    let mut handing = hand_on(claim.expect("asked").expect("claimed"));
    how(&mut handing);
    node.hand_on(handing).is_ok_and(|(taken, _)| taken)
}

/// [`QUEUE`]'s places as the store keeps them, and as what it holds says
/// they are: the first due since held, the first retry, and how many keep
/// a retry's due time.
fn kept_and_held(node: &Node) -> (DuePlaces, DuePlaces) {
    let read = node.read_held(QUEUE, 0, 1_000).expect("read");
    let waiting = |held: &&crate::storage::Held| !held.failed && held.due_unix_nanos.is_some();
    let fresh = read
        .held
        .iter()
        .filter(|held| !held.failed && held.due_unix_nanos.is_none());
    let held = DuePlaces {
        fresh: fresh.map(|held| held.sequence).min().unwrap_or(read.next),
        retry: read
            .held
            .iter()
            .filter(waiting)
            .map(|held| (held.due_at(), held.sequence))
            .min()
            .unwrap_or(NO_RETRY),
        retrying: u64::try_from(read.held.iter().filter(waiting).count()).expect("a count"),
    };
    let kept = node.runtime().get(DUE_PLACES, &places_key(QUEUE));
    (said(kept.expect("read")).expect("its places"), held)
}

fn sent(hand_on: &mut HandOn) {
    hand_on.leaves = vec![QUEUE];
}

fn waits(after_nanos: u64) -> impl FnOnce(&mut HandOn) {
    move |hand_on: &mut HandOn| {
        hand_on.waits = Some(Wait {
            queue: QUEUE,
            after_nanos,
        });
    }
}

fn fails(hand_on: &mut HandOn) {
    hand_on.failed = Some(Failure {
        queue: QUEUE,
        reason: "refused".to_string(),
    });
}

#[test]
fn what_the_writer_keeps_is_what_the_store_holds_after_every_write_a_refusal_and_a_restart() {
    let opened = Opened::new();
    let mut node = opened.opened();
    let holds = |node: &Node, after: &str| {
        let (kept, held) = kept_and_held(node);
        assert_eq!(kept, held, "after {after}");
        for (at, value) in node.writer_kept() {
            // A place is its kind, a zero byte and its key.
            let zero = at.iter().position(|&byte| byte == 0).expect("a kind");
            let kind = std::str::from_utf8(&at[..zero]).expect("a kind");
            let key = &at[zero + 1..];
            let stored = node.runtime().get(kind, key).expect("read");
            assert_eq!(value, stored, "{kind} {key:?} after {after}");
        }
    };
    node.publish(publication(1..7)).expect("published");
    holds(&node, "a Publication");
    assert!(
        !node.writer_kept().is_empty(),
        "the writer keeps what it wrote"
    );
    assert!(handed(&node, 2, waits(HOUR)));
    holds(&node, "a retry an hour away");
    assert!(handed(&node, 3, waits(10)));
    holds(&node, "an earlier retry");
    assert!(handed(&node, 4, fails));
    holds(&node, "a failure");
    assert!(handed(&node, 1, sent));
    holds(&node, "the first sent");
    opened.clock.pass(10);
    assert!(handed(&node, 3, sent));
    holds(&node, "the retry due, sent");

    // A writer opened anew keeps nothing, as a standby promoted over what
    // it copied beneath no writer of its own does.
    drop(node);
    node = opened.opened();
    assert!(node.writer_kept().is_empty(), "a new writer keeps nothing");
    holds(&node, "a restart");
    node.publish(publication(7..9)).expect("published");
    holds(&node, "a Publication after a restart");
    assert!(handed(&node, 5, sent));
    assert!(handed(&node, 6, waits(HOUR / 2)));
    holds(&node, "a sent one and a retry after a restart");

    // The write that lets go of the Message of the Journey 7 refused
    // whole: what the writer keeps is what the store still holds.
    let place = node.read_held(QUEUE, 0, 100).expect("read").held;
    let place = place
        .iter()
        .find(|held| held.hold.journey == JourneyId::new(7));
    let place = place.expect("held").sequence;
    let key = node.runtime().lookup_key(HELD, &entry_key(QUEUE, place));
    opened.hooks.refuse_deferred(Some((key.to_vec(), None)));
    assert!(!handed(&node, 7, sent), "refused");
    holds(&node, "a refused write");
    opened.hooks.refuse_deferred(None);
    assert!(handed(&node, 7, sent));
    holds(&node, "the write asked again");

    for id in [2, 4, 6, 8] {
        assert!(handed(&node, id, sent));
        holds(&node, "each let go of");
    }
    let (emptied, _) = kept_and_held(&node);
    assert_eq!(
        (emptied.retry, emptied.retrying),
        (NO_RETRY, 0),
        "none held"
    );
    node.publish(publication(9..10)).expect("published");
    holds(&node, "a Publication into the emptied queue");
}
