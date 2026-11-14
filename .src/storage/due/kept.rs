//! Each queue's [`DuePlaces`] as the one writer left them, kept by it
//! (2026-11-14): a batch reads none it keeps from the store, and none is
//! decrypted again. Read once, the first time a batch of the writer's
//! writes a held Message of the queue, and kept as each batch it wrote
//! leaves it — only once the batch is in the engine, so a batch refused
//! keeps what the store keeps. The writer is the one that writes them
//! (`super::settle`), so what it keeps is what the store holds for as long
//! as it writes: a Storage node opened anew — restarted, or a standby
//! promoted, which copied its primary's writes beneath no writer of its
//! own — starts with none kept and reads each again; and a writer that
//! panicked forgets them all. How many queues it keeps is its Storage
//! node's `[tuning] storage_due_places` (`Embedded::keep_due`), the first
//! kept let go of first past it and read again by the next batch that
//! writes it.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::{DuePlaces, kept};
use crate::{EncryptedStore, Engine, PersistError};

/// The places the writer keeps, by queue, and the order it first kept
/// them in.
#[derive(Debug)]
pub(crate) struct Kept {
    places: HashMap<u128, (DuePlaces, u64)>,
    /// Each queue kept, by the number it was first kept under: the lowest
    /// is let go of first.
    order: BTreeMap<u64, u128>,
    /// The number the next queue kept takes.
    next: u64,
    /// The most queues kept, as its Storage node says now.
    most: Arc<AtomicUsize>,
}

/// What one batch leaves of each queue it settled: kept once the batch is
/// in the engine ([`Kept::written`]).
#[derive(Debug, Default)]
pub(crate) struct Settled(pub(super) Vec<(u128, DuePlaces)>);

impl Kept {
    /// None kept yet, at most `most` queues from now on.
    pub(crate) fn new(most: Arc<AtomicUsize>) -> Self {
        Self {
            places: HashMap::new(),
            order: BTreeMap::new(),
            next: 0,
            most,
        }
    }

    /// `queue`'s places, as kept, or as `store` keeps them, kept from now.
    pub(super) fn places<R: Engine>(
        &mut self,
        store: &EncryptedStore<R>,
        queue: u128,
    ) -> Result<DuePlaces, PersistError> {
        if let Some((places, _)) = self.places.get(&queue) {
            return Ok(*places);
        }
        let places = kept(store, queue)?;
        self.keep(queue, places);
        Ok(places)
    }

    /// What `settled` leaves, its batch in the engine now.
    pub(crate) fn written(&mut self, settled: Settled) {
        for (queue, places) in settled.0 {
            self.keep(queue, places);
        }
    }

    /// Nothing kept: each read again from the store.
    pub(crate) fn forget(&mut self) {
        self.places.clear();
        self.order.clear();
    }

    fn keep(&mut self, queue: u128, places: DuePlaces) {
        if let Some((kept, _)) = self.places.get_mut(&queue) {
            *kept = places;
            return;
        }
        let most = self.most.load(Ordering::Relaxed);
        while self.places.len() >= most {
            let Some((_, first)) = self.order.pop_first() else {
                break;
            };
            self.places.remove(&first);
        }
        if most == 0 {
            return;
        }
        let at = self.next;
        self.next += 1;
        self.order.insert(at, queue);
        self.places.insert(queue, (places, at));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use xcore::{Clock, JourneyId};

    use super::super::{NO_RETRY, kept};
    use super::*;
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

    /// The Message of the Journey `id` handed on as `how` leaves it:
    /// whether Xmip Storage took it.
    fn handed(node: &Node, id: u128, how: impl FnOnce(&mut HandOn)) -> bool {
        let holder = configure::fixture::test_cluster().node_scope(0);
        let claim = node.claim(JourneyId::new(id), &holder, id, LEASE);
        let mut handing = hand_on(claim.expect("asked").expect("claimed"));
        how(&mut handing);
        node.hand_on(&handing).is_ok_and(|taken| taken)
    }

    /// [`QUEUE`]'s places as the store keeps them, and as what it holds
    /// says they are: the first due since held, the first retry, and how
    /// many keep a retry's due time.
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
        (kept(node.runtime(), QUEUE).expect("its places"), held)
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
        };
        node.publish(publication(1..7)).expect("published");
        holds(&node, "a Publication");
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

        drop(node);
        node = opened.opened();
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

    #[test]
    fn past_the_most_its_node_says_the_first_kept_is_let_go_of_and_one_kept_is_replaced() {
        let most = Arc::new(AtomicUsize::new(3));
        let mut kept = Kept::new(Arc::clone(&most));
        let places = |fresh| DuePlaces {
            fresh,
            ..DuePlaces::default()
        };
        for queue in 0..4 {
            kept.keep(queue, places(1));
        }
        assert_eq!(kept.places.len(), 3);
        assert!(!kept.places.contains_key(&0), "the first let go of");
        kept.keep(1, places(9));
        assert_eq!(kept.places[&1].0.fresh, 9);
        assert_eq!(kept.order.len(), 3, "replaced, not kept twice");
        // Its node says fewer: the first kept let go of at the next one kept.
        most.store(1, Ordering::Relaxed);
        kept.keep(7, places(2));
        assert_eq!(kept.places.keys().copied().collect::<Vec<_>>(), [7]);
        most.store(0, Ordering::Relaxed);
        kept.keep(8, places(2));
        assert!(kept.places.is_empty() && kept.order.is_empty(), "none kept");
        most.store(3, Ordering::Relaxed);
        kept.keep(9, places(2));
        kept.forget();
        assert!(kept.places.is_empty() && kept.order.is_empty());
    }
}
