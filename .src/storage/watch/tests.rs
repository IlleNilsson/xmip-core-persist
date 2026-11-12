use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::{self, JoinHandle};

use xcore::{AuditId, JourneyId, MessageId};

use super::*;
use crate::fixture::crowd::{LEASE, Node, QUEUE, handed_by, node, published};
use crate::storage::{
    AuditEntry, AuditFacts, Claim, Hold, JourneyFacts, JourneyRecord, MessageFacts, MessageRecord,
    Publication, XmipStorage,
};

/// Far longer than any test here takes: a wait that ends sooner was
/// answered, never timed out.
const LONG: Duration = Duration::from_secs(60);

/// A queue nothing is put in but where a test says.
const OTHER: u128 = 9;

mod kinds;

fn at(queue: u128, next: u64) -> Position {
    Position::queue(queue, next)
}

fn holder() -> String {
    configure::fixture::test_cluster().node_scope(0)
}

/// Where each of `moved` stands.
fn positions(moved: Result<Vec<Moved>, PersistError>) -> Vec<Position> {
    let moved = moved.expect("answered");
    moved.into_iter().map(|moved| moved.position).collect()
}

/// `seen` waited on in `node` for `bound` as `waiter`, on a thread of
/// its own, returned once the node lists it.
fn waiting(
    node: &Arc<Node>,
    seen: Vec<Position>,
    bound: Duration,
    waiter: &Arc<Waiter>,
) -> JoinHandle<Result<Vec<Moved>, PersistError>> {
    let before = Arc::strong_count(waiter);
    let (node, held) = (Arc::clone(node), Arc::clone(waiter));
    let waiting = thread::spawn(move || node.wait_queues(&seen, (bound, 0), &held));
    // Listed: held by the thread, and by the node in its list and its
    // listing.
    let deadline = Instant::now() + LONG;
    while Arc::strong_count(waiter) < before + 3 {
        assert!(Instant::now() < deadline, "never listed");
        thread::yield_now();
    }
    waiting
}

/// Where what `waited` answered stands.
fn answered(waited: JoinHandle<Result<Vec<Moved>, PersistError>>) -> Vec<Position> {
    positions(waited.join().expect("waited"))
}

/// `seen` waited on in `node` for `bound`, nothing due asked for.
fn waited(node: &Node, seen: &[Position], bound: Duration, waiter: &Arc<Waiter>) -> Vec<Position> {
    positions(node.wait_queues(seen, (bound, 0), waiter))
}

/// The Message of the Journey `id` published, held in [`QUEUE`] and claimed
/// by `holder` in the same write, as a node publishes what it sends itself.
fn claimed(node: &Node, holder: &str, id: u128) {
    let journey = JourneyId::new(id);
    let publication = Publication {
        message: MessageRecord {
            message: MessageId::new(id),
            body: b"order".to_vec(),
            facts: MessageFacts::default(),
        },
        journeys: vec![JourneyRecord {
            journey,
            body: b"to send".to_vec(),
            facts: JourneyFacts::default(),
        }],
        held: vec![Hold {
            queue: QUEUE,
            journey,
            body: Vec::new(),
        }],
        dead: None,
        audit: AuditEntry {
            id: AuditId::new(id),
            body: b"published".to_vec(),
            audited: None,
            facts: AuditFacts::default(),
        },
        claims: vec![Claim {
            journey,
            holder: holder.to_string(),
            token: id,
            until_unix_nanos: 0,
            fence: 0,
        }],
        lease_nanos: u64::try_from(LEASE.as_nanos()).expect("a lease"),
    };
    let (claims, _) = node.publish(publication).expect("published");
    assert_eq!(claims.len(), 1, "claimed in the write");
}

#[test]
fn a_queue_that_stands_elsewhere_than_seen_is_answered_at_once_as_it_stands() {
    let (node, _) = node();
    published(&node, 1..3);
    let waiter = Arc::new(Waiter::new());
    let moved = waited(&node, &[at(QUEUE, 0), at(OTHER, 0)], LONG, &waiter);
    assert_eq!(moved, [at(QUEUE, 2)]);
    // A Message that lands between a read and the wait after it is
    // never waited past.
    published(&node, 3..4);
    assert_eq!(
        waited(&node, &[at(QUEUE, 2)], LONG, &waiter),
        [at(QUEUE, 3)]
    );
}

#[test]
fn a_wait_answers_with_what_is_due_in_each_queue_that_moved_and_none_where_none_is_asked() {
    let (node, _) = node();
    published(&node, 1..4);
    let waiter = Arc::new(Waiter::new());
    let moved = node
        .wait_queues(&[at(QUEUE, 0)], (LONG, 2), &waiter)
        .expect("answered");
    let [Moved { position, due }] = moved.as_slice() else {
        panic!("{moved:?}");
    };
    assert_eq!(*position, at(QUEUE, 3));
    let due = due.as_ref().expect("what is due, read as it answered");
    let journeys: Vec<u128> = due.held.iter().map(|h| h.hold.journey.value()).collect();
    assert_eq!(journeys, [1, 2], "the first two, in their order");
    let moved = node
        .wait_queues(&[at(QUEUE, 0)], (LONG, 0), &waiter)
        .expect("answered");
    assert_eq!(moved[0].due, None, "none asked for");
}

#[test]
fn a_queue_waited_on_before_is_found_where_its_writer_left_it_never_read_again() {
    let (node, _) = node();
    published(&node, 1..3);
    let waiter = Arc::new(Waiter::new());
    let short = Duration::from_millis(20);
    let counted = node.runtime().engine();
    counted.touched();
    assert_eq!(
        waited(&node, &[at(QUEUE, 0)], short, &waiter),
        [at(QUEUE, 2)]
    );
    let first = counted.touched();
    assert_ne!(first, (0, 0, 0), "the first wait reads where it stands");
    assert_eq!(waited(&node, &[at(QUEUE, 2)], short, &waiter), []);
    assert_eq!(counted.touched(), (0, 0, 0), "a later wait reads nothing");
    // Moved while none waited on it: its writer told the watch even so.
    published(&node, 3..5);
    counted.touched();
    assert_eq!(
        waited(&node, &[at(QUEUE, 2)], short, &waiter),
        [at(QUEUE, 4)]
    );
    assert_eq!(counted.touched(), (0, 0, 0), "nor does one after a write");
}

#[test]
fn a_publication_wakes_every_waiter_on_its_queue_and_none_on_another() {
    let (node, _) = node();
    let node = Arc::new(node);
    let waiters: Vec<Arc<Waiter>> = (0..3).map(|_| Arc::new(Waiter::new())).collect();
    let on_queue: Vec<_> = waiters
        .iter()
        .map(|waiter| waiting(&node, vec![at(QUEUE, 0)], LONG, waiter))
        .collect();
    let on_other = {
        let node = Arc::clone(&node);
        let other = Arc::new(Waiter::new());
        let bound = Duration::from_millis(200);
        thread::spawn(move || node.wait_queues(&[at(OTHER, 0)], (bound, 0), &other))
    };
    published(&node, 1..2);
    for waited in on_queue {
        assert_eq!(answered(waited), [at(QUEUE, 1)]);
    }
    assert_eq!(answered(on_other), [], "another queue's waiter");
}

#[test]
fn a_publication_its_holder_claims_whole_wakes_every_waiter_but_that_holders() {
    let (node, _) = node();
    let node = Arc::new(node);
    let holder = holder();
    let own = Arc::new(Waiter::of(&holder));
    let other = Arc::new(Waiter::of(
        &configure::fixture::test_cluster().node_scope(1),
    ));
    let bound = Duration::from_millis(300);
    let own_began = Instant::now();
    let own_waited = waiting(&node, vec![at(QUEUE, 0)], bound, &own);
    let other_waited = waiting(&node, vec![at(QUEUE, 0)], LONG, &other);
    claimed(&node, &holder, 1);
    assert_eq!(answered(other_waited), [at(QUEUE, 1)], "another's woken");
    assert_eq!(
        answered(own_waited),
        [at(QUEUE, 1)],
        "its own told at its bound"
    );
    assert!(
        own_began.elapsed() >= bound,
        "never woken by its own claims"
    );
    // What its holder did not claim wakes it.
    let woken = waiting(&node, vec![at(QUEUE, 1)], LONG, &own);
    published(&node, 2..3);
    assert_eq!(answered(woken), [at(QUEUE, 2)]);
}

#[test]
fn a_hand_on_that_queues_a_message_wakes_its_queue_and_a_send_moves_none() {
    let (node, _) = node();
    let node = Arc::new(node);
    published(&node, 1..3);
    handed_by(&node, &holder(), 1, |sent| sent.leaves = vec![QUEUE]);
    let waiter = Arc::new(Waiter::new());
    let short = Duration::from_millis(20);
    assert_eq!(
        waited(&node, &[at(QUEUE, 2)], short, &waiter),
        [],
        "a Message sent out of it"
    );
    let woken = waiting(&node, vec![at(QUEUE, 2), at(OTHER, 0)], LONG, &waiter);
    handed_by(&node, &holder(), 2, |onward| {
        onward.queued = vec![Hold {
            queue: OTHER,
            journey: JourneyId::new(2),
            body: Vec::new(),
        }];
    });
    assert_eq!(answered(woken), [at(OTHER, 1)]);
}

#[test]
fn a_wait_nothing_moves_ends_at_its_bound_with_none_never_an_error() {
    let (node, _) = node();
    published(&node, 1..2);
    let waiter = Arc::new(Waiter::new());
    let bound = Duration::from_millis(30);
    let began = Instant::now();
    assert_eq!(waited(&node, &[at(QUEUE, 1)], bound, &waiter), []);
    assert!(began.elapsed() >= bound, "waited its bound");
}

#[test]
fn a_stopped_waiter_ends_its_wait_at_once_and_waits_no_more() {
    let (node, _) = node();
    let node = Arc::new(node);
    let waiter = Arc::new(Waiter::new());
    let woken = waiting(&node, vec![at(QUEUE, 0)], LONG, &waiter);
    let stopped = Instant::now();
    waiter.stop();
    assert_eq!(answered(woken), []);
    assert!(stopped.elapsed() < Duration::from_secs(30), "not its bound");
    assert_eq!(
        waited(&node, &[at(QUEUE, 0)], LONG, &waiter),
        [],
        "a stopped waiter waits no more"
    );
    let rested = Instant::now();
    waiter.rest(LONG);
    assert!(rested.elapsed() < Duration::from_secs(30), "nor rests");
}

#[test]
fn a_renewed_waiter_ends_the_wait_under_way_at_once_and_the_next_waits() {
    let (node, _) = node();
    let node = Arc::new(node);
    let waiter = Arc::new(Waiter::new());
    waiter.asking();
    let woken = waiting(&node, vec![at(QUEUE, 0)], LONG, &waiter);
    let renewed = Instant::now();
    waiter.renew();
    assert_eq!(answered(woken), []);
    assert!(renewed.elapsed() < Duration::from_secs(30), "not its bound");
    // Until its caller asks anew, every wait and rest ends at once.
    assert_eq!(waited(&node, &[at(QUEUE, 0)], LONG, &waiter), []);
    waiter.rest(LONG);
    // A renewal before its caller asks is held by the wait it asks.
    waiter.renew();
    waiter.asking();
    let bound = Duration::from_millis(30);
    let began = Instant::now();
    assert_eq!(waited(&node, &[at(QUEUE, 0)], bound, &waiter), []);
    assert!(began.elapsed() >= bound, "the next waits its bound");
}

#[test]
fn what_ends_a_wait_over_the_wire_is_called_once_as_its_waiter_stops() {
    let called = Arc::new(AtomicUsize::new(0));
    let ending = || {
        let called = Arc::clone(&called);
        Box::new(move || {
            called.fetch_add(1, Ordering::SeqCst);
        }) as Ending
    };
    let waiter = Waiter::new();
    assert!(waiter.ending(ending()));
    waiter.ended();
    assert!(waiter.ending(ending()));
    waiter.renew();
    assert_eq!(called.load(Ordering::SeqCst), 1, "renewed: ended once");
    waiter.ended();
    assert!(!waiter.ending(ending()), "renewed: asked again only anew");
    waiter.asking();
    assert!(waiter.ending(ending()));
    waiter.stop();
    waiter.stop();
    assert_eq!(
        called.load(Ordering::SeqCst),
        2,
        "once, and never once answered"
    );
    assert!(
        !waiter.ending(ending()),
        "a stopped waiter asks nothing more"
    );
    assert_eq!(called.load(Ordering::SeqCst), 2);
}

#[test]
fn a_watch_refused_ends_the_wait_under_way_refused_and_refuses_every_wait_after() {
    let (node, _) = node();
    let node = Arc::new(node);
    let waiter = Arc::new(Waiter::new());
    let woken = waiting(&node, vec![at(QUEUE, 0)], LONG, &waiter);
    let refused = Instant::now();
    node.refuse_waits("stepped down");
    let ended = woken.join().expect("ended");
    assert!(
        matches!(&ended, Err(PersistError::Standby { reason }) if reason == "stepped down"),
        "{ended:?}"
    );
    assert!(refused.elapsed() < Duration::from_secs(30), "not its bound");
    let after = node.wait_queues(&[at(QUEUE, 0)], (LONG, 0), &waiter);
    assert!(
        matches!(after, Err(PersistError::Standby { .. })),
        "{after:?}"
    );
}

#[test]
fn a_position_crosses_its_bytes_as_it_was() {
    for watched in [
        Watched::Queue(u128::MAX - 3),
        Watched::Head(1),
        Watched::Instance(2),
        Watched::Key(3),
    ] {
        let position = Position {
            watched,
            at: 1 << 40,
        };
        assert_eq!(
            Position::from_bytes(&position.bytes()).expect("read"),
            position
        );
    }
    assert!(Watched::from_bytes(&[4; 17]).is_err(), "no fifth kind");
}
