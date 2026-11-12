//! A wait on a primary's queues ended refused, at once, as it steps down and
//! as it stops (open problem 46; ADR-0018, amendment 2026-11-12), so the
//! node waiting asks the next Storage node rather than wait out its bound.

use crate::PersistError;
use crate::storage::{Moved, Position, Waiter};

use super::super::Role;
use super::*;

/// Far longer than any wait here lasts.
const LONG: Duration = Duration::from_secs(60);

/// A Storage node alone, bounded by `timings`, promoted, its servers never
/// started: what a wait on its queues is asked of.
fn promoted(timings: Timings) -> Arc<StorageNode> {
    let authority = Authority::new();
    let own = Own::new();
    let keys = (
        Arc::clone(&own.keys),
        KekName::new("storage").expect("a name"),
    );
    let listed = ["127.0.0.1:1".to_string()];
    let identity = authority.issue(&authority);
    let node = StorageNode::new(own.engines.clone(), keys, &listed, &identity, timings)
        .expect("a Storage node");
    node.promote(1).expect("promoted");
    node
}

/// A wait on `node`'s queues, as a node asking it waits, on a thread of its
/// own, returned once its primary lists it.
fn waiting(node: &Arc<StorageNode>) -> std::thread::JoinHandle<Result<Vec<Moved>, PersistError>> {
    let waiter = Arc::new(Waiter::new());
    let (served, held) = (node.served(), Arc::clone(&waiter));
    let seen = [Position::queue(7, 0)];
    let waiting = std::thread::spawn(move || served.wait_queues(&seen, (LONG, 0), &held));
    // Listed: held by the thread, and by the primary in its list and its
    // listing.
    assert!(
        until(Duration::from_secs(30), || Arc::strong_count(&waiter) >= 4),
        "never listed"
    );
    waiting
}

/// What `waited` ended as, and that it ended long before its bound.
fn refused(waited: std::thread::JoinHandle<Result<Vec<Moved>, PersistError>>, since: Instant) {
    let ended = waited.join().expect("ended");
    assert!(
        matches!(ended, Err(PersistError::Standby { .. })),
        "{ended:?}"
    );
    assert!(since.elapsed() < LONG / 2, "at once, not at its bound");
}

#[test]
fn a_wait_on_a_primary_that_steps_down_is_refused_at_once() {
    let node = promoted(timings());
    let waited = waiting(&node);
    let since = Instant::now();
    node.stand(Role::Finding);
    refused(waited, since);
}

#[test]
fn a_wait_on_a_primary_that_stops_is_refused_at_once() {
    let node = promoted(timings());
    let waited = waiting(&node);
    let since = Instant::now();
    node.stop();
    refused(waited, since);
}

/// The review of 2026-11-12: a primary among several keeps of what none
/// waits on at most its node's `[tuning] storage_unwaited`, as the only
/// Storage node does, never the default alone.
#[test]
fn a_primary_of_several_keeps_what_none_waits_on_within_its_node_s_bound() {
    let node = promoted(Timings {
        unwaited: 2,
        ..timings()
    });
    let (served, waiter) = (node.served(), Arc::new(Waiter::new()));
    for queue in 100..110 {
        // Seen where it never stood: answered at once, then none waits on it.
        let moved = served.wait_queues(&[Position::queue(queue, 1)], (LONG, 0), &waiter);
        assert_eq!(moved.expect("answered").len(), 1, "queue {queue}");
    }
    let primary = node.primary().expect("the primary");
    assert_eq!(primary.watch_kept(), (2, 2), "its node's bound");
}
