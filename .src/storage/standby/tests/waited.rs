//! A wait on a primary's queues ended refused, at once, as it steps down and
//! as it stops (open problem 46; ADR-0018, amendment 2026-11-12), so the
//! node waiting asks the next Storage node rather than wait out its bound.

use crate::PersistError;
use crate::storage::{Position, Waiter};

use super::super::Role;
use super::*;

/// Far longer than any wait here lasts.
const LONG: Duration = Duration::from_secs(60);

/// A Storage node alone, promoted, its servers never started: what a wait
/// on its queues is asked of.
fn promoted() -> Arc<StorageNode> {
    let authority = Authority::new();
    let own = Own::new();
    let keys = (
        Arc::clone(&own.keys),
        KekName::new("storage").expect("a name"),
    );
    let listed = ["127.0.0.1:1".to_string()];
    let identity = authority.issue(&authority);
    let node = StorageNode::new(own.engines.clone(), keys, &listed, &identity, timings())
        .expect("a Storage node");
    node.promote(1).expect("promoted");
    node
}

/// A wait on `node`'s queues, as a node asking it waits, on a thread of its
/// own, returned once its primary lists it.
fn waiting(
    node: &Arc<StorageNode>,
) -> std::thread::JoinHandle<Result<Vec<Position>, PersistError>> {
    let waiter = Arc::new(Waiter::new());
    let (served, held) = (node.served(), Arc::clone(&waiter));
    let seen = [Position { queue: 7, next: 0 }];
    let waiting = std::thread::spawn(move || served.wait_queues(&seen, LONG, &held));
    // Listed: held by the thread, and by the primary in its list and its
    // listing.
    assert!(
        until(Duration::from_secs(30), || Arc::strong_count(&waiter) >= 4),
        "never listed"
    );
    waiting
}

/// What `waited` ended as, and that it ended long before its bound.
fn refused(waited: std::thread::JoinHandle<Result<Vec<Position>, PersistError>>, since: Instant) {
    let ended = waited.join().expect("ended");
    assert!(
        matches!(ended, Err(PersistError::Standby { .. })),
        "{ended:?}"
    );
    assert!(since.elapsed() < LONG / 2, "at once, not at its bound");
}

#[test]
fn a_wait_on_a_primary_that_steps_down_is_refused_at_once() {
    let node = promoted();
    let waited = waiting(&node);
    let since = Instant::now();
    node.stand(Role::Finding);
    refused(waited, since);
}

#[test]
fn a_wait_on_a_primary_that_stops_is_refused_at_once() {
    let node = promoted();
    let waited = waiting(&node);
    let since = Instant::now();
    node.stop();
    refused(waited, since);
}
