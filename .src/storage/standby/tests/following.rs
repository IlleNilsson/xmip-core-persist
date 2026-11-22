//! Every node shows a standby from its primary's one list of who follows
//! it (open problem 95; ADR-0056, amendment 2026-11-22): a node's client,
//! with a wait under way as every node's membership has, is told as a
//! standby begins following and as it stops, by its place among the
//! Storage nodes listed, over Xmip's own mutual TLS on loopback. Until
//! 2026-11-22 a node showed a standby as what it said when last asked, and
//! asked it nothing once it knew it for one: a stopped standby stayed
//! ready.

use std::sync::atomic::{AtomicBool, Ordering};

use super::*;
use crate::storage::{Position, Waiter};

/// A thread waiting on `client` for a queue nothing moves, again and again
/// until `waiting` is let go: what every node's membership does.
fn waiting_on(
    client: &Arc<StorageClient>,
    waiting: &Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    let (client, waiting) = (Arc::clone(client), Arc::clone(waiting));
    std::thread::spawn(move || {
        let waiter = Arc::new(Waiter::new());
        let seen = [Position::queue(9, 0)];
        while waiting.load(Ordering::SeqCst) {
            let bound = (Duration::from_millis(200), 0);
            let _ = client.wait_queues(&seen, bound, &waiter);
        }
    })
}

#[test]
fn a_node_waiting_on_the_primary_is_told_a_standby_stopped_following() {
    let authority = Authority::new();
    let (bound, listed) = listeners(2);
    let [(one, one_server), (two, two_server)] =
        both_started([&Own::new(), &Own::new()], (bound, &listed), &authority);
    assert!(one.is_primary(), "{}", one.said());
    let client = Arc::new(client(&listed, &authority));
    assert_eq!(client.following(), None, "no primary said yet");

    let waiting = Arc::new(AtomicBool::new(true));
    let thread = waiting_on(&client, &waiting);
    let told = |following: [bool; 2]| client.following() == Some(following.to_vec());
    assert!(
        until(Duration::from_secs(5), || told([false, true])),
        "the standby at place 1 following: {:?}, {}",
        client.following(),
        one.said()
    );

    two.stop();
    two_server.stop();
    assert!(
        until(Duration::from_secs(5), || told([false, false])),
        "the standby stopped: {:?}, {}",
        client.following(),
        one.said()
    );
    assert_eq!(one.followers(), [], "as its primary knows it");

    waiting.store(false, Ordering::SeqCst);
    thread.join().expect("the waiting thread");
    one.stop();
    one_server.stop();
}
