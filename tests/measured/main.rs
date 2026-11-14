//! The module's timing program (`doc/governance/rust-style.md` section
//! 5c): Xmip Storage's reads held to the millisecond rule. A plain `cargo
//! test` leaves it out; the landing runs it after every other test of the
//! module, alone, one test at a time (`cargo test --test measured
//! --all-features -- --test-threads=1`), so what is measured is Xmip's work
//! and not its neighbors':
//!
//! - a read of what is due among many held takes under a millisecond, over
//!   the crowded node `storage::due`'s tests read (`fixture::crowd`);
//! - a page across a gap of a million places, and the writer's removal
//!   before it, each take under a millisecond (`storage::page_across_a_gap`);
//! - a wait on a queue is woken within a millisecond of the Publication
//!   that puts a Message in it being asked, answered with what is due in it
//!   (2026-11-12), the median and the 99th percentile of two hundred
//!   (`storage::watch`; open problem 46). Until 2026-11-12 it was timed
//!   from the Publication's answer, which comes after the wake, and only
//!   the fastest of five held: a saturated zero;
//! - a wait on a queue's head — a sequence — is woken within a millisecond
//!   of the claim and the hand-on that take its first entry out being
//!   asked, the same way (2026-11-12);
//! - a node's row of the `membership` table is written within a millisecond
//!   beyond Xmip Storage's own write of an administration record, over a
//!   thousand starts of each node kept, and a wait on the table is woken
//!   within a millisecond of a row's write being asked (`membership`;
//!   2026-11-13).

mod membership;

use std::sync::Arc;
use std::time::{Duration, Instant};

/// How many wakes are timed.
const ROUNDS: u64 = 200;

use xmip_core_persist::fixture::crowd::{Node, QUEUE, crowded, handed_by, node, published};
use xmip_core_persist::storage::{
    DuePart, Moved, Position, Waiter, Watched, XmipStorage, page_across_a_gap,
};

#[test]
fn a_read_of_what_is_due_among_many_held_takes_under_a_millisecond() {
    let holder = configure::fixture::test_cluster().node_scope(0);
    let node = crowded(&holder, 10_000, 1_000);
    let fastest = (0..5)
        .map(|_| {
            let started = Instant::now();
            let read = node
                .read_due(QUEUE, DuePart::Due, (0, 0), 64)
                .expect("read");
            assert_eq!(read.held.len(), 3);
            started.elapsed()
        })
        .min()
        .unwrap_or_default();
    eprintln!("three due of 11,003 held: read in {fastest:?}");
    assert!(fastest < Duration::from_millis(1), "{fastest:?}");
}

/// One wake: `seen` waited on in `node`, answered with what is due in a
/// queue that moved, and `write` asked once the node lists the wait: what
/// it answered, and how long after `write` was asked.
fn woken(node: &Arc<Node>, seen: Position, write: impl FnOnce()) -> (Vec<Moved>, Duration) {
    let waiter = Arc::new(Waiter::new());
    let (waiting, held) = (Arc::clone(node), Arc::clone(&waiter));
    let woken = std::thread::spawn(move || {
        let moved = waiting.wait_queues(&[seen], (Duration::from_secs(60), 64), &held);
        (moved.expect("answered"), Instant::now())
    });
    // Listed: held by the thread, and by the node twice.
    let deadline = Instant::now() + Duration::from_secs(60);
    while Arc::strong_count(&waiter) < 4 {
        assert!(Instant::now() < deadline, "never listed");
        std::thread::yield_now();
    }
    let asked = Instant::now();
    write();
    let (moved, at) = woken.join().expect("woken");
    assert_eq!(moved.len(), 1, "{moved:?}");
    (moved, at - asked)
}

/// The median and the 99th percentile of `woken`, said as `what`, each held
/// under a millisecond.
fn held_to_the_millisecond(what: &str, mut woken: Vec<Duration>) {
    woken.sort();
    let median = woken[woken.len() / 2];
    let slowest = woken[woken.len() * 99 / 100];
    eprintln!("{what}: median {median:?}, 99th percentile {slowest:?}");
    let millisecond = Duration::from_millis(1);
    assert!(median < millisecond, "the median {median:?}");
    assert!(slowest < millisecond, "the 99th percentile {slowest:?}");
}

#[test]
fn a_wait_on_a_queue_is_woken_within_a_millisecond_of_the_publication_into_it() {
    let (node, _) = node();
    let node = Arc::new(node);
    let woken = (0..ROUNDS)
        .map(|place| {
            let id = u128::from(place) + 1;
            let (moved, took) = woken(&node, Position::queue(QUEUE, place), || {
                published(&node, id..id + 1);
            });
            let due = moved[0].due.as_ref().expect("what is due, with the answer");
            let held = usize::try_from(place + 1).expect("a count").min(64);
            assert_eq!(due.held.len(), held, "every Message due, at most as asked");
            took
        })
        .collect();
    held_to_the_millisecond(
        "a wait on a queue woken, with what is due, after the Publication into it was asked",
        woken,
    );
}

#[test]
fn a_wait_on_a_queue_s_head_is_woken_within_a_millisecond_of_the_hand_on_taking_it_out() {
    let (node, _) = node();
    let node = Arc::new(node);
    let holder = configure::fixture::test_cluster().node_scope(0);
    published(&node, 1..u128::from(ROUNDS) + 1);
    let woken = (0..ROUNDS)
        .map(|taken| {
            let head = Position {
                watched: Watched::Head(QUEUE),
                at: taken,
            };
            let id = u128::from(taken) + 1;
            let (_, took) = woken(&node, head, || {
                handed_by(&node, &holder, id, |sent| sent.leaves = vec![QUEUE]);
            });
            took
        })
        .collect();
    held_to_the_millisecond(
        "a wait on a sequence's head woken after the claim and the hand-on taking its first out \
         were asked",
        woken,
    );
}

#[test]
fn a_page_across_a_gap_and_the_writer_taking_out_before_it_each_take_under_a_millisecond() {
    let (paged, taken) = page_across_a_gap();
    eprintln!("a gap of a million places: a page {paged:?}, a removal decided {taken:?}");
    let millisecond = Duration::from_millis(1);
    assert!(paged < millisecond, "a page took {paged:?}");
    assert!(taken < millisecond, "a removal took {taken:?}");
}
