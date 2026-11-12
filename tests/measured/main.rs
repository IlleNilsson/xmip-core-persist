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
//!   that puts a Message in it being asked, the median and the 99th
//!   percentile of two hundred (`storage::watch`; open problem 46). Until
//!   2026-11-12 it was timed from the Publication's answer, which comes
//!   after the wake, and only the fastest of five held: a saturated zero.

use std::sync::Arc;
use std::time::{Duration, Instant};

/// How many wakes are timed.
const ROUNDS: u64 = 200;

use xmip_core_persist::fixture::crowd::{QUEUE, crowded, node, published};
use xmip_core_persist::storage::{DuePart, Position, Waiter, XmipStorage, page_across_a_gap};

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

#[test]
fn a_wait_on_a_queue_is_woken_within_a_millisecond_of_the_publication_into_it() {
    let (node, _) = node();
    let node = Arc::new(node);
    let mut woken: Vec<Duration> = (0..ROUNDS)
        .map(|place| {
            let waiter = Arc::new(Waiter::new());
            let seen = [Position {
                queue: QUEUE,
                next: place,
            }];
            let (waiting, held) = (Arc::clone(&node), Arc::clone(&waiter));
            let woken = std::thread::spawn(move || {
                let moved = waiting.wait_queues(&seen, Duration::from_secs(60), &held);
                (moved.expect("answered"), Instant::now())
            });
            // Listed: held by the thread, and by the node twice.
            let deadline = Instant::now() + Duration::from_secs(60);
            while Arc::strong_count(&waiter) < 4 {
                assert!(Instant::now() < deadline, "never listed");
                std::thread::yield_now();
            }
            let id = u128::from(place) + 1;
            let asked = Instant::now();
            published(&node, id..id + 1);
            let (moved, at) = woken.join().expect("woken");
            assert_eq!(moved.len(), 1, "{moved:?}");
            at - asked
        })
        .collect();
    woken.sort();
    let median = woken[woken.len() / 2];
    let slowest = woken[woken.len() * 99 / 100];
    eprintln!(
        "a wait on a queue woken after the Publication into it was asked: median {median:?}, \
         99th percentile {slowest:?}"
    );
    let millisecond = Duration::from_millis(1);
    assert!(median < millisecond, "the median {median:?}");
    assert!(slowest < millisecond, "the 99th percentile {slowest:?}");
}

#[test]
fn a_page_across_a_gap_and_the_writer_taking_out_before_it_each_take_under_a_millisecond() {
    let (paged, taken) = page_across_a_gap();
    eprintln!("a gap of a million places: a page {paged:?}, a removal decided {taken:?}");
    let millisecond = Duration::from_millis(1);
    assert!(paged < millisecond, "a page took {paged:?}");
    assert!(taken < millisecond, "a removal took {taken:?}");
}
