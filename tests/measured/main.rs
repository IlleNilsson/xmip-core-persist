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
//!   before it, each take under a millisecond (`storage::across_a_gap`);
//!   each of these an answer from what is held, the fastest of five
//!   (`xcore::latency::at_once`);
//! - a wait on a queue is woken within a millisecond of the Publication
//!   that puts a Message in it being asked, answered with what is due in it
//!   (2026-11-12), over two hundred rounds (`storage::watch`; open problem
//!   46). Until 2026-11-12 it was timed
//!   from the Publication's answer, which comes after the wake, and only
//!   the fastest of five held: a saturated zero;
//! - a wait on a queue's head — a sequence — is woken within a millisecond
//!   of the claim and the hand-on that take its first entry out being
//!   asked, the same way (2026-11-12);
//! - a node's row of the `membership` table is written within a millisecond
//!   beyond Xmip Storage's own write of an administration record, over a
//!   thousand starts of each node kept, beside it in the same rounds, the
//!   fastest of five each; and a wait on the table is woken
//!   within a millisecond of a row's write being asked (`membership`;
//!   2026-11-13);
//! - a standby stopped ends within a millisecond beyond the two threads its
//!   stop wakes, following its primary and in the middle of a whole copy,
//!   where the platform wakes a read blocked on a socket shut, Linux and
//!   macOS; and a primary handing on a whole copy, stopped with its server,
//!   within a millisecond beyond the six its stop wakes (`stopped`; open
//!   problem 94, 2026-11-22);
//! - a primary stopped in order is succeeded by its standby, serving as the
//!   primary within a millisecond beyond the four wakes its hand-over makes
//!   in turn, over a rolling restart of two Storage nodes; and how long a
//!   node writing all along waits across each stop is measured and printed
//!   (`succession`; ADR-0056, amendments 2026-11-23 and 2026-11-24).
//!
//! Each wake is held as every wait that works as it wakes is, beside a
//! plain wake measured in the same rounds (`xcore::latency::Rounds`): its
//! wall clock under the millisecond plus a plain wake, and the process's
//! processor time across it under the millisecond plus a plain wake's,
//! each the fastest of five. One thread waits, kept across the rounds
//! (`Waiting`). Until 2026-11-14 the wall clock's median and 99th
//! percentile were held under a millisecond flat, and each round started
//! a waiting thread of its own: on a machine the owner's own programs kept
//! a quarter to half busy they failed alone, before the round of changes
//! they were run for as much as after it.

// The cluster's authority the crate's own tests issue identities from,
// written once (`storage::authority`).
#[allow(dead_code)]
#[path = "../../.src/storage/authority.rs"]
mod authority;
mod membership;
mod nodes;
mod stopped;
mod succession;

use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

/// How many wakes are timed.
const ROUNDS: u64 = 200;

/// How many threads a write wakes where a plain wake wakes one: the
/// waiting thread.
const WAKES: u32 = 1;

use xcore::latency::{Rounds, at_once};
use xcore::processor_time::ProcessorTime;
use xmip_core_persist::fixture::crowd::{Node, QUEUE, crowded, handed_by, node, published};
use xmip_core_persist::storage::{
    DuePart, Moved, Position, Waiter, Watched, XmipStorage, across_a_gap,
};

#[test]
fn a_read_of_what_is_due_among_many_held_takes_under_a_millisecond() {
    let holder = configure::fixture::test_cluster().node_scope(0);
    let node = crowded(&holder, 10_000, 1_000);
    // An answer from what is held, no wake in it: the fastest of five
    // (`xcore::latency::at_once`). Until 2026-11-15 a copy of it here.
    let (read, fastest, _) = at_once("three due of 11,003 held, read", || {
        node.read_due(QUEUE, DuePart::Due, (0, 0), 64)
            .expect("read")
    });
    assert_eq!(read.held.len(), 3);
    assert!(
        fastest < Duration::from_millis(1),
        "{fastest:?}, the fastest of five"
    );
}

/// One thread waiting on a node, kept across rounds: each round hands it
/// what to wait on, and it answers with what moved and when it woke, so a
/// round times a wake and never a thread's start.
struct Waiting {
    asked: mpsc::Sender<Position>,
    woke: mpsc::Receiver<(Vec<Moved>, Instant)>,
    waiter: Arc<Waiter>,
}

impl Waiting {
    /// A thread waiting on `node` for what it is handed.
    fn on(node: &Arc<Node>) -> Self {
        let waiter = Arc::new(Waiter::new());
        let (asked, asks) = mpsc::channel::<Position>();
        let (answer, woke) = mpsc::channel();
        let (waiting, held) = (Arc::clone(node), Arc::clone(&waiter));
        std::thread::spawn(move || {
            for seen in asks {
                let moved = waiting.wait_queues(&[seen], (Duration::from_secs(60), 64), &held);
                let woke = (moved.expect("answered"), Instant::now());
                if answer.send(woke).is_err() {
                    break;
                }
            }
        });
        Self {
            asked,
            woke,
            waiter,
        }
    }

    /// One of `rounds`: `seen` waited on, answered with what is due in a
    /// queue that moved, and `write` asked once the node lists the wait,
    /// timed from then to the wait woken, by the wall clock and by the
    /// process's processor time, a plain wake beside it. What it answered.
    fn round(&self, rounds: &mut Rounds, seen: Position, write: impl FnOnce()) -> Vec<Moved> {
        self.asked.send(seen).expect("the waiting thread waits");
        // Listed: held here and by the waiting thread, and by the node twice.
        let deadline = Instant::now() + Duration::from_secs(60);
        while Arc::strong_count(&self.waiter) < 4 {
            assert!(Instant::now() < deadline, "never listed");
            std::thread::yield_now();
        }
        let (asked, from) = (Instant::now(), ProcessorTime::process());
        write();
        let (moved, at) = self
            .woke
            .recv_timeout(Duration::from_secs(60))
            .expect("woken");
        rounds.took.push(at - asked);
        rounds.worked.push(from.elapsed());
        assert_eq!(moved.len(), 1, "{moved:?}");
        rounds.beside();
        moved
    }
}

#[test]
fn a_wait_on_a_queue_is_woken_within_a_millisecond_of_the_publication_into_it() {
    let (node, _) = node();
    let node = Arc::new(node);
    let (waiting, mut rounds) = (Waiting::on(&node), Rounds::crossing(WAKES));
    for place in 0..ROUNDS {
        let id = u128::from(place) + 1;
        let moved = waiting.round(&mut rounds, Position::queue(QUEUE, place), || {
            published(&node, id..id + 1);
        });
        let due = moved[0].due.as_ref().expect("what is due, with the answer");
        let held = usize::try_from(place + 1).expect("a count").min(64);
        assert_eq!(due.held.len(), held, "every Message due, at most as asked");
    }
    let what = "a wait on a queue woken, with what is due, after the Publication into it was asked";
    rounds.hold(what);
}

#[test]
fn a_wait_on_a_queue_s_head_is_woken_within_a_millisecond_of_the_hand_on_taking_it_out() {
    let (node, _) = node();
    let node = Arc::new(node);
    let holder = configure::fixture::test_cluster().node_scope(0);
    published(&node, 1..u128::from(ROUNDS) + 1);
    let (waiting, mut rounds) = (Waiting::on(&node), Rounds::crossing(WAKES));
    for taken in 0..ROUNDS {
        let head = Position {
            watched: Watched::Head(QUEUE),
            at: taken,
        };
        let id = u128::from(taken) + 1;
        waiting.round(&mut rounds, head, || {
            handed_by(&node, &holder, id, |sent| sent.leaves = vec![QUEUE]);
        });
    }
    rounds.hold(
        "a wait on a sequence's head woken after the claim and the hand-on taking its first out \
         were asked",
    );
}

#[test]
fn a_page_across_a_gap_and_the_writer_taking_out_before_it_each_take_under_a_millisecond() {
    // Each an answer from what is held, no wake in it: the fastest of five
    // (`xcore::latency::at_once`). Until 2026-11-15 a copy of it in
    // `storage::queue::gap`.
    let (paged, taken) = across_a_gap(|page, removal| {
        let ((), paged, _) = at_once("a page across a gap of a million places", page);
        let ((), taken, _) = at_once("the entry before it taken out, decided", removal);
        (paged, taken)
    });
    let millisecond = Duration::from_millis(1);
    assert!(
        paged < millisecond,
        "a page took {paged:?}, the fastest of five"
    );
    assert!(
        taken < millisecond,
        "a removal took {taken:?}, the fastest of five"
    );
}
