//! What a watch keeps (the review of 2026-11-12; ADR-0018, amendment
//! 2026-11-12): every count waited on now, and of those none waits on at
//! most its bound, the longest unwaited let go first and read again when
//! next waited on — no write lost for it.

use std::sync::atomic::AtomicBool;

use super::kinds::turned;
use super::*;

fn instance(id: u128, at: u64) -> Position {
    Position {
        watched: Watched::Instance(id),
        at,
    }
}

fn key(id: u128, at: u64) -> Position {
    Position {
        watched: Watched::Key(id),
        at,
    }
}

/// `queue` left at `at` by its writer, as a batch that moved it tells.
fn wrote(watch: &Watch, queue: u128, at: u64) {
    watch.moved(&[Change {
        watched: Watched::Queue(queue),
        at: Some(at),
        own: None,
    }]);
}

/// The review of 2026-11-12: a Storage node kept the count of every
/// Instance and key any wait ever named, growing with every conversation
/// it carried rather than with the waits under way.
#[test]
fn distinct_instances_made_watched_and_ended_leave_the_watch_at_its_bound() {
    let (node, _) = node();
    let most = 8;
    node.keep_unwaited(most);
    let waiter = Arc::new(Waiter::new());
    let counted = node.runtime().engine();
    for n in 0..100u128 {
        let (id, correlation) = (1_000 + n, 5_000 + n);
        turned(&node, 2 * n + 1, (id, 0), (correlation, false));
        // Seen as before it was made: answered at once, as it stands.
        assert_eq!(
            waited(
                &node,
                &[instance(id, 0), key(correlation, 0)],
                LONG,
                &waiter
            ),
            [instance(id, 1), key(correlation, 1)]
        );
        turned(&node, 2 * n + 2, (id, 1), (correlation, true));
        let (kept, unwaited) = node.watch_kept();
        assert_eq!(kept, unwaited, "none waits on any now");
        assert!(unwaited <= most, "{unwaited} kept after {n}");
    }
    assert_eq!(node.watch_kept(), (most, most), "settled at its bound");
    counted.touched();
    assert_eq!(
        waited(&node, &[instance(1_099, 1), key(5_099, 1)], LONG, &waiter),
        [instance(1_099, 2), key(5_099, 0)],
        "the latest kept where its writer left it as it ended"
    );
    assert_eq!(counted.touched(), (0, 0, 0), "and read no more");
    assert_eq!(
        waited(&node, &[instance(1_000, 1), key(5_000, 1)], LONG, &waiter),
        [instance(1_000, 2), key(5_000, 0)],
        "the longest unwaited let go, read as it stands"
    );
    assert_ne!(counted.touched(), (0, 0, 0), "read again");
}

/// A count let go is read again by the next wait on it, as the first wait
/// on it ever was, and a write around that read is told or read whichever
/// way the two fall: in before the read or after it, told before the
/// wait's standing or after it.
#[test]
fn a_count_let_go_is_read_again_and_no_write_around_that_read_is_lost() {
    let watch = Watch::default();
    watch.keep_unwaited(0);
    let waiter = Arc::new(Waiter::new());
    let (queue, seen) = (Watched::Queue(QUEUE), [at(QUEUE, 0)]);
    for (read, told_first) in [(1, true), (1, false), (0, true), (0, false)] {
        waiter.begin();
        let (listening, unknown) = watch.listening(&seen, &waiter).expect("listed");
        assert_eq!(unknown, [queue], "let go: to be read");
        if told_first {
            wrote(&watch, QUEUE, 1);
        }
        let now = watch.standing(&seen, &[(queue, read)]);
        if !told_first {
            wrote(&watch, QUEUE, 1);
        }
        assert_eq!(
            waiter
                .waited(&seen, now, Some(Instant::now()))
                .expect("answered"),
            [at(QUEUE, 1)],
            "read at {read}, told first: {told_first}"
        );
        assert_eq!(
            watch.standing(&seen, &[]),
            [at(QUEUE, 1)],
            "kept as its writer left it"
        );
        drop(listening);
        assert_eq!(watch.kept(), (0, 0), "let go as its wait ended");
    }
}

#[test]
fn a_count_waited_on_is_never_let_go_however_small_the_bound() {
    let watch = Watch::default();
    let (waiter, other) = (Arc::new(Waiter::new()), Arc::new(Waiter::new()));
    let seen = [at(QUEUE, 0)];
    let (listening, _) = watch.listening(&seen, &waiter).expect("listed");
    watch.standing(&seen, &[(Watched::Queue(QUEUE), 0)]);
    watch.keep_unwaited(0);
    for queue in 100..110 {
        let seen = [at(queue, 0)];
        let (_others, _) = watch.listening(&seen, &other).expect("listed");
        watch.standing(&seen, &[(Watched::Queue(queue), 0)]);
    }
    assert_eq!(watch.kept(), (1, 0), "the one waited on, none unwaited");
    wrote(&watch, QUEUE, 1);
    assert_eq!(
        watch.standing(&seen, &[]),
        [at(QUEUE, 1)],
        "where its writer left it"
    );
    drop(listening);
    assert_eq!(watch.kept(), (0, 0), "let go once none waits on it");
}

/// Waits racing writes while every other count is let go as its wait ends:
/// each wait answered with the write that raced it, and one asked after a
/// write none waited on with that write — never older than it stands.
#[test]
fn waits_racing_writes_miss_none_while_counts_are_let_go_all_along() {
    let (node, _) = node();
    let node = Arc::new(node);
    node.keep_unwaited(1);
    let churning = Arc::new(AtomicBool::new(true));
    let churn = {
        let (node, churning) = (Arc::clone(&node), Arc::clone(&churning));
        thread::spawn(move || {
            let waiter = Arc::new(Waiter::new());
            for queue in 1_000u128.. {
                if !churning.load(Ordering::SeqCst) {
                    break;
                }
                // Seen where it never stood: answered at once, then let go.
                let _ = node.wait_queues(&[at(queue, 1)], (LONG, 0), &waiter);
            }
        })
    };
    let waiter = Arc::new(Waiter::new());
    let (mut id, mut seen) = (1u128, 0u64);
    for _ in 0..200 {
        // Written while none waits on the queue, its count kept or let go.
        published(&node, id..id + 1);
        id += 1;
        assert_eq!(
            waited(&node, &[at(QUEUE, seen)], LONG, &waiter),
            [at(QUEUE, seen + 1)]
        );
        seen += 1;
        // Written as a wait is listed, before or after its read.
        let woken = waiting(&node, vec![at(QUEUE, seen)], LONG, &waiter);
        published(&node, id..id + 1);
        id += 1;
        assert_eq!(answered(woken), [at(QUEUE, seen + 1)]);
        seen += 1;
    }
    churning.store(false, Ordering::SeqCst);
    churn.join().expect("churned");
}
