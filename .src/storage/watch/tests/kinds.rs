//! What a wait watches beside a queue's tail (open problem 46, 2026-11-12):
//! a queue's head, a Work Process Instance and a key of the Instance Key
//! table, each moved by the write that moves it and by no other.

use super::*;
use crate::storage::{HandOn, InstanceKey, InstanceRecord, InstanceWrite, Turn};

fn head(queue: u128, taken: u64) -> Position {
    Position {
        watched: Watched::Head(queue),
        at: taken,
    }
}

/// The owner, 2026-11-12, open problem 46: a Journey behind another of its
/// sequence is taken up once the head moves, never at the next scan.
#[test]
fn a_queue_s_head_is_moved_by_an_entry_taken_out_and_never_by_one_put_in() {
    let (node, _) = node();
    let node = Arc::new(node);
    published(&node, 1..3);
    let waiter = Arc::new(Waiter::new());
    let short = Duration::from_millis(20);
    assert_eq!(
        waited(&node, &[head(QUEUE, 1)], short, &waiter),
        [head(QUEUE, 0)]
    );
    let woken = waiting(&node, vec![head(QUEUE, 0)], LONG, &waiter);
    published(&node, 3..4);
    handed_by(&node, &holder(), 1, |sent| sent.leaves = vec![QUEUE]);
    assert_eq!(answered(woken), [head(QUEUE, 1)], "its head moved on");
    let put = waiting(
        &node,
        vec![head(QUEUE, 1)],
        Duration::from_millis(100),
        &waiter,
    );
    published(&node, 4..5);
    assert_eq!(answered(put), [], "an entry put in moves no head");
}

/// The Journey `id`, published, claimed by [`holder`] and turned over the
/// Instance `instance` standing at `over`, its key `key` set or let go.
fn turned(node: &Node, id: u128, (instance, over): (u128, u64), (key, ended): (u128, bool)) {
    published(node, id..id + 1);
    let claim = node.claim(JourneyId::new(id), &holder(), id, LEASE);
    let owned = InstanceKey { key, instance };
    let turn = Turn {
        hand_on: HandOn {
            claim: claim.expect("asked").expect("claimed"),
            result: JourneyRecord {
                journey: JourneyId::new(id),
                body: b"turned".to_vec(),
                facts: JourneyFacts::default(),
            },
            messages: Vec::new(),
            next: Vec::new(),
            leaves: vec![QUEUE],
            queued: Vec::new(),
            requeued: Vec::new(),
            failed: None,
            cleared: Vec::new(),
            waits: None,
            kept_for_nanos: None,
        },
        instances: vec![InstanceWrite {
            record: InstanceRecord {
                instance,
                work_process: "Approval".to_string(),
                turns: over + 1,
                ended,
                body: Vec::new(),
            },
            over,
        }],
        keys: if ended { Vec::new() } else { vec![owned] },
        unkeyed: if ended { vec![owned] } else { Vec::new() },
    };
    assert!(node.turn(&turn).expect("turned"), "written over {over}");
}

/// The owner, 2026-11-12, open problem 46: a turn its Instance did not take
/// is taken up once another node moves that Instance, or gives its key to
/// one, never at the next scan.
#[test]
fn an_instance_is_moved_by_a_turn_over_it_and_a_key_by_its_giving_and_letting_go() {
    let (node, _) = node();
    let node = Arc::new(node);
    let (instance, key) = (50, 60);
    let at_key = |at| Position {
        watched: Watched::Key(key),
        at,
    };
    let at_instance = |at| Position {
        watched: Watched::Instance(instance),
        at,
    };
    let waiter = Arc::new(Waiter::new());
    let short = Duration::from_millis(20);
    assert_eq!(
        waited(&node, &[at_key(0), at_instance(0)], short, &waiter),
        []
    );
    let given = waiting(&node, vec![at_key(0)], LONG, &waiter);
    turned(&node, 1, (instance, 0), (key, false));
    assert_eq!(answered(given), [at_key(1)], "its key given to an Instance");
    let moved = waiting(&node, vec![at_instance(1)], LONG, &waiter);
    turned(&node, 2, (instance, 1), (key, false));
    assert_eq!(answered(moved), [at_instance(2)], "a turn over it");
    let again = waiting(&node, vec![at_key(1)], Duration::from_millis(100), &waiter);
    turned(&node, 3, (instance, 2), (key, false));
    assert_eq!(
        answered(again),
        [],
        "a key kept by its Instance moves nothing"
    );
    let gone = waiting(&node, vec![at_key(1)], LONG, &waiter);
    turned(&node, 4, (instance, 3), (key, true));
    assert_eq!(answered(gone), [at_key(0)], "let go as it ended");
}
