//! A standby promoted reads each queue's places by when each is due as its
//! primary's writer left them, and its own writer keeps them from there
//! (`storage::due`, 2026-11-14): what a Send Port's queue holds due, waiting
//! for a retry and failed reads the same after the promotion, and on.

use super::*;
use crate::fixture::crowd::{HOUR, LEASE, QUEUE, hand_on, publication};
use crate::storage::{DuePart, Failure, HandOn, Wait};

/// The Message of the Journey `id` claimed through `client` by the test
/// cluster's first node and handed on as `how` leaves it.
fn handed(client: &StorageClient, id: u128, how: impl FnOnce(&mut HandOn)) {
    let holder = configure::fixture::test_cluster().node_scope(0);
    let claim = client.claim(JourneyId::new(id), &holder, id, LEASE);
    let mut handing = hand_on(claim.expect("asked").expect("claimed"));
    how(&mut handing);
    assert!(client.hand_on(handing).expect("handed on").0);
}

/// The Journeys `part` of [`QUEUE`] holds, in its order, and how many keep
/// a retry's due time.
fn part(client: &StorageClient, part: DuePart) -> (Vec<u128>, u64) {
    let read = client.read_due(QUEUE, part, (0, 0), 64).expect("read");
    let held = read.held.iter().map(|held| held.hold.journey.value());
    (held.collect(), read.retrying)
}

fn sent(hand_on: &mut HandOn) {
    hand_on.leaves = vec![QUEUE];
}

fn waits(hand_on: &mut HandOn) {
    hand_on.waits = Some(Wait {
        queue: QUEUE,
        after_nanos: HOUR,
    });
}

fn fails(hand_on: &mut HandOn) {
    hand_on.failed = Some(Failure {
        queue: QUEUE,
        reason: "refused".to_string(),
    });
}

#[test]
fn a_standby_promoted_reads_what_is_due_as_its_primary_left_it_and_keeps_it_on() {
    let authority = Authority::new();
    let (bound, listed) = listeners(2);
    let (first, second) = (Own::new(), Own::new());
    let first_address = listed[0].clone();
    let [(one, one_server), (two, two_server)] =
        both_started([&first, &second], (bound, &listed), &authority);
    assert!(one.is_primary(), "{}", one.said());
    let client = client(&listed, &authority);
    client.publish(publication(1..7)).expect("published");
    handed(&client, 2, waits);
    handed(&client, 3, fails);
    handed(&client, 1, sent);
    assert_eq!(part(&client, DuePart::Due), (vec![4, 5, 6], 1));
    assert!(
        until(Duration::from_secs(5), || two.standing().next
            == one.standing().next),
        "{} / {}",
        one.said(),
        two.said()
    );
    one.stop();
    one_server.stop();

    // The former primary back: the standby, holding as much, promoted.
    let listener = TcpListener::bind(&first_address).expect("bound again");
    let (again, again_server) = started(&first, (listener, &listed), &authority);
    assert!(
        until(Duration::from_secs(5), || two.is_primary()),
        "{}",
        two.said()
    );
    assert_eq!(part(&client, DuePart::Due), (vec![4, 5, 6], 1), "as left");
    assert_eq!(part(&client, DuePart::Waiting).0, [2]);
    assert_eq!(part(&client, DuePart::Failed).0, [3]);
    handed(&client, 4, sent);
    handed(&client, 5, waits);
    client.publish(publication(7..8)).expect("published");
    assert_eq!(
        part(&client, DuePart::Due),
        (vec![6, 7], 2),
        "kept on by the promoted writer"
    );
    assert_eq!(part(&client, DuePart::Waiting).0, [2, 5]);
    for id in [6, 7] {
        handed(&client, id, sent);
    }
    assert_eq!(part(&client, DuePart::Due), (Vec::new(), 2));
    again.stop();
    again_server.stop();
    two.stop();
    two_server.stop();
}
