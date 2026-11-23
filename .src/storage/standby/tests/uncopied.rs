//! A standby waiting for its first whole copy (ADR-0056, amendment
//! 2026-11-15): it stands by its primary copying, kept durable before that
//! primary's epoch. With the primary killed before the copy began and both
//! started again — equal in epoch and in what each says it holds, since a
//! former primary gave up its positions, and one killed keeps none again —
//! the primary is promoted and the standby copied whole, never the standby
//! promoted and the Ledger wiped.

use super::*;

#[test]
fn a_standby_waiting_for_its_first_whole_copy_is_never_promoted_over_the_ledger() {
    let authority = Authority::new();
    let (mut bound, listed) = listeners(2);
    let (empty_own, ledger_own) = (Own::new(), Own::new());
    let ledger_listener = bound.pop().expect("two");
    let empty_listener = bound.pop().expect("one");
    // Listed first and started empty; listed second and of a later epoch,
    // so the one promoted.
    let (empty, empty_server) = served(&empty_own, (empty_listener, &listed), &authority);
    let (ledger, ledger_server) = served(&ledger_own, (ledger_listener, &listed), &authority);
    ledger.keep_epoch(1).expect("kept");
    ledger.start(Duration::from_secs(5)).expect("started");
    assert!(ledger.is_primary(), "{}", ledger.said());
    let client = client(&listed, &authority);
    for number in 0..20 {
        client
            .write_journey(&journey(number, b"the Ledger"))
            .expect("written");
    }

    // The first listed stands by it, and the primary is killed before the
    // whole copy reaches it.
    let epoch = ledger.standing().epoch;
    empty.stand_by(&listed[1], epoch).expect("standing by");
    assert_eq!(empty.bootstrap(), Bootstrap::Copying, "{}", empty.said());
    killed((ledger, ledger_server));
    empty.stop();
    empty_server.stop();

    // Both started again: the first listed still copying, in the primary's
    // epoch, and saying it holds what the former primary says it holds.
    let mut rebound = listed
        .iter()
        .map(|at| TcpListener::bind(at).expect("bound again"));
    let first = rebound.next().expect("one");
    let (empty, empty_server) = served(&empty_own, (first, &listed), &authority);
    let second = rebound.next().expect("two");
    let (ledger, ledger_server) = served(&ledger_own, (second, &listed), &authority);
    assert_eq!(
        empty.bootstrap(),
        Bootstrap::Copying,
        "kept across its stop"
    );
    let (said, held) = (empty.standing(), ledger.standing());
    assert_eq!((said.epoch, said.held()), (held.epoch, held.held()));

    // The Ledger is promoted, and the standby copied whole from it.
    ledger.start(Duration::from_secs(5)).expect("started");
    assert!(ledger.is_primary(), "{} / {}", ledger.said(), empty.said());
    empty.start(Duration::from_secs(5)).expect("started");
    let copied =
        || empty.bootstrap() == Bootstrap::Ready && empty.standing().next == ledger.standing().next;
    assert!(
        until(Duration::from_secs(5), copied),
        "{} / {}",
        ledger.said(),
        empty.said()
    );
    for number in 0..20 {
        let read = client
            .read_journey(journey(number, b"").journey)
            .expect("read");
        assert_eq!(
            read.map(|r| r.body),
            Some(b"the Ledger".to_vec()),
            "{number}"
        );
    }
    assert!(!empty.is_primary(), "{}", empty.said());
    empty.stop();
    empty_server.stop();
    ledger.stop();
    ledger_server.stop();
}
