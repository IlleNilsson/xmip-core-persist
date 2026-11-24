//! A primary stopped in order hands over to a standby that succeeds it at
//! once (ADR-0056, amendment 2026-11-23; the owner, 2026-11-23: *That has to
//! be sorted, nothing can be left*), over Xmip's own mutual TLS on loopback:
//! a rolling restart of two Storage nodes, each primary stopped in order in
//! turn and its standby ready, a node writing all along — writes go on
//! through the standby succeeding it, and the former primary, back, follows
//! it on from where it stopped, never copied whole — and of three, the
//! standby not named following the one that is. Until that day a standby
//! waited for its primary to return, and the former primary was copied
//! whole: the outcome is held here, its time by the timing program
//! (`tests/measured/succession.rs`).

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use super::*;

/// What a node writing all along was answered: each Journey, and when.
type Answered = Vec<(u128, Instant)>;

/// A node writing all along on a client of its own, from Journey 20 000 on,
/// while `going`, counting each write answered in `count`: each, and when.
fn writing(
    (listed, authority): (&[String], &Authority),
    (going, count): (Arc<AtomicBool>, Arc<AtomicUsize>),
) -> std::thread::JoinHandle<Answered> {
    let client = client(listed, authority);
    std::thread::spawn(move || {
        let mut answered = Vec::new();
        for number in 20_000.. {
            if !going.load(Ordering::SeqCst) {
                break;
            }
            if client.write_journey(&journey(number, b"along")).is_ok() {
                answered.push((number, Instant::now()));
                count.fetch_add(1, Ordering::SeqCst);
            }
        }
        answered
    })
}

/// Until the node writing all along has been answered `more` writes more
/// than it was so far, by `count`.
fn written_more(count: &AtomicUsize, more: usize) -> bool {
    let from = count.load(Ordering::SeqCst);
    until(Duration::from_secs(5), || {
        count.load(Ordering::SeqCst) >= from + more
    })
}

/// How long writes waited across the stop that `began`, the next at
/// `until` where there is one: the longest wait between two answered one
/// after the other from the last answered before it.
fn gap(answered: &Answered, began: Instant, until: Option<&Instant>) -> Duration {
    answered
        .windows(2)
        .filter(|pair| pair[1].1 > began && until.is_none_or(|until| pair[0].1 < *until))
        .map(|pair| pair[1].1 - pair[0].1)
        .max()
        .unwrap_or(Duration::MAX)
}

/// Every Journey `answered` as written read back, and every one written
/// before, `0..before`.
fn read_back(client: &StorageClient, answered: &Answered, before: u128) {
    let numbers = (0..before).chain(answered.iter().map(|(number, _)| *number));
    for number in numbers {
        let read = client
            .read_journey(journey(number, b"").journey)
            .expect("read");
        assert!(read.is_some(), "{number} lost");
    }
}

#[test]
fn a_rolling_restart_stopping_each_primary_in_order_writes_on_and_copies_neither_whole() {
    let authority = Authority::new();
    let (bound, listed) = listeners(2);
    let owns = [Own::new(), Own::new()];
    let started = both_started([&owns[0], &owns[1]], (bound, &listed), &authority);
    let mut nodes = started.map(Some);
    let client = client(&listed, &authority);
    for number in 0..100 {
        client
            .write_journey(&journey(number, b"before"))
            .expect("written");
    }
    let (going, count) = (
        Arc::new(AtomicBool::new(true)),
        Arc::new(AtomicUsize::new(0)),
    );
    let along = writing(
        (&listed, &authority),
        (Arc::clone(&going), Arc::clone(&count)),
    );
    let mut stops = Vec::new();
    for (round, primary) in [0, 1].into_iter().enumerate() {
        let (node, server) = nodes[primary].take().expect("running");
        let successor = Arc::clone(&nodes[1 - primary].as_ref().expect("running").0);
        assert!(node.is_primary(), "{}", node.said());
        // The deploy restarts the next only once the one before is ready.
        let ready = || following(&successor);
        assert!(until(Duration::from_secs(5), ready), "{}", successor.said());
        assert!(written_more(&count, 20), "writes go on before the stop");
        let epoch = node.standing().epoch;

        let began = Instant::now();
        node.stop();
        server.stop();
        stops.push(began);
        let succeeded = || successor.is_primary();
        assert!(
            until(Duration::from_secs(5), succeeded),
            "round {round}: {} / {}",
            successor.said(),
            node.said()
        );
        assert_eq!(
            successor.standing().epoch,
            epoch + 1,
            "{}",
            successor.said()
        );
        assert!(node.said().contains("handing over"), "{}", node.said());

        // Back, it follows its successor on from where it stopped, handed
        // no whole copy.
        nodes[primary] = Some(again(&owns[primary], (&listed, primary), &authority));
        let back = Arc::clone(&nodes[primary].as_ref().expect("running").0);
        let follows = || following(&back) && successor.followers().len() == 1;
        assert!(
            until(Duration::from_secs(5), follows),
            "{} / {}",
            back.said(),
            successor.said()
        );
        let handed = successor.followers();
        assert_eq!(handed[0].entries, 0, "copied whole: {handed:?}");
    }
    assert!(written_more(&count, 20), "writes go on after the last stop");
    going.store(false, Ordering::SeqCst);
    let answered = along.join().expect("written along");
    let gaps: Vec<Duration> = stops
        .iter()
        .enumerate()
        .map(|(at, began)| gap(&answered, *began, stops.get(at + 1)))
        .collect();
    println!(
        "writes waited {gaps:?} across each primary's stop in order, {} answered",
        answered.len()
    );
    // An outcome, not a time: the timing program holds the millisecond.
    assert!(
        gaps.iter().all(|gap| *gap < Duration::from_secs(1)),
        "{gaps:?}"
    );
    read_back(&client, &answered, 100);
    for (node, server) in nodes.into_iter().flatten() {
        node.stop();
        server.stop();
    }
}

#[test]
fn of_three_storage_nodes_the_standby_not_named_follows_the_one_succeeding() {
    let authority = Authority::new();
    let (bound, listed) = listeners(3);
    let owns = [Own::new(), Own::new(), Own::new()];
    let nodes: Vec<(Arc<StorageNode>, StorageServer)> = bound
        .into_iter()
        .zip(&owns)
        .map(|(listener, own)| served(own, (listener, &listed), &authority))
        .collect();
    for (node, _) in &nodes {
        node.start(Duration::from_secs(5)).expect("started");
    }
    let mut nodes = nodes.into_iter();
    let (first, first_server) = nodes.next().expect("three");
    let standbys: Vec<(Arc<StorageNode>, StorageServer)> = nodes.collect();
    assert!(first.is_primary(), "{}", first.said());
    let client = client(&listed, &authority);
    for number in 0..50 {
        client
            .write_journey(&journey(number, b"before"))
            .expect("written");
    }
    let ready = || standbys.iter().all(|(node, _)| following(node));
    assert!(until(Duration::from_secs(5), ready), "{}", first.said());

    let epoch = first.standing().epoch;
    first.stop();
    first_server.stop();
    let settled = || {
        let primaries: Vec<&Arc<StorageNode>> = standbys
            .iter()
            .map(|(node, _)| node)
            .filter(|node| node.is_primary())
            .collect();
        primaries.len() == 1
            && primaries[0].followers().len() == 1
            && standbys
                .iter()
                .all(|(node, _)| node.is_primary() || following(node))
    };
    let said = || {
        let each: Vec<String> = standbys.iter().map(|(node, _)| node.said()).collect();
        each.join(" / ")
    };
    assert!(until(Duration::from_secs(5), settled), "{}", said());
    let (successor, _) = standbys
        .iter()
        .find(|(node, _)| node.is_primary())
        .expect("one primary");
    assert_eq!(successor.standing().epoch, epoch + 1, "{}", said());
    let handed = successor.followers();
    assert_eq!(handed[0].entries, 0, "copied whole: {handed:?}");
    client
        .write_journey(&journey(50, b"after"))
        .expect("written through the one succeeding");
    read_back(&client, &Vec::new(), 51);
    for (node, server) in standbys {
        node.stop();
        server.stop();
    }
}
