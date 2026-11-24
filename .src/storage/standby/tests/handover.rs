//! What the review of the switchover found (open problems 134 to 136 and
//! 139; ADR-0056, amendment 2026-11-24; the owner, 2026-11-24: *Do as you
//! propose*), over Xmip's own mutual TLS on loopback: a successor that keeps
//! its epoch past the silence while its former primary comes back leaves one
//! primary of the epoch; only a Storage node listed, by the certificate it
//! proves, may follow; a stop an operation outlasts takes no write after it
//! and names no successor; and a write asked while the primary drains is
//! answered by its successor, never refused.

use std::sync::atomic::Ordering;

use super::*;
use crate::storage::server::ALPN;
use crate::storage::standby::Follow;
use crate::storage::wire::{self, Answer, Request};

#[test]
fn a_successor_keeping_its_epoch_past_the_silence_leaves_one_primary_of_the_epoch() {
    let authority = Authority::new();
    let (bound, listed) = listeners(2);
    let disk = Arc::new(Hooks::default());
    let first = Own::new();
    let mut second = Own::new();
    second.engines[0] = Arc::new(Hooked::new(Memory::default(), &disk));
    let [(one, one_server), (two, two_server)] = settled(
        [&first, &second],
        (bound, &listed),
        (&authority, timings()),
        20,
    );
    let epoch = one.standing().epoch;

    // The successor's disk stalls past the silence: its primary's stop waits
    // that long for its answer, and ends without it.
    disk.hold_writes();
    one.stop();
    one_server.stop();
    assert!(
        disk.write_held(Duration::from_secs(5)),
        "the successor keeps its epoch"
    );

    // The former primary back, first listed, holding as many writes as its
    // successor, which has not kept the next epoch yet: promoted in it.
    let (back, back_server) = again(&first, (&listed, 0), &authority);
    assert!(
        until(Duration::from_secs(5), || back.is_primary()),
        "{} / {}",
        back.said(),
        two.said()
    );
    assert_eq!(back.standing().epoch, epoch + 1, "{}", back.said());

    // The successor's disk goes on: it kept that epoch too, and must not be
    // promoted in it beside the one back.
    disk.release_writes();
    let deadline = Instant::now() + timings().silence * 4;
    while Instant::now() < deadline {
        let (mine, its) = (two.standing(), back.standing());
        assert!(
            !(mine.primary && its.primary && mine.epoch == its.epoch),
            "two primaries of epoch {}: {} / {}",
            mine.epoch,
            two.said(),
            back.said()
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(
        until(Duration::from_secs(5), || following(&two)),
        "it follows the one back: {} / {}",
        two.said(),
        back.said()
    );
    assert!(!two.is_primary(), "{}", two.said());
    for (node, server) in [(two, two_server), (back, back_server)] {
        node.stop();
        server.stop();
    }
}

/// What the primary at `address` answers a node presenting `identity` that
/// asks to follow it, read once the node has watched `primary` for a beat:
/// whether it listed that node among those it hands writes to meanwhile,
/// and the refusal where it answered one.
fn asked_to_follow(
    address: &str,
    identity: &tls::Identity,
    primary: &StorageNode,
) -> (bool, Option<String>) {
    let tcp = net::connect(address, Some(Duration::from_secs(2))).expect("connected");
    tcp.set_read_timeout(Some(Duration::from_secs(2)))
        .expect("bounded");
    let config = Arc::new(tls::alpn::offering(
        identity.client().expect("a client"),
        &[ALPN],
    ));
    let mut connection = tls::client_with("127.0.0.1", tcp, config).expect("a handshake");
    let asked = Follow {
        epoch: 0,
        history: 0,
        next: [1; 3],
    };
    wire::send(&mut connection, &Request::Follow(asked)).expect("asked");
    let listed = until(timings().beat * 10, || primary.followers().len() > 1);
    let refused = match wire::receive::<Answer>(&mut connection) {
        Ok(Some(Answer::Failed(refused))) => Some(refused),
        _ => None,
    };
    (listed, refused)
}

#[test]
fn only_a_storage_node_listed_follows_by_the_certificate_it_proves_and_its_place_is_its_own() {
    let authority = Authority::new();
    let (bound, listed) = listeners(2);
    let [(one, one_server), (two, two_server)] = settled(
        [&Own::new(), &Own::new()],
        (bound, &listed),
        (&authority, timings()),
        5,
    );
    let standby = one.followers();
    assert_eq!(standby.len(), 1, "{standby:?}");
    assert_eq!(standby[0].place, Some(1), "worked out by the primary");

    // A node of another role: a certificate of the cluster's authority, for
    // the same host, and none of the Storage nodes listed.
    let other = authority.issue(&authority);
    let (listed_it, refused) = asked_to_follow(&listed[0], &other, &one);
    assert!(
        !listed_it,
        "a node of another role follows: {:?}",
        one.followers()
    );
    let refused = refused.expect("refused in words");
    assert!(refused.starts_with("REFUSED"), "{refused}");
    assert!(
        refused.contains("none of the Storage nodes listed"),
        "{refused}"
    );
    assert_eq!(one.followers(), standby, "the standby alone");
    for (node, server) in [(two, two_server), (one, one_server)] {
        node.stop();
        server.stop();
    }
}

#[test]
fn a_stop_an_operation_outlasts_takes_no_write_after_it_and_names_no_successor() {
    let authority = Authority::new();
    let (bound, listed) = listeners(2);
    let bounded = Timings {
        timeout: Duration::from_millis(200),
        ..timings()
    };
    let [(one, one_server), (two, two_server)] = settled(
        [&Own::new(), &Own::new()],
        (bound, &listed),
        (&authority, bounded),
        5,
    );

    // An operation lent longer than the stop waits for: a replay, a scan,
    // a Publication on a stalled disk.
    let loan = one.lent().expect("lent");
    one.stop();
    let late = loan.storage.write_journey(&journey(900, b"late"));
    assert!(
        late.is_err(),
        "a write after the stop's bound was acknowledged, its successor named: {}",
        one.said()
    );
    drop(loan);
    assert!(!one.said().contains("handing over"), "{}", one.said());
    one_server.stop();
    std::thread::sleep(timings().silence * 2);
    assert!(
        !two.is_primary(),
        "promoted without its primary: {}",
        two.said()
    );
    two.stop();
    two_server.stop();
}

#[test]
fn a_write_asked_while_the_primary_drains_is_answered_by_its_successor() {
    let authority = Authority::new();
    let (bound, listed) = listeners(2);
    let [(one, one_server), (two, two_server)] = settled(
        [&Own::new(), &Own::new()],
        (bound, &listed),
        (&authority, timings()),
        5,
    );
    let client = client(&listed, &authority);
    client
        .write_journey(&journey(99, b"warm"))
        .expect("written");

    // The drain held by an operation under way, a write asked meanwhile.
    let loan = one.lent().expect("lent");
    let stopping = {
        let one = Arc::clone(&one);
        std::thread::spawn(move || one.stop())
    };
    assert!(
        until(Duration::from_secs(5), || one
            .stopping
            .load(Ordering::SeqCst)),
        "stopping"
    );
    let writing = std::thread::spawn(move || {
        let written = client.write_journey(&journey(500, b"during"));
        (written, client)
    });
    std::thread::sleep(timings().beat * 2);
    drop(loan);
    stopping.join().expect("stopped");
    let (written, client) = writing.join().expect("written");
    assert!(
        written.is_ok(),
        "refused across the stop: {written:?}; {}",
        two.said()
    );
    assert!(two.is_primary(), "{}", two.said());
    let read = client
        .read_journey(journey(500, b"").journey)
        .expect("read");
    assert!(read.is_some(), "the write held through the drain is kept");
    one_server.stop();
    two.stop();
    two_server.stop();
}
