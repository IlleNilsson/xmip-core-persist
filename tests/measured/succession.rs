//! A primary stopped in order hands over to its standby, which succeeds it
//! at once (ADR-0056, amendments 2026-11-23 and 2026-11-24), over a rolling
//! restart of two Storage nodes stopped in turn, each restarted only once
//! the other follows ready, as the deploy restarts them.
//!
//! - **Held**: the standby serves as the primary within a millisecond
//!   beyond the four wakes the hand-over makes, one after another — the
//!   primary's shipping from its wait, the standby's following from its
//!   read of the primary's last frame, the primary's from its read of the
//!   epoch kept, the standby's from its read of that acknowledged — beside
//!   a plain wake measured in the same rounds, the fastest of five, timed
//!   from the stop's start; no node asks anything meanwhile. Until
//!   2026-11-24 two: the primary acknowledged nothing, and a successor
//!   slower than the silence could be promoted beside a former primary
//!   come back (open problem 134).
//! - **Measured, and printed**: how long a node writing all along waits
//!   across each stop, the longest wait between two of its writes answered
//!   one after the other from the stop until the next, beside one of its
//!   writes, and how long the hand-over takes meanwhile. The node has a
//!   wait under way, as every node has its membership's, so it is told who
//!   follows the primary and keeps as many connections to it as it holds
//!   to the primary (`client::waiting`); what it asks across the stop is
//!   held by the primary until it hands over and by its successor until it
//!   serves, never refused (open problems 139 and 142). Its writer asks at
//!   once where one is refused, so a refusal would show as a wait. Storage
//!   on disk, a key store and a third Storage node are measured by the
//!   root's timing program (`.src/test/measured/switchover.rs`; open
//!   problem 143).
//!
//! Until that day a standby waited for its primary to return, and the
//! Hyper-V environment's rolling restart of 2026-11-22 failed writes for
//! 2.5 s and 5.2 s (open problem 92).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use xcore::JourneyId;
use xcore::latency::{PlainWake, spin, spread, woke_beside_a_plain_wake};
use xmip_core_persist::storage::standby::Bootstrap;
use xmip_core_persist::storage::{
    JourneyFacts, JourneyRecord, Position, StorageClient, StorageNode, Waiter, XmipStorage,
};

use super::authority::Authority;
use super::nodes::{Own, Served, listeners, served, timings};

/// How many primaries are stopped in order, each restarted after.
const STOPS: usize = 25;

/// The threads a hand-over wakes one after another where a plain wake wakes
/// one: the primary's shipping, the standby's following, the primary's
/// hand-over reading the epoch kept, the standby's reading it acknowledged.
const WOKEN: u32 = 4;

/// How long a test waits for what it waits on.
const WITHIN: Duration = Duration::from_secs(10);

/// How long the node writing all along leaves a write refused before it
/// asks again: its own pace, so it is not load it makes on the nodes.
const AGAIN: Duration = Duration::from_micros(50);

/// Until `holds`, or [`WITHIN`] passed.
fn until(holds: impl Fn() -> bool) -> bool {
    let deadline = Instant::now() + WITHIN;
    while Instant::now() < deadline {
        if holds() {
            return true;
        }
        std::thread::yield_now();
    }
    holds()
}

/// Whether `node` follows its primary, ready: what the deploy waits for
/// before it restarts the next (`running::activation::readiness`).
fn following(node: &StorageNode) -> bool {
    node.is_following() && node.bootstrap() == Bootstrap::Ready
}

/// A beat of 20 ms and a silence of 400 ms.
fn beat() -> xmip_core_persist::storage::standby::Timings {
    timings(Duration::from_millis(20), Duration::from_millis(400))
}

/// The Storage node over `own` at `listed[at]`, served and started.
fn started(own: &Own, (listed, at): (&[String], usize), authority: &Authority) -> Served {
    let listener = std::net::TcpListener::bind(&listed[at]).expect("bound");
    let node = served(own, (listener, listed), authority, beat());
    node.0.start(WITHIN).expect("started");
    node
}

/// Two Storage nodes over `owns`, started together; the addresses listed.
fn cluster(owns: &[Own; 2], authority: &Authority) -> (Vec<Option<Served>>, Vec<String>) {
    let (bound, listed) = listeners(2);
    let nodes: Vec<Option<Served>> = bound
        .into_iter()
        .zip(owns)
        .map(|(listener, own)| Some(served(own, (listener, &listed), authority, beat())))
        .collect();
    for node in nodes.iter().flatten() {
        node.0.start(WITHIN).expect("started");
    }
    (nodes, listed)
}

/// A client of the Storage nodes `listed`.
fn client(listed: &[String], authority: &Authority) -> Arc<StorageClient> {
    let identity = authority.issue(authority);
    let client = StorageClient::new(listed, &identity, Duration::from_secs(2), beat().pass_over)
        .expect("a client");
    Arc::new(client)
}

/// Journey `number`, a small one.
fn journey(number: u128) -> JourneyRecord {
    JourneyRecord {
        journey: JourneyId::new(0x0199_0000_0000_7000_8000_0000_0000_0000 + number),
        body: vec![b'x'; 64],
        facts: JourneyFacts::default(),
    }
}

/// When `node` first serves as the primary, watched on a thread of its own.
fn promoted(node: &Arc<StorageNode>) -> std::thread::JoinHandle<Instant> {
    let node = Arc::clone(node);
    std::thread::spawn(move || {
        while !node.is_primary() {
            std::hint::spin_loop();
        }
        Instant::now()
    })
}

/// What a rolling restart came to: when each stop began and when its former
/// primary was back, how long its standby took to serve, and a plain wake
/// measured beside each.
struct Rolled {
    stops: Vec<Instant>,
    backs: Vec<Instant>,
    took: Vec<Duration>,
    woke: Vec<Duration>,
}

/// The rolling restart: `rounds` times the primary of `nodes` stopped in
/// order once the other follows ready and `ready` holds, and started again.
fn rolled(
    (nodes, owns): (&mut [Option<Served>], &[Own; 2]),
    (listed, authority): (&[String], &Authority),
    ready: &dyn Fn() -> bool,
) -> Rolled {
    let plain = PlainWake::start();
    let mut rolled = Rolled {
        stops: Vec::new(),
        backs: Vec::new(),
        took: Vec::new(),
        woke: Vec::new(),
    };
    for round in 0..STOPS {
        let primary = usize::from(!nodes[0].as_ref().is_some_and(|(node, _)| node.is_primary()));
        let (node, server) = nodes[primary].take().expect("running");
        let successor = Arc::clone(&nodes[1 - primary].as_ref().expect("running").0);
        assert!(until(|| following(&successor)), "{}", successor.said());
        assert!(until(ready), "round {round}: not ready");
        let watched = promoted(&successor);
        let began = Instant::now();
        node.stop();
        let at = watched.join().expect("watched");
        server.stop();
        rolled.stops.push(began);
        rolled.took.push(at - began);
        rolled.woke.push(plain.round().0);
        nodes[primary] = Some(started(&owns[primary], (listed, primary), authority));
        rolled.backs.push(Instant::now());
    }
    rolled
}

#[test]
fn a_primary_stopped_in_order_is_succeeded_within_a_millisecond_beyond_its_wakes() {
    const WHAT: &str = "a primary stopped in order, succeeded by its standby";
    let authority = Authority::new();
    let owns = [Own::new(), Own::new()];
    let (mut nodes, listed) = cluster(&owns, &authority);
    let client = client(&listed, &authority);
    for number in 0..500 {
        client.write_journey(&journey(number)).expect("written");
    }
    let rolled = rolled((&mut nodes, &owns), (&listed, &authority), &|| true);
    let took = spread(WHAT, rolled.took).fastest_of_five;
    let usual = spread("a plain wake beside it", rolled.woke).fastest_of_five;
    woke_beside_a_plain_wake(WHAT, took, usual, WOKEN);
    for (node, server) in nodes.into_iter().flatten() {
        node.stop();
        server.stop();
    }
}

#[test]
fn a_node_writing_all_along_across_a_rolling_restart_is_measured() {
    let authority = Authority::new();
    let owns = [Own::new(), Own::new()];
    let (mut nodes, listed) = cluster(&owns, &authority);
    let client = client(&listed, &authority);
    let (going, count) = (
        Arc::new(AtomicBool::new(true)),
        Arc::new(AtomicUsize::new(0)),
    );
    let waited = {
        let (client, going) = (Arc::clone(&client), Arc::clone(&going));
        std::thread::spawn(move || {
            let waiter = Arc::new(Waiter::new());
            let seen = [Position::queue(9, 0)];
            while going.load(Ordering::SeqCst) {
                let bound = (Duration::from_millis(200), 0);
                if client.wait_queues(&seen, bound, &waiter).is_err() {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        })
    };
    let along = {
        let (client, going, count) = (Arc::clone(&client), Arc::clone(&going), Arc::clone(&count));
        std::thread::spawn(move || {
            let mut answered = Vec::new();
            for number in 0_u128.. {
                if !going.load(Ordering::SeqCst) {
                    break;
                }
                if client.write_journey(&journey(number)).is_ok() {
                    answered.push(Instant::now());
                    count.fetch_add(1, Ordering::SeqCst);
                } else {
                    spin(AGAIN);
                }
            }
            answered
        })
    };
    // A write's own time measured first, over writes enough; then a hundred
    // writes going on once the standby follows, before each stop, as a
    // deploy's restart of the next node takes its time: the node keeps its
    // connections to a standby it is told follows on a thread of its own,
    // which a stop the moment it follows would race. Until 2026-11-24 the
    // writes were counted from the stop before, and nearly every stop came
    // the moment the standby followed.
    assert!(until(|| count.load(Ordering::SeqCst) > 500), "no writes");
    let ready = || {
        let from = count.load(Ordering::SeqCst);
        until(|| count.load(Ordering::SeqCst) > from + 100)
    };
    let rolled = rolled((&mut nodes, &owns), (&listed, &authority), &ready);
    let from = count.load(Ordering::SeqCst);
    assert!(
        until(|| count.load(Ordering::SeqCst) > from + 50),
        "no writes"
    );
    going.store(false, Ordering::SeqCst);
    let answered = along.join().expect("written along");
    waited.join().expect("waited along");

    let longest = |from: Instant, until: Option<&Instant>| {
        let waits = answered
            .windows(2)
            .filter(|pair| pair[1] > from && until.is_none_or(|until| pair[0] < *until));
        let waited = waits.map(|pair| pair[1] - pair[0]).max();
        waited.expect("a write answered after the stop")
    };
    let Rolled {
        stops, backs, took, ..
    } = rolled;
    // From each stop to the next, as measured until 2026-11-24; and apart,
    // across the stop until its former primary was back, then from then
    // until the next stop, while it came back to follow its successor.
    let gaps: Vec<Duration> = stops
        .iter()
        .enumerate()
        .map(|(at, began)| longest(*began, stops.get(at + 1)))
        .collect();
    let across: Vec<Duration> = stops
        .iter()
        .zip(&backs)
        .map(|(began, back)| longest(*began, Some(back)))
        .collect();
    let after: Vec<Duration> = backs
        .iter()
        .enumerate()
        .map(|(at, back)| longest(*back, stops.get(at + 1)))
        .collect();
    let writes: Vec<Duration> = answered
        .windows(2)
        .take_while(|pair| pair[1] < stops[0])
        .map(|pair| pair[1] - pair[0])
        .collect();
    let _printed = spread("a write, one after another", writes);
    let _printed = spread("writes waited across a primary's stop in order", gaps);
    let _printed = spread(
        "of it, from the stop until the former primary was back",
        across,
    );
    let _printed = spread("of it, as the former primary came back to follow", after);
    let _printed = spread("the hand-over meanwhile", took);
    for (node, server) in nodes.into_iter().flatten() {
        node.stop();
        server.stop();
    }
}
