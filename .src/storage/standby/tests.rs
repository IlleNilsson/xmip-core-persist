//! Storage nodes of one cluster settling a primary once every one listed
//! answers, a standby copying it, and the primary gone without a word
//! waited for, never replaced, each over Xmip's own mutual TLS on loopback.

use std::net::TcpListener;
use std::sync::Arc;
use std::time::{Duration, Instant};

use secret::{Held, KekName, KeyStore};
use xcore::JourneyId;

use super::{Bootstrap, StorageNode, Timings};
use crate::Engine;
use crate::fixture::{Hooked, Hooks, Memory};
use crate::storage::authority::Authority;
use crate::storage::{JourneyFacts, JourneyRecord, StorageClient, StorageServer, XmipStorage};

fn timings() -> Timings {
    Timings {
        beat: Duration::from_millis(20),
        silence: Duration::from_millis(400),
        timeout: Duration::from_secs(2),
        pass_over: Duration::from_millis(200),
        kept: 1 << 20,
        unwaited: crate::storage::UNWAITED,
        writer_kept: crate::storage::WRITER_KEPT,
    }
}

/// One Storage node's own: its engines and its key store, each node's key
/// store its own, so a standby holds its primary's records under keys it
/// wrapped itself.
struct Own {
    engines: [Arc<dyn Engine>; 3],
    keys: Arc<dyn KeyStore>,
}

impl Own {
    fn new() -> Self {
        Self {
            engines: [0, 1, 2].map(|_| Arc::new(Memory::default()) as Arc<dyn Engine>),
            keys: Arc::new(Held::new(secret::fixture::Memory::default())),
        }
    }
}

/// A Storage node of `listed`, at `listener`, served, settling nothing yet.
fn served(
    own: &Own,
    at: (TcpListener, &[String]),
    authority: &Authority,
) -> (Arc<StorageNode>, StorageServer) {
    served_by(own, at, authority, timings())
}

/// [`served`], bounded by `timings`.
fn served_by(
    own: &Own,
    (listener, listed): (TcpListener, &[String]),
    authority: &Authority,
    timings: Timings,
) -> (Arc<StorageNode>, StorageServer) {
    let identity = authority.issue(authority);
    let keys = (
        Arc::clone(&own.keys),
        KekName::new("storage").expect("a name"),
    );
    let node = StorageNode::new(own.engines.clone(), keys, listed, &identity, timings)
        .expect("a Storage node");
    let server = StorageServer::several(&node, listener, &identity).expect("served");
    (node, server)
}

/// A Storage node of `listed`, at `listener`, served and started.
fn started(
    own: &Own,
    at: (TcpListener, &[String]),
    authority: &Authority,
) -> (Arc<StorageNode>, StorageServer) {
    let (node, server) = served(own, at, authority);
    node.start(Duration::from_secs(5)).expect("started");
    (node, server)
}

/// The Storage node over `own` started again at `listed[at]`.
fn again(
    own: &Own,
    (listed, at): (&[String], usize),
    authority: &Authority,
) -> (Arc<StorageNode>, StorageServer) {
    let listener = TcpListener::bind(&listed[at]).expect("bound again");
    started(own, (listener, listed), authority)
}

/// Two Storage nodes of `listed`, each over its own, both served before
/// either starts: every one listed answers, and they settle at once.
fn both_started(
    own: [&Own; 2],
    (mut bound, listed): (Vec<TcpListener>, &[String]),
    authority: &Authority,
) -> [(Arc<StorageNode>, StorageServer); 2] {
    let second = bound.pop().expect("two");
    let first = bound.pop().expect("one");
    let one = served(own[0], (first, listed), authority);
    let two = served(own[1], (second, listed), authority);
    for (node, _) in [&one, &two] {
        node.start(Duration::from_secs(5)).expect("started");
    }
    [one, two]
}

fn listeners(count: usize) -> (Vec<TcpListener>, Vec<String>) {
    let listeners: Vec<TcpListener> = (0..count)
        .map(|_| TcpListener::bind("127.0.0.1:0").expect("bound"))
        .collect();
    let listed = listeners
        .iter()
        .map(|l| l.local_addr().expect("an address").to_string())
        .collect();
    (listeners, listed)
}

/// A node's client of the Storage nodes `listed`.
fn client(listed: &[String], authority: &Authority) -> StorageClient {
    StorageClient::new(
        listed,
        &authority.issue(authority),
        timings().timeout,
        timings().pass_over,
    )
    .expect("a client")
}

fn journey(number: u128, body: &[u8]) -> JourneyRecord {
    JourneyRecord {
        journey: JourneyId::new(0x0199_0000_0000_7000_8000_0000_0000_0000 + number),
        body: body.to_vec(),
        facts: JourneyFacts::default(),
    }
}

/// A Storage node gone as a killed process goes: its server first, so no
/// connection of its says anything more — no standby is told it stopped,
/// nor named its successor — then its settling, with no stop in order, so
/// it keeps no positions. Its databases are as the process left them.
fn killed((node, server): (Arc<StorageNode>, StorageServer)) {
    server.stop();
    node.stopping
        .store(true, std::sync::atomic::Ordering::SeqCst);
    {
        let _settling = xcore::lock::Locked::locked(&node.settling);
        node.settled.notify_all();
    }
    let finding = xcore::lock::Locked::locked(&node.finding).take();
    if let Some(thread) = finding {
        thread.join().expect("its settling thread");
    }
}

/// Until `holds`, or `within` passed.
fn until(within: Duration, holds: impl Fn() -> bool) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if holds() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    holds()
}

#[test]
fn the_first_listed_alone_is_not_promoted_until_every_one_listed_answers() {
    let authority = Authority::new();
    let (mut bound, listed) = listeners(2);
    let second_listener = bound.pop().expect("two");
    let first_listener = bound.pop().expect("one");
    // The second is not listening yet: it does not answer.
    drop(second_listener);
    let (first, second) = (Own::new(), Own::new());
    let (one, one_server) = served(&first, (first_listener, &listed), &authority);
    one.start(timings().silence).expect("started");
    std::thread::sleep(timings().silence * 3);
    assert!(one.is_finding(), "{}", one.said());
    assert_eq!(one.standing().epoch, 0, "nothing promoted");

    let listener = TcpListener::bind(&listed[1]).expect("bound");
    let (two, two_server) = started(&second, (listener, &listed), &authority);
    assert!(
        until(Duration::from_secs(5), || one.is_primary()),
        "{}",
        one.said()
    );
    assert!(
        until(Duration::from_secs(5), || !two.is_finding()),
        "{}",
        two.said()
    );
    assert!(!two.is_primary(), "{}", two.said());
    two.stop();
    two_server.stop();
    one.stop();
    one_server.stop();
}

#[test]
fn a_standby_is_not_promoted_while_its_primary_is_gone_and_one_is_once_every_one_answers() {
    let authority = Authority::new();
    let (bound, listed) = listeners(2);
    let (first, second) = (Own::new(), Own::new());
    let first_address = listed[0].clone();
    let [(one, one_server), (two, two_server)] =
        both_started([&first, &second], (bound, &listed), &authority);
    assert!(one.is_primary(), "{}", one.said());
    assert!(!two.is_primary(), "{}", two.said());

    let client = client(&listed, &authority);
    for number in 0..50 {
        client
            .write_journey(&journey(number, b"before"))
            .expect("written");
    }
    // A Message held mid-way when the primary stops (the owner, 2026-10-27:
    // a fencing token, a number that grows with each holder).
    let held = journey(7, b"").journey;
    let lease = Duration::from_millis(50);
    let before = client
        .claim(
            held,
            &configure::fixture::test_cluster().node_scope(0),
            1,
            lease,
        )
        .expect("asked")
        .expect("taken");
    let epoch = one.standing().epoch;
    killed((one, one_server));

    // No witness: the primary gone without a word is waited for, never
    // replaced; one stopped in order hands over (`succession`).
    assert!(
        until(Duration::from_secs(5), || two.is_finding()),
        "{}",
        two.said()
    );
    std::thread::sleep(timings().silence * 3);
    assert!(!two.is_primary(), "{}", two.said());
    assert_eq!(two.standing().epoch, epoch, "nothing promoted");
    assert!(
        client.read_journey(journey(1, b"").journey).is_err(),
        "no primary answers"
    );

    // The former primary back: every one listed answers, none a primary,
    // and the standby, holding more — the former primary gave up its
    // positions when promoted, and was killed before it kept them again —
    // is promoted; the former primary follows.
    let listener = TcpListener::bind(&first_address).expect("bound again");
    let (again, again_server) = started(&first, (listener, &listed), &authority);
    assert!(
        until(Duration::from_secs(5), || two.is_primary()),
        "{}",
        two.said()
    );
    assert_eq!(two.standing().epoch, epoch + 1);
    assert!(
        until(Duration::from_secs(5), || again.standing().next
            == two.standing().next),
        "{} / {}",
        two.said(),
        again.said()
    );
    assert!(!again.is_primary(), "{}", again.said());
    // Its lease lapsed while the primary was gone: three silences, far
    // longer than the lease, on the one clock both nodes read.
    assert!(
        timings().silence * 3 > lease * 2,
        "the lease lapsed meanwhile"
    );
    let after = client
        .claim(
            held,
            &configure::fixture::test_cluster().node_scope(1),
            2,
            lease,
        )
        .expect("asked")
        .expect("lapsed, taken over");
    assert_eq!(after.fence >> 32, two.standing().epoch, "the new epoch");
    assert!(
        after.fence > before.fence,
        "{} after {}: the new epoch's fence is above every fence before it",
        after.fence,
        before.fence
    );
    assert_eq!(
        client.renew(&[before], lease).expect("asked"),
        [],
        "the old holder's claim is refused"
    );
    for number in 0..50 {
        let read = client
            .read_journey(journey(number, b"").journey)
            .expect("read");
        assert_eq!(read.map(|r| r.body), Some(b"before".to_vec()), "{number}");
    }
    client
        .write_journey(&journey(50, b"after"))
        .expect("carried on");
    let reached = client.reached();
    assert!(
        reached[1].1.as_ref().is_some_and(|r| r.answered),
        "{reached:?}"
    );
    again.stop();
    again_server.stop();
    two.stop();
    two_server.stop();
}

#[test]
fn the_primary_serves_on_while_a_standby_is_gone_and_one_started_empty_is_copied_whole() {
    let authority = Authority::new();
    let (bound, listed) = listeners(2);
    let (first, second) = (Own::new(), Own::new());
    let second_address = listed[1].clone();
    let [(one, one_server), (two, two_server)] =
        both_started([&first, &second], (bound, &listed), &authority);
    two.stop();
    two_server.stop();
    let client = client(&listed, &authority);
    for number in 0..20 {
        client
            .write_journey(&journey(number, b"early"))
            .expect("written, the standby gone");
    }
    assert!(one.is_primary(), "{}", one.said());

    // The standby replaced by one started empty, late: copied whole.
    let listener = TcpListener::bind(&second_address).expect("bound again");
    let (late, late_server) = started(&Own::new(), (listener, &listed), &authority);
    assert!(!late.is_primary(), "{}", late.said());
    assert!(
        until(Duration::from_secs(5), || late.standing().next
            == one.standing().next),
        "{} / {}",
        one.said(),
        late.said()
    );
    assert!(
        until(Duration::from_secs(5), || late.standing().bootstrap
            == Bootstrap::Ready),
        "a whole copy taken, with no write while it was scanned, is ready: {}",
        late.said()
    );
    let read = client.read_journey(journey(7, b"").journey).expect("read");
    assert_eq!(read.map(|r| r.body), Some(b"early".to_vec()));
    client
        .write_journey(&journey(21, b"later"))
        .expect("written");
    assert!(
        until(Duration::from_secs(5), || late.standing().next
            == one.standing().next),
        "copied on after the whole copy: {} / {}",
        one.said(),
        late.said()
    );
    late.stop();
    late_server.stop();
    one.stop();
    one_server.stop();
}

mod bootstrap;
mod due;
mod following;
mod handed;
mod interrupted;
mod membership;
mod refused;
mod rolling;
mod served;
mod stopped;
mod succession;
mod synced;
mod uncopied;
mod waited;
