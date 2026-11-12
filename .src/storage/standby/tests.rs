//! Storage nodes of one cluster settling a primary once every one listed
//! answers, a standby copying it, and the primary gone waited for, never
//! replaced, each over Xmip's own mutual TLS on loopback.

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
    (listener, listed): (TcpListener, &[String]),
    authority: &Authority,
) -> (Arc<StorageNode>, StorageServer) {
    let identity = authority.issue(authority);
    let keys = (
        Arc::clone(&own.keys),
        KekName::new("storage").expect("a name"),
    );
    let node = StorageNode::new(own.engines.clone(), keys, listed, &identity, timings())
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
    one.stop();
    one_server.stop();

    // No witness: the primary gone is waited for, never replaced.
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
    // positions when promoted — is promoted; the former primary follows.
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
    std::thread::sleep(lease * 2);
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

#[test]
fn a_standby_serves_no_operation_and_says_whose_standby_it_is() {
    let authority = Authority::new();
    let (bound, listed) = listeners(2);
    let (first, second) = (Own::new(), Own::new());
    let [(one, one_server), (two, two_server)] =
        both_started([&first, &second], (bound, &listed), &authority);
    let refused = two.served().read_journey(journey(1, b"").journey);
    let Err(crate::PersistError::Standby { reason }) = refused else {
        panic!("{refused:?}");
    };
    assert!(reason.contains(&listed[0]), "{reason}");
    assert!(
        two.reaching()
            .write_journey(&journey(1, b"own work"))
            .is_ok()
    );
    assert!(
        one.served()
            .read_journey(journey(1, b"").journey)
            .expect("read")
            .is_some(),
        "a standby's own work reaches its primary"
    );
    two.stop();
    two_server.stop();
    one.stop();
    one_server.stop();
}

mod bootstrap;
mod interrupted;
mod refused;
mod waited;
