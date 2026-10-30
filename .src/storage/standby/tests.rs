//! Storage nodes of one cluster settling a primary, a standby copying it
//! and promoted once it stops, each over Xmip's own mutual TLS on loopback.

use std::net::TcpListener;
use std::sync::Arc;
use std::time::{Duration, Instant};

use secret::{Held, KekName, KeyStore};
use xcore::JourneyId;

use super::{StorageNode, Timings};
use crate::Engine;
use crate::fixture::Memory;
use crate::storage::authority::Authority;
use crate::storage::{JourneyFacts, JourneyRecord, StorageClient, StorageServer, XmipStorage};

fn timings() -> Timings {
    Timings {
        beat: Duration::from_millis(20),
        silence: Duration::from_millis(400),
        timeout: Duration::from_secs(2),
        pass_over: Duration::from_millis(200),
        kept: 1 << 20,
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

/// A Storage node of `listed`, at `listener`, served and started.
fn started(
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
    node.start(Duration::from_secs(5)).expect("started");
    (node, server)
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
fn the_first_listed_is_primary_the_next_copies_it_and_is_promoted_once_it_stops() {
    let authority = Authority::new();
    let (mut bound, listed) = listeners(2);
    let (first, second) = (Own::new(), Own::new());
    let second_listener = bound.pop().expect("two");
    let (one, one_server) = started(&first, (bound.pop().expect("one"), &listed), &authority);
    let (two, two_server) = started(&second, (second_listener, &listed), &authority);
    assert!(one.is_primary(), "{}", one.said());
    assert!(!two.is_primary(), "{}", two.said());

    let client = StorageClient::new(
        &listed,
        &authority.issue(&authority),
        timings().timeout,
        timings().pass_over,
    )
    .expect("a client");
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
    one.stop();
    one_server.stop();

    assert!(
        until(Duration::from_secs(5), || two.is_primary()),
        "{}",
        two.said()
    );
    assert_eq!(two.standing().epoch, one.standing().epoch + 1);
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
    two.stop();
    two_server.stop();
}

#[test]
fn a_standby_started_late_is_copied_whole_and_a_former_primary_follows_the_new_one() {
    let authority = Authority::new();
    let (mut bound, listed) = listeners(2);
    let (first, second) = (Own::new(), Own::new());
    let second_listener = bound.pop().expect("two");
    let first_listener = bound.pop().expect("one");
    let first_address = listed[0].clone();
    let (one, one_server) = started(&first, (first_listener, &listed), &authority);
    let client = StorageClient::new(
        &listed,
        &authority.issue(&authority),
        timings().timeout,
        timings().pass_over,
    )
    .expect("a client");
    for number in 0..20 {
        client
            .write_journey(&journey(number, b"early"))
            .expect("written");
    }
    let (two, two_server) = started(&second, (second_listener, &listed), &authority);
    assert!(
        until(Duration::from_secs(5), || two.standing().next
            == one.standing().next),
        "{} / {}",
        one.said(),
        two.said()
    );
    one.stop();
    one_server.stop();
    assert!(
        until(Duration::from_secs(5), || two.is_primary()),
        "{}",
        two.said()
    );
    let read = client.read_journey(journey(7, b"").journey).expect("read");
    assert_eq!(read.map(|r| r.body), Some(b"early".to_vec()));
    client
        .write_journey(&journey(21, b"later"))
        .expect("written");

    // The former primary comes back over what it kept, and follows.
    let listener = TcpListener::bind(&first_address).expect("bound again");
    let (again, again_server) = started(&first, (listener, &listed), &authority);
    assert!(!again.is_primary(), "{}", again.said());
    assert!(
        until(Duration::from_secs(5), || again.standing().next
            == two.standing().next),
        "{} / {}",
        two.said(),
        again.said()
    );
    two.stop();
    two_server.stop();
    assert!(
        until(Duration::from_secs(5), || again.is_primary()),
        "{}",
        again.said()
    );
    let read = client.read_journey(journey(21, b"").journey).expect("read");
    assert_eq!(read.map(|r| r.body), Some(b"later".to_vec()));
    again.stop();
    again_server.stop();
}

#[test]
fn a_standby_serves_no_operation_and_says_whose_standby_it_is() {
    let authority = Authority::new();
    let (mut bound, listed) = listeners(2);
    let (first, second) = (Own::new(), Own::new());
    let second_listener = bound.pop().expect("two");
    let (one, one_server) = started(&first, (bound.pop().expect("one"), &listed), &authority);
    let (two, two_server) = started(&second, (second_listener, &listed), &authority);
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
