//! The `membership` table on the embedded Storage node: each node's latest
//! row, the rows before it kept, Xmip Storage's times, the wait on the
//! table, and the same over the wire, where a person writes no row.

use std::net::TcpListener;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use secret::{Held, KekName};

use super::*;
use crate::fixture::Memory;
use crate::fixture::crowd::{Node, node};
use crate::storage::authority::Authority;
use crate::storage::client::{PASS_OVER, TIMEOUT};
use crate::storage::{Position, StorageClient, StorageServer, Waiter, XmipStorage};

/// Far longer than any test here takes: a wait that ends sooner was
/// answered, never timed out.
const LONG: Duration = Duration::from_secs(60);

/// The scope of the test cluster's node at `place`.
fn scope(place: usize) -> String {
    configure::fixture::test_cluster().node_scope(place)
}

/// `node`'s start as process `process` writes it: Xmip Storage's own values
/// unset, stopped where `stopped` says. Its evidence is a capability's, as
/// the runtime writes it; persist reads no word of it.
fn start(process: u128, node: &str, stopped: bool) -> MemberRecord {
    MemberRecord {
        process,
        node: node.to_string(),
        roles: "receiving".to_string(),
        evidence: "declares receiving; offline".to_string(),
        operate: Some("127.0.0.1:7443".to_string()),
        sync: None,
        storage_place: None,
        save: "sha256:01".to_string(),
        serial: 5,
        started_unix_nanos: 0,
        stopped_unix_nanos: stopped.then_some(0),
        written: 0,
        latest: false,
    }
}

/// Each member now by its process.
fn processes(members: &[MemberRecord]) -> Vec<u128> {
    let mut each: Vec<u128> = members.iter().map(|member| member.process).collect();
    each.sort_unstable();
    each
}

fn membership(at: u64) -> Position {
    Position {
        watched: Watched::Membership,
        at,
    }
}

#[test]
fn each_node_s_latest_row_is_a_member_and_the_rows_before_it_are_kept() {
    let (storage, clock) = node();
    let (first, second) = (scope(0), scope(1));
    assert_eq!(storage.read_members().expect("read"), []);
    assert_eq!(storage.members_written().expect("counted"), 0);

    storage
        .write_member(&start(1, &first, false))
        .expect("written");
    clock.pass(1_000);
    storage
        .write_member(&start(1, &first, false))
        .expect("asked again");
    let members = storage.read_members().expect("read");
    assert_eq!(processes(&members), [1]);
    let one = &members[0];
    assert_eq!(one.started_unix_nanos, 1_000, "Storage's time, kept");
    assert!(one.latest && one.runs());

    clock.pass(1_000);
    storage
        .write_member(&start(1, &first, true))
        .expect("stopped");
    let stopped = storage.read_members().expect("read");
    assert_eq!(stopped[0].stopped_unix_nanos, Some(3_000), "Storage's time");
    assert!(
        !stopped[0].runs(),
        "a member stopped in order is still listed"
    );

    clock.pass(1_000);
    storage
        .write_member(&start(2, &first, false))
        .expect("started again");
    storage
        .write_member(&start(3, &second, false))
        .expect("another");
    assert_eq!(processes(&storage.read_members().expect("read")), [2, 3]);

    let kept = storage
        .member(1)
        .expect("read")
        .expect("kept, never deleted");
    assert!(!kept.latest, "no longer its node's latest");
    assert_eq!(kept.stopped_unix_nanos, Some(3_000));
    // Five rows written and one replaced as the latest: six writes.
    assert_eq!(storage.members_written().expect("counted"), 6);
}

#[test]
fn a_wait_on_the_membership_is_told_as_a_row_is_written_and_answered_at_once_when_behind() {
    let (storage, _) = node();
    let storage: Arc<Node> = Arc::new(storage);
    let behind = Arc::new(Waiter::new());
    storage
        .write_member(&start(1, &scope(0), false))
        .expect("written");
    let seen = [membership(0)];
    let at_once = storage
        .wait_queues(&seen, (LONG, 0), &behind)
        .expect("waited");
    assert_eq!(
        at_once[0].position,
        membership(1),
        "behind, answered at once"
    );

    let waiter = Arc::new(Waiter::new());
    let waiting = {
        let (storage, waiter) = (Arc::clone(&storage), Arc::clone(&waiter));
        thread::spawn(move || storage.wait_queues(&[membership(1)], (LONG, 0), &waiter))
    };
    let deadline = Instant::now() + LONG;
    while storage.waiting_on(Watched::Membership) == 0 {
        assert!(Instant::now() < deadline, "never listed");
        thread::yield_now();
    }
    let asked = Instant::now();
    storage
        .write_member(&start(2, &scope(1), false))
        .expect("written");
    let told = waiting.join().expect("waited").expect("answered");
    assert!(asked.elapsed() < LONG, "told, never timed out");
    assert_eq!(told[0].position, membership(2));
    assert_eq!(told[0].due, None, "nothing is due in a table");
}

/// A Storage node served over mutual TLS, and a client of `identity`'s.
fn served(identity: impl Fn(&Authority) -> tls::Identity) -> (StorageServer, StorageClient) {
    let keys = Held::new(secret::fixture::Memory::default());
    let kek = KekName::new("storage").expect("a name");
    let opened = Embedded::open(
        Memory::default(),
        Memory::default(),
        Memory::default(),
        &keys,
        &kek,
    )
    .expect("opened");
    let storage: Arc<dyn XmipStorage> = Arc::new(opened);
    let authority = Authority::new();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bound");
    let server =
        StorageServer::start(storage, listener, &authority.issue(&authority)).expect("served");
    let at = [server.address().to_string()];
    let client = StorageClient::new(&at, &identity(&authority), TIMEOUT, PASS_OVER).expect("one");
    (server, client)
}

#[test]
fn a_node_writes_and_waits_on_the_membership_over_the_wire_and_a_person_writes_none() {
    let (server, client) = served(|authority| authority.issue(authority));
    let client = Arc::new(client);
    let waiter = Arc::new(Waiter::new());
    let waiting = {
        let (client, waiter) = (Arc::clone(&client), Arc::clone(&waiter));
        thread::spawn(move || client.wait_queues(&[membership(0)], (LONG, 0), &waiter))
    };
    // The wait's own connection is the client's; a second asks the write,
    // and the wait is told whether it began before the write or after.
    client
        .write_member(&start(1, &scope(0), false))
        .expect("written over the wire");
    let told = waiting.join().expect("waited").expect("answered");
    assert_eq!(told[0].position, membership(1));
    let members = client.read_members().expect("read over the wire");
    assert_eq!(processes(&members), [1]);
    assert!(members[0].started_unix_nanos > 0, "the Storage node's time");
    server.stop();

    let (server, person) = served(|authority| authority.issue_person(authority));
    let refused = person.write_member(&start(2, &scope(1), false));
    assert!(refused.is_err(), "a person's connection writes no row");
    server.stop();
}
