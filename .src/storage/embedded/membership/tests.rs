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
/// unset, stopped where `stopped` says, a later process a later start. Its
/// evidence is a capability's, as the runtime writes it; persist reads no
/// word of it.
fn start(process: u128, node: &str, stopped: bool) -> MemberRecord {
    numbered(
        process,
        u64::try_from(process).expect("a start"),
        node,
        stopped,
    )
}

/// [`start`], numbered `start` whatever its process.
fn numbered(process: u128, start: u64, node: &str, stopped: bool) -> MemberRecord {
    MemberRecord {
        process,
        start,
        node: node.to_string(),
        roles: "receiving".to_string(),
        evidence: "declares receiving; no Node capability".to_string(),
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
    clock.pass(Duration::from_micros(1));
    storage
        .write_member(&start(1, &first, false))
        .expect("asked again");
    let members = storage.read_members().expect("read");
    assert_eq!(processes(&members), [1]);
    let one = &members[0];
    assert_eq!(one.started_unix_nanos, 1_000, "Storage's time, kept");
    assert!(one.latest && one.runs());

    clock.pass(Duration::from_micros(1));
    storage
        .write_member(&start(1, &first, true))
        .expect("stopped");
    let stopped = storage.read_members().expect("read");
    assert_eq!(stopped[0].stopped_unix_nanos, Some(3_000), "Storage's time");
    assert!(
        !stopped[0].runs(),
        "a member stopped in order is still listed"
    );

    clock.pass(Duration::from_micros(1));
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

/// A node started again while its earlier start still runs, then that
/// earlier start stopped in order: its stop is written to its own row
/// alone, and the later start stays its node's member. Until 2026-11-19
/// every write made its row the latest, so the earlier start's stop took
/// the node from its successor.
#[test]
#[expect(non_snake_case, reason = "A and B name the starts")]
fn register_A_register_B_stop_A_leaves_B_the_member() {
    let (storage, _) = node();
    let at = scope(0);
    let (a, b) = (1, 2);
    storage.write_member(&start(a, &at, false)).expect("A");
    storage.write_member(&start(b, &at, false)).expect("B");
    let registered = storage.member(b).expect("read").expect("B's row");
    storage.write_member(&start(a, &at, true)).expect("A stops");

    let members = storage.read_members().expect("read");
    assert_eq!(processes(&members), [b], "B is the member");
    assert_eq!(members[0], registered, "B's row untouched");
    let stopped = storage.member(a).expect("read").expect("kept");
    assert!(!stopped.latest, "A is no longer its node's latest");
    assert!(!stopped.runs(), "A's stop written to its own row");
    // Two rows written, one replaced as the latest, one stop: four writes.
    assert_eq!(storage.members_written().expect("counted"), 4);
}

/// A row names one node: a process writing its row again as another is
/// refused, and nothing is written.
#[test]
fn a_process_rewriting_its_row_as_another_node_is_refused() {
    let (storage, _) = node();
    let (first, second) = (scope(0), scope(1));
    storage.write_member(&start(1, &first, false)).expect("A");
    let own = storage.member(1).expect("read").expect("A's row");

    let refused = storage.write_member(&start(1, &second, false));
    let Err(PersistError::Failed { reason }) = refused else {
        panic!("refused, not {refused:?}");
    };
    assert!(
        reason.contains(&first) && reason.contains(&second),
        "{reason}"
    );
    assert_eq!(storage.member(1).expect("read"), Some(own), "kept");
    assert_eq!(processes(&storage.read_members().expect("read")), [1]);
    assert_eq!(storage.members_written().expect("counted"), 1);
}

/// A start whose first write lands only after a later start of its node
/// registered — its earlier tries unanswered, or their answers lost — is
/// refused as superseded, saying the later start's number, and nothing is
/// written: the later start stays the member. Until 2026-11-20 it
/// registered then, and took its node from its successor.
#[test]
#[expect(non_snake_case, reason = "A and B name the starts")]
fn register_B_then_A_s_late_first_write_is_refused_and_B_stays_the_member() {
    let (storage, _) = node();
    let at = scope(0);
    let (a, b) = (1, 2);
    storage.write_member(&start(b, &at, false)).expect("B");
    let registered = storage.member(b).expect("read").expect("B's row");

    let refused = storage.write_member(&start(a, &at, false));
    let Err(PersistError::Superseded { latest, reason }) = refused else {
        panic!("superseded, not {refused:?}");
    };
    assert_eq!(latest, 2, "B's start");
    for said in ["start 1", "start 2", &at, &format!("{b:032x}")] {
        assert!(reason.contains(said), "{said} in {reason}");
    }
    assert_eq!(storage.member(a).expect("read"), None, "A has no row");
    let members = storage.read_members().expect("read");
    assert_eq!(processes(&members), [b], "B is the member");
    assert_eq!(members[0], registered, "B's row untouched");
    assert_eq!(storage.members_written().expect("counted"), 1, "none more");
}

/// Starts are ordered strictly: a start numbered as its node's latest is
/// no later than it, and is refused as one before it is.
#[test]
fn a_start_equal_to_its_node_s_latest_is_refused() {
    let (storage, _) = node();
    let at = scope(0);
    storage
        .write_member(&numbered(1, 5, &at, false))
        .expect("the first");
    let refused = storage.write_member(&numbered(2, 5, &at, false));
    assert!(
        matches!(refused, Err(PersistError::Superseded { latest: 5, .. })),
        "{refused:?}"
    );
    assert_eq!(processes(&storage.read_members().expect("read")), [1]);
    storage
        .write_member(&numbered(3, 6, &at, false))
        .expect("a later start registers");
    assert_eq!(processes(&storage.read_members().expect("read")), [3]);
}

/// A row's start never changes: a process writing its row again under
/// another start is refused, and nothing is written.
#[test]
fn a_process_rewriting_its_row_under_another_start_is_refused() {
    let (storage, _) = node();
    let at = scope(0);
    storage
        .write_member(&numbered(1, 1, &at, false))
        .expect("A");
    let own = storage.member(1).expect("read").expect("A's row");

    let refused = storage.write_member(&numbered(1, 3, &at, true));
    let Err(PersistError::Failed { reason }) = refused else {
        panic!("refused, not {refused:?}");
    };
    assert!(
        reason.contains("start 1") && reason.contains("start 3"),
        "{reason}"
    );
    assert_eq!(storage.member(1).expect("read"), Some(own), "kept");
    assert_eq!(storage.members_written().expect("counted"), 1);
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

/// The refusal of a start superseded crosses the wire as itself, the later
/// start's number with it, so the node it reaches stops trying.
#[test]
fn a_late_first_write_is_refused_over_the_wire_as_superseded() {
    let (server, client) = served(|authority| authority.issue(authority));
    let at = scope(0);
    client.write_member(&start(2, &at, false)).expect("B");
    let refused = client.write_member(&start(1, &at, false));
    let Err(PersistError::Superseded { latest, reason }) = refused else {
        panic!("superseded over the wire, not {refused:?}");
    };
    assert_eq!(latest, 2);
    assert!(reason.contains("start 1"), "{reason}");
    let members = client.read_members().expect("read over the wire");
    assert_eq!(processes(&members), [2]);
    server.stop();
}
