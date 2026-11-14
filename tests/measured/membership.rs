//! The `membership` table held to the millisecond rule: a node's row
//! written within a millisecond beyond Xmip Storage's own write of an
//! administration record, over the history of many starts kept, and a wait
//! on the table woken within a millisecond of a row's write being asked.

use std::sync::Arc;
use std::time::{Duration, Instant};

use xmip_core_persist::fixture::crowd::node;
use xmip_core_persist::storage::{
    AdministrationKind, AdministrationRecord, MemberRecord, Position, Watched, XmipStorage,
};

use super::{ROUNDS, held_to_the_millisecond, woken};

/// How many times each node of the test cluster started before the rows
/// timed: the history kept, never deleted (ADR-0040).
const STARTS: u128 = 1_000;

/// The scope of the test cluster's node at `place`, of however many it has.
fn scope(place: usize) -> String {
    let cluster = configure::fixture::test_cluster();
    cluster.node_scope(place % cluster.nodes.len())
}

/// How many nodes the test cluster has.
fn nodes() -> usize {
    configure::fixture::test_cluster().nodes.len()
}

/// The start `process` of the node at `place`, as its node writes it.
fn start(process: u128, place: usize) -> MemberRecord {
    MemberRecord {
        process,
        node: scope(place),
        roles: "receiving".to_string(),
        evidence: "declares receiving; offline; authentication and runtime capability are not \
                   declared in this record"
            .to_string(),
        operate: Some("127.0.0.1:7443".to_string()),
        sync: None,
        storage_place: None,
        save: format!("sha256:{:064x}", 7),
        serial: 1_763_000_000_000_000_000,
        started_unix_nanos: 0,
        stopped_unix_nanos: None,
        written: 0,
        latest: false,
    }
}

/// The median of `taken`.
fn median(mut taken: Vec<Duration>) -> Duration {
    taken.sort();
    taken[taken.len() / 2]
}

#[test]
fn a_node_s_row_is_written_within_a_millisecond_beyond_storage_s_own_write() {
    let (node, _) = node();
    let nodes = nodes();
    let mut process = 0;
    for _ in 0..STARTS {
        for place in 0..nodes {
            process += 1;
            node.write_member(&start(process, place))
                .expect("a start kept");
        }
    }
    let own: Vec<Duration> = (0..ROUNDS)
        .map(|round| {
            let record = AdministrationRecord {
                kind: AdministrationKind::Operator,
                id: u128::from(round) + 1,
                body: start(0, 0).node.into_bytes(),
                updated_unix_nanos: 0,
            };
            let started = Instant::now();
            node.write_administration(&record).expect("written");
            started.elapsed()
        })
        .collect();
    let rows: Vec<Duration> = (0..ROUNDS)
        .map(|round| {
            process += 1;
            let place = usize::try_from(round).expect("a place");
            let row = start(process, place);
            let started = Instant::now();
            node.write_member(&row).expect("written");
            started.elapsed()
        })
        .collect();
    let (own, rows) = (median(own), median(rows));
    let beyond = rows.saturating_sub(own);
    eprintln!(
        "a node's row over {STARTS} starts of each of {nodes} nodes kept: {rows:?}, Storage's \
         own write {own:?}, beyond it {beyond:?}"
    );
    assert_eq!(
        node.read_members().expect("read").len(),
        nodes,
        "the latest"
    );
    assert!(
        beyond < Duration::from_millis(1),
        "{beyond:?} beyond Storage's own write"
    );
}

#[test]
fn a_wait_on_the_membership_is_woken_within_a_millisecond_of_a_row_s_write() {
    let (node, _) = node();
    let node = Arc::new(node);
    // Where the table stands: a node started again rewrites its row before
    // too, so a write moves it by one or by two.
    let mut stands = 0;
    let woken = (0..ROUNDS)
        .map(|round| {
            let seen = Position {
                watched: Watched::Membership,
                at: stands,
            };
            let place = usize::try_from(round).expect("a place");
            let process = u128::from(round) + 1;
            let (moved, took) = woken(&node, seen, || {
                node.write_member(&start(process, place)).expect("written");
            });
            assert!(moved[0].position.at > stands, "{moved:?}");
            stands = moved[0].position.at;
            took
        })
        .collect();
    held_to_the_millisecond(
        "a wait on the membership woken after a node's row was asked written",
        woken,
    );
}
