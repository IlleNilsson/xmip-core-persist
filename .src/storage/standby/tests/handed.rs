//! What a primary hands each standby, counted by the node the standby's
//! certificate names (ADR-0052 and ADR-0056, amendments 2026-11-13): a
//! whole copy's entries, then each write, and the bytes of both, over
//! Xmip's own mutual TLS on loopback.

use super::*;

/// The Storage node of `listed` at `listener`, presenting a certificate
/// naming `node`, served and started.
fn named(
    own: &Own,
    (listener, listed): (TcpListener, &[String]),
    (authority, node): (&Authority, &str),
) -> (Arc<StorageNode>, StorageServer) {
    let identity = authority.issue_node(authority, node);
    let keys = (
        Arc::clone(&own.keys),
        KekName::new("storage").expect("a name"),
    );
    let storage = StorageNode::new(own.engines.clone(), keys, listed, &identity, timings())
        .expect("a Storage node");
    let server = StorageServer::several(&storage, listener, &identity).expect("served");
    (storage, server)
}

#[test]
fn a_primary_counts_what_it_hands_each_standby_by_the_node_its_certificate_names() {
    let authority = Authority::new();
    let cluster = configure::fixture::test_cluster();
    let [first_node, second_node] = [0, 1].map(|at| cluster.node(at).name.clone());
    let (mut bound, listed) = listeners(2);
    let second_listener = bound.pop().expect("two");
    let first_listener = bound.pop().expect("one");
    let (first, second) = (Own::new(), Own::new());
    let (one, one_server) = named(&first, (first_listener, &listed), (&authority, &first_node));
    let (two, two_server) = named(
        &second,
        (second_listener, &listed),
        (&authority, &second_node),
    );
    for storage in [&one, &two] {
        storage.start(Duration::from_secs(5)).expect("started");
    }
    assert!(one.is_primary(), "{}", one.said());
    assert!(
        until(Duration::from_secs(5), || one.followers().len() == 1),
        "{}",
        one.said()
    );
    assert_eq!(two.followers(), [], "a standby hands nothing on");
    two.stop();
    two_server.stop();
    assert!(
        until(Duration::from_secs(5), || one.followers().is_empty()),
        "a standby gone is not following: {}",
        one.said()
    );

    let client = client(&listed, &authority);
    for number in 0..20 {
        client
            .write_journey(&journey(number, b"early"))
            .expect("written");
    }
    // Started again empty, the same node: copied whole, then each write.
    let listener = TcpListener::bind(&listed[1]).expect("bound again");
    let (late, late_server) = named(&Own::new(), (listener, &listed), (&authority, &second_node));
    late.start(Duration::from_secs(5)).expect("started");
    let caught_up = || {
        one.followers()
            .first()
            .is_some_and(|follower| follower.behind == 0)
            && late.standing().bootstrap == Bootstrap::Ready
    };
    assert!(until(Duration::from_secs(5), caught_up), "{}", one.said());
    let copied = one.followers();
    let follower = &copied[0];
    assert_eq!(follower.node.as_deref(), Some(second_node.as_str()));
    assert!(follower.entries > 0, "a whole copy's entries: {follower:?}");
    assert!(follower.bytes > 0, "{follower:?}");
    assert!(
        one.said()
            .contains(&format!("standbys {second_node} from 127.0.0.1:")),
        "by its node, its address as evidence: {}",
        one.said()
    );

    for number in 20..25 {
        client
            .write_journey(&journey(number, b"later"))
            .expect("written");
    }
    // Each write the primary's engines took is one handed on, however its
    // committer grouped the Journeys into them.
    let tail = || {
        one.followers()
            .first()
            .is_some_and(|after| after.writes > follower.writes && after.behind == 0)
    };
    assert!(until(Duration::from_secs(5), tail), "{}", one.said());
    let after = &one.followers()[0];
    assert!(after.bytes > follower.bytes, "{after:?} after {follower:?}");
    assert_eq!(after.entries, follower.entries, "no second whole copy");
    late.stop();
    late_server.stop();
    one.stop();
    one_server.stop();
}
