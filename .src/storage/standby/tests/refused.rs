//! A Storage node refused a promotion: one holding part of a Ledger, and
//! one whose epoch cannot be kept.

use super::*;

#[test]
fn a_standby_whose_whole_copy_was_cut_short_is_never_promoted() {
    let authority = Authority::new();
    let (mut bound, listed) = listeners(1);
    let own = Own::new();
    // What a whole copy cut short between pages leaves: part of a Ledger,
    // no position, and the copy still under way.
    own.engines[0]
        .write(super::super::COPYING, &[1])
        .expect("written");
    own.engines[1]
        .write(b"part", b"of a Ledger")
        .expect("written");
    let (node, server) = started(&own, (bound.pop().expect("one"), &listed), &authority);
    std::thread::sleep(timings().silence * 3);
    assert!(!node.is_primary(), "{}", node.said());
    assert!(node.standing().copying);
    node.stop();
    server.stop();

    // An empty Storage node is a new cluster's, and is promoted.
    let (mut bound, listed) = listeners(1);
    let fresh = Own::new();
    let (node, server) = started(&fresh, (bound.pop().expect("one"), &listed), &authority);
    assert!(
        until(Duration::from_secs(5), || node.is_primary()),
        "{}",
        node.said()
    );
    assert!(!node.standing().copying);
    node.stop();
    server.stop();
}

#[test]
fn a_promotion_whose_epoch_cannot_be_kept_fails_and_takes_no_role() {
    let authority = Authority::new();
    let hooks = Arc::new(Hooks::default());
    let mut own = Own::new();
    own.engines[0] = Arc::new(Hooked::new(Memory::default(), &hooks));
    let keys = (
        Arc::clone(&own.keys),
        KekName::new("storage").expect("a name"),
    );
    let listed = ["127.0.0.1:1".to_string()];
    let identity = authority.issue(&authority);
    let node = StorageNode::new(own.engines.clone(), keys, &listed, &identity, timings())
        .expect("a Storage node");
    hooks.fail_writes(true);
    assert!(node.promote(1).is_err(), "the epoch was not kept");
    assert!(!node.is_primary(), "no role taken in an epoch not kept");
    assert_eq!(node.standing().epoch, 0, "the epoch held is as it was");
    assert_eq!(
        own.engines[0].read(super::super::EPOCH).expect("read"),
        None
    );
    hooks.fail_writes(false);
    node.promote(1).expect("promoted once its epoch is kept");
    assert!(node.is_primary());
    assert_eq!(
        own.engines[0].read(super::super::EPOCH).expect("read"),
        Some(1_u64.to_be_bytes().to_vec()),
        "kept before the role was taken"
    );
}
