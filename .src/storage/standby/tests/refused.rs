//! A Storage node refused a promotion: one started again copying or
//! catching up, holding what may be part of a Ledger, and one whose epoch
//! cannot be kept.

use super::*;
use crate::storage::standby::bootstrap::BOOTSTRAP;
use codec::form::Form;

#[test]
fn a_standby_started_again_copying_or_catching_up_is_never_promoted_alone() {
    let authority = Authority::new();
    let catching_up = Bootstrap::CatchingUp {
        epoch: 1,
        through: [9, 1, 1],
    };
    for bootstrap in [Bootstrap::Copying, catching_up] {
        let (mut bound, listed) = listeners(1);
        let own = Own::new();
        // What a whole copy cut short leaves, or one whose pages are in and
        // whose catch-up is not: what may be part of a Ledger.
        own.engines[0]
            .write(BOOTSTRAP, &bootstrap.bytes())
            .expect("written");
        own.engines[1]
            .write(b"part", b"of a Ledger")
            .expect("written");
        let (node, server) = started(&own, (bound.pop().expect("one"), &listed), &authority);
        std::thread::sleep(timings().silence * 3);
        assert!(!node.is_primary(), "{}", node.said());
        assert_eq!(node.standing().bootstrap, bootstrap);
        let said = node.said();
        assert!(said.contains("not promoted until ready"), "{said}");
        let refused = node.promote(node.standing().epoch + 1);
        let Err(crate::PersistError::Failed { reason }) = refused else {
            panic!("promoted: {refused:?}");
        };
        assert!(reason.contains(bootstrap.said([1; 3]).as_str()), "{reason}");
        assert!(!node.is_primary());
        node.stop();
        server.stop();
    }

    // An empty Storage node is a new cluster's, and is promoted.
    let (mut bound, listed) = listeners(1);
    let fresh = Own::new();
    let (node, server) = started(&fresh, (bound.pop().expect("one"), &listed), &authority);
    assert!(
        until(Duration::from_secs(5), || node.is_primary()),
        "{}",
        node.said()
    );
    assert_eq!(node.standing().bootstrap, Bootstrap::Ready);
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
