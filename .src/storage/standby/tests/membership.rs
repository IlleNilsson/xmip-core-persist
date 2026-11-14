//! The membership among Storage nodes of several (ADR-0056, amendment
//! 2026-11-13): a standby refuses a node its row, the members and a wait on
//! them, as it refuses every operation; its own row reaches the primary;
//! and each finds its place among those listed by the process it answers
//! as, never by a name.

use crate::PersistError;
use crate::storage::{MemberRecord, Position, Waiter, Watched};

use super::*;

/// The node of the test cluster at `place`, a Storage node at `storage`
/// among those listed.
fn row(place: usize, storage: Option<usize>) -> MemberRecord {
    MemberRecord {
        process: u128::try_from(place).expect("a process") + 1,
        node: configure::fixture::test_cluster().node_scope(place),
        roles: "storage".to_string(),
        evidence: "declares storage; offline".to_string(),
        operate: None,
        sync: None,
        storage_place: storage.and_then(|at| u32::try_from(at).ok()),
        save: "sha256:00".to_string(),
        serial: 1,
        started_unix_nanos: 0,
        stopped_unix_nanos: None,
        written: 0,
        latest: false,
    }
}

#[test]
fn a_standby_refuses_the_membership_and_its_own_row_reaches_the_primary_with_its_place() {
    let authority = Authority::new();
    let (bound, listed) = listeners(2);
    let (first, second) = (Own::new(), Own::new());
    let [(one, one_server), (two, two_server)] =
        both_started([&first, &second], (bound, &listed), &authority);
    assert_eq!((one.place(), two.place()), (Some(0), Some(1)));

    let own = row(1, two.place());
    let membership = [Position {
        watched: Watched::Membership,
        at: 0,
    }];
    let waiter = Arc::new(Waiter::new());
    let asked = (Duration::from_secs(60), 0);
    for refused in [
        two.served().write_member(&own).err(),
        two.served().read_members().err(),
        two.served().wait_queues(&membership, asked, &waiter).err(),
    ] {
        assert!(
            matches!(refused, Some(PersistError::Standby { .. })),
            "{refused:?}"
        );
    }
    two.reaching().write_member(&own).expect("its own row");
    let members = one.served().read_members().expect("read at the primary");
    assert_eq!(members.first().and_then(|m| m.storage_place), Some(1));
    two.stop();
    two_server.stop();
    one.stop();
    one_server.stop();
}
