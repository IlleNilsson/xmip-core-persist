//! A standby serves no operation: asked one, it says it is a standby and
//! whose, while its node's own work reaches its primary over the wire.

use super::*;

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
