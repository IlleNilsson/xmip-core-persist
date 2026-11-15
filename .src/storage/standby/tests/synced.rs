//! A standby syncing the copies it takes (`follow`): once `SYNC` has passed
//! on the node's clock since it last synced, and never sooner.

use super::*;
use crate::storage::standby::Shipped;
use crate::storage::standby::follow::{SYNC, Taking};
use xcore::Clock;
use xcore::clock::Stepped;

/// The least a stepped clock passes.
const NANOSECOND: Duration = Duration::from_nanos(1);

#[test]
fn a_standby_syncs_its_copies_once_its_sync_span_passed_and_never_sooner() {
    let authority = Authority::new();
    let own = Own::new();
    let keys = (
        Arc::clone(&own.keys),
        KekName::new("storage").expect("a name"),
    );
    let listed = ["127.0.0.1:1".to_string()];
    let clock = Arc::new(Stepped::default());
    let counted = Arc::clone(&clock) as Arc<dyn Clock>;
    let identity = authority.issue(&authority);
    let node = StorageNode::with_clock(
        own.engines.clone(),
        keys,
        &listed,
        &identity,
        timings(),
        counted,
    )
    .expect("a Storage node");
    let synced = || node.copies.since(0, 1, usize::MAX).expect("kept").len();
    let copy = |number: u64| {
        let change = (b"copied".to_vec(), Some(number.to_be_bytes().to_vec()));
        Shipped::Copy(0, number, vec![change])
    };

    let mut taking = Taking::new(clock.monotonic_nanos());
    node.take(copy(1), &mut taking).expect("taken");
    assert_eq!(synced(), 0, "taken, not yet synced");
    clock.pass(SYNC.saturating_sub(NANOSECOND));
    node.take(copy(2), &mut taking).expect("taken");
    assert_eq!(synced(), 0, "a nanosecond short");
    clock.pass(NANOSECOND);
    node.take(copy(3), &mut taking).expect("taken");
    assert_eq!(synced(), 3, "its span passed: every copy so far synced");
    node.take(copy(4), &mut taking).expect("taken");
    assert_eq!(synced(), 3, "counted again from that sync");
}
