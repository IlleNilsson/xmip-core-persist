//! The writer attacked where the disk is: a sync held while a request is
//! asked again, a sync that fails, and a batch that panics once written.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use secret::{Held, KekName};
use xcore::{Clock, JourneyId, SystemClock};

use super::{Committer, Done, Op};
use crate::fixture::{Hooked, Hooks, Memory};
use crate::storage::columns::Columns;
use crate::storage::record::Claim;
use crate::storage::schema::Database;
use crate::{EncryptedStore, PersistError};

type Store = EncryptedStore<Hooked<Arc<Memory>>>;
type Keys = Held<secret::fixture::Memory>;

/// A writer over a store in `memory` its `hooks` steer, sealed under `keys`.
fn writer(memory: &Arc<Memory>, hooks: &Arc<Hooks>, keys: &Keys) -> (Committer, Arc<Store>) {
    let kek = KekName::new("storage").expect("a name");
    let engine = Hooked::new(Arc::clone(memory), hooks);
    let store = Arc::new(EncryptedStore::open(engine, keys, &kek).expect("opened"));
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let columns = Arc::new(Columns::of(Database::Runtime));
    let epoch = Arc::new(AtomicU64::new(1));
    let watch = Arc::default();
    let committer =
        Committer::start(Arc::clone(&store), (clock, columns, epoch, watch)).expect("the writer");
    (committer, store)
}

fn claim(number: u128, token: u128) -> Op {
    Op::Claim {
        claim: Claim {
            journey: JourneyId::new(0x0199_0000_0000_7000_8000_0000_0000_0000 + number),
            holder: configure::fixture::test_cluster().node_scope(0),
            token,
            until_unix_nanos: 0,
            fence: 0,
        },
        lease_nanos: 30_000_000_000,
    }
}

fn keys() -> Keys {
    Held::new(secret::fixture::Memory::default())
}

#[test]
fn a_request_asked_again_while_its_sync_is_held_is_not_answered_durable_before_it() {
    let (memory, hooks, keys) = (Arc::default(), Arc::default(), keys());
    let (committer, _) = writer(&memory, &hooks, &keys);
    hooks.hold_syncs();
    let (taken, first) = committer.ahead(claim(1, 7)).expect("decided");
    assert!(matches!(taken, Done::Claim(Some(_))), "taken");
    assert!(hooks.wait_held(Duration::from_secs(10)), "its sync is held");
    // Asked again under its token: found written, nothing of its own.
    let (again, repeated) = committer.ahead(claim(1, 7)).expect("decided");
    assert!(matches!(again, Done::Claim(Some(_))), "held, as asked");
    assert_eq!(first.told(), None, "the first is not durable yet");
    assert_eq!(repeated.told(), None, "nor is what found it written");
    hooks.release_syncs();
    first.wait().expect("durable");
    repeated.wait().expect("durable with it");
}

#[test]
fn a_sync_that_fails_stops_every_read_and_write_until_reopened_from_its_log() {
    let (memory, hooks, keys) = (Arc::default(), Arc::default(), keys());
    let (committer, store) = writer(&memory, &hooks, &keys);
    committer.submit(claim(1, 7)).expect("written, durable");
    hooks.fail_syncs(true);
    let failed = committer.submit(claim(2, 7));
    assert!(
        matches!(failed.err(), Some(PersistError::Engine { .. })),
        "the sync's failure"
    );
    hooks.fail_syncs(false);
    let refused = committer.submit(claim(3, 7));
    assert!(
        refused.is_err_and(|why| why.to_string().contains("stopped")),
        "no write after a failed sync"
    );
    assert!(store.get("claim", b"any").is_err(), "no read either");
    drop((committer, store));

    // Opened again over the engine: written and read again.
    let (committer, _) = writer(&memory, &hooks, &keys);
    committer
        .submit(claim(4, 7))
        .expect("written after a reopen");
}

#[test]
fn a_batch_that_panics_once_written_stops_the_node_and_tells_it_failed() {
    let (memory, hooks, keys) = (Arc::default(), Arc::default(), keys());
    let (committer, store) = writer(&memory, &hooks, &keys);
    hooks.panic_after_deferred(true);
    assert!(committer.submit(claim(1, 7)).is_err(), "answered, failed");
    hooks.panic_after_deferred(false);
    let refused = committer.submit(claim(2, 7));
    assert!(
        refused.is_err_and(|why| why.to_string().contains("panicked")),
        "the node stopped, and says why"
    );
    assert!(store.get("claim", b"any").is_err(), "nothing read either");
}
