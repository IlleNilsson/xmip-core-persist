//! The acceptance test of a standby's bootstrap (ADR-0056, amendment
//! 2026-11-12): one write lands between two pages of a whole copy, so the
//! pages reflect it in part; replication is cut after the copy's last page,
//! before that write is replayed; both Storage nodes are started again. The
//! standby holding the write in part is never promoted, and holds its
//! primary's one state once caught up.

use super::*;
use crate::encrypted_store::OWN;
use crate::storage::standby::Copied;
use crate::storage::standby::bootstrap::kept;
use crate::{Change, Entry};

/// The write's first key, the first the runtime database keeps, on the
/// copy's first page.
const FIRST: &[u8] = b"\0\0first";

/// The write's other key, after every key the copy's first page holds.
const LATER: &[u8] = b"\xfelater";

fn change(key: &[u8], value: &[u8]) -> Change {
    (key.to_vec(), Some(value.to_vec()))
}

/// Every entry `engine` keeps but a store's own: what a standby copies.
fn copied(engine: &dyn Engine) -> Vec<Entry> {
    let everything = engine
        .scan(&[], &[0xff; 1024], usize::MAX, false)
        .expect("scanned");
    everything
        .into_iter()
        .filter(|(key, _)| !key.starts_with(OWN))
        .collect()
}

#[test]
fn a_write_a_whole_copy_reflects_in_part_never_becomes_the_ledger_across_a_restart() {
    let authority = Authority::new();
    let (bound, listed) = listeners(2);
    let paged = Arc::new(Hooks::default());
    let mut first = Own::new();
    first.engines[0] = Arc::new(Hooked::new(Memory::default(), &paged));
    let [(one, one_server), (two, two_server)] =
        both_started([&first, &Own::new()], (bound, &listed), &authority);
    assert!(one.is_primary(), "{}", one.said());
    two.stop();
    two_server.stop();

    // The primary's runtime database, numbered as its own writes are, past
    // the copy's first page; then one write of both keys let in between the
    // copy's first two pages.
    let written = Arc::new(Copied::new(
        Arc::clone(&first.engines[0]),
        0,
        Arc::clone(&one.copies),
    ));
    let mut filled = vec![change(FIRST, b"before"), change(LATER, b"before")];
    for number in 0..300_u16 {
        let [high, low] = number.to_be_bytes();
        filled.push(change(&[0x10, high, low], b"filler"));
    }
    written.apply(&filled).expect("written");
    let between = Arc::clone(&written);
    paged.between_pages(move || {
        let both = [change(FIRST, b"after"), change(LATER, b"after")];
        between.apply(&both).expect("written between the pages");
    });

    // A standby started empty and copied whole, whose disk refuses that
    // write's replay: replication cut after the copy's last page.
    let refusing = Arc::new(Hooks::default());
    let mut third = Own::new();
    third.engines[0] = Arc::new(Hooked::new(Memory::default(), &refusing));
    refusing.refuse_deferred(Some(change(FIRST, b"after")));
    let listener = TcpListener::bind(&listed[1]).expect("bound again");
    let (late, late_server) = started(&third, (listener, &listed), &authority);
    let catching_up = || matches!(late.bootstrap(), Bootstrap::CatchingUp { .. });
    assert!(
        until(Duration::from_secs(5), catching_up),
        "{}",
        late.said()
    );
    std::thread::sleep(timings().silence * 2);
    assert!(
        catching_up(),
        "never ready short of the write: {}",
        late.said()
    );
    assert!(late.said().contains("catching up"), "{}", late.said());
    let held = |key| third.engines[0].read(key).expect("read");
    assert_eq!(held(FIRST), Some(b"before".to_vec()), "the write in part");
    assert_eq!(held(LATER), Some(b"after".to_vec()), "the write in part");

    // Both started again: the standby holds more than the former primary,
    // which gave up its positions when promoted, yet is still catching up,
    // and never promoted.
    late.stop();
    late_server.stop();
    one.stop();
    one_server.stop();
    let kept_bootstrap = kept(third.engines[0].as_ref()).expect("read");
    assert!(matches!(kept_bootstrap, Bootstrap::CatchingUp { .. }));
    refusing.refuse_deferred(None);
    let bound: Vec<TcpListener> = listed
        .iter()
        .map(|at| TcpListener::bind(at).expect("bound again"))
        .collect();
    let [(one, one_server), (late, late_server)] =
        both_started([&first, &third], (bound, &listed), &authority);
    assert!(one.is_primary(), "{} / {}", one.said(), late.said());
    assert!(!late.is_primary(), "{}", late.said());
    let caught_up =
        || late.bootstrap() == Bootstrap::Ready && late.standing().next == one.standing().next;
    assert!(
        until(Duration::from_secs(5), caught_up),
        "{} / {}",
        one.said(),
        late.said()
    );
    assert!(one.is_primary() && !late.is_primary(), "{}", late.said());
    assert_eq!(held(FIRST), Some(b"after".to_vec()), "one state");
    assert_eq!(held(LATER), Some(b"after".to_vec()), "one state");
    for (primary, standby) in first.engines.iter().zip(&third.engines) {
        assert_eq!(copied(primary.as_ref()), copied(standby.as_ref()));
    }
    late.stop();
    late_server.stop();
    one.stop();
    one_server.stop();
}
