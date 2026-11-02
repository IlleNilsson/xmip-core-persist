//! The module's timing program (`doc/governance/rust-style.md` section
//! 5c): Xmip Storage's reads held to the millisecond rule. A plain `cargo
//! test` leaves it out; the landing runs it after every other test of the
//! module, alone, one test at a time (`cargo test --test measured
//! --all-features -- --test-threads=1`), so what is measured is Xmip's work
//! and not its neighbors':
//!
//! - a read of what is due among many held takes under a millisecond, over
//!   the crowded node `storage::due`'s tests read (`fixture::crowd`);
//! - a page across a gap of a million places, and the writer's removal
//!   before it, each take under a millisecond (`storage::page_across_a_gap`).

use std::time::{Duration, Instant};

use xmip_core_persist::fixture::crowd::{QUEUE, crowded};
use xmip_core_persist::storage::{DuePart, XmipStorage, page_across_a_gap};

#[test]
fn a_read_of_what_is_due_among_many_held_takes_under_a_millisecond() {
    let holder = configure::fixture::test_cluster().node_scope(0);
    let node = crowded(&holder, 10_000, 1_000);
    let fastest = (0..5)
        .map(|_| {
            let started = Instant::now();
            let read = node
                .read_due(QUEUE, DuePart::Due, (0, 0), 64)
                .expect("read");
            assert_eq!(read.held.len(), 3);
            started.elapsed()
        })
        .min()
        .unwrap_or_default();
    eprintln!("three due of 11,003 held: read in {fastest:?}");
    assert!(fastest < Duration::from_millis(1), "{fastest:?}");
}

#[test]
fn a_page_across_a_gap_and_the_writer_taking_out_before_it_each_take_under_a_millisecond() {
    let (paged, taken) = page_across_a_gap();
    eprintln!("a gap of a million places: a page {paged:?}, a removal decided {taken:?}");
    let millisecond = Duration::from_millis(1);
    assert!(paged < millisecond, "a page took {paged:?}");
    assert!(taken < millisecond, "a removal took {taken:?}");
}
