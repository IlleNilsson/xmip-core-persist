//! A standby stopped ends at once (open problem 94): one following its
//! primary, one in the middle of a whole copy waiting for its primary's next
//! page, and one wiping its own databases for a whole copy, each over Xmip's
//! own mutual TLS on loopback; and so does a primary, following or handing
//! on a whole copy. Until 2026-11-22 the first two waited out a read of the
//! primary's next frame — a beat away, or the silence — and the third its
//! whole wipe, which looked at no stop; a node the service manager stopped
//! mid-copy was killed after 90 s. A primary's stop waited out its beat to
//! a standby, and the whole of a copy it was handing on.

use std::sync::mpsc;

use super::*;
use crate::storage::standby::Copied;
use crate::storage::standby::bootstrap::kept;
use crate::storage::standby::follow::LAST;
use crate::{Change, borrowed};

/// A beat and a silence of ten seconds: a read of the primary's next frame
/// waits that long, so a stop held by it is told from one that ends it.
const LONG: Duration = Duration::from_secs(10);

/// How long a stop may take here: an outcome, not a time (the timing
/// program holds the time, `tests/measured/stopped.rs`).
const AT_ONCE: Duration = Duration::from_secs(1);

fn waiting() -> Timings {
    Timings {
        beat: LONG,
        silence: LONG,
        ..timings()
    }
}

/// Two Storage nodes of `listed` over `own`, served by [`waiting`] and both
/// served before either starts: the first is promoted, the second follows.
fn settled(
    own: [&Own; 2],
    (mut bound, listed): (Vec<TcpListener>, &[String]),
    authority: &Authority,
) -> [(Arc<StorageNode>, StorageServer); 2] {
    let second = bound.pop().expect("two");
    let first = bound.pop().expect("one");
    let one = served_by(own[0], (first, listed), authority, waiting());
    let two = served_by(own[1], (second, listed), authority, waiting());
    for (node, _) in [&one, &two] {
        node.start(Duration::from_secs(5)).expect("started");
    }
    assert!(one.0.is_primary(), "{}", one.0.said());
    [one, two]
}

/// How long `node` takes to stop.
fn stop_taken(node: &StorageNode) -> Duration {
    let began = Instant::now();
    node.stop();
    began.elapsed()
}

#[test]
fn a_standby_following_its_primary_ends_at_once_when_stopped() {
    let authority = Authority::new();
    let (bound, listed) = listeners(2);
    let [(one, one_server), (two, two_server)] =
        settled([&Own::new(), &Own::new()], (bound, &listed), &authority);
    let following = || two.bootstrap().is_ready() && one.followers().len() == 1;
    assert!(until(Duration::from_secs(5), following), "{}", two.said());

    let stopped = stop_taken(&two);
    assert!(
        stopped < AT_ONCE,
        "{stopped:?}: a stop waited for the primary's next beat, {LONG:?} apart"
    );
    two_server.stop();
    // Its primary's too, its shipping to the standby gone waiting for none.
    let began = Instant::now();
    one.stop();
    one_server.stop();
    let stopped = began.elapsed();
    assert!(
        stopped < AT_ONCE,
        "{stopped:?}: the primary's shipping waited out its beat, {LONG:?}"
    );
}

#[test]
fn a_standby_in_the_middle_of_a_whole_copy_ends_at_once_when_stopped() {
    let authority = Authority::new();
    let (bound, listed) = listeners(2);
    let paged = Arc::new(Hooks::default());
    let mut first = Own::new();
    first.engines[0] = Arc::new(Hooked::new(Memory::default(), &paged));
    let [(one, one_server), (two, two_server)] =
        settled([&first, &Own::new()], (bound, &listed), &authority);
    two.stop();
    two_server.stop();

    // More than a page of a whole copy's in the primary's runtime database,
    // numbered as its own writes are; and its scan held between its first
    // two pages until the test lets it go on.
    let written = Copied::new(Arc::clone(&first.engines[0]), 0, Arc::clone(&one.copies));
    let filled: Vec<Change> = (0..600_u16)
        .map(|number| (number.to_be_bytes().to_vec(), Some(b"held".to_vec())))
        .collect();
    written.apply(&borrowed(&filled)).expect("written");
    let (paging, paused) = mpsc::channel();
    let (go_on, gone_on) = mpsc::channel::<()>();
    paged.between_pages(move || {
        let _ = paging.send(());
        let _ = gone_on.recv();
    });

    // A standby started empty, copied whole: its first page in, waiting for
    // the next.
    let listener = TcpListener::bind(&listed[1]).expect("bound again");
    let (late, late_server) = served_by(&Own::new(), (listener, &listed), &authority, waiting());
    late.start(Duration::from_secs(5)).expect("started");
    paused
        .recv_timeout(Duration::from_secs(5))
        .expect("the primary between two pages");
    assert_eq!(late.bootstrap(), Bootstrap::Copying, "{}", late.said());

    let stopped = stop_taken(&late);
    assert!(
        stopped < AT_ONCE,
        "{stopped:?}: a stop waited for the primary's next page, its silence {LONG:?}"
    );
    drop(go_on);
    late_server.stop();
    one.stop();
    one_server.stop();
}

#[test]
fn a_primary_handing_on_a_whole_copy_ends_at_once_when_stopped_and_its_standby_stays_copying() {
    let authority = Authority::new();
    let (bound, listed) = listeners(2);
    let paged = Arc::new(Hooks::default());
    let mut first = Own::new();
    first.engines[0] = Arc::new(Hooked::new(Memory::default(), &paged));
    let [(one, one_server), (two, two_server)] =
        settled([&first, &Own::new()], (bound, &listed), &authority);
    two.stop();
    two_server.stop();
    let written = Copied::new(Arc::clone(&first.engines[0]), 0, Arc::clone(&one.copies));
    let filled: Vec<Change> = (0..600_u16)
        .map(|number| (number.to_be_bytes().to_vec(), Some(b"held".to_vec())))
        .collect();
    written.apply(&borrowed(&filled)).expect("written");

    // Stopped as its copy's second page is read: no page after it goes.
    let (paging, paused) = mpsc::channel();
    let node = Arc::clone(&one);
    paged.between_pages(move || {
        let _ = paging.send(());
        let deadline = Instant::now() + Duration::from_secs(5);
        while !node.stopping() && Instant::now() < deadline {
            std::thread::yield_now();
        }
    });
    let listener = TcpListener::bind(&listed[1]).expect("bound again");
    let (late, late_server) = served_by(&Own::new(), (listener, &listed), &authority, waiting());
    late.start(Duration::from_secs(5)).expect("started");
    paused
        .recv_timeout(Duration::from_secs(5))
        .expect("the primary between two pages");
    let began = Instant::now();
    one.stop();
    one_server.stop();
    let stopped = began.elapsed();
    assert!(
        stopped < AT_ONCE,
        "{stopped:?}: the primary's stop waited for the copy it handed on"
    );
    assert!(
        until(Duration::from_secs(5), || late.is_finding()),
        "{}",
        late.said()
    );
    assert_eq!(
        late.bootstrap(),
        Bootstrap::Copying,
        "its copy cut short, never ready: {}",
        late.said()
    );
    assert!(!late.is_primary(), "{}", late.said());
    late.stop();
    late_server.stop();
}

#[test]
fn a_standby_wiping_its_databases_for_a_whole_copy_ends_at_once_when_stopped() {
    let authority = Authority::new();
    let (mut bound, listed) = listeners(2);
    let second = bound.pop().expect("two");
    let first = bound.pop().expect("one");
    let (one, one_server) = served_by(&Own::new(), (first, &listed), &authority, waiting());

    // A standby holding four pages of its wipe's entries, in no history's
    // numbering: copied whole, its own entries wiped first.
    let wiping = Arc::new(Hooks::default());
    let held = Memory::default();
    let entries: Vec<Change> = (0..4_000_u16)
        .map(|number| (number.to_be_bytes().to_vec(), Some(b"held".to_vec())))
        .collect();
    held.apply(&borrowed(&entries)).expect("written");
    let mut own = Own::new();
    own.engines[0] = Arc::new(Hooked::new(held, &wiping));
    let (two, two_server) = served_by(&own, (second, &listed), &authority, waiting());

    // Stopped as the wipe's second page is read, by its service manager on
    // a thread of its own: that page is its last.
    let (tell, told) = mpsc::channel();
    let node = Arc::clone(&two);
    wiping.between_pages(move || {
        let asked = Arc::clone(&node);
        std::thread::spawn(move || {
            let _ = tell.send(stop_taken(&asked));
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        while !node.stopping() && Instant::now() < deadline {
            std::thread::yield_now();
        }
    });
    for node in [&one, &two] {
        node.start(Duration::from_secs(5)).expect("started");
    }
    let stopped = told
        .recv_timeout(Duration::from_secs(5))
        .expect("the wipe went on from the last key it removed, and was stopped there");
    assert!(stopped < AT_ONCE, "{stopped:?}: the wipe looked at no stop");
    let left = own.engines[0]
        .scan(&[], &LAST, usize::MAX, false)
        .expect("scanned");
    assert!(left.len() > 1_900, "the wipe went on: {} left", left.len());
    assert_eq!(
        kept(own.engines[0].as_ref()).expect("read"),
        Bootstrap::Copying,
        "copied whole again at its next start"
    );
    two_server.stop();
    one.stop();
    one_server.stop();
}
