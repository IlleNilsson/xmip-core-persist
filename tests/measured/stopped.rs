//! A standby stopped ends within a millisecond beyond the two wakes its stop
//! makes — the thread following its primary woken from its read, and the
//! stopping thread from its join — the fastest of five, beside a plain wake
//! measured in the same rounds (`xcore::latency::Rounds`; open problem 94):
//! one following its primary, waiting for a beat ten seconds away, and one in
//! the middle of a whole copy, its primary held between two pages. Until
//! 2026-11-22 each waited out that read, and a standby the service manager
//! stopped while it was copied whole was killed 90 s later. Held where the
//! platform wakes a read blocked on a socket shut from another thread, Linux
//! and macOS; Windows wakes it only as its peer closes, so a standby there
//! ends within the Storage server's watch of 50 ms, the standby tests'
//! outcome (`standby/tests/stopped.rs`), and is not held here. And a primary
//! handing on a whole copy, stopped with its server, ends within a
//! millisecond beyond the six wakes that stop makes, held by its wall clock
//! alone (`woke_beside_a_plain_wake`): its standby, in the same process,
//! answers the primary's going with handshakes of its own. Until that day
//! its stop lasted as long as the copy. The copy is held between its pages
//! until the stop, so the stop always cuts it short, and the standby copied
//! is never handed over to (`succession`, ADR-0056, amendment 2026-11-23).

use std::net::TcpListener;
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use xcore::JourneyId;
#[cfg(unix)]
use xcore::latency::Rounds;
use xcore::latency::{PlainWake, spread, woke_beside_a_plain_wake};
use xmip_core_persist::fixture::{Hooked, Hooks, Memory};
#[cfg(unix)]
use xmip_core_persist::storage::standby::Bootstrap;
use xmip_core_persist::storage::{JourneyFacts, JourneyRecord};

use super::authority::Authority;
use super::nodes::{Own, Served, listeners, timings};

/// How many stops of each are timed.
const STOPS: usize = 25;

/// The threads a stop wakes where a plain wake wakes one: the following
/// thread, from its read, and the stopping thread, from its join.
#[cfg(unix)]
const WOKEN: u32 = 2;

/// The threads a primary's stop with its server's wakes: its settling
/// thread, from its rest, and its server's accepting thread, knocked; and
/// the stopping thread from the end of the copy it waits for, and from each
/// join.
const PRIMARY_WOKEN: u32 = 6;

/// A beat and a silence of ten seconds: what a stop held by a read of the
/// primary's next frame would wait.
const LONG: Duration = Duration::from_secs(10);

/// How long a test waits for what it waits on.
const WITHIN: Duration = Duration::from_secs(10);

/// The Storage node of `listed` at `listener` over `own`, served, its beat
/// and silence [`LONG`].
fn served(own: &Own, at: (TcpListener, &[String]), authority: &Authority) -> Served {
    super::nodes::served(own, at, authority, timings(LONG, LONG))
}

/// Until `holds`, or [`WITHIN`] passed.
#[cfg(unix)]
fn until(holds: impl Fn() -> bool) -> bool {
    let deadline = Instant::now() + WITHIN;
    while Instant::now() < deadline {
        if holds() {
            return true;
        }
        std::thread::yield_now();
    }
    holds()
}

/// A primary over `first`, settled with a standby that is then stopped,
/// holding more than a page of a whole copy's journeys; and the addresses
/// listed, the second free for each standby a round starts.
fn primary(first: &Own, authority: &Authority) -> (Served, Vec<String>) {
    let (listeners, listed) = listeners(2);
    let mut bound = listeners.into_iter();
    let one = served(first, (bound.next().expect("one"), &listed), authority);
    let two = served(
        &Own::new(),
        (bound.next().expect("two"), &listed),
        authority,
    );
    for (node, _) in [&one, &two] {
        node.start(WITHIN).expect("settled");
    }
    assert!(one.0.is_primary(), "{}", one.0.said());
    two.0.stop();
    two.1.stop();
    let storage = one.0.reaching();
    for number in 0..300 {
        let record = JourneyRecord {
            journey: JourneyId::new(0x0199_0000_0000_7000_8000_0000_0000_0000 + number),
            body: vec![b'x'; 64],
            facts: JourneyFacts::default(),
        };
        storage.write_journey(&record).expect("written");
    }
    (one, listed)
}

/// A standby started empty at the second address `listed`, served.
fn standby(listed: &[String], authority: &Authority) -> Served {
    let listener = TcpListener::bind(&listed[1]).expect("bound again");
    let late = served(&Own::new(), (listener, listed), authority);
    late.0.start(WITHIN).expect("started");
    late
}

// Linux and macOS: see the module's words on Windows.
#[cfg(unix)]
#[test]
fn a_standby_following_its_primary_stops_within_a_millisecond_beyond_its_wakes() {
    let authority = Authority::new();
    let ((one, one_server), listed) = primary(&Own::new(), &authority);
    let mut rounds = Rounds::crossing(WOKEN);
    for _ in 0..STOPS {
        // Caught up, ready, and reading for a beat ten seconds away. The
        // primary learns it went only at that beat; its shipping to each
        // round's standby ends as the primary stops.
        let (late, late_server) = standby(&listed, &authority);
        let following = || {
            !late.is_finding()
                && late.bootstrap() == Bootstrap::Ready
                && late.standing().next == one.standing().next
        };
        assert!(until(following), "{}", late.said());
        rounds.round(|| late.stop());
        late_server.stop();
    }
    rounds.hold("a standby following its primary, stopped");
    one.stop();
    one_server.stop();
}

// Linux and macOS: see the module's words on Windows.
#[cfg(unix)]
#[test]
fn a_standby_copied_whole_stops_within_a_millisecond_beyond_its_wakes() {
    let authority = Authority::new();
    let paged = Arc::new(Hooks::default());
    let mut first = Own::new();
    first.engines[0] = Arc::new(Hooked::new(Memory::default(), &paged));
    let ((one, one_server), listed) = primary(&first, &authority);
    let mut rounds = Rounds::crossing(WOKEN);
    for _ in 0..STOPS {
        // The primary's scan held between its first two pages, the
        // standby's first page in and the next waited for.
        let (paging, paused) = mpsc::channel();
        let (go_on, gone_on) = mpsc::channel::<()>();
        paged.between_pages(move || {
            let _ = paging.send(());
            let _ = gone_on.recv();
        });
        let (late, late_server) = standby(&listed, &authority);
        paused.recv_timeout(WITHIN).expect("between two pages");
        assert_eq!(late.bootstrap(), Bootstrap::Copying, "{}", late.said());
        rounds.round(|| late.stop());
        drop(go_on);
        late_server.stop();
        assert!(until(|| one.followers().is_empty()), "{}", one.said());
    }
    rounds.hold("a standby in the middle of a whole copy, stopped");
    one.stop();
    one_server.stop();
}

#[test]
fn a_primary_handing_on_a_whole_copy_stops_within_a_millisecond_beyond_its_wakes() {
    const WHAT: &str = "a primary handing on a whole copy, stopped with its server";
    let authority = Authority::new();
    let plain = PlainWake::start();
    let (mut took, mut woke) = (Vec::new(), Vec::new());
    for _ in 0..STOPS {
        let paged = Arc::new(Hooks::default());
        let mut first = Own::new();
        first.engines[0] = Arc::new(Hooked::new(Memory::default(), &paged));
        let ((one, one_server), listed) = primary(&first, &authority);
        let (paging, paused) = mpsc::channel();
        // Held until the primary stops: it is the primary no more as its
        // stop begins, and the copy's next page is never read.
        let stopping = Arc::clone(&one);
        paged.between_pages(move || {
            let _ = paging.send(());
            while stopping.standing().primary {
                std::hint::spin_loop();
            }
        });
        let (late, late_server) = standby(&listed, &authority);
        paused.recv_timeout(WITHIN).expect("between two pages");
        let began = Instant::now();
        one.stop();
        one_server.stop();
        took.push(began.elapsed());
        woke.push(plain.round().0);
        late.stop();
        late_server.stop();
    }
    let took = spread(WHAT, took).fastest_of_five;
    let usual = spread("a plain wake beside it", woke).fastest_of_five;
    woke_beside_a_plain_wake(WHAT, took, usual, PRIMARY_WOKEN);
}
