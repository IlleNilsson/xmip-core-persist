//! A rolling restart, the way every upgrade goes (open problem 92): the two
//! Storage nodes restarted one after the other, the second while the first
//! is still copied whole — by the primary stopping in order, or one whose
//! next page does not read, as a kill cuts a copy — or follows, ready, a
//! node writing all along. Either order ends with one primary, writes
//! accepted, and every Journey a Storage node answered as written read back;
//! and a node turning ready as its peer is promoted is never a second
//! primary of the epoch (ADR-0056, amendment 2026-11-22). A primary stopped
//! in order hands over to its standby (ADR-0056, amendment 2026-11-23), so
//! the first restarted is copied whole on its return only where what it
//! lacks is no longer kept: written past it while it is gone
//! (`succession.rs` holds the hand-over itself).

use std::sync::Mutex;
use std::sync::mpsc::{self, Receiver, Sender};
use xcore::lock::Locked;

use super::*;

/// What the second restart meets in the Storage node restarted first.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Meets {
    /// Its whole copy under way as its primary stops in order.
    Copying,
    /// Its whole copy under way as its primary stops, the next page not
    /// read: the copy cut short as a kill cuts it.
    CutShort,
    /// Following its primary, ready.
    Following,
}

/// The next whole copy read from `hooks`' engine held between two of its
/// pages: `entered` told once it is held, and on once `go` is told.
fn held_between_pages(hooks: &Arc<Hooks>, entered: Sender<()>, go: Receiver<()>) {
    let again = Arc::clone(hooks);
    hooks.between_pages(move || {
        // A whole copy is read on the connection its standby asked on.
        if std::thread::current().name() != Some("xmip-storage-connection") {
            return held_between_pages(&again, entered, go);
        }
        let _ = entered.send(());
        let _ = go.recv();
    });
}

type Running = Option<(Arc<StorageNode>, StorageServer)>;

/// The Storage node running at `at`.
fn running(nodes: &[Running; 2], at: usize) -> Arc<StorageNode> {
    Arc::clone(&nodes[at].as_ref().expect("running").0)
}

fn written(client: &StorageClient, numbers: std::ops::Range<u128>, body: &[u8]) {
    for number in numbers {
        client
            .write_journey(&journey(number, body))
            .expect("written");
    }
}

/// A node writing all along on a client of its own, from Journey 10 000 on,
/// each write through `gate`, until `stop` is told; what it was answered as
/// written, in the order it wrote it.
fn writing(
    (listed, authority): (&[String], &Authority),
    (stop, gate): (Receiver<()>, Arc<Mutex<()>>),
) -> std::thread::JoinHandle<Vec<u128>> {
    let client = client(listed, authority);
    std::thread::spawn(move || {
        let mut answered = Vec::new();
        for number in 10_000.. {
            if stop.try_recv().is_ok() {
                break;
            }
            let _open = gate.locked();
            if client.write_journey(&journey(number, b"along")).is_ok() {
                answered.push(number);
            }
        }
        answered
    })
}

/// Both Storage nodes started, the first listed the primary; the one at
/// `first` restarted, then the other while the first `meets` what it does.
fn rolled(first: usize, meets: Meets) {
    let authority = Authority::new();
    let (bound, listed) = listeners(2);
    let hooks = Arc::new(Hooks::default());
    let mut owns = [Own::new(), Own::new()];
    // The one the first restarted is copied from: the other.
    owns[1 - first].engines[0] = Arc::new(Hooked::new(Memory::default(), &hooks));
    let [one, two] = both_started([&owns[0], &owns[1]], (bound, &listed), &authority);
    assert!(one.0.is_primary(), "{} / {}", one.0.said(), two.0.said());
    let mut nodes: [Running; 2] = [Some(one), Some(two)];
    let client = client(&listed, &authority);
    written(&client, 0..300, b"before");
    let (stop_writing, gate) = (mpsc::channel(), Arc::new(Mutex::new(())));
    let along = writing((&listed, &authority), (stop_writing.1, Arc::clone(&gate)));

    // The first restarted, the primary handing over where it is the one.
    // A standby is copied whole where its primary no longer keeps what it
    // lacks: written past it while it is gone.
    let (node, server) = nodes[first].take().expect("running");
    node.stop();
    server.stop();
    let remaining = running(&nodes, 1 - first);
    let primary = || remaining.is_primary();
    assert!(
        until(Duration::from_secs(5), primary),
        "{}",
        remaining.said()
    );
    let mut away = 0..0;
    if meets != Meets::Following {
        away = 300..500;
        written(&client, away.clone(), &[7; 8 * 1024]);
    }
    let (entered, go) = (mpsc::channel(), mpsc::channel());
    // The node writes nothing until the copy is held: only the copy's pages
    // are read on a connection meanwhile.
    let closed = (meets != Meets::Following).then(|| gate.locked());
    if closed.is_some() {
        held_between_pages(&hooks, entered.0, go.1);
    }
    nodes[first] = Some(again(&owns[first], (&listed, first), &authority));
    let (restarted, other) = (running(&nodes, first), running(&nodes, 1 - first));
    if meets == Meets::Following {
        let held = other.standing().held();
        let ready = || {
            restarted.bootstrap() == Bootstrap::Ready
                && !restarted.is_finding()
                && restarted.standing().held() >= held
        };
        assert!(until(Duration::from_secs(5), ready), "{}", restarted.said());
    } else {
        let held = entered.1.recv_timeout(Duration::from_secs(5));
        assert!(held.is_ok(), "never copied whole: {}", restarted.said());
        // The copy's start is sent and its primary held before the next
        // page, so the copy cannot end; the standby turns copying as its own
        // thread takes that start, which may come after the hold.
        let copying = || restarted.bootstrap() == Bootstrap::Copying;
        assert!(
            until(Duration::from_secs(5), copying),
            "{}",
            restarted.said()
        );
    }
    drop(closed);
    assert!(
        other.is_primary(),
        "{} / {}",
        other.said(),
        restarted.said()
    );

    // The other restarted while it is: its stop in order, the copy under
    // way's next page not read where a kill cuts it.
    let (node, server) = nodes[1 - first].take().expect("running");
    node.stop();
    if meets == Meets::CutShort {
        hooks.fail_scans(true);
    }
    let _ = go.0.send(());
    server.stop();
    hooks.fail_scans(false);
    nodes[1 - first] = Some(again(&owns[1 - first], (&listed, 1 - first), &authority));
    let (one, two) = (running(&nodes, 0), running(&nodes, 1));

    // One primary, the other a standby of it, and writes accepted.
    let settled = || one.is_primary() != two.is_primary() && !one.is_finding() && !two.is_finding();
    assert!(
        until(Duration::from_secs(10), settled),
        "{} / {}",
        one.said(),
        two.said()
    );
    let accepted = || client.write_journey(&journey(900, b"after")).is_ok();
    assert!(
        until(Duration::from_secs(5), accepted),
        "{} / {}",
        one.said(),
        two.said()
    );
    // Nothing a Storage node answered as written is lost: each was stopped
    // in order, handing on everything it wrote.
    let _ = stop_writing.0.send(());
    let answered = along.join().expect("written along");
    for number in (0..300).chain(away).chain(answered) {
        let read = client
            .read_journey(journey(number, b"").journey)
            .expect("read");
        assert!(read.is_some(), "{number}: {} / {}", one.said(), two.said());
    }
    for (node, server) in nodes.into_iter().flatten() {
        node.stop();
        server.stop();
    }
}

#[test]
fn the_primary_restarted_then_its_standby_while_it_is_copied_whole_leave_one_primary() {
    rolled(0, Meets::Copying);
}

#[test]
fn the_primary_restarted_then_its_standby_cutting_its_whole_copy_short_leave_one_primary() {
    rolled(0, Meets::CutShort);
}

#[test]
fn the_primary_restarted_then_its_standby_while_it_follows_leave_one_primary() {
    rolled(0, Meets::Following);
}

#[test]
fn the_standby_restarted_then_its_primary_while_it_is_copied_whole_leave_one_primary() {
    rolled(1, Meets::Copying);
}

#[test]
fn the_standby_restarted_then_its_primary_cutting_its_whole_copy_short_leave_one_primary() {
    rolled(1, Meets::CutShort);
}

#[test]
fn the_standby_restarted_then_its_primary_while_it_follows_leave_one_primary() {
    rolled(1, Meets::Following);
}

/// The race the primary restarted second meets: the other turns ready, its
/// catch-up done, just as it comes back. The one back asks while the other
/// is not ready, so it is the one promoted as it sees it; the other, ready
/// and holding more, asks before the one back kept the epoch it is promoted
/// in, so the other is the one promoted as it sees it. Each takes its turn
/// on what it saw, and one is the primary of the epoch, never both.
#[test]
fn one_turning_ready_as_its_peer_is_promoted_leaves_one_primary_of_the_epoch() {
    let authority = Authority::new();
    let (mut bound, listed) = listeners(2);
    let owns = [Own::new(), Own::new()];
    let second = bound.pop().expect("two");
    let first = bound.pop().expect("one");
    // Neither settles on its own: the test takes their turns, in the race's
    // order.
    let (back, back_server) = served(&owns[0], (first, &listed), &authority);
    let (other, other_server) = served(&owns[1], (second, &listed), &authority);
    // Back as a former primary is, holding no history's writes; the other
    // catching up on a whole copy that holds more.
    other.copies.restart(7, [40, 1, 1]);
    let catching_up = Bootstrap::CatchingUp {
        epoch: 0,
        through: [40, 1, 1],
    };
    other.bootstrapped(catching_up, &[]).expect("kept");
    let back_saw = back.standings();
    other.bootstrapped(Bootstrap::Ready, &[]).expect("kept");
    let other_saw = other.standings();
    assert!(back.promoting(&back_saw, 0), "{}", back.said());
    assert!(other.promoting(&other_saw, 0), "{}", other.said());
    let primaries = [&back, &other]
        .iter()
        .filter(|node| node.is_primary())
        .count();
    assert_eq!(primaries, 1, "{} / {}", back.said(), other.said());
    for (node, server) in [(back, back_server), (other, other_server)] {
        node.stop();
        server.stop();
    }
}
