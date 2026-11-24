//! What a switchover costs beyond its own frames (open problems 140 to 142;
//! ADR-0056, amendment 2026-11-24), over Xmip's own mutual TLS on loopback:
//! a standby following an idle primary on is handed a frame at once, not a
//! beat later; a successor syncs each database once and makes one durable
//! write, its epoch, before it serves; and a node keeps as many connections
//! to a standby as it holds to the primary, so no thread of it opens one
//! as the standby succeeds.

use std::sync::atomic::{AtomicBool, Ordering};

use super::*;
use crate::storage::standby::DATABASES;
use crate::storage::{Position, Waiter};

/// A beat and a silence of ten seconds: what waits for a beat is told from
/// what does not.
fn long() -> Timings {
    Timings {
        beat: Duration::from_secs(10),
        silence: Duration::from_secs(10),
        ..timings()
    }
}

#[test]
fn a_standby_following_an_idle_primary_on_is_handed_a_frame_at_once() {
    let authority = Authority::new();
    let (bound, listed) = listeners(2);
    let second = Own::new();
    let [(one, one_server), (two, two_server)] = settled(
        [&Own::new(), &second],
        (bound, &listed),
        (&authority, long()),
        5,
    );
    // What it was handed so far, counted on across its connections.
    let copied = one.followers()[0].entries;
    two.stop();
    two_server.stop();

    // Back, it follows on from where it is, and its primary has nothing to
    // hand it: ready only once it is handed a frame, and its node told then,
    // so it publishes at once.
    let listener = TcpListener::bind(&listed[1]).expect("bound again");
    let (back, back_server) = served_by(&second, (listener, &listed), &authority, long());
    let told = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let telling = Arc::clone(&told);
    back.telling(move || {
        telling.fetch_add(1, Ordering::SeqCst);
    });
    back.start(Duration::from_secs(5)).expect("started");
    assert!(
        until(Duration::from_secs(1), || following(&back)),
        "handed nothing for a beat: {}",
        back.said()
    );
    let both = || told.load(Ordering::SeqCst) >= 2;
    assert!(
        until(Duration::from_secs(1), both),
        "told {} time(s), where standing by and handed a frame are two",
        told.load(Ordering::SeqCst)
    );
    let handed = one.followers();
    assert_eq!(handed[0].entries, copied, "followed on, not copied whole");
    for (node, server) in [(back, back_server), (one, one_server)] {
        node.stop();
        server.stop();
    }
}

#[test]
fn a_successor_syncs_each_database_once_and_makes_one_durable_write_before_it_serves() {
    let authority = Authority::new();
    let (bound, listed) = listeners(2);
    let disk = Arc::new(Hooks::default());
    let mut second = Own::new();
    second.engines =
        [0, 1, 2].map(|_| Arc::new(Hooked::new(Memory::default(), &disk)) as Arc<dyn Engine>);
    let [(one, one_server), (two, two_server)] = settled(
        [&Own::new(), &second],
        (bound, &listed),
        (&authority, long()),
        20,
    );

    let (syncs, writes) = disk.counted();
    one.stop();
    one_server.stop();
    assert!(
        until(Duration::from_secs(5), || two.is_primary()),
        "{}",
        two.said()
    );
    let (synced, written) = disk.counted();
    assert_eq!(written - writes, 1, "durable writes but its epoch's");
    assert!(
        synced - syncs <= DATABASES,
        "{} syncs of {DATABASES} databases",
        synced - syncs
    );
    two.stop();
    two_server.stop();
}

#[test]
fn a_node_keeps_as_many_connections_to_a_standby_as_it_holds_to_the_primary() {
    let authority = Authority::new();
    let (bound, listed) = listeners(2);
    let second = Own::new();
    let [(one, one_server), (two, two_server)] = settled(
        [&Own::new(), &second],
        (bound, &listed),
        (&authority, timings()),
        5,
    );
    let client = Arc::new(client(&listed, &authority));
    let going = Arc::new(AtomicBool::new(true));
    // A wait always under way, as every node's membership has, and three
    // threads writing beside it: four connections to the primary at once.
    let waiting = {
        let (client, going) = (Arc::clone(&client), Arc::clone(&going));
        std::thread::spawn(move || {
            let waiter = Arc::new(Waiter::new());
            while going.load(Ordering::SeqCst) {
                let bound = (Duration::from_millis(200), 0);
                let _ = client.wait_queues(&[Position::queue(9, 0)], bound, &waiter);
            }
        })
    };
    let writers: Vec<std::thread::JoinHandle<()>> = (0..3_u128)
        .map(|writer| {
            let (client, going) = (Arc::clone(&client), Arc::clone(&going));
            std::thread::spawn(move || {
                for number in (writer + 1) * 1_000_000.. {
                    if !going.load(Ordering::SeqCst) {
                        break;
                    }
                    let _ = client.write_journey(&journey(number, b"along"));
                }
            })
        })
        .collect();
    std::thread::sleep(timings().silence);

    // The standby back: the node is told it follows, through its wait.
    two.stop();
    two_server.stop();
    let listener = TcpListener::bind(&listed[1]).expect("bound again");
    let (back, back_server) = served_by(&second, (listener, &listed), &authority, timings());
    back.start(Duration::from_secs(5)).expect("started");
    let kept = || client.idle_at(1) >= 3;
    assert!(
        until(Duration::from_secs(5), kept),
        "{} connection(s) kept to the standby",
        client.idle_at(1)
    );
    going.store(false, Ordering::SeqCst);
    waiting.join().expect("waited");
    for writer in writers {
        writer.join().expect("written");
    }
    for (node, server) in [(back, back_server), (one, one_server)] {
        node.stop();
        server.stop();
    }
}
