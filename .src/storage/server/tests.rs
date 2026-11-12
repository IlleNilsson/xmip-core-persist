//! A Storage node's server stopped while a node keeps asking it.

use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use secret::{Held, KekName};
use xcore::JourneyId;

use super::StorageServer;
use crate::fixture::Memory;
use crate::storage::authority::Authority;
use crate::storage::client::{PASS_OVER, TIMEOUT};
use crate::storage::{Embedded, StorageClient, XmipStorage};

/// What bounds the test, never Xmip: a stop that waits for the node asking
/// waits for as long as it asks.
const BOUND: Duration = Duration::from_secs(5);

/// How many answers the node has before the server is stopped: it asks on
/// one connection, kept between its questions.
const ANSWERED: usize = 8;

/// A Storage node's server over Xmip Storage in memory, and a node reaching
/// it alone.
fn served() -> (StorageServer, StorageClient) {
    let keys = Held::new(secret::fixture::Memory::default());
    let kek = KekName::new("storage").expect("a name");
    let storage: Arc<dyn XmipStorage> = Arc::new(
        Embedded::open(
            Memory::default(),
            Memory::default(),
            Memory::default(),
            &keys,
            &kek,
        )
        .expect("opened"),
    );
    let authority = Authority::new();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bound");
    let server =
        StorageServer::start(storage, listener, &authority.issue(&authority)).expect("served");
    let node = StorageClient::new(
        &[server.address().to_string()],
        &authority.issue(&authority),
        TIMEOUT,
        PASS_OVER,
    )
    .expect("a node");
    (server, node)
}

/// A server looked whether it is stopping only once a connection had been
/// idle a while, so a node asking on its kept connection again as soon as
/// it was answered — one asking a primary that refuses it as it stops —
/// held the stop for as long as it asked: the root's failover test saw a
/// primary still draining ten seconds after its stop (2026-11-12). Each
/// connection now ends at its next read once its server is stopping.
#[test]
fn a_stop_ends_the_connection_a_node_keeps_asking_on() {
    let (server, node) = served();
    let asking = Arc::new(AtomicBool::new(true));
    let (told, answered) = mpsc::channel();
    let questions = {
        let asking = Arc::clone(&asking);
        std::thread::spawn(move || {
            let mut count = 0;
            while asking.load(Ordering::SeqCst) && node.read_journey(JourneyId::new(1)).is_ok() {
                count += 1;
                if count == ANSWERED {
                    let _ = told.send(());
                }
            }
        })
    };
    answered
        .recv_timeout(BOUND)
        .expect("the node is answered on its connection");

    let (stopped, stop) = mpsc::channel();
    let stopping = std::thread::spawn(move || {
        server.stop();
        let _ = stopped.send(());
    });
    let ended = stop.recv_timeout(BOUND);
    asking.store(false, Ordering::SeqCst);
    questions.join().expect("the node's questions");
    stopping.join().expect("the stop");
    assert!(
        ended.is_ok(),
        "the stop waited for the node that kept asking"
    );
}
