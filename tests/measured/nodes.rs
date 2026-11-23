//! Storage nodes of several as the timing program starts them, each over
//! engines and a key store of its own in memory, served over Xmip's own
//! mutual TLS on loopback: written once for each of its programs
//! (`super::stopped`, `super::succession`).

use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use xmip_core_persist::Engine;
use xmip_core_persist::fixture::Memory;
use xmip_core_persist::storage::standby::Timings;
use xmip_core_persist::storage::{StorageNode, StorageServer, UNWAITED, WRITER_KEPT};

use super::authority::Authority;

/// A Storage node and its server.
pub type Served = (Arc<StorageNode>, StorageServer);

/// One Storage node's own: its engines and its key store.
pub struct Own {
    pub engines: [Arc<dyn Engine>; 3],
    pub keys: Arc<dyn secret::KeyStore>,
}

impl Own {
    pub fn new() -> Self {
        Self {
            engines: [0, 1, 2].map(|_| Arc::new(Memory::default()) as Arc<dyn Engine>),
            keys: Arc::new(secret::Held::new(secret::fixture::Memory::default())),
        }
    }
}

/// What bounds a Storage node here, its beat and silence given: a connect
/// and a read of another within two seconds, one that did not answer passed
/// over for 200 ms.
pub fn timings(beat: Duration, silence: Duration) -> Timings {
    Timings {
        beat,
        silence,
        timeout: Duration::from_secs(2),
        pass_over: Duration::from_millis(200),
        kept: 1 << 20,
        unwaited: UNWAITED,
        writer_kept: WRITER_KEPT,
    }
}

/// The Storage node of `listed` at `listener` over `own`, served, bounded
/// by `timings`; it settles nothing until started.
pub fn served(
    own: &Own,
    (listener, listed): (TcpListener, &[String]),
    authority: &Authority,
    timings: Timings,
) -> Served {
    let identity = authority.issue(authority);
    let keys = (
        Arc::clone(&own.keys),
        secret::KekName::new("storage").expect("a name"),
    );
    let node = StorageNode::new(own.engines.clone(), keys, listed, &identity, timings)
        .expect("a Storage node");
    let server = StorageServer::several(&node, listener, &identity).expect("served");
    (node, server)
}

/// `count` loopback listeners, and their addresses as a cluster lists them.
pub fn listeners(count: usize) -> (Vec<TcpListener>, Vec<String>) {
    let listeners: Vec<TcpListener> = (0..count)
        .map(|_| TcpListener::bind("127.0.0.1:0").expect("bound"))
        .collect();
    let listed = listeners
        .iter()
        .map(|listener| listener.local_addr().expect("an address").to_string())
        .collect();
    (listeners, listed)
}
