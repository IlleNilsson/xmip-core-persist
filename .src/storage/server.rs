//! A Storage node serving Xmip Storage: every connection mutual TLS
//! through Xmip's own TLS (ADR-0063, amendment 2026-10-01), every request
//! answered by the operations of the databases behind it, a thread per
//! connection, synchronous, with no async runtime.
//!
//! The protocol inside the TLS is agreed in its handshake by its own
//! identifier, [`ALPN`] (RFC 7301), so a client that speaks something else
//! is refused before it says anything.
//!
//! **Every request is admitted before it is answered** (`authorize::plane`,
//! [`RULES`]): a certificate under the anchors proves a caller of the
//! cluster, and only one that authenticates a server — a node's, never a
//! person's, which authenticates a client alone (ADR-0034) — asks Xmip
//! Storage anything, the secrets' operations named apart. A person's
//! certificate, which reaches a node's operate listener, is refused here in
//! words and its connection closed.

use std::io::{ErrorKind, Read};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;
use xcore::lock::Locked;

use authorize::plane;

use super::XmipStorage;
use super::standby::{Standing, StorageNode};
use super::wire::{self, Answer, Request};
use crate::PersistError;
use admitted::{RULES, caller};
use answer::answer;
pub use counted::Peer;
use counted::{Asked, Peers};

/// The application protocol a node and a Storage node agree in the TLS
/// handshake: this wire, its first version.
pub const ALPN: &[u8] = b"xmip-storage/1";

/// How long a stop waits on its own wake-up connection.
const WAKE: Duration = Duration::from_secs(1);

/// How often a connection waiting for its next request looks whether the
/// server is stopping. A request wakes it at once; this bounds only how
/// long a stop waits for an idle connection. A shutdown of the socket would
/// not do it: Windows leaves a blocked receive waiting until the peer
/// closes, and a node keeps its connections open.
const WATCH: Duration = Duration::from_millis(50);

/// The most a write of an answer waits on a node that does not read it.
const WRITE: Duration = Duration::from_secs(30);

/// The threads serving connections, so a stop can wait for each.
mod admitted;
mod answer;
mod counted;

type Served = Arc<Mutex<Vec<JoinHandle<()>>>>;

/// A Storage node's server, running until it is stopped.
pub struct StorageServer {
    address: SocketAddr,
    peers: Arc<Peers>,
    stopping: Arc<AtomicBool>,
    accepting: Option<JoinHandle<()>>,
    served: Served,
}

impl StorageServer {
    /// Serve `storage` to every node that connects to `listener` and
    /// presents a certificate reaching `identity`'s anchors, presenting
    /// `identity`'s own.
    ///
    /// # Errors
    ///
    /// Where `identity` does not make a server configuration, or the
    /// listener's address cannot be read or its thread started.
    pub fn start(
        storage: Arc<dyn XmipStorage>,
        listener: TcpListener,
        identity: &tls::Identity,
    ) -> Result<Self, PersistError> {
        let serving = Serving {
            storage,
            node: None,
            peers: Arc::default(),
        };
        Self::serving(serving, listener, identity)
    }

    /// Serve `node`, one Storage node of several, as [`StorageServer::start`]
    /// serves one: its operations while it is the primary, and to the other
    /// Storage nodes what it is now and, while primary, its writes
    /// (`super::standby`).
    ///
    /// # Errors
    ///
    /// As [`StorageServer::start`].
    pub fn several(
        node: &Arc<StorageNode>,
        listener: TcpListener,
        identity: &tls::Identity,
    ) -> Result<Self, PersistError> {
        let serving = Serving {
            storage: node.served(),
            node: Some(Arc::clone(node)),
            peers: Arc::default(),
        };
        Self::serving(serving, listener, identity)
    }

    fn serving(
        serving: Serving,
        listener: TcpListener,
        identity: &tls::Identity,
    ) -> Result<Self, PersistError> {
        let peers = Arc::clone(&serving.peers);
        let storage = Arc::new(serving);
        let failed = |reason: String| PersistError::engine("xmip-storage", reason);
        let config =
            tls::alpn::selecting(identity.server().map_err(|e| failed(e.message))?, &[ALPN]);
        let config = Arc::new(config);
        let address = listener.local_addr().map_err(|e| failed(e.to_string()))?;
        let stopping = Arc::new(AtomicBool::new(false));
        let served: Served = Arc::default();
        let accepting = {
            let (stopping, served) = (Arc::clone(&stopping), Arc::clone(&served));
            std::thread::Builder::new()
                .name("xmip-storage-accept".to_string())
                .spawn(move || accept(&listener, &storage, &config, &stopping, &served))
                .map_err(|e| failed(e.to_string()))?
        };
        Ok(Self {
            address,
            peers,
            stopping,
            accepting: Some(accepting),
            served,
        })
    }

    /// Where it listens.
    #[must_use]
    pub const fn address(&self) -> SocketAddr {
        self.address
    }

    /// What each node that called it wrote and took on, by the node its
    /// certificate was issued for, each counted from its answer: the
    /// traffic between nodes, measured where it passes (`counted`; ADR-0052,
    /// amendment 2026-11-11).
    #[must_use]
    pub fn peers(&self) -> Vec<Peer> {
        self.peers.counted()
    }

    /// Stop: no connection accepted after, every one served ended, every
    /// thread it started finished.
    pub fn stop(mut self) {
        self.halt();
    }

    fn halt(&mut self) {
        if self.stopping.swap(true, Ordering::SeqCst) {
            return;
        }
        // The accept is woken by a connection of its own, knocked until it
        // has heard (`net::accepting`).
        if let Some(accepting) = self.accepting.take() {
            let address = self.address;
            let _ = net::accepting::woken(accepting, WAKE, || net::accepting::knock(address, WAKE));
        }
        let served = std::mem::take(&mut *self.served.locked());
        for thread in served {
            let _ = thread.join();
        }
    }
}

impl Drop for StorageServer {
    fn drop(&mut self) {
        self.halt();
    }
}

/// What a server serves: Xmip Storage's operations, and the Storage node
/// of several they are, where they are one.
struct Serving {
    storage: Arc<dyn XmipStorage>,
    node: Option<Arc<StorageNode>>,
    /// What each node that called it asked of it (`counted`).
    peers: Arc<Peers>,
}

impl Serving {
    /// What it is now: a Storage node of several says; one alone is the
    /// primary of no epoch, and hands its writes to nobody.
    fn standing(&self) -> Standing {
        self.node.as_ref().map_or(
            Standing {
                process: 0,
                primary: true,
                epoch: 0,
                history: 0,
                next: [1; super::standby::DATABASES],
                copying: false,
            },
            |node| node.standing(),
        )
    }
}

fn accept(
    listener: &TcpListener,
    storage: &Arc<Serving>,
    config: &Arc<tls::ServerConfig>,
    stopping: &Arc<AtomicBool>,
    served: &Served,
) {
    loop {
        // bounded: a listening Storage node waits as long as it runs; a stop wakes it
        let accepted = listener.accept();
        if stopping.load(Ordering::SeqCst) {
            return;
        }
        let Ok((connection, peer)) = accepted else {
            continue;
        };
        let settled = connection
            .set_nodelay(true)
            .and_then(|()| connection.set_read_timeout(Some(WATCH)))
            .and_then(|()| connection.set_write_timeout(Some(WRITE)));
        if settled.is_err() {
            continue;
        }
        let (storage, config) = (Arc::clone(storage), Arc::clone(config));
        let stopping = Arc::clone(stopping);
        let Ok(thread) = std::thread::Builder::new()
            .name("xmip-storage-connection".to_string())
            .spawn(move || serve((connection, peer), &config, &storage, &stopping))
        else {
            continue;
        };
        let mut served = served.locked();
        served.retain(|thread| !thread.is_finished());
        served.push(thread);
    }
}

/// One connection, one request after another, until the node hangs up or
/// the server stops, each admitted by [`RULES`] before it is read further.
/// A standby asking to follow is handed the primary's writes on it until
/// either ends it.
fn serve(
    (connection, peer): (TcpStream, SocketAddr),
    config: &Arc<tls::ServerConfig>,
    serving: &Serving,
    stopping: &AtomicBool,
) {
    let Ok(guarded) = tls::server(connection, Arc::clone(config)) else {
        return;
    };
    let mut watched = Watched { guarded, stopping };
    let mut proven = None;
    while let Ok(Some(request)) = wire::receive::<Request>(&mut watched) {
        // Read once the first request is: the handshake has finished by then,
        // and its counter resolved with it.
        let ((who, standing), counted) = proven.get_or_insert_with(|| {
            let (who, standing) = caller(&watched.guarded);
            let counted = serving.peers.of(who.as_deref(), standing);
            ((who, standing), counted)
        });
        if let Err(refused) = plane::admit(who.as_deref(), *standing, request.operation(), RULES) {
            let _ = wire::send(&mut watched.guarded, &Answer::Failed(refused));
            return;
        }
        let kind = Asked::of(&request);
        let answer = match request {
            Request::Standing => Answer::Standing(serving.standing()),
            Request::Follow(asked) => {
                if let Some(node) = &serving.node {
                    let peer = peer.to_string();
                    node.ship(&mut watched.guarded, &asked, (&peer, stopping));
                    return;
                }
                Answer::Failed(
                    "this Storage node is the only one: it hands its writes to none".into(),
                )
            }
            request => answer(serving.storage.as_ref(), request),
        };
        // Counted from what it was answered, never as it was asked.
        if let Some(counted) = counted {
            counted.count(kind, &answer);
        }
        if wire::send(&mut watched.guarded, &answer).is_err() {
            return;
        }
    }
}

/// A connection read until the server stops: a read that waited [`WATCH`]
/// with nothing come is tried again, where the server is not stopping.
/// Nothing read is lost by it: what TLS has taken off the socket it keeps
/// until it is read.
struct Watched<'a> {
    guarded: tls::GuardedServer,
    stopping: &'a AtomicBool,
}

impl Read for Watched<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        loop {
            match self.guarded.read(buffer) {
                Err(error)
                    if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) =>
                {
                    if self.stopping.load(Ordering::SeqCst) {
                        return Err(ErrorKind::ConnectionAborted.into());
                    }
                }
                read => return read,
            }
        }
    }
}
