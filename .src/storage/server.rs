//! A Storage node serving Xmip Storage: every connection mutual TLS
//! through Xmip's own TLS (ADR-0063, amendment 2026-10-01), every request
//! answered by the operations of the databases behind it, a thread per
//! connection, synchronous, with no async runtime.
//!
//! The protocol inside the TLS is agreed in its handshake by its own
//! identifier, [`ALPN`] (RFC 7301), so a client that speaks something else
//! is refused before it says anything.

use std::io::{ErrorKind, Read};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::JoinHandle;
use std::time::Duration;

use super::XmipStorage;
use super::wire::{self, Answer, Request};
use crate::PersistError;

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
type Served = Arc<Mutex<Vec<JoinHandle<()>>>>;

/// A Storage node's server, running until it is stopped.
pub struct StorageServer {
    address: SocketAddr,
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

    /// Stop: no connection accepted after, every one served ended, every
    /// thread it started finished.
    pub fn stop(mut self) {
        self.halt();
    }

    fn halt(&mut self) {
        if self.stopping.swap(true, Ordering::SeqCst) {
            return;
        }
        // The accept is woken by a connection of its own.
        let _ = net::connect(wakeable(self.address), Some(WAKE));
        if let Some(accepting) = self.accepting.take() {
            let _ = accepting.join();
        }
        let served =
            std::mem::take(&mut *self.served.lock().unwrap_or_else(PoisonError::into_inner));
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

/// An address a connection can reach the listener at: loopback where it
/// listens on every address.
fn wakeable(address: SocketAddr) -> SocketAddr {
    match address.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => (Ipv4Addr::LOCALHOST, address.port()).into(),
        IpAddr::V6(ip) if ip.is_unspecified() => (Ipv6Addr::LOCALHOST, address.port()).into(),
        _ => address,
    }
}

fn accept(
    listener: &TcpListener,
    storage: &Arc<dyn XmipStorage>,
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
        let Ok((connection, _)) = accepted else {
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
            .spawn(move || serve(connection, &config, storage.as_ref(), &stopping))
        else {
            continue;
        };
        let mut served = served.lock().unwrap_or_else(PoisonError::into_inner);
        served.retain(|thread| !thread.is_finished());
        served.push(thread);
    }
}

/// One connection, one request after another, until the node hangs up or
/// the server stops.
fn serve(
    connection: TcpStream,
    config: &Arc<tls::ServerConfig>,
    storage: &dyn XmipStorage,
    stopping: &AtomicBool,
) {
    let Ok(guarded) = tls::server(connection, Arc::clone(config)) else {
        return;
    };
    let mut watched = Watched { guarded, stopping };
    while let Ok(Some(request)) = wire::receive::<Request>(&mut watched) {
        let answer = answer(storage, request);
        if wire::send(&mut watched.guarded, &answer).is_err() {
            return;
        }
    }
}

/// What `storage` answers `request`: the one place an operation on the
/// wire becomes a call of the operations.
pub(crate) fn answer(storage: &dyn XmipStorage, request: Request) -> Answer {
    let answered = match request {
        Request::WriteChunk(chunk) => storage.write_chunk(&chunk).map(|()| Answer::Done),
        Request::ReadChunk(stream, index) => storage.read_chunk(stream, index).map(Answer::Chunk),
        Request::WriteMessage(message) => storage.write_message(&message).map(|()| Answer::Done),
        Request::ReadMessage(id) => storage.read_message(id).map(Answer::Message),
        Request::WriteJourney(journey) => storage.write_journey(&journey).map(|()| Answer::Done),
        Request::ReadJourney(id) => storage.read_journey(id).map(Answer::Journey),
        Request::Claim(claim, lease) => storage
            .claim(
                claim.journey,
                &claim.holder,
                claim.token,
                Duration::from_nanos(lease),
            )
            .map(Answer::Claim),
        Request::Renew(claim, lease) => storage
            .renew(&claim, Duration::from_nanos(lease))
            .map(Answer::Claim),
        Request::Release(claim) => storage.release(&claim).map(Answer::Yes),
        Request::HandOn(hand_on) => storage.hand_on(&hand_on).map(Answer::Yes),
        Request::WriteAudit(entry) => storage.write_audit(&entry).map(|()| Answer::Done),
        Request::KeepAudit(most) => storage.keep_audit(most).map(Answer::Count),
        Request::ReadKeptAudit(id) => storage.read_kept_audit(id).map(Answer::Audit),
        Request::WriteAdministration(record) => {
            storage.write_administration(&record).map(|()| Answer::Done)
        }
        Request::ReadAdministration(kind, id) => storage
            .read_administration(kind, id)
            .map(Answer::Administration),
        Request::RemoveAdministration(kind, id) => storage
            .remove_administration(kind, id)
            .map(|()| Answer::Done),
        Request::Publish(publication) => storage.publish(&publication).map(Answer::Claims),
        Request::ReadHeld(queue, from, most) => {
            storage.read_held(queue, from, most).map(Answer::Held)
        }
        Request::ReadDead(queue, from, most) => {
            storage.read_dead(queue, from, most).map(Answer::DeadQueue)
        }
        Request::ReadDeadMessage(queue, message) => {
            storage.read_dead_message(queue, message).map(Answer::Dead)
        }
        Request::Replay(replay) => storage.replay(&replay).map(Answer::Replayed),
    };
    answered.unwrap_or_else(|error| match error {
        PersistError::Refused { scope, reason } => Answer::Refused(scope, reason),
        other => Answer::Failed(other.to_string()),
    })
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
