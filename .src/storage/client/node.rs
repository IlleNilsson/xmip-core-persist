//! One Storage node as a client lists it (`super`): the connections to it
//! no request is using, how many it held at once, until when it is passed
//! over, whether it said it is a standby, and what it last came to.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use xcore::clock::nanos;
use xcore::lock::Locked;

use super::{Asked, Connection, Reached, unreachable};
use crate::PersistError;

/// One Storage node, the connections to it no request is using, until
/// when it is passed over, whether it said it is a standby, and what it
/// last came to.
pub(super) struct Node {
    pub(super) address: String,
    pub(super) host: String,
    pub(super) idle: Mutex<Vec<Connection>>,
    /// How many of its connections requests use now.
    asking: AtomicUsize,
    /// The most connections to it held at once, used and idle: as many as
    /// a client keeps to a standby, so none of its threads opens one as the
    /// standby succeeds (open problem 142).
    most: AtomicUsize,
    /// Until when, on the client's monotonic reading.
    passed_over: Mutex<Option<i128>>,
    pub(super) standby: AtomicBool,
    pub(super) last: Mutex<Option<Reached>>,
}

/// One request's use of a connection to a Storage node, counted while it
/// lasts.
pub(super) struct Asking<'a>(&'a Node);

impl Drop for Asking<'_> {
    fn drop(&mut self) {
        self.0.asking.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Node {
    /// The Storage node at `address`, `host:port` as listed, not asked yet.
    ///
    /// # Errors
    ///
    /// Where `address` names no port.
    pub(super) fn listed(address: &str) -> Result<Self, PersistError> {
        let (host, _) = address
            .rsplit_once(':')
            .ok_or_else(|| unreachable(format!("'{address}' names no port")))?;
        Ok(Self {
            address: address.to_string(),
            host: host
                .trim_start_matches('[')
                .trim_end_matches(']')
                .to_string(),
            idle: Mutex::new(Vec::new()),
            asking: AtomicUsize::new(0),
            most: AtomicUsize::new(0),
            passed_over: Mutex::new(None),
            standby: AtomicBool::new(false),
            last: Mutex::new(None),
        })
    }

    /// A request's use of a connection to it begun, counted beside those
    /// in use and idle until it ends.
    pub(super) fn asking(&self) -> Asking<'_> {
        let asking = self.asking.fetch_add(1, Ordering::Relaxed) + 1;
        let held = asking + self.idle.locked().len();
        self.most.fetch_max(held, Ordering::Relaxed);
        Asking(self)
    }

    /// The most connections to it held at once.
    pub(super) fn most(&self) -> usize {
        self.most.load(Ordering::Relaxed)
    }

    /// `connection` to it, where `stands`, what it said it is, lets it be
    /// asked; kept, where it said it is a standby, for once it is promoted
    /// (ADR-0056, amendment 2026-11-23).
    pub(super) fn held(
        &self,
        connection: Connection,
        stands: Result<(), Asked>,
    ) -> Result<Connection, Asked> {
        match stands {
            Ok(()) => Ok(connection),
            Err(Asked::Standby(said)) => {
                self.idle.locked().push(connection);
                Err(Asked::Standby(said))
            }
            Err(failed) => Err(failed),
        }
    }

    /// Whether it is passed over at `now`, the client's monotonic reading.
    pub(super) fn passed_over(&self, now: i128) -> bool {
        let until = *self.passed_over.locked();
        until.is_some_and(|until| until > now)
    }

    /// What `asked` came to at `now`, passed over for `pass_over` where
    /// it did not answer.
    pub(super) fn answered<T>(&self, asked: &Result<T, Asked>, pass_over: Duration, now: i128) {
        let (answered, standby, said) = match asked {
            Ok(_) => (true, false, "answered".to_string()),
            Err(Asked::Standby(said)) => (false, true, said.clone()),
            Err(Asked::Failed(said)) => (false, self.standby.load(Ordering::Relaxed), said.clone()),
        };
        self.standby.store(standby, Ordering::Relaxed);
        *self.passed_over.locked() = (!answered).then(|| now.saturating_add(nanos(pass_over)));
        *self.last.locked() = Some(Reached {
            address: self.address.clone(),
            answered,
            standby,
            said,
            at: now,
        });
    }
}
