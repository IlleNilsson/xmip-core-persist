//! One Storage node as a client lists it (`super`): the connections to it
//! no request is using, until when it is passed over, whether it said it is
//! a standby, and what it last came to.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
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
    /// Until when, on the client's monotonic reading.
    passed_over: Mutex<Option<i128>>,
    pub(super) standby: AtomicBool,
    pub(super) last: Mutex<Option<Reached>>,
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
            passed_over: Mutex::new(None),
            standby: AtomicBool::new(false),
            last: Mutex::new(None),
        })
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
