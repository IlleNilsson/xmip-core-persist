//! The waits on queues a Storage node's server holds now (`super::super::
//! watch`), each by the token its node asked it with: ended by that token,
//! asked over another connection as the node's waiter stops, so no node
//! looks whether its waiter stopped; every one ended as the server stops, so
//! a stop never waits one out; and none held longer than the Storage node's
//! `[tuning] storage_wait`.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use xcore::lock::Locked;

use super::super::watch::Waiter;

/// The longest a Storage node holds one node's wait on its queues, where
/// its `[tuning] storage_wait` does not say: a minute, far beyond the
/// second a node waits by default (its `send_scan` and `work_scan`). A wait
/// held that long is answered with none, and its node asks again.
pub const HOLD: Duration = Duration::from_secs(60);

/// How many ends a server keeps of waits it does not hold yet: an end
/// asked over another connection may overtake its wait.
const AHEAD: usize = 64;

/// Every wait the server holds now.
pub(crate) struct Waiters(Mutex<Held>);

struct Held {
    waiting: HashMap<u128, Arc<Waiter>>,
    /// Waits ended before they came, newest last, at most [`AHEAD`].
    ahead: VecDeque<u128>,
    stopping: bool,
    /// The longest a wait is held.
    most: Duration,
}

impl Default for Waiters {
    fn default() -> Self {
        Self(Mutex::new(Held {
            waiting: HashMap::new(),
            ahead: VecDeque::new(),
            stopping: false,
            most: HOLD,
        }))
    }
}

impl Waiters {
    /// The wait asked with `token`, for the node whose claims name `holder`
    /// (none where it is empty), held until what this returns is dropped:
    /// its waiter ended at once where the server is stopping or its end
    /// came first, and its bound at most the server's.
    pub(crate) fn waiting(&self, token: u128, holder: &str) -> Waiting<'_> {
        let waiter = Arc::new(if holder.is_empty() {
            Waiter::new()
        } else {
            Waiter::of(holder)
        });
        let mut held = self.0.locked();
        let ended = held.ahead.iter().position(|ahead| *ahead == token);
        if let Some(at) = ended {
            held.ahead.remove(at);
        }
        held.waiting.insert(token, Arc::clone(&waiter));
        let (stopped, most) = (held.stopping || ended.is_some(), held.most);
        drop(held);
        if stopped {
            waiter.stop();
        }
        Waiting {
            waiters: self,
            token,
            waiter,
            most,
        }
    }

    /// The wait asked with `token` ended: answered with none at once, or,
    /// where it has not come yet, the moment it does.
    pub(crate) fn end(&self, token: u128) {
        let mut held = self.0.locked();
        let Some(waiter) = held.waiting.get(&token).cloned() else {
            if held.ahead.len() == AHEAD {
                held.ahead.pop_front();
            }
            held.ahead.push_back(token);
            return;
        };
        drop(held);
        waiter.stop();
    }

    /// Every wait ended, and every one asked after: the server is stopping.
    pub(crate) fn stop(&self) {
        let mut held = self.0.locked();
        held.stopping = true;
        let waiting: Vec<Arc<Waiter>> = held.waiting.values().cloned().collect();
        drop(held);
        for waiter in waiting {
            waiter.stop();
        }
    }

    /// Whether the server is stopping.
    pub(crate) fn stopping(&self) -> bool {
        self.0.locked().stopping
    }

    /// No wait held longer than `most` from now on.
    pub(crate) fn hold(&self, most: Duration) {
        self.0.locked().most = most;
    }
}

/// One wait the server holds, let go of as it is dropped.
pub(crate) struct Waiting<'a> {
    waiters: &'a Waiters,
    token: u128,
    /// What it waits as.
    pub(crate) waiter: Arc<Waiter>,
    /// The longest it is held.
    pub(crate) most: Duration,
}

impl Waiting<'_> {
    /// Whether its server is stopping.
    pub(crate) fn stopping(&self) -> bool {
        self.waiters.stopping()
    }
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        let mut held = self.waiters.0.locked();
        if held
            .waiting
            .get(&self.token)
            .is_some_and(|waiter| Arc::ptr_eq(waiter, &self.waiter))
        {
            held.waiting.remove(&self.token);
        }
    }
}

#[cfg(test)]
impl Waiters {
    /// How many waits it holds now.
    pub(crate) fn held(&self) -> usize {
        self.0.locked().waiting.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Whether the wait asked with `token`, for no holder, is ended as it
    /// is held.
    fn ended(waiters: &Waiters, token: u128) -> bool {
        waiters.waiting(token, "").waiter.is_stopped()
    }

    #[test]
    fn an_end_ends_its_wait_whether_it_came_before_or_after_and_no_other() {
        let waiters = Waiters::default();
        let (first, other) = (waiters.waiting(1, ""), waiters.waiting(2, ""));
        waiters.end(1);
        assert!(first.waiter.is_stopped());
        assert!(!other.waiter.is_stopped(), "another wait");
        waiters.end(3);
        assert!(ended(&waiters, 3), "ended before it came");
        assert!(!ended(&waiters, 3), "an end ends one wait");
    }

    #[test]
    fn a_stop_ends_every_wait_held_and_every_one_after() {
        let waiters = Waiters::default();
        let held = waiters.waiting(1, "");
        waiters.stop();
        assert!(held.waiter.is_stopped() && held.stopping());
        assert!(ended(&waiters, 2));
    }

    #[test]
    fn the_ends_kept_ahead_of_their_waits_are_bounded() {
        let waiters = Waiters::default();
        for token in 0..=u128::try_from(AHEAD).expect("a token") {
            waiters.end(token);
        }
        assert!(!ended(&waiters, 0), "the oldest let go");
        assert!(ended(&waiters, 1));
        assert_eq!(waiters.waiting(9, "").most, HOLD);
        waiters.hold(Duration::from_secs(5));
        assert_eq!(waiters.waiting(9, "").most, Duration::from_secs(5));
    }

    #[test]
    fn a_wait_asked_for_a_holder_waits_for_it_and_one_for_none_for_none() {
        let waiters = Waiters::default();
        let holder = configure::fixture::test_cluster().node_scope(0);
        assert_eq!(
            waiters.waiting(1, &holder).waiter.holder(),
            Some(holder.as_str())
        );
        assert_eq!(waiters.waiting(2, "").waiter.holder(), None);
    }
}
