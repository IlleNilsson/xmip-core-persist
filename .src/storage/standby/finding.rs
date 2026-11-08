//! How the Storage nodes settle which is the primary (`super`): each asks
//! every one listed what it is now, follows a primary that answers, and is
//! promoted only where every one listed answered, none is a primary, and
//! it is the one to be. A primary that does not answer is waited for.

use std::sync::{Arc, PoisonError};
use std::time::{Duration, Instant};
use xcore::lock::Locked;

use super::super::embedded::Embedded;
use super::super::wire::{self, Answer, Request};
use super::{Copied, POSITION, Primary, Role, Standing, StorageNode};
use crate::PersistError;

/// What one Storage node listed answered, at its place in the list.
pub(super) struct Answered {
    pub place: usize,
    pub address: String,
    pub standing: Standing,
}

impl StorageNode {
    /// Settle, and keep settling, until stopped.
    pub(super) fn find(self: Arc<Self>) {
        while !self.stopping() {
            if self.is_primary() {
                self.keep_primary();
                continue;
            }
            let answered = self.standings();
            let epoch = self.held().epoch;
            let primary = answered.iter().find(|answer| {
                answer.standing.primary
                    && answer.standing.process != self.process
                    && answer.standing.epoch >= epoch
            });
            if let Some(primary) = primary {
                let (address, epoch) = (primary.address.clone(), primary.standing.epoch);
                if let Err(problem) = self.take_up(Role::Standby(address.clone()), epoch) {
                    self.told(&format!("could not follow {address}: {problem}"));
                    self.rest(self.timings.beat);
                    continue;
                }
                self.follow(&address);
                if !self.stopping() {
                    self.stand(Role::Finding);
                }
                continue;
            }
            if self.promotes(&answered) {
                let latest = answered.iter().map(|a| a.standing.epoch).max();
                if let Err(problem) = self.promote(latest.unwrap_or(0).max(epoch) + 1) {
                    self.told(&format!("could not be promoted: {problem}"));
                }
                continue;
            }
            self.rest(self.timings.beat);
        }
    }

    /// Whether this node is the one promoted: only where every Storage node
    /// listed answered and none of them is a primary, and then the one
    /// holding most of the latest epoch, the first listed of equals, none
    /// whose whole copy is under way or was cut short. A primary that does
    /// not answer is waited for, never replaced: with no witness, a primary
    /// cut off from the others would serve on beside the one promoted (the
    /// owner, 2026-11-08; ADR-0056, amendment 2026-11-08).
    fn promotes(&self, answered: &[Answered]) -> bool {
        let every = (0..self.listed.len()).all(|place| answered.iter().any(|a| a.place == place));
        if !every || answered.iter().any(|a| a.standing.primary) {
            return false;
        }
        let Some(me) = answered.iter().find(|a| a.standing.process == self.process) else {
            return false;
        };
        if me.standing.copying {
            return false;
        }
        let whole = answered.iter().filter(|a| !a.standing.copying);
        let best = whole.max_by(|one, other| {
            (one.standing.epoch, one.standing.held())
                .cmp(&(other.standing.epoch, other.standing.held()))
                .then(other.place.cmp(&one.place))
        });
        best.map(|best| best.place) == Some(me.place)
    }

    /// Promoted in `epoch`: the epoch kept first, durable, then its
    /// databases opened over what it copied, its writes numbered on in the
    /// history it followed — a new one where it followed none — and its
    /// positions given up, so a copy of another primary later starts whole.
    ///
    /// # Errors
    ///
    /// Where a whole copy into it was cut short, the epoch cannot be kept
    /// or its databases not opened: it is not promoted then.
    pub(super) fn promote(&self, epoch: u64) -> Result<(), PersistError> {
        if self.is_copying() {
            return Err(PersistError::Failed {
                reason: "a whole copy into it was cut short; it holds part of a Ledger".to_string(),
            });
        }
        self.keep_epoch(epoch)?;
        if self.copies.history() == 0 {
            self.copies.restart(
                u128::from_be_bytes(codec::random::array()),
                [1; super::DATABASES],
            );
        }
        for engine in &self.engines {
            engine.remove(POSITION)?;
        }
        let opened =
            |at: usize| Copied::new(Arc::clone(&self.engines[at]), at, Arc::clone(&self.copies));
        let primary: Primary = Embedded::open(
            opened(0),
            opened(1),
            opened(2),
            self.keys.as_ref(),
            &self.kek,
        )?;
        primary.in_epoch(epoch);
        self.take_up(Role::Primary(Arc::new(primary)), epoch)
    }

    /// While the primary: ask the others, each silence, and step down
    /// where one is the primary of a later epoch.
    fn keep_primary(&self) {
        self.rest(self.timings.silence);
        let epoch = self.held().epoch;
        let later = self.standings().into_iter().any(|answer| {
            answer.standing.primary
                && answer.standing.process != self.process
                && answer.standing.epoch > epoch
        });
        if later {
            self.told("stepped down: another Storage node is the primary of a later epoch");
            self.stand(Role::Finding);
        }
    }

    /// What each Storage node listed answers, those that do.
    pub(super) fn standings(&self) -> Vec<Answered> {
        self.listed
            .iter()
            .enumerate()
            .filter_map(|(place, address)| {
                let standing = self.ask_standing(address).ok()?;
                Some(Answered {
                    place,
                    address: address.clone(),
                    standing,
                })
            })
            .collect()
    }

    /// What the Storage node at `address` is, asked within the silence: one
    /// that does not answer by then did not answer. Windows takes about two
    /// seconds to refuse a connection to a loopback port nobody listens on,
    /// so a shorter bound is what keeps each round of asking, and the
    /// settling once every one listed answers again, within the silence.
    fn ask_standing(&self, address: &str) -> Result<Standing, String> {
        let mut connection = self.connect(address, self.timings.silence)?;
        wire::send(&mut connection, &Request::Standing).map_err(|e| e.to_string())?;
        match wire::receive::<Answer>(&mut connection).map_err(|e| e.to_string())? {
            Some(Answer::Standing(standing)) => Ok(standing),
            other => Err(format!("answered {other:?}")),
        }
    }

    /// A mutual TLS connection to the Storage node at `address`, each read
    /// bounded by `within`.
    pub(super) fn connect(&self, address: &str, within: Duration) -> Result<tls::Guarded, String> {
        let host = address
            .rsplit_once(':')
            .map_or(address, |(host, _)| host)
            .trim_start_matches('[')
            .trim_end_matches(']');
        let tcp = net::connect(address, Some(self.timings.timeout.min(within)))
            .map_err(|failure| failure.to_string())?;
        tcp.set_read_timeout(Some(within))
            .map_err(|failure| failure.to_string())?;
        tls::client_with(host, tcp, Arc::clone(&self.config)).map_err(|failure| failure.message)
    }

    /// Wait `most`, or until stopped: a stop wakes it at once.
    pub(super) fn rest(&self, most: Duration) {
        let until = Instant::now() + most;
        let mut settling = self.settling.locked();
        while !self.stopping() {
            let Some(left) = until.checked_duration_since(Instant::now()) else {
                return;
            };
            settling = self
                .settled
                .wait_timeout(settling, left)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }

    /// `what` kept as the last thing it has to say of its standing, which
    /// what it says of itself ends with ([`StorageNode::said`]).
    pub(super) fn told(&self, what: &str) {
        *self.last.locked() = Some(what.to_string());
    }
}
