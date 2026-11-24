//! How the Storage nodes settle which is the primary (`super`): each asks
//! every one listed what it is now, follows a primary that answers, and is
//! promoted only where every one listed answered, none is a primary, and
//! it is the one to be — and, the epoch it is promoted in kept, asked again,
//! none holds that epoch or a later one. A primary that does not answer is
//! waited for; one that stopped in order handed over to its successor
//! (`super::succession`).

use std::sync::{Arc, PoisonError};
use std::time::{Duration, Instant};
use xcore::lock::Locked;

use super::super::embedded::Embedded;
use super::super::wire::{self, Answer, Request};
use super::succession::Ended;
use super::{Bootstrap, Copied, POSITION, Primary, Role, Standing, StorageNode};
use crate::{EncryptedStore, PersistError};

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
                if let Err(problem) = self.stand_by(&address, epoch) {
                    self.told(&format!("could not follow {address}: {problem}"));
                    self.rest(self.timings.beat);
                    continue;
                }
                let place = primary.place;
                let ended = self.follow(&address);
                if self.stopping() {
                    continue;
                }
                self.stand(Role::Finding);
                match ended {
                    Ended::Succeeding(epoch, acknowledged) => {
                        self.succeed(epoch, acknowledged.then_some(place));
                    }
                    Ended::Deferring => self.rest(self.timings.beat),
                    Ended::Lost => {}
                }
                continue;
            }
            if self.promoting(&answered, epoch) {
                continue;
            }
            self.rest(self.timings.beat);
        }
    }

    /// Promoted where it is the one to be ([`StorageNode::promotes`]), in the
    /// epoch after the latest of `epoch`, its own, and those `answered`: that
    /// epoch kept first, durable, and the others asked again
    /// ([`StorageNode::unrivalled`]). Whether it was the one, promoted or not;
    /// one not promoted rests a beat, so two that each found the other in its
    /// epoch do not ask on at once.
    pub(super) fn promoting(&self, answered: &[Answered], epoch: u64) -> bool {
        if !self.promotes(answered) {
            return false;
        }
        let latest = answered.iter().map(|a| a.standing.epoch).max();
        let next = latest.unwrap_or(0).max(epoch) + 1;
        let promoted = self
            .keep_epoch(next)
            .and_then(|()| self.unrivalled(next, None))
            .and_then(|()| self.promote(next));
        if let Err(problem) = promoted {
            self.told(&format!("could not be promoted: {problem}"));
            self.rest(self.timings.beat);
        }
        true
    }

    /// Whether it is alone in `epoch`, which it kept: every Storage node
    /// listed, asked again, answers, none is a primary and none other holds
    /// `epoch` or a later one. Two may each be the one to promote by what
    /// they saw — one asked while the other was not ready, the other once it
    /// was — and each keeps the epoch before it asks again, so the one asking
    /// second finds the other's kept: at most one is promoted in an epoch,
    /// with no witness, as every one listed answers (ADR-0056, amendment
    /// 2026-11-22). The one at `excused`, a primary that stopped in order,
    /// handed over to it and acknowledged the epoch it kept, is not asked:
    /// it read that epoch kept, so started again it can never be promoted in
    /// it (`super::succession`). One that did not acknowledge it is asked as
    /// every other (ADR-0056, amendment 2026-11-24).
    ///
    /// # Errors
    ///
    /// The Storage node in its way, or that one listed did not answer again.
    pub(super) fn unrivalled(
        &self,
        epoch: u64,
        excused: Option<usize>,
    ) -> Result<(), PersistError> {
        let answered = self.standings_but(excused);
        let rival = answered.iter().find(|a| {
            a.standing.process != self.process && (a.standing.primary || a.standing.epoch >= epoch)
        });
        let every = (0..self.listed.len())
            .filter(|place| Some(*place) != excused)
            .all(|place| answered.iter().any(|a| a.place == place));
        let reason = match rival {
            Some(rival) if rival.standing.primary => format!(
                "{} is the primary, epoch {}",
                rival.address, rival.standing.epoch
            ),
            Some(rival) => format!(
                "{} holds epoch {} already",
                rival.address, rival.standing.epoch
            ),
            None if every => return Ok(()),
            None => "not every Storage node listed answered again".to_string(),
        };
        Err(PersistError::Failed { reason })
    }

    /// A standby of the primary at `address` from now, in its `epoch`. One
    /// that holds no history's writes — started empty, or a former primary,
    /// which gave up its positions when promoted — is copied whole by
    /// whichever primary it follows, so it is copying first, kept durable
    /// before that epoch: a standby waiting for its first whole copy is never
    /// ready, never promoted, and never taken for one holding the epoch's
    /// Ledger (ADR-0056, amendment 2026-11-15).
    ///
    /// # Errors
    ///
    /// Where copying or the epoch could not be kept: it takes up no role.
    pub(super) fn stand_by(&self, address: &str, epoch: u64) -> Result<(), PersistError> {
        if self.copies.history() == 0 && self.bootstrap().is_ready() {
            self.bootstrapped(Bootstrap::Copying, &[])?;
        }
        self.take_up(Role::Standby(address.to_string()), epoch)
    }

    /// Whether this node is the one promoted: only where every Storage node
    /// listed answered and none of them is a primary, and then the one
    /// holding most of the latest epoch, the first listed of equals, among
    /// those ready — none copying or catching up, whose pages may reflect a
    /// write in part (ADR-0056, amendment 2026-11-12). A primary that does
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
        if !me.standing.bootstrap.is_ready() {
            return false;
        }
        let ready = answered.iter().filter(|a| a.standing.bootstrap.is_ready());
        let best = ready.max_by(|one, other| {
            (one.standing.epoch, one.standing.held())
                .cmp(&(other.standing.epoch, other.standing.held()))
                .then(other.place.cmp(&one.place))
        });
        best.map(|best| best.place) == Some(me.place)
    }

    /// Promoted in `epoch`: the epoch kept first, durable, then its
    /// databases opened over what it copied, its writes numbered on in the
    /// history it followed — a new one where it followed none — its watch
    /// keeping its `[tuning] storage_unwaited` of what none waits on, its
    /// writer its `[tuning] storage_writer_kept` bytes of the records it
    /// writes, and its positions given up, so a copy of another primary
    /// later starts whole. Each position is given up by a write not synced
    /// on its own: every write is durable in order, so the first it makes
    /// durable as the primary makes that durable first, and a process gone
    /// before any holds the databases as its positions say (open problem
    /// 141; until 2026-11-24 three durable writes one after another).
    ///
    /// # Errors
    ///
    /// Where it is not ready — copying or catching up, its pages perhaps
    /// reflecting a write in part — the epoch cannot be kept or its
    /// databases not opened: it is not promoted then.
    pub(super) fn promote(&self, epoch: u64) -> Result<(), PersistError> {
        let bootstrap = self.bootstrap();
        if !bootstrap.is_ready() {
            return Err(PersistError::Failed {
                reason: format!(
                    "a standby {}: what it holds is not yet one state of its primary's",
                    bootstrap.said(self.copies.next())
                ),
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
            engine.apply_deferred(&[(POSITION, None)])?;
        }
        let opened = |at: usize| {
            let copied = Copied::new(Arc::clone(&self.engines[at]), at, Arc::clone(&self.copies));
            EncryptedStore::open(copied, self.keys.as_ref(), &self.kek)
        };
        let clock = Arc::clone(&self.clock);
        let primary: Primary = Embedded::over(opened(0)?, opened(1)?, opened(2)?, clock)?;
        primary.in_epoch(epoch);
        primary.keep_unwaited(self.timings.unwaited);
        primary.keep_written(self.timings.writer_kept);
        self.take_up(Role::Primary(Arc::new(primary)), epoch)
    }

    /// While the primary: ask the others, each silence, and step down
    /// where one is the primary of a later epoch.
    fn keep_primary(&self) {
        self.rest(self.timings.silence);
        // Woken by its stop: it asks nobody, a stop never waits on another.
        if self.stopping() {
            return;
        }
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

    /// What each Storage node listed answers, those that do; and where it
    /// answered as this node's own process, its own place, kept
    /// ([`StorageNode::place`]).
    pub(super) fn standings(&self) -> Vec<Answered> {
        self.standings_but(None)
    }

    /// [`StorageNode::standings`], the one at `but` not asked; its own place
    /// once known answered by its own standing, never over the wire, which
    /// answers the same.
    fn standings_but(&self, but: Option<usize>) -> Vec<Answered> {
        let own = self.place();
        let answered: Vec<Answered> = self
            .listed
            .iter()
            .enumerate()
            .filter(|(place, _)| Some(*place) != but)
            .filter_map(|(place, address)| {
                let standing = if Some(place) == own {
                    self.standing()
                } else {
                    self.ask_standing(place, address).ok()?
                };
                Some(Answered {
                    place,
                    address: address.clone(),
                    standing,
                })
            })
            .collect();
        if let Some(me) = answered.iter().find(|a| a.standing.process == self.process) {
            let _ = self.place.set(me.place);
        }
        answered
    }

    /// What the Storage node listed at `place`, `address`, is, asked within
    /// the silence: one that does not answer by then did not answer. The
    /// certificate it presented, its host checked against `address`, is
    /// kept as what it proves itself by to follow (`super::proven`).
    /// Windows takes about two seconds to refuse a connection to a loopback
    /// port nobody listens on, so a shorter bound is what keeps each round
    /// of asking, and the settling once every one listed answers again,
    /// within the silence.
    fn ask_standing(&self, place: usize, address: &str) -> Result<Standing, String> {
        let mut connection = self.connect(address, self.timings.silence)?;
        wire::send(&mut connection, &Request::Standing).map_err(|e| e.to_string())?;
        let answered = wire::receive::<Answer>(&mut connection).map_err(|e| e.to_string())?;
        self.presented_on(place, &connection);
        match answered {
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
