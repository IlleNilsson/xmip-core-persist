//! A primary stopped in order hands over to a standby that succeeds it at
//! once (the owner, 2026-11-23: *That has to be sorted, nothing can be
//! left*; ADR-0056, amendments 2026-11-23 and 2026-11-24). Until that day
//! the others waited for it to come back, as for one gone, and every node's
//! writes failed while it restarted: 2.5 s and 5.2 s in the Hyper-V
//! environment's rolling restart of 2026-11-22 (open problem 92).
//!
//! **The primary** stops serving, waits for every operation lent, and syncs
//! what its standbys could not be handed yet. Each connection it hands
//! writes on hands the last of them, then says it stopped
//! ([`Shipped::Stopped`]): its epoch, the next write of each database, and
//! its successor — the first standby handed every write, named by its place
//! among the Storage nodes listed, as its certificate proves it
//! (`super::proven`). It names one only where its stop drained and synced:
//! an operation lent past the bound may write yet, so its databases take no
//! write more, and the cluster settles as ever (open problem 136). It
//! answers its successor's answer, that epoch kept, with its own
//! ([`Shipped::Acknowledged`]), then keeps its positions again, so once back
//! it follows its successor on from where it stopped, never copied whole.
//! An operation asked of it while it stops is held until it has said who
//! succeeds it, then refused in words naming that one, so the node asks it
//! next (open problem 139).
//!
//! **The successor** syncs what it took and, where it is ready, holds every
//! write its primary said and is of its epoch, keeps the next epoch,
//! durable, and says so ([`Succeeding::Kept`]), or why not. It holds every
//! operation asked of it from then until it serves. It is promoted where
//! every other Storage node listed answers, none is a primary and none other
//! holds that epoch or a later one. The primary that handed over is not
//! asked once it acknowledged the epoch kept: it can then never be promoted
//! in it. Where it did not — its stop ended past the silence, a disk
//! stalled — it is asked as every other, so a former primary come back and
//! promoted in that epoch first is found, and never two are promoted in it
//! (open problem 134). A standby told another succeeds rests a beat before
//! it asks, and finds the successor the primary.
//!
//! **What does not change.** A primary that vanishes — killed, its machine
//! lost, cut off — says nothing, and is still waited for (amendment
//! 2026-11-08): with no witness, a standby cannot tell one gone from one cut
//! off and serving on.

use std::io::{Read, Write};
use std::sync::atomic::Ordering;
use xcore::lock::Locked;

use net::watched::Watched;

use super::super::wire;
use super::copies::DATABASES;
use super::follow::Taking;
use super::position::position;
use super::ship::send;
use super::{Shipped, StorageNode, Succeeding};
use crate::borrowed;

/// How following a primary ended (`super::follow`).
pub(super) enum Ended {
    /// It was lost: the node stops, the primary fell silent, declined or
    /// ended the connection, or stopped in order naming no successor.
    Lost,
    /// The primary stopped in order and named this Storage node its
    /// successor, and it kept this epoch, to be promoted in; and whether
    /// the primary acknowledged it, so it need not be asked.
    Succeeding(u64, bool),
    /// The primary stopped in order and named another its successor.
    Deferring,
}

/// A connection a Storage node hands writes on, or refuses to, counted
/// while it lasts: a stop waits for each to end ([`StorageNode::stop`]).
pub(super) struct Shipping<'a>(&'a StorageNode);

impl<'a> Shipping<'a> {
    /// One more connection of `node`'s, counted.
    pub(super) fn of(node: &'a StorageNode) -> Self {
        node.shipping.fetch_add(1, Ordering::SeqCst);
        Self(node)
    }
}

impl Drop for Shipping<'_> {
    fn drop(&mut self) {
        if self.0.shipping.fetch_sub(1, Ordering::SeqCst) == 1 && self.0.stopping() {
            self.0.notify_settled();
        }
    }
}

impl StorageNode {
    /// Stop: serve no operation more, wait for each one under way, then hand
    /// every standby what it wrote and, a primary whose stop drained, hand
    /// over to the one that succeeds it, its own positions kept after; and
    /// stop settling, a standby's read of its primary's next frame ended at
    /// once. An operation asked meanwhile is held until it has said who
    /// succeeds it, and every wait on its queues refused then, in words
    /// naming that one.
    pub fn stop(&self) {
        self.stopping.store(true, Ordering::SeqCst);
        self.succeeding.store(false, Ordering::SeqCst);
        self.followed.shut();
        let primary = self.primary();
        self.settle_while(self.timings.timeout, || {
            self.lent.load(Ordering::SeqCst) > 0
        });
        // An operation still under way past the bound may write yet: its
        // databases take no write more, so none is acknowledged that its
        // standbys were not handed, and no successor is named.
        let drained = self.lent.load(Ordering::SeqCst) == 0;
        if !drained {
            self.copies.close();
            self.told("stopping in order, an operation outlasted its bound: no successor named");
        }
        // Every write synced, so every one may be handed on.
        let synced = primary.is_some() && self.synced_all();
        self.handing.store(drained && synced, Ordering::SeqCst);
        self.drained.store(true, Ordering::SeqCst);
        self.notify_settled();
        // Each standby's shipping hands on the last writes now, not a beat on.
        self.copies.wake();
        self.settle_while(self.timings.silence, || {
            self.shipping.load(Ordering::SeqCst) > 0
        });
        // Said who succeeds it: what it holds goes there now, before its own
        // positions are kept.
        self.handed.store(true, Ordering::SeqCst);
        if let Some(primary) = &primary {
            primary.refuse_waits(&self.said());
        }
        self.notify_settled();
        if synced {
            self.keep_positions();
        }
        if let Some(thread) = self.finding.locked().take() {
            let _ = thread.join();
        }
    }

    /// Each database synced where it holds a write not synced yet: whether
    /// every one is.
    fn synced_all(&self) -> bool {
        self.engines.iter().enumerate().all(|(at, engine)| {
            self.copies.all_synced(at) || self.copies.sync(at, engine.as_ref()).is_ok()
        })
    }

    /// A primary stopped in order, every write synced: the position of each
    /// database kept again, durable, as far as it numbered its writes, so
    /// started again it follows its successor on from there. One killed
    /// keeps none, and is copied whole (`super::finding`, `promote`).
    fn keep_positions(&self) {
        let history = self.copies.history();
        for (engine, next) in self.engines.iter().zip(self.copies.next()) {
            if engine.apply(&borrowed(&[position(history, next)])).is_err() {
                return;
            }
        }
    }

    /// The last frame a primary stopped in order hands the standby at
    /// `place` on `connection`, handed every write before `handed`: that it
    /// stopped, in `epoch`, the next write of each database, and its
    /// successor — this standby, where its stop drained and it is the first
    /// handed every write, or the one named before; none where its stop did
    /// not drain. Where this one succeeds it, its answer is waited for,
    /// within the silence, said, and, the epoch kept, acknowledged.
    ///
    /// # Errors
    ///
    /// Where the frame could not be sent: the standby went.
    pub(super) fn hand_over(
        &self,
        connection: &mut (impl Read + Write),
        (place, handed): (usize, [u64; DATABASES]),
        epoch: u64,
    ) -> Result<(), String> {
        let next = self.copies.next();
        let place = u32::try_from(place).ok();
        let successor = {
            let mut named = self.successor.locked();
            if named.is_none() && handed == next && self.handing.load(Ordering::SeqCst) {
                *named = place;
            }
            *named
        };
        send(connection, &Shipped::Stopped(epoch, next, successor))?;
        if successor.is_none() || successor != place {
            return Ok(());
        }
        let address = successor
            .and_then(|place| usize::try_from(place).ok())
            .and_then(|place| self.listed.get(place))
            .map_or("its successor", String::as_str);
        let mut answering = Watched {
            silence: Some(self.timings.silence),
            ..Watched::new(connection, || false, || self.clock.monotonic_nanos())
        };
        let said = match wire::receive::<Succeeding>(&mut answering) {
            Ok(Some(Succeeding::Kept(kept))) => {
                // It kept the epoch: this node, started again, finds it kept
                // and is never promoted in it, so its successor need not ask.
                let _ = send(&mut answering, &Shipped::Acknowledged(kept));
                format!("stopped in order, handing over to {address}, which kept epoch {kept}")
            }
            Ok(Some(Succeeding::Declined(why))) => {
                format!("stopped in order; {address} does not succeed it: {why}")
            }
            Ok(None) => format!("stopped in order; {address} went unanswering"),
            Err(error) => format!("stopped in order; {address} did not answer: {error}"),
        };
        self.told(&said);
        Ok(())
    }

    /// Its primary stopped in order (`Shipped::Stopped`), in `epoch`, its
    /// next writes `next`, naming `successor`. Named, what it took synced
    /// first — ready where that catches it up — and, where it is ready,
    /// holds every write the primary said and is of its epoch, the next
    /// epoch kept, durable; its primary told on `connection` either way, and
    /// its acknowledgment read there. Another named, it defers to that one.
    pub(super) fn succeeding(
        &self,
        connection: &mut (impl Read + Write),
        (epoch, next, successor): (u64, [u64; DATABASES], Option<u32>),
        taking: &mut Taking,
    ) -> Ended {
        let Some(successor) = successor else {
            return Ended::Lost;
        };
        if self.place().and_then(|place| u32::try_from(place).ok()) != Some(successor) {
            return Ended::Deferring;
        }
        let kept = self
            .synced(&mut taking.dirty)
            .map_err(|problem| problem.to_string())
            .and_then(|()| self.may_succeed(epoch, next))
            .and_then(|()| {
                self.keep_epoch(epoch + 1)
                    .map_err(|problem| problem.to_string())
            });
        let answer = match &kept {
            Ok(()) => {
                // Every operation asked of it is held from now until it serves.
                self.succeeding.store(true, Ordering::SeqCst);
                Succeeding::Kept(epoch + 1)
            }
            Err(why) => Succeeding::Declined(why.clone()),
        };
        let _ = wire::send(connection, &answer);
        match kept {
            Ok(()) => {
                let acknowledged = matches!(
                    wire::receive::<Shipped>(connection),
                    Ok(Some(Shipped::Acknowledged(acknowledged))) if acknowledged == epoch + 1
                );
                Ended::Succeeding(epoch + 1, acknowledged)
            }
            Err(why) => {
                self.told(&format!("could not succeed its primary: {why}"));
                Ended::Lost
            }
        }
    }

    /// Whether it may succeed a primary of `epoch` whose next writes are
    /// `next`: ready, holding every one, and of that epoch.
    fn may_succeed(&self, epoch: u64, next: [u64; DATABASES]) -> Result<(), String> {
        let (bootstrap, held) = (self.bootstrap(), self.copies.next());
        if !bootstrap.is_ready() {
            return Err(format!("a standby {}", bootstrap.said(held)));
        }
        if held != next {
            return Err(format!(
                "it holds the writes before {held:?}, its primary those before {next:?}"
            ));
        }
        let met = self.held().epoch;
        if met != epoch {
            return Err(format!("it met epoch {met}, its primary is of {epoch}"));
        }
        Ok(())
    }

    /// Promoted in `epoch`, which it kept as its primary handed over to it:
    /// where every other one listed answers, none is a primary and none
    /// other holds `epoch` or a later one ([`StorageNode::unrivalled`]), the
    /// one at `excused`, its primary that acknowledged the epoch kept, not
    /// asked. One that cannot be says why and rests a beat, then settles as
    /// ever. Either way the operations it held go on.
    pub(super) fn succeed(&self, epoch: u64, excused: Option<usize>) {
        let promoted = self
            .unrivalled(epoch, excused)
            .and_then(|()| self.promote(epoch));
        self.succeeding.store(false, Ordering::SeqCst);
        self.notify_settled();
        if let Err(problem) = promoted {
            self.told(&format!("could not succeed its primary: {problem}"));
            self.rest(self.timings.beat);
        }
    }
}
