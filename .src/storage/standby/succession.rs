//! A primary stopped in order hands over to a standby that succeeds it at
//! once (the owner, 2026-11-23: *That has to be sorted, nothing can be
//! left*; ADR-0056, amendment 2026-11-23). Until that day the others waited
//! for it to come back, as for one gone, and every node's writes failed
//! while it restarted: 2.5 s and 5.2 s in the Hyper-V environment's rolling
//! restart of 2026-11-22 (open problem 92).
//!
//! **The primary** stops serving, waits for every operation lent, and syncs
//! what its standbys could not be handed yet. Each connection it hands
//! writes on hands the last of them, then says it stopped
//! ([`Shipped::Stopped`]): its epoch, the next write of each database, and
//! its successor — the first standby handed every write, named by its place
//! among the Storage nodes listed. Its stop waits for each connection to
//! have said it, and for its successor's answer, so it never ends before
//! its successor has kept the epoch it is promoted in: started again, it
//! finds that epoch kept and never takes itself for the one to promote. It
//! then keeps its positions again, so once back it follows its successor on
//! from where it stopped, never copied whole.
//!
//! **The successor** syncs what it took and, where it is ready, holds every
//! write its primary said and is of its epoch, keeps the next epoch,
//! durable, and says so ([`Succeeding::Kept`]), or why not. It is promoted
//! where every other Storage node listed answers, none is a primary and
//! none other holds that epoch or a later one: the primary that handed over
//! is not asked, its hand-over its answer, so two Storage nodes ask nobody.
//! A standby told another succeeds rests a beat before it asks, and finds
//! the successor the primary.
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
use super::{Follow, Shipped, StorageNode, Succeeding};
use crate::borrowed;

/// How following a primary ended (`super::follow`).
pub(super) enum Ended {
    /// It was lost: the node stops, the primary fell silent, declined or
    /// ended the connection, or stopped in order naming no successor.
    Lost,
    /// The primary stopped in order and named this Storage node its
    /// successor, and it kept this epoch, to be promoted in.
    Succeeding(u64),
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
            let _settling = self.0.settling.locked();
            self.0.settled.notify_all();
        }
    }
}

impl StorageNode {
    /// Stop: serve no operation more, wait for each one under way, then hand
    /// every standby what it wrote and, a primary, hand over to the one that
    /// succeeds it, its own positions kept after; and stop settling, a
    /// standby's read of its primary's next frame ended at once.
    pub fn stop(&self) {
        self.stopping.store(true, Ordering::SeqCst);
        self.followed.shut();
        let primary = self.primary();
        // No wait holds a loan: each one on its queues is ended now, so the
        // node waiting asks the next Storage node at once.
        if let Some(primary) = &primary {
            primary.refuse_waits("this Storage node is stopping");
        }
        self.settle_while(self.timings.timeout, || {
            self.lent.load(Ordering::SeqCst) > 0
        });
        // Every write synced, so every one may be handed on. An operation
        // still under way past the bound may write yet: then no positions
        // are kept, and started again it is copied whole, as one killed is.
        let drained = self.lent.load(Ordering::SeqCst) == 0;
        let synced = primary.is_some() && self.synced_all() && drained;
        self.drained.store(true, Ordering::SeqCst);
        {
            let _settling = self.settling.locked();
            self.settled.notify_all();
        }
        // Each standby's shipping hands on the last writes now, not a beat on.
        self.copies.wake();
        self.settle_while(self.timings.silence, || {
            self.shipping.load(Ordering::SeqCst) > 0
        });
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

    /// The last frame a primary stopped in order hands the standby that
    /// `asked` on `connection`, handed every write before `handed`: that it
    /// stopped, in `epoch`, the next write of each database, and its
    /// successor — this standby, where it is the first handed every write
    /// and said its place, or the one named before. Where this one succeeds
    /// it, its answer is waited for, within the silence, and said.
    ///
    /// # Errors
    ///
    /// Where the frame could not be sent: the standby went.
    pub(super) fn hand_over(
        &self,
        connection: &mut (impl Read + Write),
        (asked, handed): (&Follow, [u64; DATABASES]),
        epoch: u64,
    ) -> Result<(), String> {
        let next = self.copies.next();
        let successor = {
            let mut named = self.successor.locked();
            if named.is_none() && handed == next {
                *named = asked.place;
            }
            *named
        };
        send(connection, &Shipped::Stopped(epoch, next, successor))?;
        let Some(place) = successor.filter(|_| successor == asked.place) else {
            return Ok(());
        };
        let address = usize::try_from(place)
            .ok()
            .and_then(|place| self.listed.get(place))
            .map_or("its successor", String::as_str);
        let mut answering = Watched {
            silence: Some(self.timings.silence),
            ..Watched::new(connection, || false, || self.clock.monotonic_nanos())
        };
        let said = match wire::receive::<Succeeding>(&mut answering) {
            Ok(Some(Succeeding::Kept(kept))) => {
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
    /// epoch kept, durable; its primary told on `connection` either way.
    /// Another named, it defers to that one.
    pub(super) fn succeeding(
        &self,
        connection: &mut impl Write,
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
            Ok(()) => Succeeding::Kept(epoch + 1),
            Err(why) => Succeeding::Declined(why.clone()),
        };
        let _ = wire::send(connection, &answer);
        match kept {
            Ok(()) => Ended::Succeeding(epoch + 1),
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

    /// Promoted in `epoch`, which it kept as its primary, at `primary`
    /// among those listed, handed over to it: where every other one listed
    /// answers, none is a primary and none other holds `epoch` or a later
    /// one, the primary that handed over not asked
    /// ([`StorageNode::unrivalled`]). One that cannot be says why and rests a
    /// beat, then settles as ever.
    pub(super) fn succeed(&self, epoch: u64, primary: usize) {
        let promoted = self
            .unrivalled(epoch, Some(primary))
            .and_then(|()| self.promote(epoch));
        if let Err(problem) = promoted {
            self.told(&format!("could not succeed its primary: {problem}"));
            self.rest(self.timings.beat);
        }
    }
}
