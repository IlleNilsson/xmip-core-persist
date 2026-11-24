//! A wait asked of a Storage node (`super::super::watch`): asked round
//! robin as every request is; a standby — and a Storage node that is
//! stopping or the primary no more, which refuses it as one — passed over
//! for the next at once; its answer read for at most its bound and the time
//! any answer may take, and the connection kept only once it answered.
//!
//! The wait names the holder its waiter waits for, so the Storage node does
//! not wake it for what its own node claimed, and how many of what is due
//! in each queue that moved its answer carries (`super::super::watch`).
//!
//! **Ended where it is held.** A waiter stopped or renewed while its wait
//! is asked ends the wait at its Storage node, by the wait's token, asked
//! over another connection ([`super::super::watch::Waiter::ending`]), and
//! its node is answered with none at once. Until 2026-11-12 the node read the
//! answer in looks of 50 ms, whether its waiter stopped, a receive timing
//! out on every look and the connection kept after: a timing assumption no
//! `[tuning]` set, and a connection Windows calls indeterminate once a
//! receive timed out (`SO_RCVTIMEO`), asked for claims and Publications
//! again.

use std::collections::VecDeque;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use xcore::clock::between;
use xcore::lock::Locked;

use super::super::standby::Follower;
use super::super::watch::{Ending, Moved, Position, Waiter};
use super::super::wire::{Answer, Request};
use super::expected::expected;
use super::standbys::Standbys;
use super::{Asked, Connection, StorageClient, again, exchange, unreachable};
use crate::PersistError;

impl StorageClient {
    /// `seen` waited on for at most `bound` as `waiter`, each queue that
    /// moved answered with up to `due` of what is due in it
    /// (`XmipStorage::wait_queues`): asked of the next Storage node, and of
    /// each after it in turn while none answers; none where `waiter` is
    /// stopped or renewed. Beside `seen`, each wait names where the
    /// primary's count of who follows it stands (`super::standbys`): where
    /// that moved, the list is read once of the Storage node that told it,
    /// connections kept to each standby it lists, as many as this client
    /// held to that one at once ([`StorageClient::reach`]), and where
    /// nothing else moved, the wait is asked again for what is left of
    /// `bound`.
    pub(super) fn waited(
        &self,
        seen: &[Position],
        (bound, due): (Duration, u32),
        waiter: &Arc<Waiter>,
    ) -> Result<Vec<Moved>, PersistError> {
        let began = self.clock.monotonic_nanos();
        loop {
            let left = bound.saturating_sub(between(began, self.clock.monotonic_nanos()));
            let mut asked = seen.to_vec();
            asked.push(self.standbys.position());
            let (told, moved) = self.waited_once(&asked, (left, due), waiter)?;
            let (moved, count) = Standbys::parted(moved);
            let (Some(index), Some(at)) = (told, count) else {
                return Ok(moved);
            };
            if self.standbys.moved_to(at) {
                let said = self.standbys_of(index);
                let most = self.nodes[index].most();
                for follower in said.iter().flat_map(|(_, followers)| followers) {
                    if let Some(place) = follower.place.and_then(|p| usize::try_from(p).ok()) {
                        self.reach(place, most);
                    }
                }
                self.standbys.heard(at, said);
            }
            if !moved.is_empty() || left.is_zero() {
                return Ok(moved);
            }
        }
    }

    /// `most` live connections kept to the Storage node listed at `place`,
    /// its primary says follows it — as many as this client held to the
    /// primary at once — on a thread of its own, never
    /// the wait's nor a Message's: each kept one asked what it is, one that
    /// went with a process since restarted let go, and new ones opened,
    /// kept though it says it is a standby (`Node::held`). So where it
    /// succeeds its primary stopped in order, every thread of the node asks
    /// it at once, none opening a connection: a full handshake each, some
    /// 1.8 ms on Linux and 4 to 8 ms on Windows (ADR-0056, amendments
    /// 2026-11-23 and 2026-11-24; open problem 142). Until 2026-11-24 one
    /// connection was kept, opened on the wait's thread.
    fn reach(&self, place: usize, most: usize) {
        if place >= self.nodes.len() {
            return;
        }
        let client = self.pinned_to(place);
        let _ = std::thread::Builder::new()
            .name("xmip-storage-reach".to_string())
            .spawn(move || client.kept_to(place, most.max(1)));
    }

    /// [`StorageClient::reach`]'s work, on its own thread: what it keeps to
    /// the Storage node listed at `place` asked what it is, and opened anew
    /// until it keeps `most`.
    fn kept_to(&self, place: usize, most: usize) {
        let node = &self.nodes[place];
        let kept: Vec<Connection> = std::mem::take(&mut *node.idle.locked());
        let mut live: Vec<Connection> = kept
            .into_iter()
            .filter_map(|mut connection| match self.standing(&mut connection) {
                Ok(()) | Err(Asked::Standby(_)) => Some(connection),
                Err(Asked::Failed(_)) => None,
            })
            .collect();
        let mut asked = Ok(());
        while live.len() < most {
            match self.opened(node) {
                Ok((connection, Ok(()))) => {
                    asked = Ok(());
                    live.push(connection);
                }
                Ok((connection, Err(Asked::Standby(said)))) => {
                    asked = Err(Asked::Standby(said));
                    live.push(connection);
                }
                Ok((_, Err(failed))) | Err(failed) => {
                    asked = Err(failed);
                    break;
                }
            }
        }
        node.idle.locked().extend(live);
        node.answered(&asked, self.pass_over, self.clock.monotonic_nanos());
    }

    /// Who follows the Storage node listed at `index`, as it says now, and
    /// where it is listed.
    fn standbys_of(&self, index: usize) -> Option<(usize, Vec<Follower>)> {
        match self.ask_node(&self.nodes[index], &Request::Standbys) {
            Ok(Answer::Standbys(followers)) => Some((index, followers)),
            _ => None,
        }
    }

    /// One wait, as [`StorageClient::waited`] asks it, and which Storage
    /// node listed answered it; none where `waiter` is stopped or renewed.
    fn waited_once(
        &self,
        seen: &[Position],
        (bound, due): (Duration, u32),
        waiter: &Arc<Waiter>,
    ) -> Result<(Option<usize>, Vec<Moved>), PersistError> {
        let token = u128::from_be_bytes(codec::random::array());
        let request = Request::wait((token, waiter.holder()), seen, (bound, due));
        let within = bound.saturating_add(self.timeout);
        let read = |connection: &mut Connection, request: &Request| {
            awaited(connection, request, (within, self.timeout))
        };
        let mut order = match self.pinned {
            Some(index) => VecDeque::from([(index, false)]),
            None => self.order(),
        };
        let mut failures = Vec::new();
        while let Some((index, first)) = order.pop_front() {
            if !waiter.ending(self.ending(index, token)) {
                return Ok((None, Vec::new()));
            }
            let node = &self.nodes[index];
            let asked = self.ask_node_by(node, &request, &read);
            waiter.ended();
            if waiter.is_stopped() {
                return Ok((None, Vec::new()));
            }
            node.answered(&asked, self.pass_over, self.clock.monotonic_nanos());
            match asked {
                Ok(answer) => {
                    let moved = expected(answer, |answer| match answer {
                        Answer::Moved(moved) => Ok(moved),
                        other => Err(other),
                    })?;
                    return Ok((Some(index), moved));
                }
                Err(failure) => {
                    again(&mut order, (index, first), &failure);
                    failures.push(format!("{}: {failure}", node.address));
                }
            }
        }
        Err(unreachable(format!(
            "no Storage node answered a wait: {}",
            failures.join("; ")
        )))
    }

    /// What ends the wait asked with `token` of the Storage node listed at
    /// `index` as its waiter stops: that end asked over another connection,
    /// on a thread of its own, so a stop waits on no Storage node.
    fn ending(&self, index: usize, token: u128) -> Ending {
        let client = self.pinned_to(index);
        Box::new(move || {
            let _ = std::thread::Builder::new()
                .name("xmip-storage-end-wait".to_string())
                .spawn(move || {
                    let _ = client.ask(&Request::EndWait(token));
                });
        })
    }
}

/// `request` sent on `connection` and its answer read for at most `within`,
/// its reads bounded by `timeout` again once answered.
fn awaited(
    connection: &mut Connection,
    request: &Request,
    (within, timeout): (Duration, Duration),
) -> io::Result<Answer> {
    connection.sock.set_read_timeout(Some(within))?;
    let answer = exchange(connection, request)?;
    connection.sock.set_read_timeout(Some(timeout))?;
    Ok(answer)
}
