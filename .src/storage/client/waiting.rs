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

use std::io;
use std::sync::Arc;
use std::time::Duration;
use xcore::clock::between;

use super::super::standby::Follower;
use super::super::watch::{Ending, Moved, Position, Waiter};
use super::super::wire::{Answer, Request};
use super::expected::expected;
use super::standbys::Standbys;
use super::{Connection, StorageClient, exchange, unreachable};
use crate::PersistError;

impl StorageClient {
    /// `seen` waited on for at most `bound` as `waiter`, each queue that
    /// moved answered with up to `due` of what is due in it
    /// (`XmipStorage::wait_queues`): asked of the next Storage node, and of
    /// each after it in turn while none answers; none where `waiter` is
    /// stopped or renewed. Beside `seen`, each wait names where the
    /// primary's count of who follows it stands (`super::standbys`): where
    /// that moved, the list is read once of the Storage node that told it,
    /// a connection kept to each standby it lists ([`StorageClient::reach`]),
    /// and where nothing else moved, the wait is asked again for what is
    /// left of `bound`.
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
                for follower in said.iter().flat_map(|(_, followers)| followers) {
                    if let Some(place) = follower.place.and_then(|p| usize::try_from(p).ok()) {
                        self.reach(place);
                    }
                }
                self.standbys.heard(at, said);
            }
            if !moved.is_empty() || left.is_zero() {
                return Ok(moved);
            }
        }
    }

    /// A live connection kept to the Storage node listed at `place`, its
    /// primary says follows it: what it is asked on the connection kept,
    /// opened afresh where that one went with a process since restarted —
    /// here, on the wait, never on a Message's path — and kept though it says
    /// it is a standby (`Node::held`). So where it succeeds its primary
    /// stopped in order, the node asks it on it at once, with no new
    /// handshake (ADR-0056, amendment 2026-11-23): some 1.8 ms on Linux and
    /// 4 to 8 ms on Windows of every write's wait across that stop.
    fn reach(&self, place: usize) {
        if let Some(node) = self.nodes.get(place) {
            let asked = self.ask_node(node, &Request::Standing);
            node.answered(&asked, self.pass_over, self.clock.monotonic_nanos());
        }
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
        let order: Vec<usize> = match self.pinned {
            Some(index) => vec![index],
            None => self.turn().collect(),
        };
        let mut failures = Vec::new();
        for index in order {
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
                Err(failure) => failures.push(format!("{}: {failure}", node.address)),
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
