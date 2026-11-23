//! A primary handing its writes to a standby that asked (`super`): from
//! where the standby is, or the whole of each database first where it
//! cannot take them on from there, then each write as it is kept, and a
//! beat while there is none. A primary stopped in order hands on what it
//! wrote, then says it stopped and who succeeds it, before the connection
//! ends (`super::succession`). What it hands is counted against the
//! standby as each frame is sent (`super::followers`), and each standby that
//! begins or ends following is told to every wait on the primary
//! (`Watched::Standbys`).

use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};

use super::super::wire;
use super::copies::DATABASES;
use super::follow::LAST;
use super::followers::Handed;
use super::succession::Shipping;
use super::{Follow, Shipped, StorageNode};
use crate::encrypted_store::{DATA_KEY, OWN};
use crate::{Engine, PersistError};

/// The most bytes of copies one turn hands on of a database.
const TURN: usize = 4 * 1024 * 1024;

/// The most entries a page of a whole copy carries.
const PAGE: usize = 256;

impl StorageNode {
    /// Hand this node's writes to the standby that asked `asked` on
    /// `connection` — the node its certificate names, where it names one,
    /// from the address it connected from — until it is no longer the
    /// primary, the standby goes or the node or `server` stops; what
    /// declines it is said to it.
    pub(crate) fn ship(
        &self,
        connection: &mut (impl Read + Write),
        asked: &Follow,
        (standby, server): ((Option<&str>, &str), &AtomicBool),
    ) {
        let _shipping = Shipping::of(self);
        let handed = self.followers.began(standby, (asked.next, asked.place));
        self.standbys_moved();
        let shipped = self.shipping(connection, asked, (&handed, server));
        self.followers.ended(&handed);
        self.standbys_moved();
        if let Err(reason) = shipped {
            let _ = wire::send(connection, &Shipped::Declined(reason));
        }
    }

    /// Who follows it changed, told to every wait on its databases while it
    /// is the primary: a node waiting reads its list again
    /// ([`StorageNode::followers`]).
    fn standbys_moved(&self) {
        if let Some(primary) = self.primary() {
            primary.standbys_moved();
        }
    }

    fn shipping(
        &self,
        connection: &mut (impl Read + Write),
        asked: &Follow,
        (handed, server): (&Handed, &AtomicBool),
    ) -> Result<(), String> {
        let epoch = self.held().epoch;
        if !self.is_primary() {
            return Err(self.said());
        }
        if asked.epoch > epoch {
            return Err(format!(
                "a primary of epoch {epoch}, before the epoch {} its standby met",
                asked.epoch
            ));
        }
        let mut next = if self.copies.follows(asked.history, asked.next).is_ok() {
            asked.next
        } else {
            // Copied whole, it holds none of this history's writes yet.
            handed.reached([1; DATABASES]);
            self.whole(connection, (epoch, handed, server))?
        };
        loop {
            // What it waits on read first, then whether it stops: a stop
            // drains, then wakes it, so either it sees the stop or the wake
            // comes after `seen` and its wait returns at once. Read the other
            // way round, a stop between the two was waited out a whole beat.
            let seen = self.copies.count();
            // Its last writes are handed on once no operation is under way,
            // every one synced (`super::succession`).
            let drained = self.drained.load(Ordering::SeqCst);
            let stopping = drained || server.load(Ordering::SeqCst);
            if !self.is_primary() {
                return Err(format!("{} stepped down", self.said()));
            }
            let moved = self.turn(connection, &mut next, handed)?;
            if stopping && !moved {
                // The node stopped in order, not its server alone: it says
                // so, and who succeeds it.
                if drained {
                    self.hand_over(connection, (asked, next), epoch)?;
                }
                return Ok(());
            }
            if !moved {
                self.copies.wait(seen, self.timings.beat);
                if self.copies.count() == seen {
                    send(connection, &Shipped::Beat(epoch))?;
                }
            }
        }
    }

    /// One turn: each write synced since `next`, up to [`TURN`] bytes of each
    /// database, handed on in one write — a frame each, never a write and a
    /// flush each, which cost a standby catching up some 60 µs a write on
    /// Windows, so a primary stopping in order handed on 180 writes behind
    /// for 11 ms before it could say who succeeds it (2026-11-23). Counted
    /// against the standby once written. Whether it handed on any.
    ///
    /// # Errors
    ///
    /// Where the standby is further behind than this node keeps, or went.
    fn turn(
        &self,
        connection: &mut impl Write,
        next: &mut [u64; DATABASES],
        handed: &Handed,
    ) -> Result<bool, String> {
        let (mut frames, mut sizes) = (Vec::new(), Vec::new());
        for (database, next) in next.iter_mut().enumerate() {
            let copies = self.copies.since(database, *next, TURN).map_err(|_| {
                "the standby fell further behind than this Storage node keeps".to_string()
            })?;
            let at = u8::try_from(database).unwrap_or_default();
            for (number, changes) in copies {
                let copy = Shipped::Copy(at, number, changes.to_vec());
                sizes.push(wire::appended(&mut frames, &copy).map_err(|e| e.to_string())?);
                *next = number + 1;
            }
        }
        if !frames.is_empty() {
            connection
                .write_all(&frames)
                .and_then(|()| connection.flush())
                .map_err(|e| format!("the standby went: {e}"))?;
        }
        for bytes in &sizes {
            handed.wrote(*bytes);
        }
        handed.reached(*next);
        Ok(!sizes.is_empty())
    }

    /// The whole of each database handed on, with its data key: where the
    /// standby's copies follow from in each, and the write its replay
    /// reaches before what the pages hold is one state of this node's
    /// (`super::bootstrap`; ADR-0056, amendment 2026-11-12). Each page's
    /// entries are counted against the standby, `handed`, as it is sent.
    /// The node or its `server` stopping cuts it short before the next
    /// page: the standby is copying still, never promoted, and copied whole
    /// again by the primary it follows next. Until 2026-11-22 a primary's
    /// stop lasted as long as the copy it was handing on.
    fn whole(
        &self,
        connection: &mut impl Write,
        (epoch, handed, server): (u64, &Handed, &AtomicBool),
    ) -> Result<[u64; DATABASES], String> {
        let failed = |e: PersistError| e.to_string();
        let stopped = || {
            (self.stopping() || server.load(Ordering::SeqCst))
                .then(|| "this Storage node is stopping: its whole copy is cut short".to_string())
        };
        let mut keys = Vec::with_capacity(DATABASES);
        for engine in &self.engines {
            let wrapped = engine
                .read(DATA_KEY)
                .map_err(failed)?
                .ok_or("a database with no data key")?;
            let key = self
                .keys
                .unwrap(&self.kek, &wrapped)
                .map_err(|e| e.to_string())?;
            keys.push(key.handed().to_vec());
        }
        let history = self.copies.history();
        send(connection, &Shipped::Whole(history, epoch, keys))?;
        let mut from = [1; DATABASES];
        for (database, engine) in self.engines.iter().enumerate() {
            let number = u8::try_from(database).unwrap_or_default();
            from[database] = self.copies.next()[database];
            let mut first = Vec::new();
            loop {
                if let Some(cut) = stopped() {
                    return Err(cut);
                }
                let page = engine.scan(&first, &LAST, PAGE, false).map_err(failed)?;
                let Some((last, _)) = page.last() else {
                    break;
                };
                first = [last.as_slice(), &[0]].concat();
                let entries: Vec<_> = page
                    .into_iter()
                    .filter(|(key, _)| !key.starts_with(OWN))
                    .collect();
                let count = entries.len();
                let bytes = send(connection, &Shipped::Entries(number, entries))?;
                handed.paged(count, bytes);
            }
            // The pages were read while writes went on, so they may reflect a
            // write in part. A write is numbered under the lock it is written
            // under (`Copies::written`), so every write they can reflect is
            // numbered before the next number now: once the standby has
            // replayed up to it, what it holds is one state of this database.
            let through = self.copies.next()[database];
            // Every entry handed on was written before this sync began, so
            // the copy is whole only of what is on disk here.
            self.copies
                .sync(database, engine.as_ref())
                .map_err(failed)?;
            send(
                connection,
                &Shipped::Copied(number, from[database], through),
            )?;
        }
        Ok(from)
    }
}

/// `frame` handed to the standby on `connection`, and how many bytes it
/// took.
pub(super) fn send(connection: &mut impl Write, frame: &Shipped) -> Result<usize, String> {
    wire::sent(connection, frame).map_err(|e| format!("the standby went: {e}"))
}
