//! A primary handing its writes to a standby that asked (`super`): from
//! where the standby is, or the whole of each database first where it
//! cannot take them on from there, then each write as it is kept, and a
//! beat while there is none. A primary stopped in order hands on what it
//! wrote before the connection ends.

use std::io::Write;
use std::sync::PoisonError;
use std::sync::atomic::{AtomicBool, Ordering};

use super::super::wire;
use super::copies::DATABASES;
use super::follow::LAST;
use super::{Follow, Shipped, StorageNode};
use crate::encrypted_store::{DATA_KEY, OWN};
use crate::{Engine, PersistError};

/// The most bytes of copies one turn hands on of a database.
const TURN: usize = 4 * 1024 * 1024;

/// The most entries a page of a whole copy carries.
const PAGE: usize = 256;

impl StorageNode {
    /// Hand this node's writes to the standby `peer` that asked `asked` on
    /// `connection`, until it is no longer the primary, the standby goes
    /// or the node or `server` stops; what declines it is said to it.
    pub(crate) fn ship(
        &self,
        connection: &mut impl Write,
        asked: &Follow,
        (peer, server): (&str, &AtomicBool),
    ) {
        let shipped = self.shipping(connection, asked, (peer, server));
        self.followers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(peer);
        if let Err(reason) = shipped {
            let _ = wire::send(connection, &Shipped::Declined(reason));
        }
    }

    fn shipping(
        &self,
        connection: &mut impl Write,
        asked: &Follow,
        (peer, server): (&str, &AtomicBool),
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
        let mut next = match self.copies.follows(asked.history, asked.next) {
            Ok(()) => asked.next,
            Err(_) => self.whole(connection, epoch)?,
        };
        loop {
            // Its last writes are handed on once no operation is under way.
            let stopping = self.drained.load(Ordering::SeqCst) || server.load(Ordering::SeqCst);
            if !self.is_primary() {
                return Err(format!("{} stepped down", self.said()));
            }
            let seen = self.copies.count();
            let mut handed = false;
            for (database, next) in next.iter_mut().enumerate() {
                let copies = self.copies.since(database, *next, TURN).map_err(|_| {
                    "the standby fell further behind than this Storage node keeps".to_string()
                })?;
                for (number, changes) in copies {
                    let database = u8::try_from(database).unwrap_or_default();
                    send(
                        connection,
                        &Shipped::Copy(database, number, changes.to_vec()),
                    )?;
                    *next = number + 1;
                    handed = true;
                }
            }
            self.followers
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(peer.to_string(), next);
            if stopping && !handed {
                return Ok(());
            }
            if !handed {
                self.copies.wait(seen, self.timings.beat);
                if self.copies.count() == seen {
                    send(connection, &Shipped::Beat(epoch))?;
                }
            }
        }
    }

    /// The whole of each database handed on, with its data key: where the
    /// standby's copies follow from in each.
    fn whole(&self, connection: &mut impl Write, epoch: u64) -> Result<[u64; DATABASES], String> {
        let failed = |e: PersistError| e.to_string();
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
                let page = engine.scan(&first, &LAST, PAGE, false).map_err(failed)?;
                let Some((last, _)) = page.last() else {
                    break;
                };
                first = [last.as_slice(), &[0]].concat();
                let entries = page
                    .into_iter()
                    .filter(|(key, _)| !key.starts_with(OWN))
                    .collect();
                send(connection, &Shipped::Entries(number, entries))?;
            }
            send(connection, &Shipped::Copied(number, from[database]))?;
        }
        Ok(from)
    }
}

/// `frame` handed to the standby on `connection`.
fn send(connection: &mut impl Write, frame: &Shipped) -> Result<(), String> {
    wire::send(connection, frame).map_err(|e| format!("the standby went: {e}"))
}
