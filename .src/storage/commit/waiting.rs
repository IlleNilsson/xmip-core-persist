//! The writes waiting for the embedded Storage node's next batch, and who
//! decides it: the caller that finds none being decided leads, on its own
//! thread, every write waiting then as one batch, and those that arrive
//! meanwhile wait for the next (`super`; group commit, as `RocksDB`'s own
//! write group is led). Until 2026-11-01 a writer thread of its own
//! decided every batch, so each write woke a thread whose processor's
//! caches had gone cold, and a lone Message's Publication cost about a
//! quarter more (open problem 33).
//!
//! **A leader leads until its own write is answered**, then hands the lead
//! to the first write still waiting, whose caller it wakes to lead the
//! next batch, so no caller decides others' writes for long. A batch that
//! panics answers its writes with an error and the writer reads its audit
//! sequence again from what was written; the lead goes on, and no caller
//! waits for an answer that never comes.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError};
use std::sync::{Mutex, MutexGuard, PoisonError};

use super::super::durable::Durable;
use super::{Done, GROUP, Op};
use crate::PersistError;

/// What a write's caller is told: what its write decided, or to lead.
pub(crate) enum Answer {
    Decided(Result<(Done, Durable), PersistError>),
    Lead,
}

/// Where a write's caller is told ([`Answer`]).
pub(crate) type Reply = SyncSender<Answer>;

/// The one writer: what decides a batch of writes and writes it.
pub(super) trait Decides {
    /// `group` decided and written, each write answered on its reply.
    fn commit(&mut self, group: Vec<(Op, Reply)>);

    /// After a batch that panicked: what the writer keeps in memory read
    /// again from what was written.
    fn recover(&mut self);
}

/// The writes waiting for a batch, and the writer that decides them.
pub(super) struct Waiting {
    queue: Mutex<Queue>,
    writer: Mutex<Box<dyn Decides + Send>>,
}

#[derive(Default)]
struct Queue {
    writes: Vec<(Op, Reply)>,
    /// Whether a caller leads: decides batches, or has been told to.
    leading: bool,
}

impl Waiting {
    pub(super) fn new(writer: Box<dyn Decides + Send>) -> Self {
        Self {
            queue: Mutex::default(),
            writer: Mutex::new(writer),
        }
    }

    /// `op` decided in a batch, led from this thread where none is being
    /// decided: what it decided, and when it is durable.
    pub(super) fn decided(&self, op: Op) -> Result<(Done, Durable), PersistError> {
        let (reply, answer) = std::sync::mpsc::sync_channel(1);
        let leads = {
            let mut queue = self.queue();
            queue.writes.push((op, reply));
            !std::mem::replace(&mut queue.leading, true)
        };
        if leads && let Some(decided) = self.lead(&answer) {
            return decided;
        }
        loop {
            match answer.recv() {
                Ok(Answer::Decided(decided)) => return decided,
                Ok(Answer::Lead) => {
                    if let Some(decided) = self.lead(&answer) {
                        return decided;
                    }
                }
                Err(_) => return Err(failed()),
            }
        }
    }

    /// Every write waiting decided, a batch at a time, until this caller's
    /// own, on `answer`, is answered: then the lead handed on. `None` where
    /// the queue was empty before its own was answered, which cannot be
    /// while it waits in it.
    fn lead(&self, answer: &Receiver<Answer>) -> Option<Result<(Done, Durable), PersistError>> {
        let mut writer = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        loop {
            let group = {
                let mut queue = self.queue();
                if queue.writes.is_empty() {
                    queue.leading = false;
                    return None;
                }
                if queue.writes.len() <= GROUP {
                    std::mem::take(&mut queue.writes)
                } else {
                    queue.writes.drain(..GROUP).collect()
                }
            };
            // A batch that panics has dropped its replies, each its
            // caller's error; the writer is read again and leads on.
            if catch_unwind(AssertUnwindSafe(|| writer.commit(group))).is_err() {
                writer.recover();
            }
            let own = match answer.try_recv() {
                Ok(Answer::Decided(decided)) => decided,
                Ok(Answer::Lead) | Err(TryRecvError::Empty) => continue,
                Err(TryRecvError::Disconnected) => Err(failed()),
            };
            drop(writer);
            self.hand_on();
            return Some(own);
        }
    }

    /// The lead to the first write waiting, whose caller is woken to lead;
    /// none leads where none waits.
    fn hand_on(&self) {
        let mut queue = self.queue();
        while let Some((_, reply)) = queue.writes.first() {
            // Its caller waits on it with nothing sent yet, so it has room.
            if reply.try_send(Answer::Lead).is_ok() {
                return;
            }
            // Its caller is gone: its write is not decided.
            queue.writes.remove(0);
        }
        queue.leading = false;
    }

    fn queue(&self) -> MutexGuard<'_, Queue> {
        self.queue.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The answer of a write whose batch failed before it was answered.
fn failed() -> PersistError {
    PersistError::engine(
        "xmip-storage",
        "the writer failed deciding the batch this write was in",
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use super::*;

    /// A writer that keeps the size of every batch it is given, holds the
    /// first until told, and panics on a batch holding `Op::Kept(PANICS)`.
    struct Counting {
        groups: Arc<Mutex<Vec<usize>>>,
        first: Option<Receiver<()>>,
        recovered: Arc<AtomicUsize>,
    }

    const PANICS: u64 = 666;

    impl Decides for Counting {
        fn commit(&mut self, group: Vec<(Op, Reply)>) {
            if let Some(go) = self.first.take() {
                go.recv().expect("told to go on");
            }
            self.groups.lock().expect("groups").push(group.len());
            assert!(
                !group.iter().any(|(op, _)| matches!(op, Op::Kept(PANICS))),
                "a batch that panics"
            );
            for (_, reply) in group {
                let _ = reply.send(Answer::Decided(Ok((Done::Written, Durable::kept()))));
            }
        }

        fn recover(&mut self) {
            self.recovered.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn waiting(
        first: Option<Receiver<()>>,
    ) -> (Arc<Waiting>, Arc<Mutex<Vec<usize>>>, Arc<AtomicUsize>) {
        let (groups, recovered) = (Arc::default(), Arc::default());
        let writer = Counting {
            groups: Arc::clone(&groups),
            first,
            recovered: Arc::clone(&recovered),
        };
        (Arc::new(Waiting::new(Box::new(writer))), groups, recovered)
    }

    #[test]
    fn writes_arriving_while_a_batch_is_decided_share_the_next_one() {
        let (go, held) = std::sync::mpsc::channel();
        let (waiting, groups, _) = waiting(Some(held));
        let decide = |waiting: &Arc<Waiting>| {
            let waiting = Arc::clone(waiting);
            std::thread::spawn(move || waiting.decided(Op::Kept(1)).map(drop))
        };
        let leader = decide(&waiting);
        // The leader holds its batch, its own write taken, until all eight
        // others wait for the next.
        let deadline = Instant::now() + Duration::from_secs(10);
        while !waiting.queue().leading || !waiting.queue().writes.is_empty() {
            assert!(Instant::now() < deadline, "the leader leads");
            std::thread::yield_now();
        }
        let others: Vec<_> = (0..8).map(|_| decide(&waiting)).collect();
        while waiting.queue().writes.len() < 8 {
            assert!(Instant::now() < deadline, "eight wait");
            std::thread::yield_now();
        }
        go.send(()).expect("goes on");
        leader.join().expect("the leader").expect("decided");
        for other in others {
            other.join().expect("a writer").expect("decided");
        }
        assert_eq!(
            *groups.lock().expect("groups"),
            [1, 8],
            "eight in one batch"
        );
        assert!(!waiting.queue().leading, "none leads once none waits");
    }

    #[test]
    fn a_batch_that_panics_answers_its_writes_and_the_next_is_decided() {
        let (waiting, groups, recovered) = waiting(None);
        assert!(
            waiting.decided(Op::Kept(PANICS)).is_err(),
            "answered, failed"
        );
        assert_eq!(
            recovered.load(Ordering::Relaxed),
            1,
            "the writer read again"
        );
        assert!(waiting.decided(Op::Kept(1)).is_ok(), "the next decided");
        assert_eq!(*groups.lock().expect("groups"), [1, 1]);
    }
}
