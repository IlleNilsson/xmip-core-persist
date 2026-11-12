//! Where the one writer left each thing a wait watches (`super`), kept so a
//! wait reads the store only for what is not kept: everything waited on
//! now, and of what none waits on now the latest let go of, at most
//! [`UNWAITED`] — the Storage node's `[tuning] storage_unwaited` — the
//! longest unwaited let go first. Until 2026-11-12 every Instance, key and
//! sequence ever waited on stayed kept as long as the node ran, so what it
//! held grew with every conversation it carried, never with its waits
//! (the review of 2026-11-12; ADR-0018, amendment 2026-11-12).
//!
//! **Let go only where none waits on it.** A count waited on was listed
//! before the store was read, and is kept as its writer tells it or the
//! read found it, a tell winning; let go between the two it would come back
//! as the read found it, older than a write told meanwhile. What none waits
//! on is only a cache of where it stands: let go, the next wait on it reads
//! the store as the first wait on it ever did, listed first, so a write
//! between the two is read or told.

use std::collections::{BTreeMap, HashMap};

use super::Watched;

/// How many of what no node waits on now a Storage node keeps where its
/// writer left it, where its `[tuning] storage_unwaited` does not say:
/// some 200 bytes each, about 3 MB in all, far above the queues and heads
/// a cluster's steps wait on again at every wake.
pub const UNWAITED: usize = 16_384;

/// Where its writer left each waited on now, and the latest none waits on.
#[derive(Debug)]
pub(super) struct Known {
    kept: HashMap<Watched, Kept>,
    /// What none waits on now, by when the last waiting on it let go: the
    /// first, the longest unwaited.
    unwaited: BTreeMap<u64, Watched>,
    /// The next of those.
    next: u64,
    /// The most of them kept.
    most: usize,
}

#[derive(Clone, Copy, Debug)]
struct Kept {
    /// Where its writer last left it, or a wait read it where its writer
    /// told nothing since; `None` while it is to be read.
    at: Option<u64>,
    /// When the last waiting on it let go, where none waits on it now.
    unwaited: Option<u64>,
}

impl Default for Known {
    fn default() -> Self {
        Self {
            kept: HashMap::new(),
            unwaited: BTreeMap::new(),
            next: 0,
            most: UNWAITED,
        }
    }
}

impl Known {
    /// `watched` waited on: kept as long as any waits on it; whether it is
    /// to be read.
    pub(super) fn listed(&mut self, watched: Watched) -> bool {
        let kept = self.kept.entry(watched).or_insert(Kept {
            at: None,
            unwaited: None,
        });
        if let Some(since) = kept.unwaited.take() {
            self.unwaited.remove(&since);
        }
        kept.at.is_none()
    }

    /// Where `watched` stands, where it is kept.
    pub(super) fn at(&self, watched: Watched) -> Option<u64> {
        self.kept.get(&watched).and_then(|kept| kept.at)
    }

    /// `watched` left at `at` by its writer — `None` where it could not be
    /// read, read again by the next wait on it — where it is kept.
    pub(super) fn told(&mut self, watched: Watched, at: Option<u64>) {
        if let Some(kept) = self.kept.get_mut(&watched) {
            kept.at = at;
        }
    }

    /// `watched` read at `at` by a wait listed on it: kept unless its
    /// writer told it since, which is later than the read.
    pub(super) fn read(&mut self, watched: Watched, at: u64) {
        if let Some(kept) = self.kept.get_mut(&watched) {
            kept.at.get_or_insert(at);
        }
    }

    /// None waits on `watched` now: let go at once where nothing is kept of
    /// it, else kept as the latest unwaited, the longest unwaited let go
    /// past the most.
    pub(super) fn unwaited(&mut self, watched: Watched) {
        let Some(kept) = self.kept.get_mut(&watched) else {
            return;
        };
        if kept.at.is_none() {
            self.kept.remove(&watched);
            return;
        }
        let since = self.next;
        self.next += 1;
        if let Some(earlier) = kept.unwaited.replace(since) {
            self.unwaited.remove(&earlier);
        }
        self.unwaited.insert(since, watched);
        self.trim();
    }

    /// At most `most` of what none waits on kept from now on.
    pub(super) fn keep(&mut self, most: usize) {
        self.most = most;
        self.trim();
    }

    /// The longest unwaited let go until at most the most are kept: each
    /// only where its stamp is still its own, so one waited on is never.
    fn trim(&mut self) {
        while self.unwaited.len() > self.most {
            let Some((since, watched)) = self.unwaited.pop_first() else {
                return;
            };
            if self
                .kept
                .get(&watched)
                .is_some_and(|kept| kept.unwaited == Some(since))
            {
                self.kept.remove(&watched);
            }
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
impl Known {
    /// How many are kept, and of them how many none waits on.
    pub(super) fn kept(&self) -> (usize, usize) {
        (self.kept.len(), self.unwaited.len())
    }
}
