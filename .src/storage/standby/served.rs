//! What a Storage node of several is to those asking it (`super`): what it
//! stands as, its databases lent to one operation at a time while it is the
//! primary, Xmip Storage as it serves the others and as its own node reaches
//! it, and what it says of itself.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use xcore::lock::Locked;

use super::super::XmipStorage;
use super::super::client::StorageClient;
use super::super::schema::DatabaseVersion;
use super::{DATABASES, Delegated, Follower, Lent, Role, Standing, StorageNode};
use crate::PersistError;

/// One operation's loan of a primary's databases, given back when the
/// operation ends.
struct Lending(Arc<StorageNode>);

impl Drop for Lending {
    fn drop(&mut self) {
        if self.0.lent.fetch_sub(1, Ordering::SeqCst) == 1 && self.0.stopping() {
            self.0.notify_settled();
        }
    }
}

impl StorageNode {
    /// What it is now, as it answers anyone asking: a primary that is
    /// stopping is the primary no more.
    #[must_use]
    pub fn standing(&self) -> Standing {
        let held = self.held();
        Standing {
            process: self.process,
            primary: matches!(held.role, Role::Primary(_)) && !self.stopping(),
            epoch: held.epoch,
            history: self.copies.history(),
            next: self.copies.next(),
            bootstrap: self.bootstrap(),
        }
    }

    /// What it is, as a node or another Storage node asking it is answered:
    /// a successor between its answer that it kept its epoch and serving
    /// answers once it serves, or cannot, so a node asking it on the
    /// connection it kept is answered by the primary it turned into, never
    /// told it is a standby and failed (open problem 139).
    #[must_use]
    pub fn standing_asked(&self) -> Standing {
        self.held_while_succeeding();
        self.standing()
    }

    /// Held while it succeeds its primary, and is not serving yet: from its
    /// answer that it kept its epoch until it serves or cannot, the silence
    /// at the most.
    fn held_while_succeeding(&self) {
        if self.succeeding.load(Ordering::SeqCst) {
            self.settle_while(self.timings.silence, || {
                self.succeeding.load(Ordering::SeqCst) && !self.is_primary()
            });
        }
    }

    /// Whether it is a standby its primary hands writes to now: it stands by
    /// a primary, and has been handed a frame on its connection to it — a
    /// beat, a write or a page — so that primary's stop in order hands over
    /// to it (`super::succession`). One that stood by and was handed nothing
    /// yet is not.
    #[must_use]
    pub fn is_following(&self) -> bool {
        matches!(self.held().role, Role::Standby(_)) && self.followed.heard()
    }

    /// Its own databases lent for one operation, while it is the primary
    /// and not stopping: held until the operation ends ([`Lent`]). Asked of
    /// a successor not serving yet, it waits until it serves; asked of one
    /// stopping, it waits until its stop has said who succeeds it, its loan
    /// given back first so the drain does not wait for it, and is refused
    /// then, in words naming that one, so the node asks it next and its
    /// operation is answered, never failed through the drain (open problem
    /// 139).
    pub(super) fn lent(self: &Arc<Self>) -> Option<Lent> {
        self.held_while_succeeding();
        self.lent.fetch_add(1, Ordering::SeqCst);
        let lending = Lending(Arc::clone(self));
        if self.stopping() {
            drop(lending);
            self.settle_while(self.timings.silence, || !self.handed.load(Ordering::SeqCst));
            return None;
        }
        let primary = self.primary()?;
        Some(Lent {
            storage: primary,
            held: Some(Box::new(lending)),
        })
    }

    /// Xmip Storage as it serves a node asking it: its own databases while
    /// it is the primary, and a refusal naming its standing while it is not.
    #[must_use]
    pub fn served(self: &Arc<Self>) -> Arc<dyn XmipStorage> {
        let node = Arc::clone(self);
        Arc::new(Delegated(Box::new(move || {
            node.lent().ok_or_else(|| PersistError::Standby {
                reason: node.said(),
            })
        })))
    }

    /// Xmip Storage as the node it runs in reaches it: its own databases
    /// while it is the primary, the primary over the wire while not.
    #[must_use]
    pub fn reaching(self: &Arc<Self>) -> Arc<dyn XmipStorage> {
        let node = Arc::clone(self);
        Arc::new(Delegated(Box::new(move || {
            Ok(node
                .lent()
                .unwrap_or_else(|| Lent::free(Arc::clone(&node.reaching) as _)))
        })))
    }

    /// The client its own work reaches the primary through while it is not
    /// one: what it says of each Storage node it asked.
    #[must_use]
    pub fn client(&self) -> &StorageClient {
        &self.reaching
    }

    /// The schema version each database holds, while it is the primary.
    #[must_use]
    pub fn versions(&self) -> Option<[DatabaseVersion; DATABASES]> {
        self.primary().map(|primary| *primary.versions())
    }

    /// Each standby it hands its writes to now, while it is the primary, by
    /// the node its certificate names, with what it handed each and how far
    /// behind each is (ADR-0052 and ADR-0056, amendments 2026-11-13): what
    /// its standing says, and what its node draws; none while it is not the
    /// primary.
    #[must_use]
    pub fn followers(&self) -> Vec<Follower> {
        if self.is_primary() {
            self.followers.following(self.copies.next())
        } else {
            Vec::new()
        }
    }

    /// What it is now, in words: what a node asking a standby is told, and
    /// what the node publishes of its Storage — while not the primary, how
    /// far its bootstrap is: copying, catching up or ready (ADR-0056,
    /// amendment 2026-11-12).
    #[must_use]
    pub fn said(&self) -> String {
        let held = self.held();
        let next = self.copies.next();
        let writes: u64 = next.iter().map(|next| next - 1).sum();
        let bootstrap = self.bootstrap().said(next);
        let what = match &held.role {
            Role::Finding => format!(
                "a Storage node finding its primary among {}, epoch {}, {bootstrap}",
                self.listed.join(", "),
                held.epoch
            ),
            Role::Primary(_) => {
                let behind: Vec<String> = self
                    .followers
                    .following(next)
                    .iter()
                    .map(Follower::said)
                    .collect();
                let standbys = if behind.is_empty() {
                    "no standby following".to_string()
                } else {
                    format!("standbys {}", behind.join(", "))
                };
                format!("the primary Storage node, epoch {}, {standbys}", held.epoch)
            }
            Role::Standby(primary) => format!(
                "a standby Storage node of the primary at {primary}, epoch {}, {bootstrap}{}",
                held.epoch,
                if self.followed.heard() {
                    ""
                } else {
                    ", handed nothing yet"
                }
            ),
        };
        let last = self.last.locked();
        match last.as_deref() {
            Some(last) => format!("{what}, {writes} writes held; {last}"),
            None => format!("{what}, {writes} writes held"),
        }
    }
}
