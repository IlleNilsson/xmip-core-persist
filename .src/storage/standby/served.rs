//! What a Storage node of several is to those asking it (`super`): what it
//! stands as, its databases lent to one operation at a time while it is the
//! primary, Xmip Storage as it serves the others and as its own node reaches
//! it, and what it says of itself.

use std::sync::atomic::Ordering;
use std::sync::{Arc, PoisonError};

use super::super::XmipStorage;
use super::super::client::StorageClient;
use super::super::schema::DatabaseVersion;
use super::{DATABASES, Delegated, Lent, Role, Standing, StorageNode};
use crate::PersistError;

/// One operation's loan of a primary's databases, given back when the
/// operation ends.
struct Lending(Arc<StorageNode>);

impl Drop for Lending {
    fn drop(&mut self) {
        if self.0.lent.fetch_sub(1, Ordering::SeqCst) == 1 && self.0.stopping() {
            let _settling = self
                .0
                .settling
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            self.0.settled.notify_all();
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
        }
    }

    /// Its own databases lent for one operation, while it is the primary
    /// and not stopping: held until the operation ends ([`Lent`]).
    fn lent(self: &Arc<Self>) -> Option<Lent> {
        self.lent.fetch_add(1, Ordering::SeqCst);
        let lending = Lending(Arc::clone(self));
        if self.stopping() {
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

    /// What it is now, in words: what a node asking a standby is told, and
    /// what the node publishes of its Storage.
    #[must_use]
    pub fn said(&self) -> String {
        let held = self.held();
        let writes: u64 = self.copies.next().iter().map(|next| next - 1).sum();
        let what = match &held.role {
            Role::Finding => format!(
                "a Storage node finding its primary among {}, epoch {}",
                self.listed.join(", "),
                held.epoch
            ),
            Role::Primary(_) => {
                let followers = self
                    .followers
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                let behind: Vec<String> = followers
                    .iter()
                    .map(|(at, next)| {
                        let at_next: u64 = next.iter().sum();
                        let mine: u64 = self.copies.next().iter().sum();
                        format!("{at} {} writes behind", mine.saturating_sub(at_next))
                    })
                    .collect();
                let standbys = if behind.is_empty() {
                    "no standby following".to_string()
                } else {
                    format!("standbys {}", behind.join(", "))
                };
                format!("the primary Storage node, epoch {}, {standbys}", held.epoch)
            }
            Role::Standby(primary) => format!(
                "a standby Storage node of the primary at {primary}, epoch {}",
                held.epoch
            ),
        };
        let last = self.last.lock().unwrap_or_else(PoisonError::into_inner);
        match last.as_deref() {
            Some(last) => format!("{what}, {writes} writes held; {last}"),
            None => format!("{what}, {writes} writes held"),
        }
    }
}
