//! Which of the Storage nodes listed a caller is, by the certificate it
//! proved (open problem 135; ADR-0056, amendment 2026-11-24): what alone
//! admits a node to follow a primary, be handed its writes and its data
//! keys, and be named its successor.
//!
//! A Storage node presents one certificate, its node's, as a server to
//! whoever reaches it at the address listed and as a client to whoever it
//! reaches (`[storage] certificate`). Each Storage node keeps the leaf every
//! one listed presented as it asked it its standing, that one's host
//! checked against the address listed by the TLS client
//! (`super::finding`). A caller presenting one of those leaves proved, in
//! its handshake, that it holds that Storage node's key: it is that one, at
//! that place. A node of another role — a certificate of the same
//! authority, for the same host even — presents none of them, and is
//! refused. One whose leaf no Storage node presented when last asked has
//! each asked again once, as a Storage node renews its certificate. The
//! place is never what the caller says: until 2026-11-24 `Follow` carried
//! it, and any node's certificate could follow, be handed whole copies with
//! their data keys, and name itself the successor.

use xcore::lock::Locked;

use super::StorageNode;

impl StorageNode {
    /// The leaf certificate the Storage node listed at `place` presented on
    /// `connection`, kept as what that one proves itself by.
    pub(super) fn presented_on(&self, place: usize, connection: &tls::Guarded) {
        let Some(leaf) = connection.conn.peer_certificates().and_then(<[_]>::first) else {
            return;
        };
        if let Some(kept) = self.presented.locked().get_mut(place) {
            *kept = Some(leaf.as_ref().to_vec());
        }
    }

    /// The place among the Storage nodes listed of the one that presents
    /// `leaf`, never this node's own: where none presented it when last
    /// asked, each is asked again once. None where it is no Storage node
    /// listed, or two present it.
    #[must_use]
    pub fn listed_as(&self, leaf: &[u8]) -> Option<usize> {
        self.presenting(leaf).or_else(|| {
            let _ = self.standings();
            self.presenting(leaf)
        })
    }

    /// [`StorageNode::listed_as`], by what was kept alone.
    fn presenting(&self, leaf: &[u8]) -> Option<usize> {
        let own = self.place();
        let presented = self.presented.locked();
        let mut places = presented
            .iter()
            .enumerate()
            .filter(|(place, kept)| Some(*place) != own && kept.as_deref() == Some(leaf))
            .map(|(place, _)| place);
        let first = places.next();
        if places.next().is_some() {
            return None;
        }
        first
    }
}
