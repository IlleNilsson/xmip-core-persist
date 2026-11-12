//! The record kinds of the runtime database, and the audit sequence's
//! places (`super`).

pub(crate) const CHUNK: &str = "chunk";
/// A Stream's own record (`super::super::stream`).
pub(crate) const STREAM: &str = "stream";
pub(crate) const MESSAGE: &str = "message";
pub(crate) const JOURNEY: &str = "journey";
pub(crate) const CLAIM: &str = "claim";
pub(crate) const AUDIT: &str = "audit";
pub(crate) const HELD: &str = "held";
/// A held Message's place, found by its queue and its Journey.
pub(crate) const HELD_JOURNEY: &str = "held-journey";
pub(crate) const PLACES: &str = "held-places";
/// Where a queue's first entries by when they are due are
/// (`super::super::due`).
pub(crate) const DUE_PLACES: &str = "held-due-places";
/// A node's Dead Message Queue: its entries, each one's place by its
/// Message, and its places.
pub(crate) const DEAD: &str = "dead-message";
pub(crate) const DEAD_MESSAGE: &str = "dead-message-by-message";
pub(crate) const DEAD_PLACES: &str = "dead-message-places";
/// That a Publication was written, by its Message: the digest of what was
/// asked (`super::super::publication`).
pub(crate) const PUBLICATION: &str = "publication";
pub(crate) const SEQUENCE: &str = "audit-sequence";
/// The sequence's two places: the next audit record's number, and the
/// first the keeper has not moved.
pub(crate) const NEXT: &[u8] = b"next";
pub(crate) const KEPT: &[u8] = b"kept";
