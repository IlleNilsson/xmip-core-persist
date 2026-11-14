//! One write the embedded Storage node's writer decides, and what it
//! decided (`super`).

use super::super::audit_entry::AuditEntry;
use super::super::dead::{Replay, Replayed};
use super::super::hand_on::HandOn;
use super::super::instance::Turn;
use super::super::publication::Publication;
use super::super::record::Claim;

/// One write the committer decides.
pub(crate) enum Op {
    /// A record as it is, replacing the last.
    Put(&'static str, Vec<u8>, Vec<u8>),
    Claim {
        claim: Claim,
        lease_nanos: i128,
    },
    Renew {
        claims: Vec<Claim>,
        lease_nanos: i128,
    },
    Release(Claim),
    HandOn(Box<HandOn>),
    /// A Work Process's turn: its hand-on with its Instances
    /// (`super::super::instance`).
    Turn(Box<Turn>),
    /// A Message, its Journeys, the ones held, its Dead Message Queue entry
    /// and its audit record, in one batch, once by its Message.
    Publish(Box<Publication>),
    /// An entry's Journeys written and the entry taken out, audited.
    Replay(Box<Replay>),
    Audit(Box<AuditEntry>),
    /// The keeper moved audit record `sequence`: forget it here, where the
    /// keeper has not moved past it already.
    Kept(u64),
}

/// What a write decided.
pub(crate) enum Done {
    Written,
    Claim(Option<Claim>),
    /// The claims a Publication's node holds of those it asked for.
    Claims(Vec<Claim>),
    Yes(bool),
    Replayed(Replayed),
}
