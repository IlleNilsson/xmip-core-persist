//! What a paused Subscription keeps durable: its standing, and each Message
//! it holds (ADR-0013, amendment 2026-09-30).
//!
//! A Subscription is configuration, not a record: it is drawn in an Xmip
//! Application and bound in a node's TOML, and it is added and removed
//! there. What the runtime writes here is what an operator did to it and
//! what that left held — so a pause, and every Message held under it,
//! survives the node, and a Subscription paused before a restart is paused
//! after it.

use serde::{Deserialize, Serialize};
use xcore::MessageId;

/// One Subscription's standing on one node, as last written.
///
/// The Messages it holds are numbered in the order they were held, from
/// `first_held` up to, not including, `next_held`; a keyed store keeps no
/// order of its own (`README.md`, what it is not), so the range is the index.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionHold {
    /// The node that routes by it: `xmip:///<cluster>/node/<name>`.
    pub node: String,
    /// Its configured name, unique on its node.
    pub subscription: String,
    /// Whether an operator has paused it.
    pub paused: bool,
    /// Who paused it; empty while it is active.
    pub by: String,
    /// When its state began, in unix nanoseconds.
    pub since_unix_nanos: i64,
    /// The number of the oldest Message it still holds.
    pub first_held: u64,
    /// The number the next Message it holds takes.
    pub next_held: u64,
}

impl SubscriptionHold {
    /// How many Messages it holds.
    #[must_use]
    pub const fn held(&self) -> u64 {
        self.next_held.saturating_sub(self.first_held)
    }
}

/// One Message a Subscription held while it was paused, as it was held:
/// its content and what the holder needs to pick it up again, in the
/// holder's words.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeldMessage {
    /// The node whose Subscription holds it.
    pub node: String,
    /// The Subscription holding it.
    pub subscription: String,
    /// Its number among what that Subscription holds.
    pub sequence: u64,
    /// When it was held, in unix nanoseconds.
    pub held_unix_nanos: i64,
    /// The Message, where the holder has one.
    pub message_id: Option<MessageId>,
    /// Its content, as it arrived.
    pub content: Vec<u8>,
    /// What else the holder needs to pick it up, name then value.
    pub said: Vec<(String, String)>,
}

impl HeldMessage {
    /// The value the holder put under `name`, if any.
    #[must_use]
    pub fn said(&self, name: &str) -> Option<&str> {
        self.said
            .iter()
            .find(|(held, _)| held == name)
            .map(|(_, value)| value.as_str())
    }
}
