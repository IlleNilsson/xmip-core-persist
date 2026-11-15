//! What bounds a Storage node's dealings with the others (`super`), each a
//! hardware assumption the node's `[tuning]` sets, and the defaults where
//! it does not say.

use std::time::Duration;

/// How often a primary says it is there to a standby it has nothing to
/// hand, where the node's `[tuning] storage_beat` does not say.
pub const BEAT: Duration = Duration::from_millis(100);

/// How long a Storage node waits to hear from another — the standing it
/// asks for, its primary's next write or beat — and how often a primary
/// asks the others what they are, where the node's `[tuning]
/// storage_silence` does not say.
pub const SILENCE: Duration = Duration::from_secs(1);

/// The most bytes of each database's writes a Storage node keeps for a
/// standby behind it, where the node's `[tuning] storage_kept` does not say.
pub const KEPT: usize = 64 * 1024 * 1024;

/// What bounds a Storage node's dealings with the others, and what it keeps
/// of their waits while it is the primary.
#[derive(Clone, Copy, Debug)]
pub struct Timings {
    /// How often a primary beats to an idle standby.
    pub beat: Duration,
    /// How long it waits to hear from another — a standing asked, its
    /// primary's next write or beat — and how often a primary asks the
    /// others what they are.
    pub silence: Duration,
    /// What bounds each connect and each read of another.
    pub timeout: Duration,
    /// How long one that did not answer is asked last, as a node asks.
    pub pass_over: Duration,
    /// The most bytes of each database's writes kept for a standby.
    pub kept: usize,
    /// The most of what no node waits on now its primary keeps where its
    /// writer left it (`storage::watch`, [`crate::storage::UNWAITED`] by
    /// default).
    pub unwaited: usize,
    /// The most bytes of the records it wrote its primary's writer keeps
    /// ([`crate::storage::WRITER_KEPT`] by default).
    pub writer_kept: usize,
}
