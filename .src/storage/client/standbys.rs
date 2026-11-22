//! Who follows the primary of several, as a node last heard it (`super`;
//! open problem 95; ADR-0056, amendment 2026-11-22): what every node shows
//! of each standby, from its primary's one list (`standby::followers`),
//! never from what the standby said when it was last asked.
//!
//! **Told, never asked again and again.** Every wait the client asks names
//! where it last saw the primary's count of who follows it
//! ([`Watched::Standbys`]) beside what its caller waits on, and a node
//! always has a wait under way — its membership's, at the least — so the
//! primary's wait tells it as soon as a standby begins or ends following.
//! Then it reads the list once, of the Storage node that told it
//! (`waiting`), and waits on for what its caller asked.

use std::sync::Mutex;
use xcore::lock::Locked;

use super::super::standby::Follower;
use super::super::watch::{Moved, Position, Watched};
use super::node::Node;

/// What the client last heard of who follows the primary.
#[derive(Default)]
pub(super) struct Standbys(Mutex<Told>);

#[derive(Default)]
struct Told {
    /// The primary's count as last seen: what each wait names.
    seen: u64,
    /// Which Storage node listed said the list, and the list; none until
    /// one said, or where its read failed.
    heard: Option<(usize, Vec<Follower>)>,
}

impl Standbys {
    /// Where its wait sees the primary's count stand.
    pub(super) fn position(&self) -> Position {
        Position {
            watched: Watched::Standbys,
            at: self.0.locked().seen,
        }
    }

    /// What `moved` says of what its caller waited on, and where the
    /// primary's count stands where it moved.
    pub(super) fn parted(moved: Vec<Moved>) -> (Vec<Moved>, Option<u64>) {
        let (count, asked): (Vec<Moved>, Vec<Moved>) = moved
            .into_iter()
            .partition(|moved| moved.position.watched == Watched::Standbys);
        (asked, count.first().map(|moved| moved.position.at))
    }

    /// Whether the primary's count standing at `at` is news: another wait
    /// under way may have read the list for it already.
    pub(super) fn moved_to(&self, at: u64) -> bool {
        self.0.locked().seen != at
    }

    /// The list the Storage node listed at `index` said at `at`, or none
    /// where it could not be read: seen either way, so a read that failed
    /// is not asked again until the count moves.
    pub(super) fn heard(&self, at: u64, said: Option<(usize, Vec<Follower>)>) {
        let mut told = self.0.locked();
        told.seen = at;
        told.heard = said;
    }

    /// Whether each of `nodes`, as the client lists them, follows the
    /// primary now, by the place each standby said asking to follow: none
    /// until a Storage node said, or where the one that said did not answer
    /// its last ask.
    pub(super) fn following(&self, nodes: &[Node]) -> Option<Vec<bool>> {
        let (from, followers) = self.0.locked().heard.clone()?;
        let answered = nodes.get(from)?.last.locked().as_ref()?.answered;
        if !answered {
            return None;
        }
        let places: Vec<usize> = followers
            .iter()
            .filter_map(|follower| usize::try_from(follower.place?).ok())
            .collect();
        Some(
            (0..nodes.len())
                .map(|place| places.contains(&place))
                .collect(),
        )
    }
}
