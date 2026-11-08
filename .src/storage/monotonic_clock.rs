//! Xmip Storage's clock: the wall clock read once, as the node opens its
//! databases, and counted on from there by the monotonic clock
//! (`xcore::Clock::monotonic_nanos`).
//!
//! A claim's lease runs on it, and so does every due time and stamp the
//! embedded Storage node's writer decides, so a lease neither stretches
//! nor lapses early when the wall clock is stepped — by the time service or
//! by hand — while the node is the primary. A Storage node promoted opens
//! its databases, and with them this clock, anew: re-based on its own wall
//! clock then, so the times its leases are counted in go on from about
//! where its former primary's stood, as near as the two machines' wall
//! clocks agree. Where they disagree a lease taken before the promotion
//! lapses that much early or late; the epoch's fence keeps a holder of the
//! former epoch from handing on either way (`storage::claim::fence`).

use std::sync::Arc;
use xcore::Clock;

/// The wall clock at one moment, counted on monotonically.
pub(crate) struct MonotonicClock {
    clock: Arc<dyn Clock>,
    /// The wall clock's reading at the moment it was based.
    wall: i128,
    /// The monotonic clock's reading at the same moment.
    at: i128,
}

impl MonotonicClock {
    /// `clock` based now: its wall clock read, and counted on by its
    /// monotonic clock.
    pub(crate) fn new(clock: Arc<dyn Clock>) -> Self {
        let (wall, at) = (clock.unix_timestamp_nanos(), clock.monotonic_nanos());
        Self { clock, wall, at }
    }
}

impl Clock for MonotonicClock {
    fn unix_timestamp_nanos(&self) -> i128 {
        let since = self.clock.monotonic_nanos().saturating_sub(self.at);
        self.wall.saturating_add(since.max(0))
    }

    fn monotonic_nanos(&self) -> i128 {
        self.clock.monotonic_nanos()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// A clock whose wall and monotonic readings a test sets apart.
    struct Stepped(Mutex<(i128, i128)>);

    impl Clock for Stepped {
        fn unix_timestamp_nanos(&self) -> i128 {
            self.0.lock().expect("clock").0
        }

        fn monotonic_nanos(&self) -> i128 {
            self.0.lock().expect("clock").1
        }
    }

    #[test]
    fn a_wall_clock_stepped_back_or_forward_moves_no_lease() {
        let stepped = Arc::new(Stepped(Mutex::new((1_000_000, 50))));
        let clock = MonotonicClock::new(Arc::clone(&stepped) as Arc<dyn Clock>);
        assert_eq!(clock.unix_timestamp_nanos(), 1_000_000);
        // An hour back on the wall, ten nanoseconds on: ten on.
        *stepped.0.lock().expect("clock") = (1_000_000 - 3_600_000_000_000, 60);
        assert_eq!(clock.unix_timestamp_nanos(), 1_000_010);
        // A day forward on the wall, five more on: five more on.
        *stepped.0.lock().expect("clock") = (86_400_000_000_000, 65);
        assert_eq!(clock.unix_timestamp_nanos(), 1_000_015);
        // Based again, as a promotion opens it: the wall clock of then.
        let rebased = MonotonicClock::new(Arc::clone(&stepped) as Arc<dyn Clock>);
        assert_eq!(rebased.unix_timestamp_nanos(), 86_400_000_000_000);
    }
}
