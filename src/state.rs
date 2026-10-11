use log::{debug, error, info};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub type SharedState = Arc<RwLock<AppState>>;

pub const MAX_TRAIN_AGE: Duration = Duration::from_mins(20);

/// Minimum gap between expiry sweeps. The sweep is a full scan, and `apply` runs per event.
const EXPIRY_INTERVAL: Duration = Duration::from_secs(30);

pub struct AppState {
    //pub corpus: crate::corpus::TiplocData,
    pub station_lookup: Arc<HashMap<String, String>>,
    pub map: StationMap,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Arrived {
        train_id: String,
        crs: String,
        timestamp: u128,
    },
    Departed {
        train_id: String,
        crs: String,
        timestamp: u128,
    },
    Deactivated {
        train_id: String,
        timestamp: u128,
    },
}

impl Event {
    pub fn train_id(&self) -> &str {
        match self {
            Event::Arrived { train_id, .. }
            | Event::Departed { train_id, .. }
            | Event::Deactivated { train_id, .. } => train_id,
        }
    }

    pub fn timestamp(&self) -> u128 {
        match self {
            Event::Arrived { timestamp, .. }
            | Event::Departed { timestamp, .. }
            | Event::Deactivated { timestamp, .. } => *timestamp,
        }
    }

    fn rank(&self) -> u8 {
        match self {
            Event::Arrived { .. } => 0,
            Event::Departed { .. } => 1,
            Event::Deactivated { .. } => 2,
        }
    }

    pub fn sort_key(&self) -> (u128, u8) {
        (self.timestamp(), self.rank())
    }
}

#[derive(Debug, Clone)]
struct TrainState {
    at: Option<String>,
    last_event: u128,
}

#[derive(Default)]
pub struct StationMap {
    counts: HashMap<String, usize>,
    trains: HashMap<String, TrainState>,
    feed_clock: u128,
    last_expiry: Option<u128>,
}

impl StationMap {
    pub fn apply(&mut self, event: &Event) {
        let train_id = event.train_id();
        let timestamp = event.timestamp();

        if timestamp > self.feed_clock {
            self.feed_clock = timestamp;
        }

        if let Some(state) = self.trains.get(train_id) {
            if state.last_event > timestamp {
                return;
            }
        }

        match event {
            Event::Arrived { crs, .. } => {
                if self.age(timestamp) > MAX_TRAIN_AGE.as_millis() {
                    debug!("Ignoring arrival of {train_id} at {crs}, older than the window");
                    return;
                }
                self.set_location(train_id, Some(crs), timestamp);
            }
            Event::Departed { .. } | Event::Deactivated { .. } => {
                self.set_location(train_id, None, timestamp)
            }
        }

        self.expire();
    }

    fn age(&self, timestamp: u128) -> u128 {
        self.feed_clock.saturating_sub(timestamp)
    }

    fn set_location(&mut self, train_id: &str, to: Option<&str>, timestamp: u128) {
        let previous = self.trains.get(train_id).and_then(|state| state.at.clone());

        if previous.as_deref() != to {
            if let Some(old_crs) = previous {
                self.decrement(&old_crs);
            }
            if let Some(new_crs) = to {
                *self.counts.entry(new_crs.to_string()).or_insert(0) += 1;
            }
        }

        self.trains.insert(
            train_id.to_string(),
            TrainState {
                at: to.map(str::to_string),
                last_event: timestamp,
            },
        );
    }

    fn decrement(&mut self, crs: &str) {
        match self.counts.get_mut(crs) {
            Some(n) if *n > 0 => *n -= 1,
            _ => error!("Count for {crs} went negative: decrement with no matching increment"),
        }
    }

    fn expire(&mut self) {
        let now = now_ms();
        if let Some(last) = self.last_expiry {
            if now.saturating_sub(last) < EXPIRY_INTERVAL.as_millis() {
                return;
            }
        }
        self.last_expiry = Some(now);

        debug!(
            "Feed clock skew: {} ms",
            self.feed_clock as i128 - now as i128
        );

        let (released, forgotten) = self.prune(MAX_TRAIN_AGE);
        if released != 0 {
            info!("Released {released} stranded trains");
            debug!("Deleted {forgotten} forgotten trains");
        }
    }
    pub fn prune(&mut self, max_age: Duration) -> (usize, usize) {
        let max_age_ms = max_age.as_millis();

        let stale: Vec<(String, Option<String>)> = self
            .trains
            .iter()
            .filter(|(_, state)| self.age(state.last_event) > max_age_ms)
            .map(|(train_id, state)| (train_id.clone(), state.at.clone()))
            .collect();

        let mut released = 0;
        let mut forgotten = 0;
        for (train_id, at) in stale {
            self.trains.remove(&train_id);
            match at {
                Some(crs) => {
                    self.decrement(&crs);
                    released += 1;
                }
                None => forgotten += 1,
            }
        }

        (released, forgotten)
    }

    /// Number of trains currently at `crs` (0 if none or unknown).
    pub fn train_count(&self, crs: &str) -> usize {
        self.counts.get(crs).copied().unwrap_or(0)
    }
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINUTE: u128 = 60_000;

    /// Feed time is its own clock. Nothing here derives from the system clock, because
    /// nothing in `StationMap` may depend on the two agreeing.
    const BASE: u128 = 1_758_758_400_000;

    fn arrived(train_id: &str, crs: &str, timestamp: u128) -> Event {
        Event::Arrived {
            train_id: train_id.into(),
            crs: crs.into(),
            timestamp,
        }
    }

    fn departed(train_id: &str, crs: &str, timestamp: u128) -> Event {
        Event::Departed {
            train_id: train_id.into(),
            crs: crs.into(),
            timestamp,
        }
    }

    fn deactivated(train_id: &str, timestamp: u128) -> Event {
        Event::Deactivated {
            train_id: train_id.into(),
            timestamp,
        }
    }

    impl StationMap {
        /// Every increment must be answered by exactly one decrement: the sum of the counts
        /// has to equal the number of trains we believe are standing at a station.
        fn assert_balanced(&self) {
            let counted: usize = self.counts.values().sum();
            let at_station = self.trains.values().filter(|s| s.at.is_some()).count();
            assert_eq!(
                counted, at_station,
                "counts {:?} out of step with trains {:?}",
                self.counts, self.trains
            );
        }
    }

    #[test]
    fn clean_journey_returns_to_zero() {
        let t = BASE;
        let mut map = StationMap::default();

        map.apply(&arrived("T1", "MAN", t));
        assert_eq!(map.train_count("MAN"), 1);
        map.apply(&departed("T1", "MAN", t + MINUTE));
        assert_eq!(map.train_count("MAN"), 0);

        map.apply(&arrived("T1", "SPT", t + 2 * MINUTE));
        assert_eq!(map.train_count("SPT"), 1);
        map.apply(&departed("T1", "SPT", t + 3 * MINUTE));
        map.apply(&deactivated("T1", t + 3 * MINUTE));

        assert_eq!(map.train_count("MAN"), 0);
        assert_eq!(map.train_count("SPT"), 0);
        map.assert_balanced();
    }

    /// The departure is applied first, then the arrival for the same stop turns up. The
    /// tombstone left by the departure is what makes the late arrival recognisably stale.
    #[test]
    fn arrival_delivered_after_its_departure_is_ignored() {
        let t = BASE;
        let mut map = StationMap::default();

        map.apply(&departed("T1", "MAN", t + MINUTE));
        map.apply(&arrived("T1", "MAN", t));

        assert_eq!(map.train_count("MAN"), 0);
        map.assert_balanced();
    }

    /// Kafka auto-commit means a rebalance or restart can replay a batch we already applied.
    #[test]
    fn redelivered_arrival_is_ignored() {
        let t = BASE;
        let mut map = StationMap::default();

        map.apply(&arrived("T1", "MAN", t));
        map.apply(&departed("T1", "MAN", t + MINUTE));
        assert_eq!(map.train_count("MAN"), 0);

        map.apply(&arrived("T1", "MAN", t));

        assert_eq!(map.train_count("MAN"), 0);
        map.assert_balanced();
    }

    /// A redelivered *departure* must not decrement twice.
    #[test]
    fn redelivered_departure_does_not_double_decrement() {
        let t = BASE;
        let mut map = StationMap::default();

        map.apply(&arrived("T1", "MAN", t));
        map.apply(&arrived("T2", "MAN", t));
        map.apply(&departed("T1", "MAN", t + MINUTE));
        map.apply(&departed("T1", "MAN", t + MINUTE));

        assert_eq!(map.train_count("MAN"), 1);
        map.assert_balanced();
    }

    /// A train moving on without a departure message: the arrival at the next station has to
    /// release the previous one.
    #[test]
    fn arrival_elsewhere_releases_the_previous_station() {
        let t = BASE;
        let mut map = StationMap::default();

        map.apply(&arrived("T1", "LDS", t));
        map.apply(&arrived("T1", "YRK", t + MINUTE));

        assert_eq!(map.train_count("LDS"), 0);
        assert_eq!(map.train_count("YRK"), 1);
        map.assert_balanced();
    }

    /// A departure naming a station we never saw the train arrive at still has to release
    /// whatever it is actually counted against, or that count strands.
    #[test]
    fn departure_from_unexpected_station_still_releases_count() {
        let t = BASE;
        let mut map = StationMap::default();

        map.apply(&arrived("T1", "LDS", t));
        map.apply(&departed("T1", "YRK", t + MINUTE));

        assert_eq!(map.train_count("LDS"), 0);
        assert_eq!(map.train_count("YRK"), 0);
        map.assert_balanced();
    }

    /// An arrival and a same-timestamp terminating departure in one batch, applied in the
    /// order `sort_key` gives them.
    #[test]
    fn same_timestamp_arrival_and_departure_net_to_zero() {
        let t = BASE;
        let mut events = vec![departed("T1", "MAN", t), arrived("T1", "MAN", t)];
        events.sort_by_key(|event| event.sort_key());

        let mut map = StationMap::default();
        for event in &events {
            map.apply(event);
        }

        assert_eq!(map.train_count("MAN"), 0);
        map.assert_balanced();
    }

    /// A departure reported under a key we have no history for must not disturb any count.
    /// This is the shape a mis-keyed train would take, and the expiry sweep is what releases
    /// the entry it left behind.
    #[test]
    fn departure_for_an_unknown_train_leaves_counts_alone() {
        let t = BASE;
        let mut map = StationMap::default();

        map.apply(&arrived("T1", "MAN", t));
        map.apply(&departed("STRANGER", "MAN", t + MINUTE));

        assert_eq!(map.train_count("MAN"), 1, "T1 is still there");
        map.assert_balanced();
    }

    /// The regression that matters: the feed's clock need not agree with the system clock —
    /// a timezone quirk or plain skew can put it an hour ahead. Ageing against the system
    /// clock saturates to zero for every train, so nothing is ever released and a missed
    /// departure strands its count forever.
    #[test]
    fn expiry_works_when_the_feed_clock_runs_ahead_of_the_system_clock() {
        let t = now_ms() + 60 * MINUTE;
        let mut map = StationMap::default();

        map.apply(&arrived("T1", "CHD", t));
        // A later train advances the feed clock well past T1's window.
        map.apply(&arrived("T2", "SHF", t + 30 * MINUTE));

        let (released, _) = map.prune(MAX_TRAIN_AGE);

        assert_eq!(released, 1, "T1 should have been released");
        assert_eq!(map.train_count("CHD"), 0);
        assert_eq!(map.train_count("SHF"), 1);
        map.assert_balanced();
    }

    /// The same, with the feed running behind. Expiry must be driven by the gap between
    /// events, not by either clock's absolute value.
    #[test]
    fn expiry_works_when_the_feed_clock_runs_behind_the_system_clock() {
        let t = now_ms() - 48 * 60 * MINUTE;
        let mut map = StationMap::default();

        map.apply(&arrived("T1", "CHD", t));
        map.apply(&arrived("T2", "SHF", t + 30 * MINUTE));

        let (released, _) = map.prune(MAX_TRAIN_AGE);

        assert_eq!(released, 1);
        assert_eq!(map.train_count("CHD"), 0);
        assert_eq!(map.train_count("SHF"), 1);
        map.assert_balanced();
    }

    #[test]
    fn arrival_older_than_the_window_is_not_counted() {
        let t = BASE;
        let mut map = StationMap::default();

        map.apply(&arrived("T1", "MAN", t + 60 * MINUTE));
        map.apply(&arrived("T2", "SPT", t));

        assert_eq!(map.train_count("SPT"), 0);
        map.assert_balanced();
    }

    /// A departure is never ignored for age: dropping it would strand the count it exists to
    /// release.
    #[test]
    fn departure_older_than_the_window_still_releases() {
        let t = BASE;
        let mut map = StationMap::default();

        map.apply(&arrived("T1", "MAN", t));
        map.apply(&arrived("T2", "SPT", t + 60 * MINUTE));
        map.apply(&departed("T1", "MAN", t + MINUTE));

        assert_eq!(map.train_count("MAN"), 0);
        map.assert_balanced();
    }

    #[test]
    fn prune_releases_stranded_trains_and_forgets_departed_ones() {
        let t = BASE;
        let mut map = StationMap::default();

        map.apply(&arrived("T1", "MAN", t));
        map.apply(&arrived("T2", "MAN", t));
        map.apply(&departed("T2", "MAN", t + MINUTE));
        // A later train advances the feed clock past both.
        map.apply(&arrived("T3", "SPT", t + 10 * MINUTE));

        let (released, forgotten) = map.prune(Duration::from_secs(5 * 60));

        assert_eq!((released, forgotten), (1, 1));
        assert_eq!(map.train_count("MAN"), 0);
        assert_eq!(map.train_count("SPT"), 1, "T3 is still fresh");
        map.assert_balanced();
    }

    #[test]
    fn fresh_trains_survive_pruning() {
        let t = BASE;
        let mut map = StationMap::default();

        map.apply(&arrived("T1", "MAN", t));
        let (released, forgotten) = map.prune(MAX_TRAIN_AGE);

        assert_eq!((released, forgotten), (0, 0));
        assert_eq!(map.train_count("MAN"), 1);
    }
}
