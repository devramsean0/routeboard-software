use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::time::{Duration, Instant};

pub type SharedState = Arc<RwLock<AppState>>;

pub struct AppState {
    pub corpus: crate::corpus::TiplocData,
    pub map: StationMap
}
 
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Arrived { train_id: String, crs: String },
    Departed { train_id: String, crs: String },
    Deactivated { train_id: String },
}
 
#[derive(Default)]
pub struct StationMap {
    counts: HashMap<String, usize>,
    train_at: HashMap<String, (String, Instant)>,
}
 
impl StationMap {
    pub fn apply(&mut self, event: &Event) {
        match event {
            Event::Arrived { train_id, crs } => {
                let current = self.train_at.get(train_id).map(|(t, _)| t.clone());
                match current {
                    Some(ref cur) if cur == crs => return,
                    Some(old) => self.decrement(&old),
                    None => {}
                }
                self.train_at
                    .insert(train_id.clone(), (crs.clone(), Instant::now()));
                *self.counts.entry(crs.clone()).or_insert(0) += 1;
            }
            Event::Departed { train_id, crs } => {
                let current = self.train_at.get(train_id).map(|(t, _)| t.clone());
                match current {
                    Some(ref cur) if cur == crs => return,
                    Some(old) => self.decrement(&old),
                    None => {}
                }
            }
            Event::Deactivated { train_id } => self.remove_train(train_id),
        }
    }
 
    fn remove_train(&mut self, train_id: &str) {
        if let Some((crs, _)) = self.train_at.remove(train_id) {
            self.decrement(&crs);
        }
    }
 
    fn decrement(&mut self, crs: &str) {
        if let Some(n) = self.counts.get_mut(crs) {
            *n = n.saturating_sub(1);
        }
    }
 
    /// Drop trains that have "been" at a station longer than `max_age`. This is a safety net
    /// for a missed message; returns how many trains were removed.
    pub fn prune(&mut self, max_age: Duration) -> usize {
        let stale: Vec<String> = self
            .train_at
            .iter()
            .filter(|(_, (_, since))| since.elapsed() > max_age)
            .map(|(train_id, _)| train_id.clone())
            .collect();
        let n = stale.len();
        for train_id in stale {
            self.remove_train(&train_id);
        }
        n
    }
 
    /// Number of trains currently at `crs` (0 if none or unknown).
    pub fn train_count(&self, crs: &str) -> usize {
        self.counts.get(crs).copied().unwrap_or(0)
    }
 
    pub fn is_occupied(&self, crs: &str) -> bool {
        self.train_count(crs) > 0
    }
 
    /// The station a train is at, or None if it's between stations / unknown.
    pub fn train_position(&self, train_id: &str) -> Option<&str> {
        self.train_at.get(train_id).map(|(t, _)| t.as_str())
    }
 
    /// Train IDs currently at `crs`. (Scans all tracked trains; fine at a few thousand.)
    pub fn trains_at(&self, crs: &str) -> Vec<&str> {
        self.train_at
            .iter()
            .filter(|(_, (t, _))| t == crs)
            .map(|(train_id, _)| train_id.as_str())
            .collect()
    }
 
    /// How many stations have at least one train.
    pub fn occupied_count(&self) -> usize {
        self.counts.values().filter(|&&n| n > 0).count()
    }
 
    /// Copy of the full station -> train count map.
    pub fn snapshot(&self) -> HashMap<String, usize> {
        self.counts.clone()
    }
}
