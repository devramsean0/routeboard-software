use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub type SharedState = Arc<RwLock<AppState>>;

pub struct AppState {
    //pub corpus: crate::corpus::TiplocData,
    pub station_lookup: Arc<HashMap<String, String>>,
    pub map: StationMap
}
 
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Arrived { train_id: String, crs: String, timestamp: u128 },
    Departed { train_id: String, timestamp: u128 },
    Deactivated { train_id: String, timestamp: u128 },
}
 
#[derive(Default)]
pub struct StationMap {
    counts: HashMap<String, usize>,
    train_at: HashMap<String, (String, u128)>,
}
 
impl StationMap {
    pub fn apply(&mut self, event: &Event) {
        match event {
            Event::Arrived { train_id, crs, timestamp } => {
                let current = self.train_at.get(train_id).cloned();
                match current {
                    Some((_, current_timestamp)) if current_timestamp > *timestamp => return,
                    Some((current_crs, _)) if current_crs == *crs => {
                        self.train_at.insert(train_id.clone(), (crs.clone(), *timestamp));
                        return;
                    }
                    Some((old_crs, _)) => self.decrement(&old_crs),
                    None => {}
                }
                self.train_at
                    .insert(train_id.clone(), (crs.clone(), *timestamp));
                *self.counts.entry(crs.clone()).or_insert(0) += 1;
            }
            Event::Departed { train_id, timestamp } => {
                let current = self.train_at.get(train_id).cloned();
                let Some((_current_crs, current_timestamp)) = current else {
                    return;
                };
                if current_timestamp > *timestamp{
                    return;
                }
                self.remove_train(train_id);
            }
            Event::Deactivated { train_id, timestamp } => {
                let Some((_, current_timestamp)) = self.train_at.get(train_id).cloned() else {
                    return;
                };
                if current_timestamp > *timestamp {
                    return;
                }
                self.remove_train(train_id)
            }
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
 
/*     /// Drop trains that have "been" at a station longer than `max_age`. This is a safety net
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
    } */
 
    /// Number of trains currently at `crs` (0 if none or unknown).
    pub fn train_count(&self, crs: &str) -> usize {
        self.counts.get(crs).copied().unwrap_or(0)
    }
 
    pub fn is_occupied(&self, crs: &str) -> bool {
        self.train_count(crs) > 0
    }
}
