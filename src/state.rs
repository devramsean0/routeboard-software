use std::sync::{Arc, RwLock};

pub type SharedState = Arc<RwLock<AppState>>;

pub struct AppState {
    pub corpus: crate::corpus::TiplocData,
}