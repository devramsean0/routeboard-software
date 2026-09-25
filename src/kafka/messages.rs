use serde::{Deserialize};

pub type Container = Vec<SingleContainer>;

#[derive(Deserialize, Debug)]
pub struct SingleContainer {
    pub header: Header,
    pub body: Body
}

#[derive(Deserialize, Debug)]
pub struct Header {
    pub msg_type: String,
}

#[derive(Deserialize, Debug)]
pub struct Body {
    pub train_id: String,
    pub current_train_id: Option<String>,
    pub loc_stanox: Option<String>,
    pub event_type: Option<String>,
    pub train_terminated: Option<String>,
    pub actual_timestamp: Option<String>,
}