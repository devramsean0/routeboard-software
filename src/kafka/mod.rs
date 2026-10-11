use log::{debug, error, info};
use rayon::prelude::*;
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    ClientConfig, Message,
};
use tokio::sync::broadcast;

use crate::state::Event;

mod messages;

fn message_to_events(
    msg: messages::SingleContainer,
    station_lookup: &std::collections::HashMap<String, String>,
) -> Vec<Event> {
    match msg.header.msg_type.as_str() {
        "0002" => {
            let Some(canx_timestamp) = msg.body.canx_timestamp.as_deref() else {
                debug!("Discarded Message without actual timestamp");
                return Vec::new();
            };

            let Ok(message_time) = canx_timestamp.trim().parse::<u128>() else {
                debug!("Discarded Message with invalid actual timestamp");
                return Vec::new();
            };

            let body = msg.body;

            vec![Event::Deactivated {
                train_id: body.train_id,
                timestamp: message_time,
            }]
        }
        "0003" => {
            let Some(actual_timestamp) = msg.body.actual_timestamp.as_deref() else {
                debug!("Discarded Message without actual timestamp");
                return Vec::new();
            };

            let Ok(message_time) = actual_timestamp.trim().parse::<u128>() else {
                debug!("Discarded Message with invalid actual timestamp");
                return Vec::new();
            };

            let body = msg.body;
            let Some(loc_stanox) = body.loc_stanox.as_deref() else {
                return Vec::new();
            };

            let Some(matched_crs) = station_lookup.get(loc_stanox).cloned() else {
                return Vec::new();
            };

            if matched_crs == " " {
                debug!("Discarded Message because not a station");
                return Vec::new();
            }

            let train_id = body.train_id;

            let mut events = Vec::new();

            debug!(
                "Processing train {} to matched CRS {}",
                train_id, matched_crs
            );

            if body.event_type.as_deref() == Some("ARRIVAL") {
                events.push(Event::Arrived {
                    train_id: train_id.clone(),
                    crs: matched_crs.clone(),
                    timestamp: message_time,
                });
            }
            if body.event_type.as_deref() == Some("DEPARTURE") {
                events.push(Event::Departed {
                    train_id: train_id.clone(),
                    crs: matched_crs.clone(),
                    timestamp: message_time,
                });
            }
            if body.train_terminated.as_deref() == Some("true") {
                events.push(Event::Deactivated {
                    train_id,
                    timestamp: message_time,
                });
            }
            events
        }
        _ => {
            vec![]
        }
    }
}

pub async fn run_kafka_consumer(
    mut shutdown_rx: broadcast::Receiver<()>,
    state: crate::state::SharedState,
) {
    let host = std::env::var("KAFKA_HOST").unwrap();
    let group = std::env::var("KAFKA_GROUP").unwrap();
    let username = std::env::var("KAFKA_USERNAME").unwrap();
    let password = std::env::var("KAFKA_PASSWORD").unwrap();

    info!("Connecting to {host} as {group}");

    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", host)
        .set("group.id", group)
        .set("security.protocol", "SASL_SSL")
        .set("sasl.mechanism", "PLAIN") // or "PLAIN", "SCRAM-SHA-512"
        .set("sasl.username", username)
        .set("sasl.password", password);

    let consumer: StreamConsumer = config.create().expect("Consumer creation failed");

    consumer
        .subscribe(&["TRAIN_MVT_ALL_TOC"])
        .expect("Can't subscribe to topic");

    info!("Kafka consumer started...");

    loop {
        tokio::select! {
            message_result = consumer.recv() => {
                match message_result {
                    Ok(msg) => {
                        if let Some(payload) = msg.payload_view::<str>() {
                            if let Ok(text) = payload {
                                let messages: messages::Container = match serde_json::from_str(text) {
                                    Ok(messages) => messages,
                                    // One bad payload must not panic the task: that would
                                    // stop every update and freeze the board.
                                    Err(e) => {
                                        error!("Discarding unparseable payload: {}", e);
                                        continue;
                                    }
                                };
                                let station_lookup = {
                                    state.read().unwrap().station_lookup.clone()
                                };

                                let mut events: Vec<Event> = messages
                                    .into_par_iter()
                                    .flat_map_iter(|msg| message_to_events(msg, station_lookup.as_ref()))
                                    .collect();

                                events.sort_by_key(|event| event.sort_key());

                                let mut state_guard = state.write().unwrap();
                                for event in &events {
                                    state_guard.map.apply(event);
                                }
                            }
                        }
                    }
                    Err(e) => error!("Kafka error: {}", e),
                }
            }
            _ = shutdown_rx.recv() => {
                println!("Kafka consumer shutting down...");
                break;
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    const STANOX: &str = "12345";
    const ARRIVED_AT: u128 = 1_758_758_400_000;

    fn lookup() -> HashMap<String, String> {
        HashMap::from([(STANOX.to_string(), "MAN".to_string())])
    }

    /// A movement message with the optional fields shaped the way the feed sends them:
    /// present but blank rather than omitted.
    fn movement(
        current_train_id: &str,
        event_type: &str,
        timestamp: u128,
    ) -> messages::SingleContainer {
        let raw = format!(
            r#"{{
                "header": {{"msg_type": "0003"}},
                "body": {{
                    "train_id": "721B22MP25",
                    "current_train_id": "{current_train_id}",
                    "loc_stanox": "{STANOX}",
                    "event_type": "{event_type}",
                    "train_terminated": "false",
                    "actual_timestamp": "{timestamp}"
                }}
            }}"#
        );
        serde_json::from_str(&raw).unwrap()
    }

    #[test]
    fn blank_current_train_id_falls_back_to_train_id() {
        let events = message_to_events(movement("", "ARRIVAL", ARRIVED_AT), &lookup());

        assert_eq!(
            events,
            vec![Event::Arrived {
                train_id: "721B22MP25".into(),
                crs: "MAN".into(),
                timestamp: ARRIVED_AT,
            }]
        );
    }

    /// A change of identity must not move the key: `train_id` is the identifier that stays
    /// put for the life of the train, so it stays the one we track.
    #[test]
    fn changed_identity_still_keys_on_train_id() {
        let events = message_to_events(movement("999X99XX99", "ARRIVAL", ARRIVED_AT), &lookup());

        assert_eq!(
            events,
            vec![Event::Arrived {
                train_id: "721B22MP25".into(),
                crs: "MAN".into(),
                timestamp: ARRIVED_AT,
            }]
        );
    }

    #[test]
    fn departure_carries_the_station_it_left() {
        let events = message_to_events(movement("", "DEPARTURE", ARRIVED_AT), &lookup());

        assert_eq!(
            events,
            vec![Event::Departed {
                train_id: "721B22MP25".into(),
                crs: "MAN".into(),
                timestamp: ARRIVED_AT,
            }]
        );
    }

    #[test]
    fn cancellation_falls_back_to_train_id() {
        let raw = r#"{
            "header": {"msg_type": "0002"},
            "body": {
                "train_id": "721B22MP25",
                "current_train_id": "",
                "canx_timestamp": "1758758400000"
            }
        }"#;
        let events = message_to_events(serde_json::from_str(raw).unwrap(), &lookup());

        assert_eq!(
            events,
            vec![Event::Deactivated {
                train_id: "721B22MP25".into(),
                timestamp: ARRIVED_AT
            }]
        );
    }
}
