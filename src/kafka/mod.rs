use rdkafka::{ClientConfig, Message, consumer::{Consumer, StreamConsumer}};
use tokio::sync::broadcast;
use log::{info, error, debug};
use std::time::{SystemTime, UNIX_EPOCH};
use rayon::prelude::*;

use crate::state::Event;

mod messages;

fn message_to_events(
    msg: messages::SingleContainer,
    station_lookup: &std::collections::HashMap<String, String>,
    stale_cutoff: u128,
) -> Vec<Event> {
    if msg.header.msg_type != "0003" {
        debug!("Discarded Message with type {}", msg.header.msg_type);
        return Vec::new();
    }

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

    let train_id = body.current_train_id.unwrap_or(body.train_id);
    let mut events = Vec::with_capacity(2);

    debug!("Processing train {} to matched CRS {}", train_id, matched_crs);

    if body.event_type.as_deref() == Some("ARRIVAL") {
        events.push(Event::Arrived {
            train_id: train_id.clone(),
            crs: matched_crs,
            timestamp: message_time,
        });
    }
    if body.event_type.as_deref() == Some("DEPARTURE") {
        events.push(Event::Departed {
            train_id: train_id.clone(),
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

pub async fn run_kafka_consumer(
    mut shutdown_rx: broadcast::Receiver<()>,
    state: crate::state::SharedState
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

    let consumer: StreamConsumer = config
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&["TRAIN_MVT_ALL_TOC"])
        .expect("Can't subscribe to topic");

    info!("Kafka consumer started...");

    loop {
        let stale_cutoff = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .saturating_sub(2 * 60 * 1000);

        tokio::select! {
            message_result = consumer.recv() => {
                match message_result {
                    Ok(msg) => {
                        if let Some(payload) = msg.payload_view::<str>() {
                            if let Ok(text) = payload {
                                let messages: messages::Container = serde_json::from_str(text).unwrap();
                                let station_lookup = {
                                    state.read().unwrap().station_lookup.clone()
                                };

                                let events: Vec<Event> = messages
                                    .into_par_iter()
                                    .flat_map_iter(|msg| message_to_events(msg, station_lookup.as_ref(), stale_cutoff))
                                    .collect();

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