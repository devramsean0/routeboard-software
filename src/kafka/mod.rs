use rdkafka::{ClientConfig, Message, consumer::{Consumer, StreamConsumer}};
use tokio::sync::broadcast;
use log::{info, error, debug};

mod messages;

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
        tokio::select! {
            message_result = consumer.recv() => {
                match message_result {
                    Ok(msg) => {
                        if let Some(payload) = msg.payload_view::<str>() {
                            if let Ok(text) = payload {
                                let messages: messages::Container = serde_json::from_str(text).unwrap();
                                for msg in messages {
                                    if msg.header.msg_type != "0003" {
                                        debug!("Discarded Message with type {}", msg.header.msg_type);
                                        continue;
                                    }
                                    let body = msg.body;
                                    let Some(loc_stanox) = body.loc_stanox.clone() else {
                                        continue;
                                    };
                                    let train_id = body.current_train_id.unwrap_or(body.train_id);

                                    let state_guard = state.read().unwrap();
                                    let matched_loc = state_guard
                                        .corpus
                                        .tiplocdata
                                        .iter()
                                        .find(|entry| entry.stanox.as_deref() == Some(loc_stanox.as_str()))
                                        .unwrap();
                                    let matched_crs = matched_loc.crs.clone().unwrap();
                                    if matched_crs == String::from(" ") {
                                        debug!("Discarded Message because not a station");
                                    }
                                    debug!("Processing train {} to matched CRS {}", train_id, matched_crs);
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