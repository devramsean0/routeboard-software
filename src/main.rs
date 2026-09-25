use actix_web::{App, HttpResponse, HttpServer, get, web};
use serde::Serialize;
use tokio::sync::broadcast;
use log::info;
use std::sync::{Arc, RwLock};
use std::collections::HashMap;

use crate::state::SharedState;

mod kafka;
mod corpus;
mod state;
mod output_config;


#[get("/output")]
pub async fn get_output(state: web::Data<SharedState>) -> HttpResponse {
    let state = state.read().unwrap();
    HttpResponse::Ok().json(OutputConfigResponse {
    strings: output_config::OUTPUT_CONFIG
            .strings
            .iter()
            .map(|strings| {
                strings
                    .elements
                    .iter()
                    .map(|element| state.map.train_count(element.crs))
                    .collect()
            })
            .collect(),
    })
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv::dotenv().ok();
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    let corpus_data = corpus::update_corpus().await.unwrap();
    let station_lookup = Arc::new(
        corpus_data
            .tiplocdata
            .iter()
            .filter_map(|entry| {
                let Some(stanox) = entry.stanox.as_ref() else {
                    return None;
                };
                let Some(crs) = entry.crs.as_ref() else {
                    return None;
                };
                if crs == " " {
                    return None;
                }
                Some((stanox.clone(), crs.clone()))
            })
            .collect::<HashMap<String, String>>()
    );

    let state: state::SharedState = Arc::new(RwLock::new(state::AppState {
        //corpus: corpus_data,
        station_lookup,
        map: state::StationMap::default()
    }));

    let host = std::env::var("HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "3000".to_string())
        .parse::<u16>()
        .unwrap_or(3000);

    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    // Spawn the Kafka Listener Thread
    tokio::spawn(kafka::run_kafka_consumer(
        shutdown_tx.subscribe(),
        state.clone()
    ));


    info!("Starting Actix web server on http://{host}:{port}");
    // Start Actix-Web
    let server = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(state.clone()))
            .service(get_output)
    })
    .bind((host, port))?
    .run();

    // Listen for Ctrl+C concurrently with the server execution
    let server_handle = server.handle();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("Shutdown signal received. Stopping services...");
        
        // Signal Kafka consumer to stop
        let _ = shutdown_tx.send(());
        
        // Stop Actix server gracefully (wait for in-flight requests)
        server_handle.stop(true).await;
    });

    server.await
}

#[derive(Serialize)]
pub struct OutputConfigResponse {
    strings: Vec<Vec<usize>>,
}