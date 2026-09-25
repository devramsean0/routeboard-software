use std::sync::LazyLock;

use actix_web::{web, HttpResponse};
use serde::Serialize;

use crate::state::{AppState, SharedState};

static OUTPUT_CONFIG: LazyLock<OutputConfig> = LazyLock::new(|| OutputConfig {
    strings: vec![OutputStrings {
        elements: vec![
            OutputElement { crs: "CHD" },
            OutputElement { crs: "SHF" },
            OutputElement { crs: "STP" },
        ],
    }],
});

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.route("/output-config", web::get().to(get_output_config));
}

async fn get_output_config(state: web::Data<SharedState>) -> HttpResponse {
    let state = state.read().unwrap();
    HttpResponse::Ok().json(render_output_config(&state))
}

fn render_output_config(state: &AppState) -> OutputConfigResponse {
    OutputConfigResponse {
        strings: OUTPUT_CONFIG
            .strings
            .iter()
            .map(|strings| OutputStringsResponse {
                elements: strings
                    .elements
                    .iter()
                    .map(|element| OutputElementResponse {
                        station: StationResponse {
                            crs: element.crs.to_string(),
                            occupied: state.map.is_occupied(element.crs),
                            train_count: state.map.train_count(element.crs),
                        },
                    })
                    .collect(),
            })
            .collect(),
    }
}

#[derive(Serialize)]
pub struct OutputConfigResponse {
    strings: Vec<OutputStringsResponse>,
}

#[derive(Serialize)]
pub struct OutputStringsResponse {
    elements: Vec<OutputElementResponse>,
}

#[derive(Serialize)]
pub struct OutputElementResponse {
    station: StationResponse,
}

#[derive(Serialize)]
pub struct StationResponse {
    crs: String,
    occupied: bool,
    train_count: usize,
}

struct OutputConfig {
    strings: Vec<OutputStrings>,
}

struct OutputStrings {
    elements: Vec<OutputElement>,
}

struct OutputElement {
    crs: &'static str,
}