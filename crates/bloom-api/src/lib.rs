pub mod auth;
pub mod chat;
pub mod control;
pub mod error;
pub mod openai;
pub mod peers;
pub mod reservations;
pub mod state;

pub use error::ApiError;
pub use peers::{build_peer_client, validate_peers};
pub use state::{AppState, BloomEvent, EventHub, JobManager, SharedState};

use axum::routing::{get, post};
use axum::{Router, middleware};

pub fn router(state: SharedState) -> Router {
    Router::new()
        .route("/v1/chat/completions", post(chat::chat_completions))
        .route("/v1/embeddings", post(control::embeddings))
        .route(
            "/v1/audio/transcriptions",
            post(control::audio_transcriptions),
        )
        .route("/v1/models", get(control::models_list))
        .route("/bloom/v1/swarm", get(control::swarm))
        .route("/bloom/v1/discovery", get(control::discovery))
        .route("/bloom/v1/nodes", get(control::nodes))
        .route("/bloom/v1/nodes/drain", post(control::drain))
        .route("/bloom/v1/resources", get(control::resources))
        .route("/bloom/v1/jobs", get(control::jobs))
        .route("/bloom/v1/jobs/cancel", post(control::cancel_job))
        .route("/bloom/v1/inference", post(control::inference))
        .route("/bloom/v1/reservations", post(control::reserve))
        .route("/bloom/v1/models/load", post(control::load_model))
        .route("/bloom/v1/models/unload", post(control::unload_model))
        .route("/bloom/v1/events", get(control::events))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth::require_token,
        ))
        .with_state(state)
}
