use crate::chat::{collect_stream, ledgered_events, prepare_inference};
use crate::error::ApiError;
use crate::openai::OpenAiMessage;
use crate::reservations::ReservationError;
use crate::state::{CancelOutcome, SharedState};
use axum::Json;
use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use bloom_core::node::NodeStatus;
use bloom_core::{CapacityVector, ModelId, RoutingMode, RoutingOptions};
use bloom_harness::types::{ChatMessage, StreamEvent};
use futures::StreamExt;
use serde::Deserialize;
use serde_json::{Value, json};
use std::convert::Infallible;
use std::time::Duration;
use tokio_stream::wrappers::BroadcastStream;

pub async fn embeddings() -> Response {
    ApiError::not_supported("embedding harness integration arrives with the modular harness matrix")
        .into_response()
}

pub async fn audio_transcriptions() -> Response {
    ApiError::not_supported("audio modality support arrives with the harness matrix")
        .into_response()
}

pub async fn models_list(State(state): State<SharedState>) -> Response {
    let inventory = state.harness.inventory().await;
    let data: Vec<Value> = inventory
        .models
        .iter()
        .map(|model| {
            json!({
                "id": model.id,
                "object": "model",
                "created": model.modified_at_ms.unwrap_or_default() / 1000,
                "owned_by": "bloom",
                "bloom": {
                    "name": model.name,
                    "capabilities": model.capabilities,
                    "quantisation": model.quantisation,
                    "size_bytes": model.size_bytes,
                    "resident": model.resident,
                }
            })
        })
        .collect();
    Json(json!({"object": "list", "data": data})).into_response()
}

async fn node_view(state: &SharedState, model: Option<&str>) -> Value {
    let health = state.harness.health().await;
    let inventory_model = match model {
        Some(model) => state.harness.inventory().await.get(model).cloned(),
        None => None,
    };
    let capacity = bloom_core::sync::read_lock(&state.capacity).clone();
    let active_jobs = state.jobs.active_count() as u32;
    let occupied_slots = u32::try_from(state.jobs.occupied_slots()).unwrap_or(u32::MAX);
    let available_slots = state.max_active_jobs.saturating_sub(occupied_slots);
    let throughput_tps = model.and_then(|model| state.throughput.tokens_per_second(model));
    let model_resident = model.and_then(|model| {
        inventory_model
            .as_ref()
            .and_then(|entry| entry.resident)
            .or_else(|| {
                capacity
                    .models
                    .iter()
                    .find(|entry| entry.model.as_str() == model)
                    .map(|entry| entry.residency == bloom_core::capacity::ModelResidency::Resident)
            })
    });
    let status = NodeStatus {
        healthy: true,
        harness_healthy: health.healthy,
        draining: state.draining.load(std::sync::atomic::Ordering::SeqCst),
        uptime_seconds: state.started_at.elapsed().as_secs(),
        active_jobs,
    };
    let capacity_updated_at_ms = capacity.updated_at_ms;
    json!({
        "descriptor": state.node,
        "status": status,
        "capacity_updated_at_ms": capacity_updated_at_ms,
        "scheduling": {
            "available_slots": available_slots,
            "available_memory_bytes": capacity.memory.available_bytes.saturating_sub(state.reservations.total_reserved()),
            "model_resident": model_resident,
            "throughput_tps": throughput_tps,
        },
    })
}

pub async fn swarm(State(state): State<SharedState>) -> Response {
    let node = node_view(&state, None).await;
    let peers: Vec<_> = state
        .peers
        .iter()
        .map(|peer| {
            json!({
                "node_id": peer.node_id.as_str(),
                "url": peer.url,
                "trusted": true,
            })
        })
        .collect();
    Json(json!({
        "id": state.node.id.as_str(),
        "mode": if peers.is_empty() { "single_node" } else { "configured_swarm" },
        "size": peers.len() + 1,
        "nodes": [node],
        "peers": peers,
    }))
    .into_response()
}

pub async fn discovery(State(state): State<SharedState>) -> Response {
    let own_id = state.node.id.as_str();
    let candidates: Vec<_> = state
        .discovery
        .snapshot()
        .into_iter()
        .filter(|candidate| candidate.node_id.as_deref() != Some(own_id))
        .collect();
    Json(json!({"candidates": candidates, "trusted": false})).into_response()
}

#[derive(Debug, Deserialize)]
pub struct NodesQuery {
    pub model: Option<String>,
}

pub async fn nodes(
    State(state): State<SharedState>,
    axum::extract::Query(query): axum::extract::Query<NodesQuery>,
) -> Response {
    let node = node_view(&state, query.model.as_deref()).await;
    Json(json!({"nodes": [node]})).into_response()
}

#[derive(Deserialize)]
pub struct DrainRequest {
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

pub async fn drain(State(state): State<SharedState>, body: Option<Json<DrainRequest>>) -> Response {
    let enabled = body.map(|Json(body)| body.enabled).unwrap_or(true);
    state
        .draining
        .store(enabled, std::sync::atomic::Ordering::SeqCst);
    state.events.publish(
        "node.drain",
        if enabled {
            "node entering drain mode"
        } else {
            "node leaving drain mode"
        },
        Some(json!({"draining": enabled})),
    );
    Json(json!({"draining": enabled})).into_response()
}

pub async fn resources(State(state): State<SharedState>) -> Response {
    let vector: CapacityVector = bloom_core::sync::read_lock(&state.capacity).clone();
    Json(vector).into_response()
}

pub async fn jobs(State(state): State<SharedState>) -> Result<Response, ApiError> {
    let jobs = state.jobs.list(100).await?;
    Ok(Json(json!({"jobs": jobs})).into_response())
}

#[derive(Deserialize)]
pub struct CancelJobRequest {
    pub job_id: String,
}

pub async fn cancel_job(
    State(state): State<SharedState>,
    Json(request): Json<CancelJobRequest>,
) -> Result<Response, ApiError> {
    use bloom_core::JobId;
    let job_id = JobId::new(request.job_id);
    match state.jobs.cancel(&job_id).await? {
        CancelOutcome::Cancelled(request_id) => {
            state.harness.cancel(request_id).await?;
            state.events.publish(
                "job.cancelled",
                format!("job {job_id} cancelled by client"),
                Some(json!({"job_id": job_id.as_str()})),
            );
            Ok(Json(json!({"status": "cancelled", "job_id": job_id.as_str()})).into_response())
        }
        CancelOutcome::AlreadyTerminal(terminal) => Err(ApiError::conflict(format!(
            "job {} is already terminal ({})",
            job_id,
            terminal.as_str()
        ))),
        CancelOutcome::NotFound => Err(ApiError::not_found(format!("job {job_id} not found"))),
    }
}

#[derive(Deserialize)]
pub struct InferenceRequest {
    pub model: String,
    #[serde(default)]
    pub prompt: Option<String>,
    #[serde(default)]
    pub messages: Option<Vec<OpenAiMessage>>,
    #[serde(default)]
    pub stream: bool,
    #[serde(default)]
    pub bloom: RoutingOptions,
}

pub(crate) async fn inference(
    State(state): State<SharedState>,
    peer: Option<axum::Extension<crate::auth::PeerRequest>>,
    Json(request): Json<InferenceRequest>,
) -> Result<Response, ApiError> {
    if peer.is_some() && request.bloom.routing != RoutingMode::Direct {
        return Err(ApiError::forbidden(
            "peer credential requires direct routing",
        ));
    }
    let preferred_local = request
        .bloom
        .preferred_node
        .as_ref()
        .is_some_and(|node| node.as_str() == state.node.id.as_str());
    if request.bloom.routing == RoutingMode::Swarm && !preferred_local {
        let mut body = json!({
            "model": request.model.clone(),
            "prompt": request.prompt.clone(),
            "messages": request.messages.clone(),
            "stream": request.stream,
            "bloom": {"routing": "direct", "allow_model_loading": request.bloom.allow_model_loading}
        });
        match crate::peers::forward_json(
            &state,
            request.bloom.preferred_node.as_ref(),
            request.bloom.allow_failover,
            "/bloom/v1/inference",
            &mut body,
        )
        .await
        {
            Ok(Some(response)) => return Ok(response),
            Ok(None) if request.bloom.allow_failover => {}
            Ok(None) => return Err(ApiError::unavailable("no eligible swarm peer is available")),
            Err(error) if request.bloom.allow_failover => {
                tracing::warn!(%error, "swarm forwarding failed; falling back to local inference");
            }
            Err(error) => return Err(error),
        }
    }
    let messages: Vec<ChatMessage> = match (&request.messages, &request.prompt) {
        (Some(messages), _) => messages
            .iter()
            .map(|m| ChatMessage::new(m.role.clone(), m.content.clone()))
            .collect(),
        (None, Some(prompt)) => vec![ChatMessage::user(prompt.clone())],
        (None, None) => {
            return Err(ApiError::bad_request(
                "provide either prompt or messages".to_string(),
            ));
        }
    };
    let (job, stream) = prepare_inference(
        &state,
        request.model.clone(),
        messages,
        Default::default(),
        request.stream,
        &request.bloom,
        "inference",
    )
    .await?;

    let model = job.model.to_string();
    let stream = ledgered_events(job, stream);
    if request.stream {
        let frames = stream.filter_map(|event| {
            let (kind, payload) = match event {
                Ok(StreamEvent::Start { .. }) => return futures::future::ready(None),
                Ok(StreamEvent::Chunk { delta }) => ("chunk", json!({"delta": delta})),
                Ok(StreamEvent::Usage {
                    prompt_tokens,
                    completion_tokens,
                }) => (
                    "usage",
                    json!({"prompt_tokens": prompt_tokens, "completion_tokens": completion_tokens}),
                ),
                Ok(StreamEvent::Done { finish_reason }) => {
                    ("done", json!({"finish_reason": finish_reason}))
                }
                Ok(StreamEvent::Failed { detail }) => ("error", json!({"message": detail})),
                Err(error) => ("error", json!({"message": error.to_string()})),
            };
            futures::future::ready(Some(Ok::<_, Infallible>(
                Event::default().event(kind).data(payload.to_string()),
            )))
        });
        return Ok(Sse::new(frames)
            .keep_alive(KeepAlive::default())
            .into_response());
    }

    let outcome = collect_stream(stream).await;
    if let Some(detail) = outcome.interrupted {
        return Err(ApiError::upstream(detail));
    }
    Ok(Json(json!({
        "model": model,
        "content": outcome.content,
        "finish_reason": outcome.finish_reason,
        "usage": {
            "prompt_tokens": outcome.prompt_tokens,
            "completion_tokens": outcome.completion_tokens,
        }
    }))
    .into_response())
}

/// Default and ceiling for admission holds. A router forwards immediately
/// after it is granted a hold, so the hold only needs to outlive one request
/// round trip; an abandoned hold frees itself when it expires.
const DEFAULT_ADMISSION_TTL_MS: u64 = 5_000;
const MAX_ADMISSION_TTL_MS: u64 = 10_000;

#[derive(Deserialize, Default)]
pub struct AdmissionRequest {
    #[serde(default)]
    pub ttl_ms: Option<u64>,
}

/// Grant a routing peer a short-lived hold on one execution slot.
pub async fn admit(
    State(state): State<SharedState>,
    body: Option<Json<AdmissionRequest>>,
) -> Result<Response, ApiError> {
    if state.draining.load(std::sync::atomic::Ordering::SeqCst) {
        return Err(ApiError::unavailable("node is draining; refusing new work"));
    }
    let ttl_ms = body
        .and_then(|Json(body)| body.ttl_ms)
        .unwrap_or(DEFAULT_ADMISSION_TTL_MS)
        .clamp(1, MAX_ADMISSION_TTL_MS);
    let admission = state
        .jobs
        .admit(state.max_active_jobs, Duration::from_millis(ttl_ms))
        .ok_or_else(|| ApiError::too_many_requests("no free execution slot on this node"))?;
    Ok((
        axum::http::StatusCode::CREATED,
        Json(json!({
            "admission_id": admission.admission_id,
            "expires_at_ms": admission.expires_at_ms,
            "node_id": state.node.id.as_str(),
        })),
    )
        .into_response())
}

#[derive(Deserialize)]
pub struct ReservationRequest {
    pub memory_bytes: u64,
    #[serde(default = "default_ttl_seconds")]
    pub ttl_seconds: u64,
}

fn default_ttl_seconds() -> u64 {
    30
}

pub async fn reserve(
    State(state): State<SharedState>,
    Json(request): Json<ReservationRequest>,
) -> Result<Response, ApiError> {
    if request.memory_bytes == 0 {
        return Err(ApiError::bad_request("memory_bytes must be positive"));
    }
    let available = bloom_core::sync::read_lock(&state.capacity)
        .memory
        .available_bytes;
    match state.reservations.reserve(
        available,
        request.memory_bytes,
        Duration::from_secs(request.ttl_seconds),
    ) {
        Ok(reservation) => Ok((axum::http::StatusCode::CREATED, Json(reservation)).into_response()),
        Err(ReservationError::Insufficient {
            requested,
            available,
            reserved,
        }) => Err(ApiError::conflict(format!(
            "cannot reserve {requested} bytes: {available} available, {reserved} already reserved"
        ))),
    }
}

#[derive(Deserialize)]
pub struct ModelRequest {
    pub model: String,
}

pub async fn load_model(
    State(state): State<SharedState>,
    Json(request): Json<ModelRequest>,
) -> Result<Response, ApiError> {
    let model = ModelId::new(request.model);
    state.harness.load_model(model.clone()).await?;
    state.events.publish(
        "model.load",
        format!("loaded model {model}"),
        Some(json!({"model": model.as_str()})),
    );
    Ok(Json(json!({"status": "loaded", "model": model.as_str()})).into_response())
}

pub async fn unload_model(
    State(state): State<SharedState>,
    Json(request): Json<ModelRequest>,
) -> Result<Response, ApiError> {
    let model = ModelId::new(request.model);
    state.harness.unload_model(model.clone()).await?;
    state.events.publish(
        "model.unload",
        format!("unloaded model {model}"),
        Some(json!({"model": model.as_str()})),
    );
    Ok(Json(json!({"status": "unloaded", "model": model.as_str()})).into_response())
}

pub async fn events(
    State(state): State<SharedState>,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    let receiver = state.events.subscribe();
    let stream = BroadcastStream::new(receiver).map(|item| match item {
        Ok(event) => {
            let mut sse_event = Event::default().event(event.kind.clone());
            if let Ok(data) = serde_json::to_string(&event) {
                sse_event = sse_event.data(data);
            }
            Ok(sse_event)
        }
        Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(skipped)) => {
            Ok(Event::default()
                .event("lagged")
                .data(json!({"skipped": skipped}).to_string()))
        }
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}
