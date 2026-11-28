use crate::SharedState;
use crate::error::ApiError;
use crate::openai::{
    ChatCompletionRequest, ChatCompletionResponse, Choice, OpenAiMessage, Usage, chunk_frame,
    done_frame, error_frame,
};
use axum::Json;
use axum::body::Body;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use bloom_core::RoutingMode;
use bloom_core::capacity::unix_millis;
use bloom_core::{JobId, ModelId, RequestId};
use bloom_harness::InferenceStream;
use bloom_harness::types::{ChatMessage, InferenceRequest, StreamEvent};
use bloom_storage::JobState;
use futures::StreamExt;
use serde_json::json;

pub(crate) struct InferenceJob {
    job_id: JobId,
    pub request_id: RequestId,
    pub model: ModelId,
    state: SharedState,
    output_chars: u64,
    complete: bool,
    /// When the first output chunk arrived; starts the decode-rate window.
    first_output_at: Option<std::time::Instant>,
    completion_tokens: u64,
}

impl InferenceJob {
    async fn finish(&mut self, job_state: JobState, detail: Option<&str>) {
        finalize(
            &self.state,
            &self.job_id,
            job_state,
            self.output_chars,
            detail,
        )
        .await;
        self.complete = true;
    }
}

impl Drop for InferenceJob {
    fn drop(&mut self) {
        if self.complete {
            return;
        }
        let state = self.state.clone();
        let job_id = self.job_id.clone();
        let output_chars = self.output_chars;
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                finalize(
                    &state,
                    &job_id,
                    JobState::Interrupted,
                    output_chars,
                    Some("client disconnected before inference completed"),
                )
                .await;
            });
        }
    }
}

pub struct CollectedInference {
    pub content: String,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub finish_reason: String,
    pub interrupted: Option<String>,
}

pub(crate) async fn prepare_inference(
    state: &SharedState,
    model: String,
    messages: Vec<ChatMessage>,
    parameters: bloom_harness::GenerationParameters,
    stream: bool,
    routing: &bloom_core::RoutingOptions,
    kind: &str,
) -> Result<(InferenceJob, InferenceStream), ApiError> {
    let allow_model_loading = routing.allow_model_loading;
    if state.draining.load(std::sync::atomic::Ordering::SeqCst) {
        return Err(ApiError::unavailable("node is draining; refusing new work"));
    }
    let model_id = ModelId::new(model);
    let inventory = state.harness.inventory().await;
    let inventory_model = inventory.get(model_id.as_str());
    if !allow_model_loading && !inventory_model.is_some_and(|model| model.resident == Some(true)) {
        return Err(ApiError::not_found(format!(
            "model {} is not known to be resident and allow_model_loading is false",
            model_id
        )));
    }
    // Installed models can be loaded by the harness on first inference. Avoid an
    // explicit warmup here, which can duplicate work or race the inference call.
    if inventory_model.is_none() && allow_model_loading {
        state.harness.load_model(model_id.clone()).await?;
        state.events.publish(
            "model.load",
            format!("loaded model {model_id}"),
            Some(json!({"model": model_id.as_str()})),
        );
    }
    let request_id = RequestId::generate();
    let job_id = state
        .jobs
        .create(
            &state.node.id,
            &model_id,
            kind,
            request_id.clone(),
            state.max_active_jobs,
            routing.admission.as_ref(),
        )
        .await?
        .ok_or_else(|| ApiError::too_many_requests("active job limit reached for this node"))?;
    let mut job = InferenceJob {
        job_id: job_id.clone(),
        request_id: request_id.clone(),
        model: model_id.clone(),
        state: state.clone(),
        output_chars: 0,
        complete: false,
        first_output_at: None,
        completion_tokens: 0,
    };
    state.events.publish(
        "job.started",
        format!("job {job_id} started on {model_id}"),
        Some(json!({"job_id": job_id.as_str(), "model": model_id.as_str(), "kind": kind})),
    );
    let request = InferenceRequest {
        id: request_id.clone(),
        model: model_id.clone(),
        messages,
        parameters,
        stream,
    };
    match state.harness.infer(request).await {
        Ok(stream) => Ok((job, stream)),
        Err(error) => {
            job.finish(JobState::Failed, Some(&error.to_string())).await;
            Err(ApiError::from(error))
        }
    }
}

pub(crate) fn ledgered_events(job: InferenceJob, stream: InferenceStream) -> InferenceStream {
    Box::pin(futures::stream::unfold(
        (stream, job),
        |(mut stream, mut job)| async move {
            if job.complete {
                return None;
            }
            let event = match stream.next().await {
                Some(event) => event,
                None => Err(bloom_harness::HarnessError::Execution(
                    "inference stream closed before completion".into(),
                )),
            };
            match &event {
                Ok(StreamEvent::Chunk { delta }) => {
                    job.output_chars += delta.chars().count() as u64;
                    job.first_output_at
                        .get_or_insert_with(std::time::Instant::now);
                }
                Ok(StreamEvent::Usage {
                    completion_tokens, ..
                }) => job.completion_tokens = *completion_tokens,
                Ok(StreamEvent::Done { finish_reason }) => {
                    let state = if finish_reason == "cancelled" {
                        JobState::Cancelled
                    } else {
                        if let Some(started) = job.first_output_at {
                            job.state.throughput.record(
                                job.model.as_str(),
                                job.completion_tokens,
                                started.elapsed(),
                            );
                        }
                        JobState::Completed
                    };
                    job.finish(state, None).await;
                }
                Ok(StreamEvent::Failed { detail }) => {
                    job.finish(JobState::Interrupted, Some(detail)).await;
                }
                Err(error) => {
                    job.finish(JobState::Interrupted, Some(&error.to_string()))
                        .await;
                }
                _ => {}
            }
            Some((event, (stream, job)))
        },
    ))
}

pub async fn collect_stream(mut stream: InferenceStream) -> CollectedInference {
    let mut outcome = CollectedInference {
        content: String::new(),
        prompt_tokens: 0,
        completion_tokens: 0,
        finish_reason: "stop".to_string(),
        interrupted: None,
    };
    while let Some(event) = stream.next().await {
        match event {
            Ok(StreamEvent::Chunk { delta }) => outcome.content.push_str(&delta),
            Ok(StreamEvent::Usage {
                prompt_tokens,
                completion_tokens,
            }) => {
                outcome.prompt_tokens = prompt_tokens;
                outcome.completion_tokens = completion_tokens;
            }
            Ok(StreamEvent::Done { finish_reason }) => outcome.finish_reason = finish_reason,
            Ok(StreamEvent::Start { .. }) => {}
            Ok(StreamEvent::Failed { detail }) => {
                outcome.interrupted = Some(detail);
                break;
            }
            Err(e) => {
                outcome.interrupted = Some(e.to_string());
                break;
            }
        }
    }
    outcome
}

async fn finalize(
    state: &SharedState,
    job_id: &JobId,
    job_state: JobState,
    output_chars: u64,
    error: Option<&str>,
) {
    match state
        .jobs
        .finish(job_id, job_state, output_chars, error)
        .await
    {
        Ok(true) => {}
        Ok(false) => return,
        Err(error) => {
            tracing::warn!(%error, %job_id, "cannot persist job completion");
            return;
        }
    }
    let kind = match job_state {
        JobState::Completed => "job.completed",
        JobState::Cancelled => "job.cancelled",
        JobState::Interrupted => "job.interrupted",
        _ => "job.failed",
    };
    state.events.publish(
        kind,
        format!("job {} finished as {}", job_id, job_state.as_str()),
        Some(json!({"job_id": job_id.as_str(), "state": job_state.as_str()})),
    );
}

pub(crate) async fn chat_completions(
    State(state): State<SharedState>,
    peer: Option<axum::Extension<crate::auth::PeerRequest>>,
    Json(request): Json<ChatCompletionRequest>,
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
            "messages": request.messages.clone(),
            "stream": request.stream,
            "temperature": request.temperature,
            "top_p": request.top_p,
            "max_tokens": request.max_tokens,
            "stop": request.stop_tokens(),
            "seed": request.seed,
            // A forwarded request is always direct to prevent forwarding loops.
            "bloom": {"routing": "direct", "allow_model_loading": request.bloom.allow_model_loading}
        });
        match crate::peers::forward_json(
            &state,
            request.bloom.preferred_node.as_ref(),
            request.bloom.allow_failover,
            "/v1/chat/completions",
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
    let messages: Vec<ChatMessage> = request
        .messages
        .iter()
        .map(|m| ChatMessage::new(m.role.clone(), m.content.clone()))
        .collect();
    let (job, stream) = prepare_inference(
        &state,
        request.model.clone(),
        messages,
        request.parameters(),
        request.stream,
        &request.bloom,
        "chat",
    )
    .await?;

    let model = job.model.to_string();
    let id = format!("chatcmpl-{}", job.request_id);
    let stream = ledgered_events(job, stream);
    if request.stream {
        let body = Body::from_stream(openai_frames(stream, id, model));
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/event-stream")
            .header(header::CACHE_CONTROL, "no-cache")
            .body(body)
            .map_err(|e| ApiError::internal(e.to_string()));
    }

    let outcome = collect_stream(stream).await;
    if let Some(detail) = outcome.interrupted {
        return Err(ApiError::upstream(detail));
    }
    let response = ChatCompletionResponse {
        id,
        object: "chat.completion",
        created: unix_millis() / 1000,
        model,
        choices: vec![Choice {
            index: 0,
            message: OpenAiMessage {
                role: "assistant".to_string(),
                content: outcome.content,
            },
            finish_reason: outcome.finish_reason,
        }],
        usage: Usage::new(outcome.prompt_tokens, outcome.completion_tokens),
    };
    Ok(Json(response).into_response())
}

fn openai_frames(
    stream: InferenceStream,
    id: String,
    model: String,
) -> impl futures::Stream<Item = Result<String, std::convert::Infallible>> {
    let mut usage = Usage::new(0, 0);
    stream.filter_map(move |event| {
        let frame = match event {
            Ok(StreamEvent::Start { .. }) => Some(chunk_frame(
                &id,
                &model,
                json!({"role": "assistant", "content": ""}),
                None,
                None,
            )),
            Ok(StreamEvent::Chunk { delta }) => Some(chunk_frame(
                &id,
                &model,
                json!({"content": delta}),
                None,
                None,
            )),
            Ok(StreamEvent::Usage {
                prompt_tokens,
                completion_tokens,
            }) => {
                usage = Usage::new(prompt_tokens, completion_tokens);
                None
            }
            Ok(StreamEvent::Done { finish_reason }) => Some(
                chunk_frame(
                    &id,
                    &model,
                    json!({}),
                    Some(&finish_reason),
                    Some(usage.clone()),
                ) + &done_frame(),
            ),
            Ok(StreamEvent::Failed { detail }) => Some(error_frame(&detail) + &done_frame()),
            Err(error) => Some(error_frame(&error.to_string()) + &done_frame()),
        };
        futures::future::ready(frame.map(Ok))
    })
}
