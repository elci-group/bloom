pub mod ndjson;

use async_trait::async_trait;
use bloom_core::{ModelId, RequestId};
use bloom_harness::HarnessError;
use bloom_harness::InferenceHarness;
use bloom_harness::types::{
    GenerationParameters, HarnessHealth, InferenceRequest, InferenceStream, InventoryModel,
    ModelCapability, ModelInventory, ResourceSnapshot, StreamEvent,
};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::process::Child;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;

const MAX_NDJSON_LINE_BYTES: usize = 1024 * 1024;
const MAX_PROCESS_RESPONSE_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OllamaMode {
    Attached,
    Managed,
}

pub struct OllamaHarness {
    base_url: String,
    binary: String,
    mode: OllamaMode,
    keep_alive: String,
    spawn_timeout: Duration,
    client: reqwest::Client,
    state: Arc<Mutex<OllamaState>>,
}

struct OllamaState {
    loaded: HashSet<ModelId>,
    cancels: HashMap<RequestId, watch::Sender<bool>>,
    child: Option<Child>,
    active_requests: u32,
    started: Option<Instant>,
}

struct RequestRegistration {
    state: Arc<Mutex<OllamaState>>,
    request_id: RequestId,
    cancel_tx: watch::Sender<bool>,
}

impl Drop for RequestRegistration {
    fn drop(&mut self) {
        let mut state = bloom_core::sync::mutex_lock(&self.state);
        let registered = state
            .cancels
            .get(&self.request_id)
            .is_some_and(|sender| sender.same_channel(&self.cancel_tx));
        if registered {
            state.cancels.remove(&self.request_id);
            state.active_requests = state.active_requests.saturating_sub(1);
        }
    }
}

impl OllamaHarness {
    pub fn new(
        base_url: impl Into<String>,
        binary: impl Into<String>,
        mode: OllamaMode,
        keep_alive: impl Into<String>,
        spawn_timeout: Duration,
    ) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            binary: binary.into(),
            mode,
            keep_alive: keep_alive.into(),
            spawn_timeout,
            client: reqwest::Client::new(),
            state: Arc::new(Mutex::new(OllamaState {
                loaded: HashSet::new(),
                cancels: HashMap::new(),
                child: None,
                active_requests: 0,
                started: None,
            })),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    async fn process_models(&self) -> Option<Vec<OllamaProcessModel>> {
        let mut response = self
            .client
            .get(self.url("/api/ps"))
            .timeout(Duration::from_secs(2))
            .send()
            .await
            .ok()?;
        if !response.status().is_success()
            || response
                .content_length()
                .is_some_and(|length| length > MAX_PROCESS_RESPONSE_BYTES as u64)
        {
            return None;
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.ok()? {
            if body.len().saturating_add(chunk.len()) > MAX_PROCESS_RESPONSE_BYTES {
                tracing::warn!("ollama process list exceeded response size limit");
                return None;
            }
            body.extend_from_slice(&chunk);
        }
        match serde_json::from_slice::<ProcessesResponse>(&body) {
            Ok(processes) => Some(processes.models),
            Err(error) => {
                tracing::warn!(%error, "cannot parse ollama process list");
                None
            }
        }
    }

    async fn version(&self) -> Option<String> {
        let response = self
            .client
            .get(self.url("/api/version"))
            .timeout(Duration::from_secs(2))
            .send()
            .await
            .ok()?;
        if !response.status().is_success() {
            return None;
        }
        let body: serde_json::Value = response.json().await.ok()?;
        body.get("version")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    }

    async fn wait_until_healthy(&self, timeout: Duration) -> Result<String, HarnessError> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(version) = self.version().await {
                return Ok(version);
            }
            if Instant::now() >= deadline {
                return Err(HarnessError::Unavailable(format!(
                    "ollama did not become reachable at {} within {:?}",
                    self.base_url, timeout
                )));
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    fn options(parameters: &GenerationParameters) -> serde_json::Value {
        let mut options = serde_json::Map::new();
        if let Some(temperature) = parameters.temperature {
            options.insert("temperature".into(), serde_json::json!(temperature));
        }
        if let Some(top_p) = parameters.top_p {
            options.insert("top_p".into(), serde_json::json!(top_p));
        }
        if let Some(max_tokens) = parameters.max_tokens {
            options.insert("num_predict".into(), serde_json::json!(max_tokens));
        }
        if let Some(seed) = parameters.seed {
            options.insert("seed".into(), serde_json::json!(seed));
        }
        if !parameters.stop.is_empty() {
            options.insert("stop".into(), serde_json::json!(parameters.stop));
        }
        serde_json::Value::Object(options)
    }
}

#[derive(Deserialize)]
struct TagsResponse {
    #[serde(default)]
    models: Vec<TagModel>,
}

#[derive(Deserialize)]
struct TagModel {
    name: String,
    #[serde(default)]
    size: Option<u64>,
    #[serde(default)]
    details: Option<TagDetails>,
}

#[derive(Deserialize)]
struct TagDetails {
    #[serde(default, rename = "quantization_level")]
    quantization_level: Option<String>,
}

#[derive(Deserialize)]
struct ProcessesResponse {
    #[serde(default)]
    models: Vec<OllamaProcessModel>,
}

#[derive(Deserialize)]
struct OllamaProcessModel {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    size: Option<u64>,
}

impl OllamaProcessModel {
    fn matches_name(&self, name: &str) -> bool {
        self.name.as_deref() == Some(name) || self.model.as_deref() == Some(name)
    }
}

#[async_trait]
impl InferenceHarness for OllamaHarness {
    fn kind(&self) -> &'static str {
        "ollama"
    }

    async fn start(&self) -> Result<(), HarnessError> {
        if self.mode == OllamaMode::Managed {
            let child = tokio::process::Command::new(&self.binary)
                .arg("serve")
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .map_err(|e| HarnessError::Config(format!("cannot spawn {}: {e}", self.binary)))?;
            bloom_core::sync::mutex_lock(&self.state).child = Some(child);
        }
        let version = match self.wait_until_healthy(self.spawn_timeout).await {
            Ok(version) => version,
            Err(error) => {
                let child = bloom_core::sync::mutex_lock(&self.state).child.take();
                if let Some(mut child) = child {
                    let _ = child.kill().await;
                    let _ = child.wait().await;
                }
                return Err(error);
            }
        };
        bloom_core::sync::mutex_lock(&self.state).started = Some(Instant::now());
        tracing::info!(version = %version, base_url = %self.base_url, "ollama harness ready");
        Ok(())
    }

    async fn stop(&self) -> Result<(), HarnessError> {
        let child = {
            let mut state = bloom_core::sync::mutex_lock(&self.state);
            for sender in state.cancels.values() {
                let _ = sender.send(true);
            }
            state.child.take()
        };
        if let Some(mut child) = child {
            let _ = child.kill().await;
        }
        Ok(())
    }

    async fn health(&self) -> HarnessHealth {
        let started = bloom_core::sync::mutex_lock(&self.state).started;
        match self.version().await {
            Some(version) => HarnessHealth {
                healthy: true,
                detail: format!("ollama {version}"),
                uptime_seconds: started.map(|s| s.elapsed().as_secs()),
            },
            None => HarnessHealth::down(format!("no response from {}", self.base_url)),
        }
    }

    async fn inventory(&self) -> ModelInventory {
        let response = self
            .client
            .get(self.url("/api/tags"))
            .timeout(Duration::from_secs(5))
            .send()
            .await;
        let response = match response {
            Ok(response) if response.status().is_success() => response,
            Ok(response) => {
                tracing::warn!(status = %response.status(), "ollama tags request failed");
                return ModelInventory::default();
            }
            Err(e) => {
                tracing::warn!(error = %e, "ollama tags request failed");
                return ModelInventory::default();
            }
        };
        let tags: TagsResponse = match response.json().await {
            Ok(tags) => tags,
            Err(e) => {
                tracing::warn!(error = %e, "cannot parse ollama tags");
                return ModelInventory::default();
            }
        };
        let resident = self.process_models().await;
        let models = tags
            .models
            .into_iter()
            .map(|tag| {
                let capability = if tag.name.contains("embed") {
                    ModelCapability::Embedding
                } else {
                    ModelCapability::Chat
                };
                let is_resident = resident.as_ref().map(|running| {
                    running
                        .iter()
                        .any(|process| process.matches_name(&tag.name))
                });
                InventoryModel {
                    id: tag.name.clone(),
                    name: tag.name,
                    size_bytes: tag.size,
                    quantisation: tag.details.and_then(|d| d.quantization_level),
                    capabilities: vec![capability],
                    modified_at_ms: None,
                    resident: is_resident,
                }
            })
            .collect();
        ModelInventory { models }
    }

    async fn resources(&self) -> ResourceSnapshot {
        let process_models = self.process_models().await;
        let mut state = bloom_core::sync::mutex_lock(&self.state);
        let (loaded_models, resident_memory_bytes) = if let Some(models) = process_models {
            state.loaded = models
                .iter()
                .filter_map(|model| model.name.as_ref().or(model.model.as_ref()))
                .map(|name| ModelId::new(name.clone()))
                .collect();
            let resident_memory_bytes = models
                .iter()
                .try_fold(0_u64, |total, model| total.checked_add(model.size?));
            (
                models.len().min(u32::MAX as usize) as u32,
                resident_memory_bytes,
            )
        } else {
            tracing::debug!("using cached Ollama residency after /api/ps failure");
            (state.loaded.len().min(u32::MAX as usize) as u32, None)
        };
        ResourceSnapshot {
            loaded_models,
            resident_memory_bytes,
            active_requests: state.active_requests,
            queue_depth: 0,
        }
    }

    async fn load_model(&self, model: ModelId) -> Result<(), HarnessError> {
        let response = self
            .client
            .post(self.url("/api/generate"))
            .timeout(Duration::from_secs(600))
            .json(&serde_json::json!({
                "model": model.as_str(),
                "prompt": "",
                "keep_alive": self.keep_alive,
                "stream": false,
            }))
            .send()
            .await
            .map_err(|e| HarnessError::Execution(format!("load request failed: {e}")))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            if body.contains("not found") {
                return Err(HarnessError::ModelNotFound(model.to_string()));
            }
            return Err(HarnessError::Execution(format!(
                "load failed with status {status}: {body}"
            )));
        }
        bloom_core::sync::mutex_lock(&self.state)
            .loaded
            .insert(model);
        Ok(())
    }

    async fn unload_model(&self, model: ModelId) -> Result<(), HarnessError> {
        let response = self
            .client
            .post(self.url("/api/generate"))
            .timeout(Duration::from_secs(60))
            .json(&serde_json::json!({
                "model": model.as_str(),
                "prompt": "",
                "keep_alive": 0,
                "stream": false,
            }))
            .send()
            .await
            .map_err(|e| HarnessError::Execution(format!("unload request failed: {e}")))?;
        if !response.status().is_success() {
            return Err(HarnessError::Execution(format!(
                "unload failed with status {}",
                response.status()
            )));
        }
        bloom_core::sync::mutex_lock(&self.state)
            .loaded
            .remove(&model);
        Ok(())
    }

    async fn infer(&self, request: InferenceRequest) -> Result<InferenceStream, HarnessError> {
        let (cancel_tx, mut cancel_rx) = watch::channel(false);
        let registration = {
            let mut state = bloom_core::sync::mutex_lock(&self.state);
            if state.cancels.contains_key(&request.id) {
                return Err(HarnessError::Execution(format!(
                    "request ID {} is already active",
                    request.id
                )));
            }
            state.cancels.insert(request.id.clone(), cancel_tx.clone());
            state.active_requests = state.active_requests.saturating_add(1);
            RequestRegistration {
                state: Arc::clone(&self.state),
                request_id: request.id.clone(),
                cancel_tx,
            }
        };

        let messages: Vec<serde_json::Value> = request
            .messages
            .iter()
            .map(|m| serde_json::json!({"role": m.role, "content": m.content}))
            .collect();
        let payload = serde_json::json!({
            "model": request.model.as_str(),
            "messages": messages,
            "stream": true,
            "keep_alive": self.keep_alive,
            "options": Self::options(&request.parameters),
        });

        let send = self
            .client
            .post(self.url("/api/chat"))
            .json(&payload)
            .send();
        let response = tokio::select! {
            result = send => result,
            changed = cancel_rx.changed() => {
                let _ = changed;
                return Err(HarnessError::Cancelled);
            }
        };
        let response = match response {
            Ok(response) if response.status().is_success() => response,
            Ok(response) => {
                let status = response.status();
                let body = tokio::select! {
                    body = response.text() => body.unwrap_or_default(),
                    changed = cancel_rx.changed() => {
                        let _ = changed;
                        return Err(HarnessError::Cancelled);
                    }
                };
                return Err(if body.contains("not found") {
                    HarnessError::ModelNotFound(request.model.to_string())
                } else {
                    HarnessError::Execution(format!("chat failed with status {status}: {body}"))
                });
            }
            Err(e) => {
                return Err(HarnessError::Execution(format!("chat request failed: {e}")));
            }
        };

        let model = request.model.clone();
        let (tx, rx) = mpsc::channel::<Result<StreamEvent, HarnessError>>(64);

        tokio::spawn(async move {
            let mut response = response;
            let mut buffer: Vec<u8> = Vec::new();
            let mut cancelled = false;
            let mut consumer_gone = false;
            let mut saw_done = false;
            let mut done_reason = "stop".to_string();
            let mut prompt_tokens: Option<u64> = None;
            let mut completion_tokens: Option<u64> = None;

            if tx
                .send(Ok(StreamEvent::Start {
                    model: model.clone(),
                }))
                .await
                .is_err()
            {
                consumer_gone = true;
            }

            let mut stream_error: Option<String> = None;
            while !consumer_gone && !cancelled && !saw_done && stream_error.is_none() {
                let chunk = tokio::select! {
                    _ = cancel_rx.changed() => {
                        cancelled = *cancel_rx.borrow();
                        continue;
                    }
                    _ = tx.closed() => {
                        consumer_gone = true;
                        continue;
                    }
                    chunk = response.chunk() => match chunk {
                        Ok(chunk) => chunk,
                        Err(e) => {
                            stream_error = Some(format!("stream interrupted: {e}"));
                            break;
                        }
                    },
                };
                if cancelled || consumer_gone {
                    break;
                }
                let Some(bytes) = chunk else {
                    break;
                };

                let mut remaining = bytes.as_ref();
                loop {
                    let newline = remaining.iter().position(|byte| *byte == b'\n');
                    let segment_len = newline.map_or(remaining.len(), |position| position + 1);
                    if buffer.len().saturating_add(segment_len) > MAX_NDJSON_LINE_BYTES {
                        stream_error = Some(format!(
                            "ollama stream line exceeded {MAX_NDJSON_LINE_BYTES} bytes"
                        ));
                        break;
                    }
                    buffer.extend_from_slice(&remaining[..segment_len]);
                    remaining = &remaining[segment_len..];

                    if newline.is_none() {
                        break;
                    }
                    let line = String::from_utf8_lossy(&buffer);
                    let event = ndjson::chat_event(&line);
                    buffer.clear();
                    match event {
                        Some(ndjson::ChatEvent::Delta(delta)) => {
                            if tx.send(Ok(StreamEvent::Chunk { delta })).await.is_err() {
                                consumer_gone = true;
                                break;
                            }
                        }
                        Some(ndjson::ChatEvent::Done {
                            reason,
                            prompt_tokens: prompt,
                            completion_tokens: completion,
                        }) => {
                            saw_done = true;
                            done_reason = reason;
                            prompt_tokens = prompt;
                            completion_tokens = completion;
                            break;
                        }
                        None => {}
                    }
                    if consumer_gone || saw_done || stream_error.is_some() {
                        break;
                    }
                }
            }

            // Accept a final JSON object even if the HTTP body omits its newline.
            if !consumer_gone
                && !cancelled
                && !saw_done
                && stream_error.is_none()
                && !buffer.is_empty()
            {
                let line = String::from_utf8_lossy(&buffer);
                match ndjson::chat_event(&line) {
                    Some(ndjson::ChatEvent::Delta(delta)) => {
                        if tx.send(Ok(StreamEvent::Chunk { delta })).await.is_err() {
                            consumer_gone = true;
                        }
                    }
                    Some(ndjson::ChatEvent::Done {
                        reason,
                        prompt_tokens: prompt,
                        completion_tokens: completion,
                    }) => {
                        saw_done = true;
                        done_reason = reason;
                        prompt_tokens = prompt;
                        completion_tokens = completion;
                    }
                    None => {}
                }
            }

            if !consumer_gone {
                if cancelled {
                    let _ = tx
                        .send(Ok(StreamEvent::Done {
                            finish_reason: "cancelled".to_string(),
                        }))
                        .await;
                } else if let Some(error) = stream_error {
                    let _ = tx.send(Err(HarnessError::Execution(error))).await;
                } else if saw_done {
                    if let Some(prompt) = prompt_tokens {
                        let _ = tx
                            .send(Ok(StreamEvent::Usage {
                                prompt_tokens: prompt,
                                completion_tokens: completion_tokens.unwrap_or_default(),
                            }))
                            .await;
                    }
                    let _ = tx
                        .send(Ok(StreamEvent::Done {
                            finish_reason: done_reason,
                        }))
                        .await;
                } else {
                    let _ = tx
                        .send(Err(HarnessError::Execution(
                            "ollama stream ended before its completion event".into(),
                        )))
                        .await;
                }
            }
            drop(registration);
        });

        Ok(Box::pin(ReceiverStream::new(rx)))
    }

    async fn cancel(&self, id: RequestId) -> Result<(), HarnessError> {
        let state = bloom_core::sync::mutex_lock(&self.state);
        if let Some(sender) = state.cancels.get(&id) {
            let _ = sender.send(true);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bloom_harness::types::{ChatMessage, GenerationParameters};
    use futures::StreamExt;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn inference_request(id: RequestId) -> InferenceRequest {
        InferenceRequest {
            id,
            model: ModelId::new("test-model"),
            messages: vec![ChatMessage::user("hello")],
            parameters: GenerationParameters::default(),
            stream: true,
        }
    }

    async fn read_http_request(stream: &mut tokio::net::TcpStream) {
        let mut request = Vec::new();
        let mut chunk = [0_u8; 1024];
        loop {
            let read = stream.read(&mut chunk).await.unwrap();
            assert_ne!(read, 0, "client closed before sending request");
            request.extend_from_slice(&chunk[..read]);
            if let Some(header_end) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&request[..header_end]);
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())
                            .flatten()
                    })
                    .unwrap_or_default();
                if request.len() >= header_end + 4 + content_length {
                    return;
                }
            }
        }
    }

    #[test]
    fn process_list_accepts_ollama_name_and_model_fields() {
        let processes: ProcessesResponse = serde_json::from_str(
            r#"{"models":[{"name":"llama3:latest","model":"llama3:latest","size":1234}]}"#,
        )
        .unwrap();
        assert!(processes.models[0].matches_name("llama3:latest"));
        assert!(!processes.models[0].matches_name("llama3"));
        assert_eq!(processes.models[0].size, Some(1234));
    }

    #[test]
    fn process_list_allows_empty_server_response() {
        let processes: ProcessesResponse = serde_json::from_str(r#"{"models":[]}"#).unwrap();
        assert!(processes.models.is_empty());
    }

    #[tokio::test]
    async fn dropping_infer_while_http_request_is_pending_releases_registration() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            let _ = accepted_tx.send(());
            std::future::pending::<()>().await;
        });
        let harness = Arc::new(OllamaHarness::new(
            format!("http://{address}"),
            "ollama",
            OllamaMode::Attached,
            "5m",
            Duration::from_secs(1),
        ));
        let request_id = RequestId::generate();
        let infer_harness = Arc::clone(&harness);
        let request =
            tokio::spawn(async move { infer_harness.infer(inference_request(request_id)).await });

        tokio::time::timeout(Duration::from_secs(2), accepted_rx)
            .await
            .unwrap()
            .unwrap();
        request.abort();
        let _ = request.await;
        let state = bloom_core::sync::mutex_lock(&harness.state);
        assert_eq!(state.active_requests, 0);
        assert!(state.cancels.is_empty());
        server.abort();
    }

    #[tokio::test]
    async fn dropping_stream_receiver_stops_pump_and_releases_registration() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let line = r#"{"message":{"content":"hello"},"done":false}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n{:X}\r\n{}\n\r\n",
            line.len() + 1,
            line
        );
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            read_http_request(&mut stream).await;
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.flush().await.unwrap();
            std::future::pending::<()>().await;
        });
        let harness = OllamaHarness::new(
            format!("http://{address}"),
            "ollama",
            OllamaMode::Attached,
            "5m",
            Duration::from_secs(1),
        );
        let mut stream = harness
            .infer(inference_request(RequestId::generate()))
            .await
            .unwrap();
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            StreamEvent::Start { .. }
        ));
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            StreamEvent::Chunk { .. }
        ));
        drop(stream);

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if bloom_core::sync::mutex_lock(&harness.state).active_requests == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(
            bloom_core::sync::mutex_lock(&harness.state)
                .cancels
                .is_empty()
        );
        server.abort();
    }

    #[tokio::test]
    async fn stream_without_done_ends_with_error_and_no_done_event() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let line = r#"{"message":{"content":"hello"},"done":false}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:X}\r\n{}\n\r\n0\r\n\r\n",
            line.len() + 1,
            line
        );
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            read_http_request(&mut stream).await;
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.flush().await.unwrap();
        });
        let harness = OllamaHarness::new(
            format!("http://{address}"),
            "ollama",
            OllamaMode::Attached,
            "5m",
            Duration::from_secs(1),
        );
        let mut stream = harness
            .infer(inference_request(RequestId::generate()))
            .await
            .unwrap();
        let mut saw_error = false;
        while let Some(event) = stream.next().await {
            match event {
                Err(_) => saw_error = true,
                Ok(StreamEvent::Done { .. }) => panic!("incomplete stream emitted Done"),
                _ => {}
            }
        }
        assert!(saw_error);
        server.await.unwrap();
        assert_eq!(
            bloom_core::sync::mutex_lock(&harness.state).active_requests,
            0
        );
    }
}
