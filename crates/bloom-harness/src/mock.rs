use crate::InferenceHarness;
use crate::error::HarnessError;
use crate::types::{
    HarnessHealth, InferenceRequest, InferenceStream, InventoryModel, ModelCapability,
    ModelInventory, ResourceSnapshot, StreamEvent,
};
use async_trait::async_trait;
use bloom_core::{ModelId, RequestId};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;

pub struct MockHarness {
    catalog: HashMap<ModelId, InventoryModel>,
    reply: String,
    chunk_delay: Duration,
    state: Arc<Mutex<MockState>>,
}

struct MockState {
    running: bool,
    loaded: HashSet<ModelId>,
    active: HashMap<RequestId, watch::Sender<bool>>,
    started: Instant,
    active_requests: u32,
}

impl MockHarness {
    pub fn new(reply: impl Into<String>) -> Self {
        let mut catalog = HashMap::new();
        for (id, capability) in [
            ("mock-chat:latest", ModelCapability::Chat),
            ("mock-embed:latest", ModelCapability::Embedding),
        ] {
            catalog.insert(
                ModelId::new(id),
                InventoryModel {
                    id: id.to_string(),
                    name: id.to_string(),
                    resident: Some(true),
                    size_bytes: Some(4_000_000_000),
                    quantisation: Some("Q4_K_M".to_string()),
                    capabilities: vec![capability],
                    modified_at_ms: None,
                },
            );
        }
        Self {
            catalog,
            reply: reply.into(),
            chunk_delay: Duration::from_millis(5),
            state: Arc::new(Mutex::new(MockState {
                running: false,
                loaded: HashSet::new(),
                active: HashMap::new(),
                started: Instant::now(),
                active_requests: 0,
            })),
        }
    }

    pub fn with_chunk_delay(mut self, delay: Duration) -> Self {
        self.chunk_delay = delay;
        self
    }

    pub fn loaded_models(&self) -> Vec<ModelId> {
        self.state.lock().unwrap().loaded.iter().cloned().collect()
    }
}

#[async_trait]
impl InferenceHarness for MockHarness {
    fn kind(&self) -> &'static str {
        "mock"
    }

    async fn start(&self) -> Result<(), HarnessError> {
        let mut state = self.state.lock().unwrap();
        state.running = true;
        state.started = Instant::now();
        Ok(())
    }

    async fn stop(&self) -> Result<(), HarnessError> {
        let mut state = self.state.lock().unwrap();
        state.running = false;
        state.loaded.clear();
        for sender in state.active.values() {
            let _ = sender.send(true);
        }
        state.active.clear();
        Ok(())
    }

    async fn health(&self) -> HarnessHealth {
        let running = self.state.lock().unwrap().running;
        if running {
            HarnessHealth::healthy("mock harness running")
        } else {
            HarnessHealth::down("mock harness stopped")
        }
    }

    async fn inventory(&self) -> ModelInventory {
        ModelInventory {
            models: self.catalog.values().cloned().collect(),
        }
    }

    async fn resources(&self) -> ResourceSnapshot {
        let state = self.state.lock().unwrap();
        ResourceSnapshot {
            loaded_models: state.loaded.len() as u32,
            resident_memory_bytes: Some(state.loaded.len() as u64 * 4_000_000_000),
            active_requests: state.active_requests,
            queue_depth: 0,
        }
    }

    async fn load_model(&self, model: ModelId) -> Result<(), HarnessError> {
        if !self.catalog.contains_key(&model) {
            return Err(HarnessError::ModelNotFound(model.to_string()));
        }
        self.state.lock().unwrap().loaded.insert(model);
        Ok(())
    }

    async fn unload_model(&self, model: ModelId) -> Result<(), HarnessError> {
        self.state.lock().unwrap().loaded.remove(&model);
        Ok(())
    }

    async fn infer(&self, request: InferenceRequest) -> Result<InferenceStream, HarnessError> {
        let mut state = self.state.lock().unwrap();
        if !state.running {
            return Err(HarnessError::Unavailable("harness not started".into()));
        }
        if !self.catalog.contains_key(&request.model) {
            return Err(HarnessError::ModelNotFound(request.model.to_string()));
        }
        state.loaded.insert(request.model.clone());
        let (cancel_tx, cancel_rx) = watch::channel(false);
        state.active.insert(request.id.clone(), cancel_tx.clone());
        state.active_requests += 1;
        drop(state);

        let reply = self.reply.clone();
        let delay = self.chunk_delay;
        let request_id = request.id.clone();
        let state = Arc::clone(&self.state);
        let (tx, rx) = mpsc::channel::<Result<StreamEvent, HarnessError>>(32);

        tokio::spawn(async move {
            let _ = tx
                .send(Ok(StreamEvent::Start {
                    model: request.model.clone(),
                }))
                .await;
            for word in reply.split_inclusive(' ') {
                if *cancel_rx.borrow() {
                    let _ = tx
                        .send(Ok(StreamEvent::Done {
                            finish_reason: "cancelled".to_string(),
                        }))
                        .await;
                    finish(&state, &request_id);
                    return;
                }
                if tx
                    .send(Ok(StreamEvent::Chunk {
                        delta: word.to_string(),
                    }))
                    .await
                    .is_err()
                {
                    finish(&state, &request_id);
                    return;
                }
                tokio::time::sleep(delay).await;
            }
            let completion_tokens = reply.split_whitespace().count() as u64;
            let _ = tx
                .send(Ok(StreamEvent::Usage {
                    prompt_tokens: request.messages.len() as u64,
                    completion_tokens,
                }))
                .await;
            let _ = tx
                .send(Ok(StreamEvent::Done {
                    finish_reason: "stop".to_string(),
                }))
                .await;
            finish(&state, &request_id);
        });

        Ok(Box::pin(ReceiverStream::new(rx)))
    }

    async fn cancel(&self, id: RequestId) -> Result<(), HarnessError> {
        let state = self.state.lock().unwrap();
        if let Some(sender) = state.active.get(&id) {
            let _ = sender.send(true);
        }
        Ok(())
    }
}

fn finish(state: &Mutex<MockState>, request_id: &RequestId) {
    let mut guard = state.lock().unwrap();
    guard.active.remove(request_id);
    guard.active_requests = guard.active_requests.saturating_sub(1);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ChatMessage;
    use crate::types::GenerationParameters;
    use futures::StreamExt;

    fn request(model: &str) -> InferenceRequest {
        InferenceRequest {
            id: RequestId::generate(),
            model: ModelId::new(model),
            messages: vec![ChatMessage::user("hi")],
            parameters: GenerationParameters::default(),
            stream: true,
        }
    }

    #[tokio::test]
    async fn streams_full_reply() {
        let harness = MockHarness::new("alpha beta gamma");
        harness.start().await.unwrap();
        let model = ModelId::new("mock-chat:latest");
        harness.load_model(model.clone()).await.unwrap();
        let mut stream = harness.infer(request("mock-chat:latest")).await.unwrap();
        let mut text = String::new();
        let mut finish = String::new();
        while let Some(event) = stream.next().await {
            match event.unwrap() {
                StreamEvent::Chunk { delta } => text.push_str(&delta),
                StreamEvent::Done { finish_reason } => finish = finish_reason,
                _ => {}
            }
        }
        assert_eq!(text, "alpha beta gamma");
        assert_eq!(finish, "stop");
    }

    #[tokio::test]
    async fn rejects_unknown_model() {
        let harness = MockHarness::new("x");
        harness.start().await.unwrap();
        let error = harness.infer(request("missing:latest")).await;
        assert!(matches!(error, Err(HarnessError::ModelNotFound(_))));
    }
}
