use crate::reservations::ReservationRegistry;
use bloom_core::capacity::unix_millis;
use bloom_core::{CapacityVector, ModelId, NodeId, RequestId};
use bloom_harness::InferenceHarness;
use bloom_storage::Store;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock, atomic::AtomicBool};
use std::time::Instant;

pub struct AppState {
    pub node: NodeDescriptor,
    pub started_at: Instant,
    pub harness: Arc<dyn InferenceHarness>,
    pub capacity: RwLock<CapacityVector>,
    pub jobs: JobManager,
    pub events: EventHub,
    pub reservations: ReservationRegistry,
    pub draining: AtomicBool,
    pub max_active_jobs: u32,
    pub api_token: Option<String>,
    pub peer_token: Option<String>,
    pub peer_client: Option<reqwest::Client>,
    /// Explicitly trusted peers eligible for swarm routing.
    pub peers: Vec<bloom_core::config::PeerConfig>,
    pub discovery: bloom_discovery::CandidateRegistry,
}

pub type SharedState = Arc<AppState>;

use bloom_core::node::NodeDescriptor;

#[derive(Debug, Clone, Serialize)]
pub struct BloomEvent {
    pub at_ms: u64,
    pub kind: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

#[derive(Clone)]
pub struct EventHub {
    tx: tokio::sync::broadcast::Sender<BloomEvent>,
}

impl Default for EventHub {
    fn default() -> Self {
        Self::new()
    }
}

impl EventHub {
    pub fn new() -> Self {
        let (tx, _) = tokio::sync::broadcast::channel(256);
        Self { tx }
    }

    pub fn publish(&self, kind: &str, message: impl Into<String>, data: Option<serde_json::Value>) {
        let event = BloomEvent {
            at_ms: unix_millis(),
            kind: kind.to_string(),
            message: message.into(),
            data,
        };
        let _ = self.tx.send(event);
    }

    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<BloomEvent> {
        self.tx.subscribe()
    }
}

struct ActiveJob {
    request: RequestId,
}

#[derive(Clone)]
pub struct JobManager {
    store: Store,
    active: Arc<Mutex<HashMap<JobIdKey, ActiveJob>>>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct JobIdKey(String);

impl From<&bloom_core::JobId> for JobIdKey {
    fn from(id: &bloom_core::JobId) -> Self {
        Self(id.as_str().to_string())
    }
}

struct PendingJob<'a> {
    active: &'a Mutex<HashMap<JobIdKey, ActiveJob>>,
    key: JobIdKey,
    keep: bool,
}

impl Drop for PendingJob<'_> {
    fn drop(&mut self) {
        if !self.keep {
            bloom_core::sync::mutex_lock(self.active).remove(&self.key);
        }
    }
}

use bloom_core::JobId;

#[derive(Debug)]
pub enum CancelOutcome {
    Cancelled(RequestId),
    AlreadyTerminal(bloom_storage::JobState),
    NotFound,
}

impl JobManager {
    pub fn new(store: Store) -> Self {
        Self {
            store,
            active: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub async fn create(
        &self,
        node: &NodeId,
        model: &ModelId,
        kind: &str,
        request: RequestId,
        max_active_jobs: u32,
    ) -> Result<Option<JobId>, bloom_storage::StorageError> {
        let id = JobId::generate();
        let key = JobIdKey::from(&id);
        {
            let mut active = bloom_core::sync::mutex_lock(&self.active);
            if active.len() >= max_active_jobs as usize {
                return Ok(None);
            }
            active.insert(key.clone(), ActiveJob { request });
        }
        let mut pending = PendingJob {
            active: &self.active,
            key,
            keep: false,
        };
        let now = unix_millis();
        let record = bloom_storage::JobRecord {
            id: id.clone(),
            node: node.clone(),
            model: model.clone(),
            kind: kind.to_string(),
            state: bloom_storage::JobState::Running,
            created_at_ms: now,
            updated_at_ms: now,
            output_chars: 0,
            error: None,
            meta: None,
        };
        self.store.create_job(&record).await?;
        pending.keep = true;
        Ok(Some(id))
    }

    pub async fn finish(
        &self,
        id: &JobId,
        state: bloom_storage::JobState,
        output_chars: u64,
        error: Option<&str>,
    ) -> Result<bool, bloom_storage::StorageError> {
        let result = self.store.finish_job(id, state, output_chars, error).await;
        bloom_core::sync::mutex_lock(&self.active).remove(&JobIdKey::from(id));
        result
    }

    pub async fn cancel(&self, id: &JobId) -> Result<CancelOutcome, bloom_storage::StorageError> {
        let request = self.request_for(id);
        let changed = self
            .store
            .finish_job(id, bloom_storage::JobState::Cancelled, 0, None)
            .await?;
        if changed {
            return Ok(CancelOutcome::Cancelled(
                request.unwrap_or_else(|| RequestId::new("")),
            ));
        }
        Ok(match self.store.job(id).await? {
            Some(record) => CancelOutcome::AlreadyTerminal(record.state),
            None => CancelOutcome::NotFound,
        })
    }

    pub fn request_for(&self, id: &JobId) -> Option<RequestId> {
        bloom_core::sync::mutex_lock(&self.active)
            .get(&JobIdKey::from(id))
            .map(|active| active.request.clone())
    }

    pub fn active_count(&self) -> usize {
        bloom_core::sync::mutex_lock(&self.active).len()
    }

    pub async fn list(
        &self,
        limit: u32,
    ) -> Result<Vec<bloom_storage::JobRecord>, bloom_storage::StorageError> {
        self.store.list_jobs(limit).await
    }
}
