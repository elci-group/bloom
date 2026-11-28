use crate::reservations::ReservationRegistry;
use bloom_core::capacity::unix_millis;
use bloom_core::{AdmissionId, CapacityVector, ModelId, NodeId, RequestId};
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
    pub throughput: ThroughputTracker,
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

/// Execution slots: running jobs plus short-lived admission holds. Both live
/// under one lock so that granting a hold and admitting a job are a single
/// atomic check against `max_active_jobs`.
#[derive(Default)]
struct Slots {
    active: HashMap<JobIdKey, ActiveJob>,
    /// Admission id -> expiry (unix ms).
    holds: HashMap<AdmissionId, u64>,
}

impl Slots {
    fn sweep(&mut self, now_ms: u64) {
        self.holds
            .retain(|_, expires_at_ms| *expires_at_ms > now_ms);
    }

    fn occupied(&self) -> usize {
        self.active.len() + self.holds.len()
    }
}

/// A granted execution-slot hold. A router obtains one from the executing
/// node before it sends the prompt, so no competing request can take the
/// slot between the routing decision and the job starting.
#[derive(Debug, Clone, Serialize)]
pub struct Admission {
    pub admission_id: AdmissionId,
    pub expires_at_ms: u64,
}

#[derive(Clone)]
pub struct JobManager {
    store: Store,
    slots: Arc<Mutex<Slots>>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct JobIdKey(String);

impl From<&bloom_core::JobId> for JobIdKey {
    fn from(id: &bloom_core::JobId) -> Self {
        Self(id.as_str().to_string())
    }
}

struct PendingJob<'a> {
    slots: &'a Mutex<Slots>,
    key: JobIdKey,
    keep: bool,
}

impl Drop for PendingJob<'_> {
    fn drop(&mut self) {
        if !self.keep {
            bloom_core::sync::mutex_lock(self.slots)
                .active
                .remove(&self.key);
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
            slots: Arc::new(Mutex::new(Slots::default())),
        }
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Hold one execution slot for `ttl`. Returns `None` when every slot is
    /// taken by a running job or an outstanding hold.
    pub fn admit(&self, max_active_jobs: u32, ttl: std::time::Duration) -> Option<Admission> {
        let now = unix_millis();
        let mut slots = bloom_core::sync::mutex_lock(&self.slots);
        slots.sweep(now);
        if slots.occupied() >= max_active_jobs as usize {
            return None;
        }
        let admission = Admission {
            admission_id: AdmissionId::generate(),
            expires_at_ms: now.saturating_add(ttl.as_millis().try_into().unwrap_or(u64::MAX)),
        };
        slots
            .holds
            .insert(admission.admission_id.clone(), admission.expires_at_ms);
        Some(admission)
    }

    /// Admit a job. A live admission hold is converted into the running job
    /// without a second capacity check; without one (or once it has expired)
    /// the job needs a free slot.
    pub async fn create(
        &self,
        node: &NodeId,
        model: &ModelId,
        kind: &str,
        request: RequestId,
        max_active_jobs: u32,
        admission: Option<&AdmissionId>,
    ) -> Result<Option<JobId>, bloom_storage::StorageError> {
        let id = JobId::generate();
        let key = JobIdKey::from(&id);
        {
            let mut slots = bloom_core::sync::mutex_lock(&self.slots);
            slots.sweep(unix_millis());
            let held = admission.is_some_and(|admission| slots.holds.remove(admission).is_some());
            if !held && slots.occupied() >= max_active_jobs as usize {
                return Ok(None);
            }
            slots.active.insert(key.clone(), ActiveJob { request });
        }
        let mut pending = PendingJob {
            slots: &self.slots,
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
        bloom_core::sync::mutex_lock(&self.slots)
            .active
            .remove(&JobIdKey::from(id));
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
        bloom_core::sync::mutex_lock(&self.slots)
            .active
            .get(&JobIdKey::from(id))
            .map(|active| active.request.clone())
    }

    pub fn active_count(&self) -> usize {
        bloom_core::sync::mutex_lock(&self.slots).active.len()
    }

    /// Running jobs plus unexpired admission holds.
    pub fn occupied_slots(&self) -> usize {
        let mut slots = bloom_core::sync::mutex_lock(&self.slots);
        slots.sweep(unix_millis());
        slots.occupied()
    }

    pub async fn list(
        &self,
        limit: u32,
    ) -> Result<Vec<bloom_storage::JobRecord>, bloom_storage::StorageError> {
        self.store.list_jobs(limit).await
    }
}

/// Measured decode throughput per model, as an exponentially weighted moving
/// average of completed jobs. In-memory only: a restarted node reports no
/// measurement until it completes work again, and routers treat an unknown
/// rate as typical rather than slow.
#[derive(Default)]
pub struct ThroughputTracker {
    rates: Mutex<HashMap<String, f64>>,
}

impl ThroughputTracker {
    const ALPHA: f64 = 0.3;
    const MIN_WINDOW: std::time::Duration = std::time::Duration::from_millis(20);

    /// Record `completion_tokens` produced over `decode_window` (first output
    /// to completion). Samples too short to time reliably are ignored.
    pub fn record(&self, model: &str, completion_tokens: u64, decode_window: std::time::Duration) {
        if completion_tokens < 2 || decode_window < Self::MIN_WINDOW {
            return;
        }
        // The first token arrives at the start of the window, so the window
        // spans `completion_tokens - 1` inter-token intervals.
        let sample = (completion_tokens - 1) as f64 / decode_window.as_secs_f64();
        let mut rates = bloom_core::sync::mutex_lock(&self.rates);
        rates
            .entry(model.to_string())
            .and_modify(|rate| *rate += Self::ALPHA * (sample - *rate))
            .or_insert(sample);
    }

    pub fn tokens_per_second(&self, model: &str) -> Option<f64> {
        bloom_core::sync::mutex_lock(&self.rates)
            .get(model)
            .copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    async fn manager() -> JobManager {
        let path = std::env::temp_dir().join(format!("bloom-slots-{}.db", JobId::generate()));
        JobManager::new(Store::open(&path).await.unwrap())
    }

    async fn start(jobs: &JobManager, admission: Option<&AdmissionId>) -> Option<JobId> {
        jobs.create(
            &NodeId::new("n"),
            &ModelId::new("m"),
            "chat",
            RequestId::generate(),
            2,
            admission,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn holds_count_against_the_slot_limit() {
        let jobs = manager().await;
        let first = jobs.admit(2, Duration::from_secs(5)).unwrap();
        assert!(jobs.admit(2, Duration::from_secs(5)).is_some());
        assert!(jobs.admit(2, Duration::from_secs(5)).is_none());
        assert_eq!(jobs.occupied_slots(), 2);
        // An unadmitted request cannot take a held slot...
        assert!(start(&jobs, None).await.is_none());
        // ...but the holder converts its hold into a job without a recheck.
        assert!(start(&jobs, Some(&first.admission_id)).await.is_some());
        assert_eq!(jobs.active_count(), 1);
        assert_eq!(jobs.occupied_slots(), 2);
        // A hold is single use.
        assert!(start(&jobs, Some(&first.admission_id)).await.is_none());
    }

    #[tokio::test]
    async fn expired_holds_release_their_slot() {
        let jobs = manager().await;
        let hold = jobs.admit(1, Duration::from_millis(1)).unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(jobs.occupied_slots(), 0);
        // An expired admission id falls back to ordinary admission.
        assert!(start(&jobs, Some(&hold.admission_id)).await.is_some());
    }

    #[test]
    fn throughput_is_a_moving_average_that_ignores_short_samples() {
        let tracker = ThroughputTracker::default();
        tracker.record("m", 101, Duration::from_millis(5));
        assert_eq!(tracker.tokens_per_second("m"), None);
        tracker.record("m", 101, Duration::from_secs(1));
        assert_eq!(tracker.tokens_per_second("m"), Some(100.0));
        tracker.record("m", 201, Duration::from_secs(1));
        let rate = tracker.tokens_per_second("m").unwrap();
        assert!((rate - 130.0).abs() < 1e-9, "{rate}");
    }
}
