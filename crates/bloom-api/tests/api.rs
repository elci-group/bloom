use bloom_api::reservations::ReservationRegistry;
use bloom_api::{AppState, EventHub, JobManager, SharedState, router};
use bloom_core::capacity::CapacityVector;
use bloom_core::node::{AvailabilityClass, NodeDescriptor, NodeModes};
use bloom_core::{JobId, NodeId};
use bloom_harness::{InferenceHarness, MockHarness};
use bloom_storage::Store;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

async fn temp_store(tag: &str) -> Store {
    let path = std::env::temp_dir().join(format!("bloom-api-{}-{}.db", tag, JobId::generate()));
    Store::open(&path).await.unwrap()
}

async fn test_state(tag: &str, token: Option<&str>, reply: &str, delay: Duration) -> SharedState {
    let harness = Arc::new(MockHarness::new(reply).with_chunk_delay(delay));
    harness.start().await.unwrap();
    let descriptor = NodeDescriptor {
        id: NodeId::generate(),
        name: "test-node".to_string(),
        version: "0.1.0".to_string(),
        endpoint: "http://127.0.0.1:7420".to_string(),
        harness: "mock".to_string(),
        availability: AvailabilityClass::Continuous,
        modes: NodeModes::default(),
    };
    let store = temp_store(tag).await;
    let mut capacity = CapacityVector::initial(NodeId::new("test"));
    capacity.memory.total_bytes = 16_000_000_000;
    capacity.memory.available_bytes = 8_000_000_000;
    Arc::new(AppState {
        node: descriptor,
        started_at: Instant::now(),
        harness,
        capacity: RwLock::new(capacity),
        jobs: JobManager::new(store),
        events: EventHub::new(),
        reservations: ReservationRegistry::default(),
        throughput: Default::default(),
        draining: AtomicBool::new(false),
        max_active_jobs: 4,
        api_token: token.map(str::to_string),
        peer_token: None,
        peer_client: None,
        peers: Vec::new(),
        discovery: bloom_discovery::CandidateRegistry::default(),
    })
}

async fn spawn_app(state: SharedState) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    format!("http://{addr}")
}

async fn chat_body(base: &str, model: &str, stream: bool) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{base}/v1/chat/completions"))
        .json(&serde_json::json!({
            "model": model,
            "messages": [{"role": "user", "content": "hello"}],
            "stream": stream,
            "bloom": {"routing": "direct", "allow_model_loading": true}
        }))
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn lists_models_and_resources() {
    let state = test_state("meta", None, "alpha beta", Duration::from_millis(1)).await;
    let base = spawn_app(state.clone()).await;

    let models: serde_json::Value = reqwest::get(format!("{base}/v1/models"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(models["object"], "list");
    assert!(models["data"].as_array().unwrap().len() >= 2);

    let resources: serde_json::Value = reqwest::get(format!("{base}/bloom/v1/resources"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resources["cpu"]["arch"], std::env::consts::ARCH);

    let swarm: serde_json::Value = reqwest::get(format!("{base}/bloom/v1/swarm"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(swarm["size"], 1);
    assert_eq!(swarm["mode"], "single_node");
}

#[tokio::test]
async fn chat_non_stream_records_job() {
    let state = test_state("chat", None, "alpha beta gamma", Duration::from_millis(1)).await;
    let base = spawn_app(state.clone()).await;

    let response = chat_body(&base, "mock-chat:latest", false).await;
    let status = response.status();
    let raw = response.text().await.unwrap_or_default();
    assert!(status.is_success(), "status: {status}, body: {raw}");
    let body: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(body["choices"][0]["message"]["content"], "alpha beta gamma");
    assert_eq!(body["usage"]["completion_tokens"], 3);

    let jobs: serde_json::Value = reqwest::get(format!("{base}/bloom/v1/jobs"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let jobs = jobs["jobs"].as_array().unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0]["state"], "completed");
    assert_eq!(jobs[0]["kind"], "chat");
    assert_eq!(jobs[0]["output_chars"], "alpha beta gamma".chars().count());
}

#[tokio::test]
async fn chat_stream_emits_sse_frames() {
    let state = test_state("stream", None, "one two three", Duration::from_millis(1)).await;
    let base = spawn_app(state).await;

    let mut response = chat_body(&base, "mock-chat:latest", true).await;
    assert!(response.status().is_success());
    assert!(
        response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap()
            .starts_with("text/event-stream")
    );

    let mut text = String::new();
    while let Some(chunk) = response.chunk().await.unwrap() {
        text.push_str(&String::from_utf8_lossy(&chunk));
    }
    assert!(text.contains("chat.completion.chunk"));
    assert!(text.contains("one"));
    assert!(text.contains("finish_reason"));
    assert!(text.contains("data: [DONE]"));
}

#[tokio::test]
async fn rejects_model_when_loading_disallowed() {
    let state = test_state("noload", None, "x", Duration::from_millis(1)).await;
    let base = spawn_app(state).await;

    let response = reqwest::Client::new()
        .post(format!("{base}/v1/chat/completions"))
        .json(&serde_json::json!({
            "model": "missing-model:latest",
            "messages": [{"role": "user", "content": "hi"}],
            "bloom": {"allow_model_loading": false}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn reservation_conflicts_on_oversubscription() {
    let state = test_state("reserve", None, "x", Duration::from_millis(1)).await;
    let base = spawn_app(state).await;

    let resources: serde_json::Value = reqwest::get(format!("{base}/bloom/v1/resources"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let available = resources["memory"]["available_bytes"].as_u64().unwrap();

    let client = reqwest::Client::new();
    let huge = client
        .post(format!("{base}/bloom/v1/reservations"))
        .json(&serde_json::json!({"memory_bytes": u64::MAX, "ttl_seconds": 5}))
        .send()
        .await
        .unwrap();
    assert_eq!(huge.status(), reqwest::StatusCode::CONFLICT);

    let half = client
        .post(format!("{base}/bloom/v1/reservations"))
        .json(&serde_json::json!({"memory_bytes": available / 2, "ttl_seconds": 5}))
        .send()
        .await
        .unwrap();
    assert_eq!(half.status(), reqwest::StatusCode::CREATED);

    let second = client
        .post(format!("{base}/bloom/v1/reservations"))
        .json(&serde_json::json!({"memory_bytes": available, "ttl_seconds": 5}))
        .send()
        .await
        .unwrap();
    assert_eq!(second.status(), reqwest::StatusCode::CONFLICT);
}

#[tokio::test]
async fn drain_rejects_new_inference() {
    let state = test_state("drain", None, "x", Duration::from_millis(1)).await;
    let base = spawn_app(state).await;

    let drain = reqwest::Client::new()
        .post(format!("{base}/bloom/v1/nodes/drain"))
        .json(&serde_json::json!({"enabled": true}))
        .send()
        .await
        .unwrap();
    assert!(drain.status().is_success());

    let response = chat_body(&base, "mock-chat:latest", false).await;
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);

    reqwest::Client::new()
        .post(format!("{base}/bloom/v1/nodes/drain"))
        .json(&serde_json::json!({"enabled": false}))
        .send()
        .await
        .unwrap();
    let response = chat_body(&base, "mock-chat:latest", false).await;
    assert!(response.status().is_success());
}

#[tokio::test]
async fn auth_fails_closed_when_token_configured() {
    let state = test_state("auth", Some("s3cret"), "x", Duration::from_millis(1)).await;
    let base = spawn_app(state).await;
    let client = reqwest::Client::new();

    let denied = client
        .get(format!("{base}/v1/models"))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), reqwest::StatusCode::UNAUTHORIZED);

    let wrong = client
        .get(format!("{base}/v1/models"))
        .header("authorization", "Bearer wrong")
        .send()
        .await
        .unwrap();
    assert_eq!(wrong.status(), reqwest::StatusCode::UNAUTHORIZED);

    let allowed = client
        .get(format!("{base}/v1/models"))
        .header("authorization", "Bearer s3cret")
        .send()
        .await
        .unwrap();
    assert!(allowed.status().is_success());
}

#[tokio::test]
async fn cancel_running_job() {
    let long_reply = "word ".repeat(400);
    let state = test_state("cancel", None, &long_reply, Duration::from_millis(20)).await;
    let base = spawn_app(state).await;
    let client = reqwest::Client::new();

    let mut stream = chat_body(&base, "mock-chat:latest", true).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let jobs: serde_json::Value = client
        .get(format!("{base}/bloom/v1/jobs"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let running = jobs["jobs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|job| job["state"] == "running")
        .expect("a running job exists while streaming");
    let job_id = running["id"].as_str().unwrap().to_string();

    let cancel = client
        .post(format!("{base}/bloom/v1/jobs/cancel"))
        .json(&serde_json::json!({"job_id": job_id}))
        .send()
        .await
        .unwrap();
    assert!(cancel.status().is_success());

    let mut text = String::new();
    while let Some(chunk) = stream.chunk().await.unwrap() {
        text.push_str(&String::from_utf8_lossy(&chunk));
        if text.contains("cancelled") {
            break;
        }
    }
    assert!(text.contains("cancelled"));

    let jobs: serde_json::Value = client
        .get(format!("{base}/bloom/v1/jobs"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let job = jobs["jobs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|job| job["id"] == job_id.as_str())
        .unwrap();
    assert_eq!(job["state"], "cancelled");

    let again = client
        .post(format!("{base}/bloom/v1/jobs/cancel"))
        .json(&serde_json::json!({"job_id": job_id}))
        .send()
        .await
        .unwrap();
    assert_eq!(again.status(), reqwest::StatusCode::CONFLICT);
}

#[tokio::test]
async fn events_stream_publishes_model_load() {
    let state = test_state("events", None, "x", Duration::from_millis(1)).await;
    let base = spawn_app(state).await;
    let client = reqwest::Client::new();

    let mut events = client
        .get(format!("{base}/bloom/v1/events"))
        .send()
        .await
        .unwrap();
    assert!(events.status().is_success());

    tokio::time::sleep(Duration::from_millis(100)).await;
    let load = client
        .post(format!("{base}/bloom/v1/models/load"))
        .json(&serde_json::json!({"model": "mock-embed:latest"}))
        .send()
        .await
        .unwrap();
    assert!(load.status().is_success());

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut seen = String::new();
    while Instant::now() < deadline && !seen.contains("model.load") {
        let Ok(Ok(Some(chunk))) =
            tokio::time::timeout(Duration::from_secs(1), events.chunk()).await
        else {
            continue;
        };
        seen.push_str(&String::from_utf8_lossy(&chunk));
    }
    assert!(seen.contains("model.load"), "events seen: {seen}");
}

#[tokio::test]
async fn bloom_inference_non_stream() {
    let state = test_state("bloominfer", None, "reply text", Duration::from_millis(1)).await;
    let base = spawn_app(state).await;

    let response = reqwest::Client::new()
        .post(format!("{base}/bloom/v1/inference"))
        .json(&serde_json::json!({
            "model": "mock-chat:latest",
            "prompt": "say something"
        }))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["content"], "reply text");
    assert_eq!(body["finish_reason"], "stop");
}

#[tokio::test]
async fn peer_credentials_only_allow_status_and_direct_inference() {
    let mut state = test_state("peer-scope", Some("operator"), "reply", Duration::ZERO).await;
    Arc::get_mut(&mut state).unwrap().peer_token = Some("peer".into());
    let base = spawn_app(state).await;
    let client = reqwest::Client::new();

    for (path, status) in [
        ("/bloom/v1/nodes", reqwest::StatusCode::OK),
        ("/bloom/v1/jobs", reqwest::StatusCode::FORBIDDEN),
        ("/bloom/v1/discovery", reqwest::StatusCode::FORBIDDEN),
    ] {
        let response = client
            .get(format!("{base}{path}"))
            .bearer_auth("peer")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), status);
    }

    for (routing, status) in [
        ("direct", reqwest::StatusCode::OK),
        ("swarm", reqwest::StatusCode::FORBIDDEN),
    ] {
        let response = client
            .post(format!("{base}/v1/chat/completions"))
            .bearer_auth("peer")
            .json(&serde_json::json!({
                "model": "mock-chat:latest",
                "messages": [{"role": "user", "content": "hello"}],
                "bloom": {"routing": routing}
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), status);
    }
}

#[tokio::test]
async fn concurrent_streams_respect_admission_and_release_on_disconnect() {
    let mut state = test_state(
        "concurrent",
        None,
        &"word ".repeat(1_000),
        Duration::from_millis(25),
    )
    .await;
    Arc::get_mut(&mut state).unwrap().max_active_jobs = 1;
    let base = spawn_app(state.clone()).await;
    let (chat, native) = tokio::join!(chat_body(&base, "mock-chat:latest", true), async {
        reqwest::Client::new()
            .post(format!("{base}/bloom/v1/inference"))
            .json(&serde_json::json!({
                "model": "mock-chat:latest",
                "prompt": "hello",
                "stream": true
            }))
            .send()
            .await
            .unwrap()
    },);
    let mut statuses = [chat.status(), native.status()];
    statuses.sort_unstable();
    assert_eq!(
        statuses,
        [
            reqwest::StatusCode::OK,
            reqwest::StatusCode::TOO_MANY_REQUESTS
        ]
    );
    drop((chat, native));

    tokio::time::timeout(Duration::from_secs(3), async {
        while state.jobs.active_count() != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("disconnect must release the active slot");
    let jobs = state.jobs.list(10).await.unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].state, bloom_storage::JobState::Interrupted);
}

#[tokio::test]
async fn node_status_reports_live_scheduling_hint() {
    let state = test_state("sched", None, "ok", Duration::from_millis(1)).await;
    let base = spawn_app(state.clone()).await;
    let client = reqwest::Client::new();
    let hint = |model: &'static str| {
        let client = client.clone();
        let base = base.clone();
        async move {
            let body: serde_json::Value = client
                .get(format!("{base}/bloom/v1/nodes?model={model}"))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            body["nodes"][0]["scheduling"].clone()
        }
    };

    let warm = hint("mock-chat:latest").await;
    assert_eq!(warm["available_slots"], 4);
    assert_eq!(warm["model_resident"], true);
    assert_eq!(warm["available_memory_bytes"], 8_000_000_000u64);
    assert!(hint("absent:latest").await["model_resident"].is_null());

    let reserved = client
        .post(format!("{base}/bloom/v1/reservations"))
        .json(&serde_json::json!({"memory_bytes": 3_000_000_000u64, "ttl_seconds": 30}))
        .send()
        .await
        .unwrap();
    assert_eq!(reserved.status(), reqwest::StatusCode::CREATED);
    // Reservations are reflected immediately, not on the next telemetry tick.
    assert_eq!(
        hint("mock-chat:latest").await["available_memory_bytes"],
        5_000_000_000u64
    );
}
