use bloom_api::{AppState, EventHub, JobManager, SharedState, router};
use bloom_core::capacity::{InferenceCapacity, ModelCapacityEntry, ModelResidency, unix_millis};
use bloom_core::config::{AvailabilitySetting, BloomConfig, HarnessMode};
use bloom_core::node::{AvailabilityClass, NodeDescriptor, NodeModes};
use bloom_core::{BloomError, ModelId, NodeId};
use bloom_discovery::{Advertisement, CandidateRegistry, MdnsConfig, MdnsDiscovery};
use bloom_harness::{DisabledHarness, InferenceHarness};
use bloom_harness_ollama::{OllamaHarness, OllamaMode};
use bloom_resources::{TelemetryCollector, host_facts};
use bloom_storage::Store;
use clap::Parser;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

#[derive(Parser)]
#[command(
    name = "bloomd",
    version,
    about = "Bloom persistent inference node daemon"
)]
struct Args {
    #[arg(short, long)]
    config: Option<PathBuf>,
}

fn default_config_candidates() -> Vec<PathBuf> {
    let mut candidates = vec![PathBuf::from("bloomd.toml")];
    if let Some(home) = std::env::var_os("HOME") {
        candidates.push(PathBuf::from(home).join(".config/bloom/bloomd.toml"));
    }
    candidates.push(PathBuf::from("/etc/bloom/bloomd.toml"));
    candidates
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let args = Args::parse();
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let config = match args.config {
        Some(path) => match BloomConfig::load(&path) {
            Ok(config) => config,
            Err(e) => {
                eprintln!("bloomd: {e}");
                return std::process::ExitCode::FAILURE;
            }
        },
        None => {
            let found = default_config_candidates()
                .into_iter()
                .find(|path| path.is_file())
                .map(|path| {
                    tracing::info!(path = %path.display(), "loading default config");
                    BloomConfig::load(&path)
                })
                .transpose();
            match found {
                Ok(Some(config)) => config,
                Ok(None) => BloomConfig::default(),
                Err(e) => {
                    eprintln!("bloomd: {e}");
                    return std::process::ExitCode::FAILURE;
                }
            }
        }
    };

    match run(config).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("bloomd: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run(config: BloomConfig) -> Result<(), BloomError> {
    let api_token = config.api.resolve_token()?;
    let peer_token = config.api.resolve_peer_token(api_token.as_deref())?;
    let peer_client = (!config.swarm.peers.is_empty())
        .then(|| bloom_api::build_peer_client(config.swarm.ca_pem.as_deref()))
        .transpose()
        .map_err(BloomError::Config)?;
    let listener = tokio::net::TcpListener::bind(&config.api.bind)
        .await
        .map_err(|e| BloomError::Api(format!("cannot bind {}: {e}", config.api.bind)))?;
    let address = listener
        .local_addr()
        .map_err(|e| BloomError::Api(e.to_string()))?;
    if !address.ip().is_loopback() && api_token.is_none() {
        return Err(BloomError::Config(
            "a non-loopback API bind requires api.token_env or api.token".into(),
        ));
    }

    let data_dir = config.data_dir();
    let _data_lock = lock_data_dir(&data_dir)?;
    let store = Store::open(&config.database_path())
        .await
        .map_err(|e| BloomError::Storage(e.to_string()))?;
    let interrupted = store
        .interrupt_unfinished()
        .await
        .map_err(|e| BloomError::Storage(e.to_string()))?;
    if interrupted > 0 {
        tracing::info!(interrupted, "recovered unfinished jobs from previous run");
    }

    let node_id = match store
        .kv_get("node_id")
        .await
        .map_err(|e| BloomError::Storage(e.to_string()))?
    {
        Some(id) => NodeId::new(id),
        None => {
            let id = NodeId::generate();
            store
                .kv_set("node_id", id.as_str())
                .await
                .map_err(|e| BloomError::Storage(e.to_string()))?;
            id
        }
    };
    bloom_api::validate_peers(&config.swarm.peers, &node_id).map_err(BloomError::Config)?;

    let facts = host_facts();
    let name = if config.daemon.name.is_empty() {
        facts
            .hostname
            .clone()
            .unwrap_or_else(|| "bloom-node".to_string())
    } else {
        config.daemon.name.clone()
    };
    let availability = match config.daemon.availability {
        AvailabilitySetting::Continuous => AvailabilityClass::Continuous,
        AvailabilitySetting::Restricted => AvailabilityClass::Restricted {
            reason: "platform restricts background execution".to_string(),
        },
    };
    let harness = configure_harness(&config);
    let descriptor = NodeDescriptor {
        id: node_id.clone(),
        name,
        version: env!("CARGO_PKG_VERSION").to_string(),
        endpoint: format!("http://{address}"),
        harness: harness.kind().to_string(),
        availability: availability.clone(),
        modes: NodeModes {
            direct_inference: config.ollama.enabled,
            swarm_inference: config.ollama.enabled || !config.swarm.peers.is_empty(),
        },
    };

    let mut collector =
        TelemetryCollector::new(node_id.clone(), availability.clone(), Some(data_dir))
            .map_err(|e| BloomError::Other(format!("cannot initialize telemetry: {e}")))?;
    let initial = collector.snapshot(Vec::new(), InferenceCapacity::default(), 0);
    let discovery = configure_discovery(&config, address, &node_id)?;
    if let Err(e) = harness.start().await {
        tracing::warn!(error = %e, "harness unavailable at startup; continuing degraded");
    }

    let state: SharedState = Arc::new(AppState {
        node: descriptor,
        started_at: Instant::now(),
        harness: Arc::clone(&harness),
        capacity: RwLock::new(initial),
        jobs: JobManager::new(store.clone()),
        events: EventHub::new(),
        reservations: Default::default(),
        draining: AtomicBool::new(config.policies.drain_on_start),
        max_active_jobs: config.policies.max_active_jobs,
        api_token,
        peer_token,
        peer_client,
        peers: config.swarm.peers,
        discovery: discovery
            .as_ref()
            .map_or_else(CandidateRegistry::new, MdnsDiscovery::registry),
    });

    let interval = Duration::from_millis(config.telemetry.interval_ms.max(100));
    let telemetry_state = Arc::clone(&state);
    let telemetry_store = store.clone();
    let telemetry = tokio::spawn(telemetry_loop(
        telemetry_state,
        collector,
        interval,
        telemetry_store,
    ));

    tracing::info!(bind = %address, node = %node_id, "bloomd serving");
    state.events.publish(
        "node.started",
        format!("node {node_id} serving on {address}"),
        None,
    );

    let app = router(Arc::clone(&state));
    use std::future::IntoFuture;
    let (begin_shutdown, shutdown_started) = tokio::sync::oneshot::channel();
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = shutdown_started.await;
        })
        .into_future();
    tokio::pin!(server);
    let served = tokio::select! {
        result = &mut server => result,
        () = shutdown_signal() => {
            let _ = begin_shutdown.send(());
            match tokio::time::timeout(Duration::from_secs(10), &mut server).await {
                Ok(result) => result,
                Err(_) => {
                    tracing::warn!("graceful HTTP shutdown reached its deadline");
                    Ok(())
                }
            }
        }
    };

    telemetry.abort();
    let _ = telemetry.await;
    if let Err(e) = harness.stop().await {
        tracing::warn!(error = %e, "harness stop reported an error");
    }
    served.map_err(|e| BloomError::Api(e.to_string()))
}

fn configure_discovery(
    config: &BloomConfig,
    address: std::net::SocketAddr,
    node_id: &NodeId,
) -> Result<Option<MdnsDiscovery>, BloomError> {
    if !config.swarm.discovery && !config.swarm.advertise {
        return Ok(None);
    }
    let https_port = config.swarm.advertise_https_port;
    if config.swarm.advertise && address.ip().is_loopback() && https_port.is_none() {
        return Err(BloomError::Config(
            "LAN advertisement requires a non-loopback API bind or advertise_https_port".into(),
        ));
    }
    let discovery = MdnsConfig {
        advertisement: config.swarm.advertise.then(|| Advertisement {
            node_id: node_id.to_string(),
            port: https_port.unwrap_or(address.port()),
            https: https_port.is_some(),
            instance_name: None,
            hostname: None,
            addresses: (https_port.is_none() && !address.ip().is_unspecified())
                .then(|| vec![address.ip()]),
        }),
        ..MdnsConfig::default()
    };
    MdnsDiscovery::start(discovery)
        .map(Some)
        .map_err(|e| BloomError::Api(format!("cannot start LAN discovery: {e}")))
}

fn lock_data_dir(path: &Path) -> Result<File, BloomError> {
    std::fs::create_dir_all(path)
        .map_err(|e| BloomError::Storage(format!("cannot create {}: {e}", path.display())))?;
    let lock_path = path.join("bloomd.lock");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|e| BloomError::Storage(format!("cannot open {}: {e}", lock_path.display())))?;
    file.try_lock()
        .map_err(|e| BloomError::Storage(format!("cannot lock {}: {e}", lock_path.display())))?;
    Ok(file)
}

fn configure_harness(config: &BloomConfig) -> Arc<dyn InferenceHarness> {
    if !config.ollama.enabled {
        return Arc::new(DisabledHarness);
    }
    let mode = match config.ollama.mode {
        HarnessMode::Managed => OllamaMode::Managed,
        HarnessMode::Attached => OllamaMode::Attached,
    };
    Arc::new(OllamaHarness::new(
        config.ollama.base_url.clone(),
        config.ollama.binary.clone(),
        mode,
        config.ollama.keep_alive.clone(),
        Duration::from_millis(config.ollama.spawn_timeout_ms),
    ))
}

async fn telemetry_loop(
    state: SharedState,
    mut collector: TelemetryCollector,
    interval: Duration,
    store: Store,
) {
    let mut ticks = tokio::time::interval(interval);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let inventory_budget = (interval / 2).min(Duration::from_millis(500));
    loop {
        ticks.tick().await;
        let inference = InferenceCapacity {
            active_requests: state.jobs.active_count() as u32,
            queue_depth: 0,
            measured_tokens_per_second: None,
        };
        let inventory = tokio::time::timeout(inventory_budget, state.harness.inventory())
            .await
            .unwrap_or_default();
        let models: Vec<ModelCapacityEntry> = inventory
            .models
            .iter()
            .map(|model| ModelCapacityEntry {
                model: ModelId::new(model.id.clone()),
                quantisation: model.quantisation.clone(),
                residency: match model.resident {
                    Some(true) => ModelResidency::Resident,
                    Some(false) => ModelResidency::OnDisk,
                    None => ModelResidency::Unknown,
                },
                capabilities: model
                    .capabilities
                    .iter()
                    .filter_map(|capability| {
                        serde_json::to_value(capability)
                            .ok()
                            .and_then(|v| v.as_str().map(str::to_string))
                    })
                    .collect(),
                size_bytes: model.size_bytes,
            })
            .collect();
        let reserved = state.reservations.total_reserved();
        let vector = collector.snapshot(models, inference, reserved);
        if let Ok(payload) = serde_json::to_string(&vector) {
            let key = format!("capacity:{}", vector.node);
            if let Err(e) = store.kv_set(&key, &payload).await {
                tracing::debug!(error = %e, "cannot persist capacity snapshot");
            }
        }
        {
            let mut guard = bloom_core::sync::write_lock(&state.capacity);
            *guard = vector;
        }
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    if let Ok(mut terminate) =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
    {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate.recv() => {},
        }
        tracing::info!(at_ms = unix_millis(), "shutdown signal received");
        return;
    }
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!(at_ms = unix_millis(), "shutdown signal received");
}
