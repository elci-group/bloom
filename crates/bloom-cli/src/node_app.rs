//! Small Linux Goya host for the Bloom Node scene.
//!
//! `goya-scene` owns the window and input. This controller watches its button
//! events, refreshes the scene through transactional hot-swap, and talks only
//! to the configured Bloom node plus its explicitly supplied HTTPS address.

use clap::Parser;
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;

const SCENE_TEMPLATE: &str = include_str!("../../../apps/bloom-node.gscene");

#[derive(Debug, Parser)]
#[command(
    name = "bloom-node-app",
    about = "Goya dashboard for a local Bloom node"
)]
struct Args {
    /// Local bloomd API. Plain HTTP is accepted only on loopback.
    #[arg(long, default_value = "http://127.0.0.1:7420")]
    api_url: String,
    /// Operator token. If configured, read it from the environment.
    #[arg(long, env = "BLOOM_API_TOKEN", hide_env_values = true)]
    token: Option<String>,
    /// bloomd.toml to check before enabling remote sharing.
    #[arg(long)]
    config: Option<PathBuf>,
    /// HTTPS address served by bloomd directly or through a reverse proxy.
    #[arg(long, env = "BLOOM_PUBLIC_URL")]
    public_url: Option<String>,
    /// Optional private CA bundle used to verify the HTTPS node address.
    #[arg(long)]
    ca_pem: Option<PathBuf>,
    /// Generate a self-signed LAN server certificate for this DNS name or IP and exit.
    #[arg(long, value_name = "DNS_OR_IP")]
    generate_cert_for: Option<String>,
    /// Directory for the generated certificate and private key.
    #[arg(long)]
    cert_dir: Option<PathBuf>,
    /// Goya's live scene host executable.
    #[arg(long, default_value = "goya-scene")]
    goya_scene: PathBuf,
    /// Refresh interval in milliseconds.
    #[arg(long, default_value_t = 3000)]
    refresh_ms: u64,
}

#[derive(Default)]
struct View {
    connection: String,
    state: String,
    node_id: String,
    endpoint: String,
    action_status: String,
    cpu: String,
    memory: String,
    slots: String,
    accelerator: String,
    models: String,
    discovery: String,
    peers: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    if let Some(host) = args.generate_cert_for.as_deref() {
        if let Err(error) = generate_certificate(host, args.cert_dir.as_deref()) {
            eprintln!("bloom-node-app: {error}");
            std::process::exit(1);
        }
        return;
    }
    if let Err(error) = run(args).await {
        eprintln!("bloom-node-app: {error}");
        std::process::exit(1);
    }
}

async fn run(args: Args) -> Result<(), String> {
    let local = parse_base_url(&args.api_url, true)?;
    if args.refresh_ms < 500 {
        return Err("--refresh-ms must be at least 500".into());
    }
    let public = args
        .public_url
        .as_deref()
        .map(|url| parse_base_url(url, false))
        .transpose()?;
    let mut client_builder = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::none());
    if let Some(path) = &args.ca_pem {
        let pem = std::fs::read(path)
            .map_err(|error| format!("cannot read CA bundle {}: {error}", path.display()))?;
        let certificates = reqwest::Certificate::from_pem_bundle(&pem)
            .map_err(|error| format!("invalid CA bundle {}: {error}", path.display()))?;
        if certificates.is_empty() {
            return Err(format!(
                "CA bundle {} contains no certificates",
                path.display()
            ));
        }
        for certificate in certificates {
            client_builder = client_builder.add_root_certificate(certificate);
        }
    }
    let client = client_builder
        .build()
        .map_err(|error| format!("cannot create API client: {error}"))?;

    let scene_path = temporary_scene_path();
    let mut view = View {
        connection: "offline".into(),
        state: "Waiting for local bloomd".into(),
        node_id: "node id: unavailable".into(),
        endpoint: "secure address: not configured".into(),
        action_status: "".into(),
        cpu: "cpu: unavailable".into(),
        memory: "memory: unavailable".into(),
        slots: "free jobs: unavailable".into(),
        accelerator: "gpu or npu: unavailable".into(),
        models: "models: unavailable".into(),
        discovery: "lan discovery: unavailable".into(),
        peers: "trusted peers: unavailable".into(),
    };
    write_scene(&scene_path, &view)?;

    let mut child = Command::new(&args.goya_scene)
        .args(["--live", "--watch", "--file"])
        .arg(&scene_path)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| {
            format!(
                "cannot start {}: {error} (install Goya and pass --goya-scene PATH)",
                args.goya_scene.display()
            )
        })?;
    let (events_tx, mut events_rx) = mpsc::channel(16);
    forward_scene_events(&mut child, events_tx)?;

    let mut refresh = tokio::time::interval(Duration::from_millis(args.refresh_ms));
    refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_source = String::new();

    loop {
        tokio::select! {
            status = child.wait() => {
                let status = status.map_err(|error| format!("Goya scene host failed: {error}"))?;
                let _ = std::fs::remove_file(&scene_path);
                if !status.success() {
                    return Err(format!("Goya scene host exited with {status}"));
                }
                return Ok(());
            }
            _ = refresh.tick() => {
                refresh_view(&client, &local, &args.token, public.as_ref(), &mut view).await;
                write_scene_if_changed(&scene_path, &view, &mut last_source)?;
            }
            event = events_rx.recv() => {
                let Some(event) = event else { continue };
                match event.as_str() {
                    "refresh" => {
                        refresh_view(&client, &local, &args.token, public.as_ref(), &mut view).await;
                        if view.connection != "offline" {
                            view.action_status = "Status refreshed".into();
                        }
                    }
                    "pause_sharing" => {
                        match set_draining(&client, &local, &args.token, true).await {
                            Ok(()) => view.action_status = "New inference paused".into(),
                            Err(error) => view.action_status = error,
                        }
                        refresh_view(&client, &local, &args.token, public.as_ref(), &mut view).await;
                    }
                    "enable_sharing" => {
                        match enable_sharing(&client, &local, public.as_ref(), &args).await {
                            Ok(()) => view.action_status = "Secure sharing is enabled".into(),
                            Err(error) => view.action_status = error,
                        }
                        refresh_view(&client, &local, &args.token, public.as_ref(), &mut view).await;
                    }
                    _ => {}
                }
                write_scene_if_changed(&scene_path, &view, &mut last_source)?;
            }
        }
    }
}

fn forward_scene_events(child: &mut Child, tx: mpsc::Sender<String>) -> Result<(), String> {
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "Goya scene host did not expose its event stream".to_string())?;
    tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if let Some(name) = line
                .split_once("button '")
                .and_then(|(_, rest)| rest.split_once("' activated").map(|(name, _)| name))
                && tx.send(name.to_owned()).await.is_err()
            {
                break;
            }
        }
    });
    Ok(())
}

fn parse_base_url(value: &str, allow_loopback_http: bool) -> Result<reqwest::Url, String> {
    let url = reqwest::Url::parse(value).map_err(|error| format!("invalid URL: {error}"))?;
    let loopback = url.host_str().is_some_and(|host| {
        host.eq_ignore_ascii_case("localhost") || host == "127.0.0.1" || host == "[::1]"
    });
    let secure = url.scheme() == "https";
    if !(secure || (allow_loopback_http && loopback && url.scheme() == "http"))
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("API URLs must use HTTPS; local HTTP is allowed only on loopback, with no URL credentials, query or fragment".into());
    }
    Ok(url)
}

fn endpoint(base: &reqwest::Url, path: &str) -> reqwest::Url {
    let mut url = base.clone();
    let prefix = base.path().trim_end_matches('/');
    url.set_path(&format!("{prefix}{path}"));
    url.set_query(None);
    url.set_fragment(None);
    url
}

async fn get_json(
    client: &reqwest::Client,
    base: &reqwest::Url,
    path: &str,
    token: Option<&str>,
) -> Result<Value, String> {
    let mut request = client.get(endpoint(base, path));
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let response = request
        .send()
        .await
        .map_err(|error| format!("{}: {error}", path))?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!("{} returned {status}", path));
    }
    response
        .json()
        .await
        .map_err(|error| format!("invalid response from {}: {error}", path))
}

async fn post_draining(
    client: &reqwest::Client,
    base: &reqwest::Url,
    token: Option<&str>,
    enabled: bool,
) -> Result<(), String> {
    let mut request = client
        .post(endpoint(base, "/bloom/v1/nodes/drain"))
        .json(&serde_json::json!({ "enabled": enabled }));
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let response = request
        .send()
        .await
        .map_err(|error| format!("could not update node sharing: {error}"))?;
    if !response.status().is_success() {
        return Err(format!(
            "node sharing update returned {}",
            response.status()
        ));
    }
    Ok(())
}

async fn set_draining(
    client: &reqwest::Client,
    local: &reqwest::Url,
    token: &Option<String>,
    enabled: bool,
) -> Result<(), String> {
    post_draining(client, local, token.as_deref(), enabled).await
}

async fn enable_sharing(
    client: &reqwest::Client,
    local: &reqwest::Url,
    public: Option<&reqwest::Url>,
    args: &Args,
) -> Result<(), String> {
    let Some(public) = public else {
        return Err("Set BLOOM_PUBLIC_URL to this node's HTTPS address".into());
    };
    let token = args
        .token
        .as_deref()
        .ok_or_else(|| "Set BLOOM_API_TOKEN before enabling remote sharing".to_string())?;
    let config_path = args
        .config
        .clone()
        .or_else(|| config_candidates().into_iter().find(|path| path.is_file()))
        .ok_or_else(|| {
            "No bloomd.toml found; set --config to the active daemon config".to_string()
        })?;
    verify_share_config(&config_path, public)?;

    let local_nodes = get_json(client, local, "/bloom/v1/nodes", Some(token)).await?;
    let local_id = local_nodes
        .pointer("/nodes/0/descriptor/id")
        .and_then(Value::as_str)
        .ok_or_else(|| "local node response did not include its node ID".to_string())?;
    let remote_nodes = get_json(client, public, "/bloom/v1/nodes", Some(token))
        .await
        .map_err(|error| format!("HTTPS endpoint verification failed: {error}"))?;
    let remote_id = remote_nodes
        .pointer("/nodes/0/descriptor/id")
        .and_then(Value::as_str)
        .ok_or_else(|| "HTTPS endpoint did not return a Bloom node ID".to_string())?;
    if remote_id != local_id {
        return Err(
            "HTTPS endpoint resolved to a different Bloom node; sharing remains paused".into(),
        );
    }
    post_draining(client, local, Some(token), false).await
}

fn verify_share_config(path: &Path, public: &reqwest::Url) -> Result<(), String> {
    let source = std::fs::read_to_string(path)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    let config: toml::Value = toml::from_str(&source)
        .map_err(|error| format!("cannot parse {}: {error}", path.display()))?;
    let swarm = config.get("swarm");
    if !swarm
        .and_then(|table| table.get("advertise"))
        .and_then(toml::Value::as_bool)
        .unwrap_or(false)
    {
        return Err("Enable [swarm] advertise in bloomd.toml and restart bloomd first".into());
    }
    let api = config.get("api");
    let has_operator_auth = api
        .and_then(|table| table.get("token"))
        .and_then(toml::Value::as_str)
        .is_some_and(|token| !token.is_empty())
        || api
            .and_then(|table| table.get("token_env"))
            .and_then(toml::Value::as_str)
            .is_some_and(|name| !name.is_empty());
    if !has_operator_auth {
        return Err("Configure api.token_env in bloomd.toml before enabling remote access".into());
    }
    let advertised_port = swarm
        .and_then(|table| table.get("advertise_https_port"))
        .and_then(toml::Value::as_integer)
        .ok_or_else(|| "Set swarm.advertise_https_port to the HTTPS proxy port".to_string())?;
    let public_port = i64::from(public.port_or_known_default().unwrap_or(443));
    if advertised_port != public_port {
        return Err(format!(
            "advertised HTTPS port {advertised_port} does not match URL port {public_port}"
        ));
    }
    Ok(())
}

fn config_candidates() -> Vec<PathBuf> {
    let mut paths = vec![PathBuf::from("bloomd.toml")];
    if let Some(home) = std::env::var_os("HOME") {
        paths.push(PathBuf::from(home).join(".config/bloom/bloomd.toml"));
    }
    paths.push(PathBuf::from("/etc/bloom/bloomd.toml"));
    paths
}

fn generate_certificate(host: &str, requested_dir: Option<&Path>) -> Result<(), String> {
    validate_certificate_host(host)?;
    let directory = requested_dir
        .map(Path::to_path_buf)
        .or_else(|| {
            std::env::var_os("HOME")
                .map(PathBuf::from)
                .map(|home| home.join(".config/bloom/tls"))
        })
        .ok_or_else(|| "HOME is unavailable; pass --cert-dir".to_string())?;
    std::fs::create_dir_all(&directory)
        .map_err(|error| format!("cannot create {}: {error}", directory.display()))?;
    let cert_path = directory.join("bloom-node.crt");
    let key_path = directory.join("bloom-node.key");
    let mut key_options = std::fs::OpenOptions::new();
    key_options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        key_options.mode(0o600);
    }
    let key_file = key_options
        .open(&key_path)
        .map_err(|error| format!("cannot create private key {}: {error}", key_path.display()))?;
    drop(key_file);
    let cert_file = match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&cert_path)
    {
        Ok(file) => file,
        Err(error) => {
            let _ = std::fs::remove_file(&key_path);
            return Err(format!(
                "cannot create certificate {}: {error}",
                cert_path.display()
            ));
        }
    };
    drop(cert_file);

    let san = if host.parse::<std::net::IpAddr>().is_ok() {
        format!("IP:{host}")
    } else {
        format!("DNS:{host}")
    };
    let output = std::process::Command::new("openssl")
        .args([
            "req",
            "-x509",
            "-newkey",
            "rsa:3072",
            "-sha256",
            "-nodes",
            "-days",
            "397",
            "-subj",
            &format!("/CN={host}"),
            "-addext",
            &format!("subjectAltName={san}"),
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,digitalSignature,keyEncipherment,keyCertSign",
            "-addext",
            "extendedKeyUsage=serverAuth",
            "-keyout",
        ])
        .arg(&key_path)
        .arg("-out")
        .arg(&cert_path)
        .output()
        .map_err(|error| {
            let _ = std::fs::remove_file(&key_path);
            let _ = std::fs::remove_file(&cert_path);
            format!("cannot run openssl: {error}")
        })?;
    if !output.status.success() {
        let _ = std::fs::remove_file(&key_path);
        let _ = std::fs::remove_file(&cert_path);
        let detail = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "openssl could not create the certificate: {}",
            detail.trim()
        ));
    }
    println!("TLS certificate: {}", cert_path.display());
    println!("Private key: {}", key_path.display());
    println!("Trust this certificate on routing nodes with swarm.ca_pem.");
    Ok(())
}

fn validate_certificate_host(host: &str) -> Result<(), String> {
    if host.parse::<std::net::IpAddr>().is_ok() {
        return Ok(());
    }
    let valid = !host.is_empty()
        && host.len() <= 253
        && !host.starts_with('.')
        && !host.ends_with('.')
        && host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label.as_bytes()[0].is_ascii_alphanumeric()
                && label.as_bytes()[label.len() - 1].is_ascii_alphanumeric()
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        });
    if valid {
        Ok(())
    } else {
        Err("certificate host must be a DNS name or IP address".into())
    }
}

async fn refresh_view(
    client: &reqwest::Client,
    local: &reqwest::Url,
    token: &Option<String>,
    public: Option<&reqwest::Url>,
    view: &mut View,
) {
    let token = token.as_deref();
    let nodes = match get_json(client, local, "/bloom/v1/nodes", token).await {
        Ok(value) => value,
        Err(error) => {
            view.connection = "offline".into();
            view.state = format!("Local bloomd unavailable: {error}");
            return;
        }
    };
    view.connection = "online".into();
    let node = nodes.pointer("/nodes/0").cloned().unwrap_or(Value::Null);
    let node_id = node
        .pointer("/descriptor/id")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    view.node_id = format!("node id: {}", short(node_id, 30));
    let draining = node
        .pointer("/status/draining")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let harness_ok = node
        .pointer("/status/harness_healthy")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    view.state = if draining {
        "Paused; not accepting new work".into()
    } else if !harness_ok {
        "Online; model runner is unavailable".into()
    } else {
        "Online; ready to accept work".into()
    };
    view.endpoint = "secure address: not configured".into();

    if let Ok(resources) = get_json(client, local, "/bloom/v1/resources", token).await {
        let cpu = resources
            .pointer("/cpu/utilisation_percent")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let threads = resources
            .pointer("/cpu/available_threads")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let free_memory = resources
            .pointer("/memory/available_bytes")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        view.cpu = format!("cpu: {cpu:.0}% / {threads} threads");
        view.memory = format!("memory free: {} GiB", free_memory / (1024 * 1024 * 1024));
        let gpus = resources
            .get("gpus")
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        let npu = resources
            .pointer("/npu/present")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        view.accelerator = format!("gpu: {gpus} / npu: {}", if npu { "yes" } else { "no" });
    }
    let free = node
        .pointer("/scheduling/available_slots")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    view.slots = format!("free jobs: {free}");
    if let Ok(models) = get_json(client, local, "/v1/models", token).await {
        let names = models
            .get("data")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|model| model.get("id").and_then(Value::as_str))
            .take(2)
            .map(|model| short(model, 17))
            .collect::<Vec<_>>();
        view.models = if names.is_empty() {
            "models: none installed".into()
        } else {
            format!("models: {}", names.join(", "))
        };
    }
    if let Ok(value) = get_json(client, local, "/bloom/v1/discovery", token).await {
        let count = value
            .get("candidates")
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        view.discovery = format!("lan discovery: {count} untrusted nearby");
    }
    if let Ok(value) = get_json(client, local, "/bloom/v1/swarm", token).await {
        let count = value
            .get("peers")
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        view.peers = format!("trusted peers: {count}");
    }
    if let Some(public) = public {
        view.endpoint = match get_json(client, public, "/bloom/v1/nodes", token).await {
            Ok(remote)
                if remote
                    .pointer("/nodes/0/descriptor/id")
                    .and_then(Value::as_str)
                    == Some(node_id) =>
            {
                format!("secure address: {}", short(public.as_str(), 38))
            }
            _ => "secure address: verification failed".into(),
        };
    }
}

fn short(value: &str, limit: usize) -> String {
    value
        .chars()
        .map(|c| {
            if c.is_ascii_graphic() || c == ' ' {
                c
            } else {
                '?'
            }
        })
        .take(limit)
        .collect()
}

fn temporary_scene_path() -> PathBuf {
    std::env::temp_dir().join(format!("bloom-node-{}.gscene", std::process::id()))
}

fn write_scene_if_changed(path: &Path, view: &View, previous: &mut String) -> Result<(), String> {
    let source = render_scene(view);
    if source == *previous {
        return Ok(());
    }
    let next = path.with_extension("gscene.next");
    std::fs::write(&next, &source)
        .map_err(|error| format!("cannot write scene update: {error}"))?;
    std::fs::rename(&next, path)
        .map_err(|error| format!("cannot publish scene update: {error}"))?;
    *previous = source;
    Ok(())
}

fn write_scene(path: &Path, view: &View) -> Result<(), String> {
    std::fs::write(path, render_scene(view))
        .map_err(|error| format!("cannot create scene copy: {error}"))
}

fn render_scene(view: &View) -> String {
    let fields = [
        ("__BLOOM_CONNECTION__", &view.connection),
        ("__BLOOM_NODE_STATE__", &view.state),
        ("__BLOOM_NODE_ID__", &view.node_id),
        ("__BLOOM_ENDPOINT__", &view.endpoint),
        ("__BLOOM_ACTION_STATUS__", &view.action_status),
        ("__BLOOM_CPU__", &view.cpu),
        ("__BLOOM_MEMORY__", &view.memory),
        ("__BLOOM_SLOTS__", &view.slots),
        ("__BLOOM_ACCELERATOR__", &view.accelerator),
        ("__BLOOM_MODELS__", &view.models),
        ("__BLOOM_DISCOVERY__", &view.discovery),
        ("__BLOOM_PEERS__", &view.peers),
    ];
    fields
        .iter()
        .fold(SCENE_TEMPLATE.to_owned(), |source, (key, value)| {
            let quoted = serde_json::to_string(value).unwrap_or_else(|_| "\"unavailable\"".into());
            source.replace(&format!("\"{key}\""), &quoted)
        })
}
