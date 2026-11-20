use clap::{Args, Parser, Subcommand};
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Parser)]
#[command(name = "bloom", version, about = "Bloom control CLI")]
struct Cli {
    #[arg(long, default_value = "http://127.0.0.1:7420")]
    url: String,
    #[arg(long, env = "BLOOM_API_TOKEN", hide_env_values = true)]
    token: Option<String>,
    #[arg(long, value_name = "PEM")]
    ca_pem: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Status,
    Nodes,
    Discovery,
    /// Verify a peer over HTTPS and print its configuration.
    PeerConfig {
        #[arg(long)]
        token_env: String,
    },
    Models,
    Resources,
    Jobs,
    Events,
    Drain {
        #[arg(long)]
        resume: bool,
    },
    Cancel {
        job_id: String,
    },
    Load {
        model: String,
    },
    Unload {
        model: String,
    },
    Infer(InferArgs),
}

#[derive(Args)]
struct InferArgs {
    #[arg(short, long, required_if_eq("swarm", "true"))]
    model: Option<String>,
    prompt: String,
    #[arg(short, long)]
    stream: bool,
    #[arg(long)]
    swarm: bool,
    #[arg(long)]
    preferred_node: Option<String>,
    #[arg(long)]
    no_failover: bool,
    #[arg(long)]
    no_model_loading: bool,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cli = Cli::parse();
    if matches!(cli.command, Command::PeerConfig { .. })
        && cli.token.as_deref().is_none_or(str::is_empty)
    {
        eprintln!("bloom: peer enrollment requires BLOOM_API_TOKEN or --token");
        std::process::exit(1);
    }
    let client = match build_client(cli.token.as_deref(), cli.ca_pem.as_deref()) {
        Ok(client) => client,
        Err(e) => {
            eprintln!("bloom: {e}");
            std::process::exit(1);
        }
    };
    let base = cli.url.trim_end_matches('/').to_string();

    let result = match cli.command {
        Command::Status => get_json(&client, &base, "/bloom/v1/swarm").await,
        Command::Nodes => get_json(&client, &base, "/bloom/v1/nodes").await,
        Command::Discovery => get_json(&client, &base, "/bloom/v1/discovery").await,
        Command::PeerConfig { token_env } => peer_config(&client, &base, token_env).await,
        Command::Models => get_json(&client, &base, "/v1/models").await,
        Command::Resources => get_json(&client, &base, "/bloom/v1/resources").await,
        Command::Jobs => get_json(&client, &base, "/bloom/v1/jobs").await,
        Command::Events => events(&client, &base).await,
        Command::Drain { resume } => {
            post_json(
                &client,
                &base,
                "/bloom/v1/nodes/drain",
                serde_json::json!({"enabled": !resume}),
            )
            .await
        }
        Command::Cancel { job_id } => {
            post_json(
                &client,
                &base,
                "/bloom/v1/jobs/cancel",
                serde_json::json!({"job_id": job_id}),
            )
            .await
        }
        Command::Load { model } => {
            post_json(
                &client,
                &base,
                "/bloom/v1/models/load",
                serde_json::json!({"model": model}),
            )
            .await
        }
        Command::Unload { model } => {
            post_json(
                &client,
                &base,
                "/bloom/v1/models/unload",
                serde_json::json!({"model": model}),
            )
            .await
        }
        Command::Infer(args) => infer(&client, &base, args).await,
    };

    match result {
        Ok(()) => {}
        Err(e) => {
            eprintln!("bloom: {e}");
            std::process::exit(1);
        }
    }
}

fn build_client(token: Option<&str>, ca_pem: Option<&Path>) -> Result<reqwest::Client, String> {
    let mut builder = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(if token.is_some() { 600 } else { 120 }));
    if let Some(path) = ca_pem {
        let pem = std::fs::read(path)
            .map_err(|e| format!("cannot read CA bundle {}: {e}", path.display()))?;
        let certificates = reqwest::Certificate::from_pem_bundle(&pem)
            .map_err(|e| format!("invalid CA bundle {}: {e}", path.display()))?;
        if certificates.is_empty() {
            return Err(format!(
                "CA bundle {} contains no certificates",
                path.display()
            ));
        }
        builder = builder.tls_certs_only(certificates);
    }
    if let Some(token) = token {
        let mut value = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|e| format!("invalid token header: {e}"))?;
        value.set_sensitive(true);
        builder = builder.default_headers({
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(reqwest::header::AUTHORIZATION, value);
            headers
        });
    }
    builder
        .build()
        .map_err(|e| format!("build http client: {e}"))
}

async fn get_json(client: &reqwest::Client, base: &str, path: &str) -> Result<(), String> {
    let body = fetch_json(client, base, path).await?;
    print_json(&body)
}

async fn post_json(
    client: &reqwest::Client,
    base: &str,
    path: &str,
    payload: Value,
) -> Result<(), String> {
    let response = client
        .post(format!("{base}{path}"))
        .json(&payload)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let body = ensure_success(response)
        .await?
        .json()
        .await
        .map_err(|e| e.to_string())?;
    print_json(&body)
}

fn print_json(body: &Value) -> Result<(), String> {
    println!(
        "{}",
        serde_json::to_string_pretty(body).map_err(|e| e.to_string())?
    );
    Ok(())
}

async fn fetch_json(client: &reqwest::Client, base: &str, path: &str) -> Result<Value, String> {
    let response = client
        .get(format!("{base}{path}"))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let response = ensure_success(response).await?;
    response.json().await.map_err(|e| e.to_string())
}

async fn peer_config(
    client: &reqwest::Client,
    base: &str,
    token_env: String,
) -> Result<(), String> {
    let url = reqwest::Url::parse(base).map_err(|e| format!("invalid peer URL: {e}"))?;
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(
            "peer configuration requires an HTTPS URL without credentials, query or fragment"
                .into(),
        );
    }
    if token_env.is_empty()
        || !token_env
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        || token_env.as_bytes()[0].is_ascii_digit()
    {
        return Err("--token-env must name an environment variable".into());
    }
    let status = fetch_json(client, base, "/bloom/v1/nodes").await?;
    let node_id = status
        .pointer("/nodes/0/descriptor/id")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or("peer did not report a node ID")?;
    let config = bloom_core::config::PeerConfig {
        node_id: node_id.into(),
        url: base.into(),
        token_env,
    };
    let encoded = toml::to_string(&config).map_err(|e| e.to_string())?;
    print!("[[swarm.peers]]\n{encoded}");
    Ok(())
}

async fn events(client: &reqwest::Client, base: &str) -> Result<(), String> {
    let response = client
        .get(format!("{base}/bloom/v1/events"))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let mut response = ensure_success(response).await?;
    while let Some(chunk) = response.chunk().await.map_err(|e| e.to_string())? {
        print!("{}", String::from_utf8_lossy(&chunk));
    }
    Ok(())
}

async fn infer(client: &reqwest::Client, base: &str, args: InferArgs) -> Result<(), String> {
    let swarm = args.swarm || args.preferred_node.is_some();
    let model = match args.model {
        Some(model) => model,
        None if swarm => return Err("--model is required for swarm inference".into()),
        None => {
            let response = client
                .get(format!("{base}/v1/models"))
                .send()
                .await
                .map_err(|e| e.to_string())?;
            let response = ensure_success(response).await?;
            let body: Value = response.json().await.map_err(|e| e.to_string())?;
            body.pointer("/data/0/id")
                .and_then(Value::as_str)
                .map(str::to_string)
                .ok_or_else(|| "no models available on the node".to_string())?
        }
    };
    let payload = serde_json::json!({
        "model": model,
        "messages": [{"role": "user", "content": args.prompt}],
        "stream": args.stream,
        "bloom": {
            "routing": if swarm { "swarm" } else { "direct" },
            "preferred_node": args.preferred_node,
            "allow_failover": !args.no_failover,
            "allow_model_loading": !args.no_model_loading,
        },
    });
    let response = client
        .post(format!("{base}/v1/chat/completions"))
        .json(&payload)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let mut response = ensure_success(response).await?;

    if !args.stream {
        let body: Value = response.json().await.map_err(|e| e.to_string())?;
        let content = body
            .pointer("/choices/0/message/content")
            .and_then(Value::as_str)
            .unwrap_or_default();
        println!("{content}");
        return Ok(());
    }

    let mut buffer = Vec::new();
    loop {
        let Some(chunk) = response.chunk().await.map_err(|e| e.to_string())? else {
            break;
        };
        buffer.extend_from_slice(&chunk);
        while let Some(position) = buffer.iter().position(|b| *b == b'\n') {
            let line: Vec<u8> = buffer.drain(..=position).collect();
            let line = String::from_utf8_lossy(&line);
            let line = line.trim();
            let Some(data) = line.strip_prefix("data: ") else {
                continue;
            };
            if data == "[DONE]" {
                println!();
                return Ok(());
            }
            if let Ok(frame) = serde_json::from_str::<Value>(data) {
                if let Some(error) = frame.pointer("/error/message").and_then(Value::as_str) {
                    return Err(error.to_string());
                }
                if let Some(delta) = frame
                    .pointer("/choices/0/delta/content")
                    .and_then(Value::as_str)
                {
                    print!("{delta}");
                    use std::io::Write;
                    let _ = std::io::stdout().flush();
                }
            }
        }
    }
    Err("inference stream closed before the completion marker".into())
}

async fn ensure_success(response: reqwest::Response) -> Result<reqwest::Response, String> {
    if response.status().is_success() {
        return Ok(response);
    }
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    Err(format!("request failed with {status}: {body}"))
}
